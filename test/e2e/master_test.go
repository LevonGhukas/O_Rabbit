package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// masterClient calls the master's HTTP API the way an operator would.
type masterClient struct {
	base  string
	token string
	http  *http.Client
}

// runView is the subset of GET /api/v1/runs/{id} the test asserts on.
type runView struct {
	ID                     string `json:"id"`
	Status                 string `json:"status"`
	CommitID               string `json:"commit_id"`
	CommitPhase            string `json:"commit_phase"`
	OperatorActionRequired bool   `json:"operator_action_required"`
	CatalogStatus          string `json:"catalog_status"`
	Readiness              string `json:"readiness"`
	RegistrationAttempt    int    `json:"registration_attempt"`
	RegistrationLastError  string `json:"registration_last_error_class"`
	Reconciliation         struct {
		Attempt int    `json:"reconciliation_attempt"`
		Outcome string `json:"reconciliation_outcome"`
	} `json:"reconciliation"`
}

func (r runView) String() string {
	return fmt.Sprintf("run %s status=%s commit_phase=%s catalog_status=%s readiness=%s registration_attempt=%d last_error=%s reconciliation=%d/%s operator_action=%t",
		r.ID, r.Status, r.CommitPhase, r.CatalogStatus, r.Readiness, r.RegistrationAttempt, r.RegistrationLastError,
		r.Reconciliation.Attempt, r.Reconciliation.Outcome, r.OperatorActionRequired)
}

func (m *masterClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.base+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+m.token)
	resp, err := m.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s: %s", path, resp.Status, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, out)
}

func (m *masterClient) run(ctx context.Context, runID string) (runView, error) {
	var out struct {
		Run runView `json:"run"`
	}
	err := m.getJSON(ctx, "/api/v1/runs/"+runID, &out)
	return out.Run, err
}

// artifactKeys returns the object keys of the files the master accepted from
// workers for a run.
func (m *masterClient) artifactKeys(ctx context.Context, runID string) ([]string, error) {
	var out struct {
		Objects []map[string]any `json:"objects"`
	}
	if err := m.getJSON(ctx, "/api/v1/runs/"+runID+"/artifacts", &out); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(out.Objects))
	for _, obj := range out.Objects {
		key, _ := obj["key"].(string)
		if key == "" {
			key, _ = obj["object_key"].(string)
		}
		if key == "" {
			return nil, fmt.Errorf("artifact without an object key: %v", obj)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// waitReady polls /ready until the master leads and serves requests.
func (m *masterClient) waitReady(ctx context.Context) error {
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.base+"/ready", nil)
		if err != nil {
			return err
		}
		if resp, err := m.http.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("master at %s not ready: %w", m.base, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

// waitPublished polls a run until its data is committed and registered in
// the catalog. Terminal failures and operator action end the wait early,
// because no amount of waiting fixes them.
func (m *masterClient) waitPublished(ctx context.Context, t *testing.T, runID string) runView {
	t.Helper()
	var last runView
	for {
		run, err := m.run(ctx, runID)
		if err != nil {
			t.Logf("poll run %s: %v", runID, err)
		} else {
			if run.String() != last.String() {
				t.Logf("%s", run)
			}
			last = run
			switch {
			case run.Status == "SUCCEEDED" && run.CatalogStatus == "REGISTERED":
				return run
			case run.Status == "FAILED" || run.Status == "CANCELED":
				t.Fatalf("run ended without publishing: %s", run)
			case run.CatalogStatus == "FAILED" || run.CatalogStatus == "QUARANTINED" || run.OperatorActionRequired:
				t.Fatalf("registration needs an operator: %s", run)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("run %s not published before the deadline; last state: %s", runID, last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// runSpec is what the test varies between exports; everything else in the
// submitted file is fixed by the e2e stack.
type runSpec struct {
	job         string
	sourceTable string
	icebergName string
	incremental bool
}

const runSpecTemplate = `master:
  http: %[1]s
source:
  name: e2e-postgres
  engine: postgres
  # Read by the worker container, so it uses the Compose service name.
  dsn: postgres://e2e:e2e@postgres:5432/e2e?sslmode=disable
# One target per table: the bucket and prefix are the dataset identity, so
# a shared prefix would also share one high-water mark between the jobs.
target:
  name: e2e-minio-%[3]s
  endpoint: http://minio:9000
  region: us-east-1
  bucket: e2e-lake
  prefix: exports/%[3]s
  force_path_style: true
  access_key_id: e2e-access
  secret_access_key: e2e-secret-key
job:
  name: %[2]s
  target_namespace: e2e
  target_table: %[3]s
  incremental: %[4]t
  table: public.%[5]s
  cursor_column: id
  auto_tune: false
  # Several tasks and files per run, so path and file checks cover more
  # than one artifact.
  target_rows_per_task: 400
  max_in_flight_tasks: 2
iceberg:
  enabled: true
  engine: rest-go
  table: e2e.%[3]s
  options:
    # The master reaches the catalog only through the test's fault proxy.
    uri: %[6]s
`

// submit writes a run file and submits it with the real CLI, returning the
// new run ID.
func (e *env) submit(ctx context.Context, t *testing.T, spec runSpec) string {
	t.Helper()
	body := fmt.Sprintf(runSpecTemplate, e.master.base, spec.job, spec.icebergName, spec.incremental, spec.sourceTable, e.proxyURLForMaster)
	path := filepath.Join(t.TempDir(), "run.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write run file: %v", err)
	}
	cmd := exec.CommandContext(ctx, e.clientBin, "run", "submit", "--quiet", "--file", path)
	cmd.Env = append(os.Environ(), "ORABBIT_HTTP_AUTH_TOKEN="+e.master.token)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("orabbit-client run submit: %v\n%s", err, out)
	}
	m := regexp.MustCompile(`submitted run (\S+)`).FindSubmatch(out)
	if m == nil {
		t.Fatalf("run ID missing from CLI output:\n%s", out)
	}
	return string(m[1])
}
