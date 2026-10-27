package httpapi

import (
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/telemetry"
)

const (
	alertRulesPath = "../../monitoring/prometheus/orabbit-alerts.yml"
	dashboardPath  = "../../monitoring/grafana/orabbit-lifecycle.json"
	runbooksPath   = "../../docs/runbooks.md"
	runbookURLBase = "https://github.com/LevonGhukas/O_Rabbit/blob/main/docs/runbooks.md#"
)

var metricNamePattern = regexp.MustCompile(`orabbit_[a-z0-9_]+`)

type alertRule struct {
	Alert       string            `yaml:"alert"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
}

func loadAlertRules(t *testing.T) []alertRule {
	t.Helper()
	raw, err := os.ReadFile(alertRulesPath)
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Groups []struct {
			Name  string      `yaml:"name"`
			Rules []alertRule `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err := yaml.Unmarshal(raw, &file); err != nil {
		t.Fatalf("parse %s: %v", alertRulesPath, err)
	}
	var rules []alertRule
	for _, g := range file.Groups {
		rules = append(rules, g.Rules...)
	}
	if len(rules) == 0 {
		t.Fatal("no alert rules")
	}
	return rules
}

// exportedMetricNames is every metric family the master's /metrics serves.
func exportedMetricNames(t *testing.T) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	body := renderLifecycleMetrics(db.LifecycleMetrics{OldestRegistrationAgeSeconds: map[string]float64{}})
	for _, line := range strings.Split(body, "\n") {
		if name, _, ok := strings.Cut(line, " "); ok {
			name, _, _ = strings.Cut(name, "{")
			names[name] = true
		}
	}
	families, err := telemetry.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		names[f.GetName()] = true
	}
	return names
}

func runbookAnchors(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(runbooksPath)
	if err != nil {
		t.Fatal(err)
	}
	anchors := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		if heading, ok := strings.CutPrefix(line, "## "); ok && strings.HasPrefix(heading, "Orabbit") {
			anchors[strings.ToLower(heading)] = true
		}
	}
	return anchors
}

// TestAlertRulesAreActionable checks what promtool cannot: every alert has a
// severity, a summary and a runbook section that exists, and every runbook
// section belongs to an alert.
func TestAlertRulesAreActionable(t *testing.T) {
	anchors := runbookAnchors(t)
	seen := map[string]bool{}
	for _, r := range loadAlertRules(t) {
		if seen[r.Alert] {
			t.Fatalf("duplicate alert %s", r.Alert)
		}
		seen[r.Alert] = true
		if s := r.Labels["severity"]; s != "critical" && s != "warning" {
			t.Errorf("%s: severity=%q, want critical or warning", r.Alert, s)
		}
		if r.Annotations["summary"] == "" || r.Annotations["description"] == "" {
			t.Errorf("%s: summary and description are required", r.Alert)
		}
		anchor, ok := strings.CutPrefix(r.Annotations["runbook_url"], runbookURLBase)
		if !ok || !anchors[anchor] || anchor != strings.ToLower(r.Alert) {
			t.Errorf("%s: runbook_url %q must point to its section in docs/runbooks.md", r.Alert, r.Annotations["runbook_url"])
		}
		// Paging on a condition that has not persisted would page on
		// ordinary retries.
		if r.Labels["severity"] == "critical" && r.For == "" {
			t.Errorf("%s: a critical alert needs a for: duration", r.Alert)
		}
	}
	for anchor := range anchors {
		found := false
		for alert := range seen {
			if strings.ToLower(alert) == anchor {
				found = true
			}
		}
		if !found {
			t.Errorf("runbook section %q has no alert", anchor)
		}
	}
}

// TestMonitoringUsesOnlyExportedMetrics keeps the rules and the dashboard in
// step with /metrics: a renamed metric would otherwise silence an alert.
func TestMonitoringUsesOnlyExportedMetrics(t *testing.T) {
	exported := exportedMetricNames(t)
	check := func(where, expr string) {
		for _, name := range metricNamePattern.FindAllString(expr, -1) {
			if !exported[name] {
				t.Errorf("%s uses %s, which /metrics does not export", where, name)
			}
		}
	}
	for _, r := range loadAlertRules(t) {
		check("alert "+r.Alert, r.Expr)
	}

	raw, err := os.ReadFile(dashboardPath)
	if err != nil {
		t.Fatal(err)
	}
	var dash struct {
		UID    string `json:"uid"`
		Panels []struct {
			Type       string         `json:"type"`
			Title      string         `json:"title"`
			Datasource map[string]any `json:"datasource"`
			Targets    []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("parse %s: %v", dashboardPath, err)
	}
	if dash.UID == "" {
		t.Fatal("dashboard needs a uid")
	}
	var titles []string
	for _, p := range dash.Panels {
		if p.Type == "row" {
			titles = append(titles, p.Title)
			continue
		}
		if p.Datasource["uid"] != "${datasource}" || len(p.Targets) == 0 {
			t.Errorf("panel %q must query the ${datasource} variable", p.Title)
		}
		for _, target := range p.Targets {
			check("panel "+p.Title, target.Expr)
		}
	}
	sort.Strings(titles)
	want := []string{"Catalog work slots", "Failures and operator action", "Leadership", "Registration queue", "Runs and commits"}
	if strings.Join(titles, "|") != strings.Join(want, "|") {
		t.Fatalf("dashboard rows=%v want %v", titles, want)
	}
}
