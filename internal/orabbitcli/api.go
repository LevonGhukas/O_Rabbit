package orabbitcli

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

type connectionPayload struct {
	Name     string         `json:"name"`
	Kind     string         `json:"kind"`
	Engine   string         `json:"engine"`
	Metadata map[string]any `json:"metadata"`
	Secret   map[string]any `json:"secret"`
}

type jobPayload struct {
	Name               string         `json:"name"`
	SourceConnectionID string         `json:"source_connection_id"`
	TargetConnectionID string         `json:"target_connection_id"`
	SourceSQL          string         `json:"source_sql"`
	TargetNamespace    string         `json:"target_namespace"`
	TargetTable        string         `json:"target_table"`
	WriteMode          string         `json:"write_mode"`
	Incremental        bool           `json:"incremental"`
	HWMColumn          string         `json:"hwm_column"`
	OptionsJSON        map[string]any `json:"options_json"`
}

type runDetails struct {
	Run   runState   `json:"run"`
	Tasks []taskInfo `json:"tasks"`
}

type runState struct {
	ID           string  `json:"id"`
	Status       string  `json:"status"`
	StartedAt    string  `json:"started_at"`
	FinishedAt   *string `json:"finished_at"`
	ErrorSummary *string `json:"error_summary"`
}

type taskInfo struct {
	ID             string          `json:"id"`
	TaskIndex      int             `json:"task_index"`
	Status         string          `json:"status"`
	WorkerID       *string         `json:"worker_id"`
	RowsRead       int64           `json:"rows_read"`
	BytesRead      int64           `json:"bytes_read"`
	BytesWritten   int64           `json:"bytes_written"`
	ParquetObjects json.RawMessage `json:"parquet_objects_json"`
	StartedAt      *string         `json:"started_at"`
	FinishedAt     *string         `json:"finished_at"`
	ErrorMessage   *string         `json:"error_message"`
}

type cancelRunResponse struct {
	Run                  runState `json:"run"`
	Canceled             bool     `json:"canceled"`
	PendingTasksCanceled int      `json:"pending_tasks_canceled"`
}

type runRecoveryResponse struct {
	Action  string `json:"action"`
	Changed bool   `json:"changed"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type runStartPayload struct {
	RegistrationConfig json.RawMessage `json:"registration_config,omitempty"`
}

type workerState struct {
	ID string `json:"id"`
}

// upsertConnection creates or updates connection idempotently.
// It exists to make repeated runs safe and deterministic.
func upsertConnection(ctx context.Context, base string, p connectionPayload) (string, error) {
	type conn struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	var existing []conn
	if err := httpJSON(ctx, http.MethodGet, base+"/connections", nil, &existing); err != nil {
		return "", err
	}
	for _, c := range existing {
		if c.Name == p.Name {
			var out any
			if err := httpJSON(ctx, http.MethodPut, base+"/connections/"+c.ID, p, &out); err != nil {
				return "", err
			}
			return c.ID, nil
		}
	}

	var created struct {
		ID string `json:"id"`
	}
	if err := httpJSON(ctx, http.MethodPost, base+"/connections", p, &created); err != nil {
		return "", err
	}
	return created.ID, nil
}

// upsertJob creates or updates job idempotently.
// It exists to make repeated runs safe and deterministic.
func upsertJob(ctx context.Context, base string, p jobPayload) (string, error) {
	type job struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	var existing []job
	if err := httpJSON(ctx, http.MethodGet, base+"/jobs", nil, &existing); err != nil {
		return "", err
	}
	for _, j := range existing {
		if j.Name == p.Name {
			var out any
			if err := httpJSON(ctx, http.MethodPut, base+"/jobs/"+j.ID, p, &out); err != nil {
				return "", err
			}
			return j.ID, nil
		}
	}

	var created struct {
		ID string `json:"id"`
	}
	if err := httpJSON(ctx, http.MethodPost, base+"/jobs", p, &created); err != nil {
		return "", err
	}
	return created.ID, nil
}

// getJobOptions handles get job options behavior.
// It exists to keep this logic isolated and reusable.
func getJobOptions(ctx context.Context, base, jobID string) (jobopts.Options, error) {
	var resp struct {
		OptionsJSON json.RawMessage `json:"options_json"`
	}
	if err := httpJSON(ctx, http.MethodGet, base+"/jobs/"+jobID, nil, &resp); err != nil {
		return jobopts.Options{}, err
	}
	return jobopts.Parse(resp.OptionsJSON)
}

// startRun handles start run behavior.
// It exists to keep this logic isolated and reusable.
func startRun(ctx context.Context, base, jobID string, registrationConfig json.RawMessage) (runID string, taskCount int, err error) {
	var resp struct {
		Run struct {
			ID string `json:"id"`
		} `json:"run"`
		Tasks []any `json:"tasks"`
	}
	if err := httpJSON(ctx, http.MethodPost, base+"/jobs/"+jobID+"/runs", runStartPayload{RegistrationConfig: registrationConfig}, &resp); err != nil {
		return "", 0, err
	}
	return resp.Run.ID, len(resp.Tasks), nil
}

func getRunStatus(ctx context.Context, base, runID string) (string, error) {
	details, err := getRunDetails(ctx, base, runID)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(details.Run.Status), nil
}

func getRunDetails(ctx context.Context, base, runID string) (runDetails, error) {
	var resp struct {
		Run   runState   `json:"run"`
		Tasks []taskInfo `json:"tasks"`
	}
	if err := httpJSON(ctx, http.MethodGet, base+"/runs/"+runID, nil, &resp); err != nil {
		return runDetails{}, err
	}
	return runDetails{Run: resp.Run, Tasks: resp.Tasks}, nil
}

func cancelRun(ctx context.Context, base, runID string) (cancelRunResponse, error) {
	var resp cancelRunResponse
	if err := httpJSON(ctx, http.MethodPost, base+"/runs/"+runID+"/cancel", map[string]any{}, &resp); err != nil {
		return cancelRunResponse{}, err
	}
	return resp, nil
}

func diagnoseRun(ctx context.Context, base, runID string) (map[string]any, error) {
	var resp map[string]any
	if err := httpJSON(ctx, http.MethodGet, base+"/api/runs/"+runID+"/diagnosis", nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func recoverRun(ctx context.Context, base, runID, action, reason string) (runRecoveryResponse, error) {
	var resp runRecoveryResponse
	body := map[string]string{"action": action, "reason": reason}
	if err := httpJSON(ctx, http.MethodPost, base+"/api/runs/"+runID+"/recover", body, &resp); err != nil {
		return runRecoveryResponse{}, err
	}
	return resp, nil
}

func listActiveWorkers(ctx context.Context, base string) ([]workerState, error) {
	var resp []workerState
	if err := httpJSON(ctx, http.MethodGet, base+"/workers", nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}
