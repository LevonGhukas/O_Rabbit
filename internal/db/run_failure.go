package db

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// Run failure phases recorded in runs.failure_phase.
const (
	RunFailurePhasePlanning = "planning"
	RunFailurePhaseExtract  = "extract"
	RunFailurePhaseCommit   = "commit"
)

// maxRunFailureMessage bounds the task error copied into error_summary.
const maxRunFailureMessage = 500

// urlCredentials matches the password part of scheme://user:password@host.
var urlCredentials = regexp.MustCompile(`(://[^/\s:@]*:)[^@\s]*@`)

// RedactCredentials removes passwords embedded in URLs; driver errors can
// echo the DSN they failed to use.
func RedactCredentials(msg string) string {
	return urlCredentials.ReplaceAllString(msg, "${1}***@")
}

// taskFailureSummary describes a run that failed because of its tasks: the
// count plus the error and class of the first task that failed, taken from
// that task's last attempt.
func taskFailureSummary(ctx context.Context, tx *sql.Tx, runID string, failed, total int) (summary, class string, err error) {
	var msg sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT t.error_message,
		       COALESCE((SELECT a.failure_class FROM task_attempts a
		                 WHERE a.task_id = t.id
		                 ORDER BY a.attempt_number DESC LIMIT 1), '')
		FROM tasks t
		WHERE t.run_id = ? AND t.status IN ('FAILED','QUARANTINED')
		ORDER BY t.finished_at, t.task_index
		LIMIT 1;`, runID).Scan(&msg, &class)
	if err != nil && err != sql.ErrNoRows {
		return "", "", err
	}
	summary = fmt.Sprintf("%d of %d task(s) failed", failed, total)
	if text := strings.TrimSpace(RedactCredentials(msg.String)); text != "" {
		if len(text) > maxRunFailureMessage {
			text = text[:maxRunFailureMessage] + "…"
		}
		summary += ": " + text
	}
	return summary, class, nil
}

// RecordRunFailure stores where and why a FAILED run failed.
func (s *Store) RecordRunFailure(ctx context.Context, runID, class, phase string) error {
	return withBusyRetry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `UPDATE runs SET failure_class=?, failure_phase=? WHERE id=? AND status='FAILED';`, class, phase, runID)
		return err
	})
}
