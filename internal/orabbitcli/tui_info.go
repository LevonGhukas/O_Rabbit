package orabbitcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
)

// Read-only views of the master's state: jobs, workers, one run, and a
// readable diagnosis. They call the API directly and print formatted text.

// tuiJob is the subset of the master's job record the shell shows.
type tuiJob struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	TargetNamespace string `json:"target_namespace"`
	TargetTable     string `json:"target_table"`
	WriteMode       string `json:"write_mode"`
	CreatedAt       string `json:"created_at"`
}

type tuiWorker struct {
	ID            string `json:"id"`
	Addr          string `json:"addr"`
	Status        string `json:"status"`
	LastHeartbeat string `json:"last_heartbeat"`
	RunningTasks  int    `json:"running_tasks"`
}

// tuiDiagnosis mirrors db.RunDiagnosis, keeping only what the view prints.
type tuiDiagnosis struct {
	RunID               string         `json:"run_id"`
	Status              string         `json:"status"`
	CommitPhase         string         `json:"commit_phase"`
	LastClassifiedError string         `json:"last_classified_error"`
	TaskCounts          map[string]int `json:"task_counts"`
	BlockingTasks       []struct {
		Index          int    `json:"task_index"`
		Status         string `json:"status"`
		WorkerID       string `json:"worker_id"`
		AttemptCount   int    `json:"attempt_count"`
		MaxAttempts    int    `json:"max_attempts"`
		NextEligibleAt string `json:"next_eligible_at"`
		RecentAttempts []struct {
			Number       int    `json:"attempt_number"`
			Status       string `json:"status"`
			FailureClass string `json:"failure_class"`
		} `json:"recent_attempts"`
	} `json:"blocking_tasks"`
	Registration *struct {
		Status         string `json:"status"`
		AttemptCount   int    `json:"attempt_count"`
		CurrentPhase   string `json:"current_phase"`
		LastErrorClass string `json:"last_error_class"`
		BlockedBy      string `json:"blocked_by"`
	} `json:"registration"`
	OperatorReview      bool   `json:"operator_review_required"`
	SuggestedNextAction string `json:"suggested_next_action"`
}

// tuiRunListMax is how far back /runs --job and job lookups search.
const tuiRunListMax = 200

func listJobs(ctx context.Context, base string) ([]tuiJob, error) {
	var jobs []tuiJob
	if err := httpJSON(ctx, http.MethodGet, base+apiV1+"/jobs", nil, &jobs); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return jobs, nil
}

func listRuns(ctx context.Context, base string, limit int) ([]tuiRunSummary, error) {
	var runs []tuiRunSummary
	if err := httpJSON(ctx, http.MethodGet, fmt.Sprintf("%s%s/runs?limit=%d", base, apiV1, limit), nil, &runs); err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	return runs, nil
}

// findJob matches a job by exact ID or by name, so users can type either.
func findJob(jobs []tuiJob, ref string) (tuiJob, bool) {
	for _, j := range jobs {
		if j.ID == ref {
			return j, true
		}
	}
	for _, j := range jobs {
		if strings.EqualFold(j.Name, ref) {
			return j, true
		}
	}
	return tuiJob{}, false
}

func jobNames(jobs []tuiJob) map[string]string {
	names := make(map[string]string, len(jobs))
	for _, j := range jobs {
		names[j.ID] = j.Name
	}
	return names
}

// shortTime renders an RFC 3339 timestamp as "2006-01-02 15:04:05" UTC so
// tables stay narrow; anything unparsable is shown unchanged.
func shortTime(raw string) string {
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return raw
	}
	return t.UTC().Format("2006-01-02 15:04:05")
}

func isNotFound(err error) bool {
	if e, ok := parseMasterError(err); ok {
		return e.Status == "404"
	}
	return err != nil && strings.Contains(err.Error(), "http 404")
}

// --- /runs ------------------------------------------------------------------

func (m tuiModel) listRunsCmd(limit int, jobRef string) tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		// Job names make the table readable; without them we fall back to IDs.
		jobs, jobsErr := listJobs(ctx, base)
		fetch := limit
		if jobRef != "" {
			fetch = tuiRunListMax
		}
		runs, err := listRuns(ctx, base, fetch)
		if err != nil {
			return tuiResultMsg{err: err}
		}
		if jobRef != "" {
			if jobsErr != nil {
				return tuiResultMsg{err: jobsErr}
			}
			job, ok := findJob(jobs, jobRef)
			if !ok {
				return tuiResultMsg{err: fmt.Errorf("no job with ID or name %q (see /jobs)", jobRef)}
			}
			var kept []tuiRunSummary
			for _, r := range runs {
				if r.JobID == job.ID && len(kept) < limit {
					kept = append(kept, r)
				}
			}
			runs = kept
		}
		return tuiResultMsg{text: formatRunsTable(runs, jobNames(jobs))}
	}
}

func formatRunsTable(runs []tuiRunSummary, names map[string]string) string {
	if len(runs) == 0 {
		return tuiStyleDim.Render("no runs found")
	}
	var b strings.Builder
	b.WriteString(tuiStyleDim.Render(fmt.Sprintf("%-32s  %-10s  %-19s  %s", "RUN ID", "STATUS", "STARTED (UTC)", "JOB")))
	for _, r := range runs {
		status := fmt.Sprintf("%-10s", r.Status)
		switch strings.ToUpper(r.Status) {
		case "SUCCEEDED":
			status = tuiStyleOK.Render(status)
		case "FAILED", "CANCELED":
			status = tuiStyleErr.Render(status)
		default:
			status = tuiStyleWarn.Render(status)
		}
		job := r.JobID
		if n := names[r.JobID]; n != "" {
			job = n
		}
		fmt.Fprintf(&b, "\n%-32s  %s  %-19s  %s", r.ID, status, shortTime(r.StartedAt), job)
		if r.ErrorSummary != nil && strings.TrimSpace(*r.ErrorSummary) != "" {
			b.WriteString("\n" + tuiStyleDim.Render("  └ "+ansi.Truncate(oneLine(redactDSN(*r.ErrorSummary)), 100, "…")))
		}
	}
	b.WriteString("\n" + tuiStyleDim.Render("/show <run-id> for details · /diagnose <run-id|job> explains a failure"))
	return b.String()
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

// --- /jobs and /workers -----------------------------------------------------

func (m tuiModel) listJobsCmd() tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		jobs, err := listJobs(ctx, base)
		if err != nil {
			return tuiResultMsg{err: err}
		}
		if len(jobs) == 0 {
			return tuiResultMsg{text: tuiStyleDim.Render("no jobs yet — /run creates one")}
		}
		sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt > jobs[j].CreatedAt })
		var b strings.Builder
		b.WriteString(tuiStyleDim.Render(fmt.Sprintf("%-32s  %-24s  %-28s  %s", "JOB ID", "NAME", "TARGET", "CREATED (UTC)")))
		for _, j := range jobs {
			target := strings.Trim(j.TargetNamespace+"."+j.TargetTable, ".")
			fmt.Fprintf(&b, "\n%-32s  %-24s  %-28s  %s", j.ID,
				ansi.Truncate(j.Name, 24, "…"), ansi.Truncate(target, 28, "…"), shortTime(j.CreatedAt))
		}
		b.WriteString("\n" + tuiStyleDim.Render("/runs --job <id|name> lists a job's runs · /diagnose <job> explains its latest run"))
		return tuiResultMsg{text: b.String()}
	}
}

func (m tuiModel) listWorkersCmd(all bool) tea.Cmd {
	base := m.base
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		u := base + apiV1 + "/workers"
		if all {
			u += "?all=1"
		}
		var ws []tuiWorker
		if err := httpJSON(ctx, http.MethodGet, u, nil, &ws); err != nil {
			return tuiResultMsg{err: fmt.Errorf("list workers: %w", err)}
		}
		if len(ws) == 0 {
			hint := "no active workers — start some with /stack start worker, or try /workers --all"
			return tuiResultMsg{text: tuiStyleWarn.Render(hint)}
		}
		var b strings.Builder
		b.WriteString(tuiStyleDim.Render(fmt.Sprintf("%-36s  %-10s  %-5s  %-19s  %s", "WORKER ID", "STATUS", "TASKS", "LAST HEARTBEAT", "ADDR")))
		for _, w := range ws {
			fmt.Fprintf(&b, "\n%-36s  %-10s  %-5d  %-19s  %s", ansi.Truncate(w.ID, 36, "…"), w.Status, w.RunningTasks, shortTime(w.LastHeartbeat), w.Addr)
		}
		return tuiResultMsg{text: b.String()}
	}
}

// --- /show ------------------------------------------------------------------

func (m tuiModel) showRunCmd(runID string) tea.Cmd {
	base, width := m.base, m.width
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		d, err := getRunDetails(ctx, base, runID)
		if err != nil {
			return tuiResultMsg{err: fmt.Errorf("show %s: %w", runID, err)}
		}
		return tuiResultMsg{text: renderRunDetails(width, d)}
	}
}

func renderRunDetails(width int, d runDetails) string {
	p := summarizeRunProgress(d)
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s\n", tuiStyleDim.Render("status:  "), d.Run.Status)
	fmt.Fprintf(&b, "%s %s", tuiStyleDim.Render("started: "), shortTime(d.Run.StartedAt))
	if d.Run.FinishedAt != nil {
		fmt.Fprintf(&b, "   %s %s", tuiStyleDim.Render("finished:"), shortTime(*d.Run.FinishedAt))
	}
	b.WriteString("\n" + formatRunProgressLine(d.Run.Status, p, 24) + "\n")
	if d.Run.ErrorSummary != nil && *d.Run.ErrorSummary != "" {
		b.WriteString(tuiStyleErr.Render("error: ") + redactDSN(oneLine(*d.Run.ErrorSummary)) + "\n")
	}
	if len(d.Tasks) > 0 {
		b.WriteString("\n" + tuiStyleDim.Render(fmt.Sprintf("%-5s  %-10s  %12s  %10s  %s", "TASK", "STATUS", "ROWS", "WRITTEN", "WORKER")))
		for _, t := range d.Tasks {
			worker := ""
			if t.WorkerID != nil {
				worker = *t.WorkerID
			}
			fmt.Fprintf(&b, "\n%-5d  %-10s  %12s  %10s  %s", t.TaskIndex, t.Status, humanCount(t.RowsRead), humanBytes(t.BytesWritten), worker)
			if t.ErrorMessage != nil && *t.ErrorMessage != "" {
				b.WriteString("\n" + tuiStyleErr.Render("  └ "+ansi.Truncate(redactDSN(oneLine(*t.ErrorMessage)), 100, "…")))
			}
		}
	}
	return renderBox(width, tuiColorInfo, "Run "+d.Run.ID, b.String())
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// --- /diagnose --------------------------------------------------------------

// diagnoseCmd accepts a run ID, a job ID or a job name. For a job it
// diagnoses that job's most recent run, which is what users usually want.
func (m tuiModel) diagnoseCmd(ref string, rawJSON bool) tea.Cmd {
	base, width := m.base, m.width
	return func() tea.Msg {
		ctx, cancel := m.requestCtx()
		defer cancel()
		runID, note, err := resolveRunRef(ctx, base, ref)
		if err != nil {
			return tuiResultMsg{err: err}
		}
		raw, err := diagnoseRun(ctx, base, runID)
		if err != nil {
			return tuiResultMsg{err: fmt.Errorf("diagnose %s: %w", runID, err)}
		}
		if rawJSON {
			b, err := json.MarshalIndent(raw, "", "  ")
			if err != nil {
				return tuiResultMsg{err: fmt.Errorf("format diagnosis: %w", err)}
			}
			return tuiResultMsg{text: string(b)}
		}
		// Round-trip through JSON to get the typed view of the same body.
		var d tuiDiagnosis
		if b, err := json.Marshal(raw); err == nil {
			err = json.Unmarshal(b, &d)
			if err != nil {
				return tuiResultMsg{err: fmt.Errorf("read diagnosis: %w", err)}
			}
		}
		return tuiResultMsg{text: renderDiagnosis(width, d, note)}
	}
}

// resolveRunRef turns a run ID, job ID or job name into a run ID. note
// explains a job lookup so the output says which run was picked.
func resolveRunRef(ctx context.Context, base, ref string) (runID, note string, err error) {
	_, runErr := getRunDetails(ctx, base, ref)
	if runErr == nil {
		return ref, "", nil
	}
	if !isNotFound(runErr) {
		return "", "", fmt.Errorf("look up run %s: %w", ref, runErr)
	}
	jobs, err := listJobs(ctx, base)
	if err != nil {
		return "", "", err
	}
	job, ok := findJob(jobs, ref)
	if !ok {
		return "", "", fmt.Errorf("%q is neither a run ID nor a job ID/name (see /runs and /jobs)", ref)
	}
	runs, err := listRuns(ctx, base, tuiRunListMax)
	if err != nil {
		return "", "", err
	}
	for _, r := range runs {
		if r.JobID == job.ID { // the master lists newest first
			return r.ID, fmt.Sprintf("latest run of job %s (%s)", job.Name, job.ID), nil
		}
	}
	return "", "", errors.New("job " + job.Name + " has no runs in the last " + fmt.Sprint(tuiRunListMax) + " runs")
}

func renderDiagnosis(width int, d tuiDiagnosis, note string) string {
	var b strings.Builder
	label := func(s string) string { return tuiStyleDim.Render(fmt.Sprintf("%-13s", s)) }
	if note != "" {
		b.WriteString(tuiStyleDim.Render(note) + "\n")
	}
	b.WriteString(label("status") + d.Status)
	if d.CommitPhase != "" {
		b.WriteString(tuiStyleDim.Render("  commit phase: ") + d.CommitPhase)
	}
	b.WriteString("\n")
	if len(d.TaskCounts) > 0 {
		keys := make([]string, 0, len(d.TaskCounts))
		for k := range d.TaskCounts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s %d", k, d.TaskCounts[k]))
		}
		b.WriteString(label("tasks") + strings.Join(parts, " · ") + "\n")
	}
	if d.LastClassifiedError != "" {
		b.WriteString(label("last error") + tuiStyleErr.Render(d.LastClassifiedError) + "\n")
	}
	for _, t := range d.BlockingTasks {
		line := fmt.Sprintf("task %d %s, attempts %d/%d", t.Index, t.Status, t.AttemptCount, t.MaxAttempts)
		if t.WorkerID != "" {
			line += ", worker " + t.WorkerID
		}
		if t.NextEligibleAt != "" {
			line += ", retry after " + shortTime(t.NextEligibleAt)
		}
		if n := len(t.RecentAttempts); n > 0 && t.RecentAttempts[n-1].FailureClass != "" {
			line += ", last failure " + t.RecentAttempts[n-1].FailureClass
		}
		b.WriteString(label("blocking") + line + "\n")
	}
	if r := d.Registration; r != nil {
		line := fmt.Sprintf("%s, attempts %d", r.Status, r.AttemptCount)
		if r.CurrentPhase != "" {
			line += ", phase " + r.CurrentPhase
		}
		if r.LastErrorClass != "" {
			line += ", last error " + r.LastErrorClass
		}
		if r.BlockedBy != "" {
			line += ", blocked by " + r.BlockedBy
		}
		b.WriteString(label("iceberg") + line + "\n")
	}
	if d.OperatorReview {
		b.WriteString(label("review") + tuiStyleWarn.Render("operator review required (see /recover --help)") + "\n")
	}
	if d.SuggestedNextAction != "" {
		b.WriteString("\n" + tuiStyleWarn.Render("→ ") + d.SuggestedNextAction)
	}
	color := tuiColorInfo
	if strings.EqualFold(d.Status, "FAILED") || d.OperatorReview {
		color = tuiColorErr
	}
	return renderBox(width, color, "Diagnosis · run "+d.RunID, b.String())
}

// --- output width -----------------------------------------------------------

// fitWidth wraps every line to less than the terminal width. Bubble Tea
// only erases the old frame under lines narrower than the terminal, and a
// line the terminal wraps on its own throws off its line count, which left
// pieces of the prompt box glued to long table rows.
//
// It wraps at word boundaries (a word longer than a line is the only thing
// split), and each line restarts the color that was active where it began
// and resets at its end. Bubble Tea writes lines one by one, so without that
// a colored word broken across lines came out half colored.
func fitWidth(text string, termWidth int) string {
	if termWidth <= 1 {
		return text
	}
	limit := termWidth - 1
	lines := strings.Split(wrapAtSpaces(text, limit), "\n")
	active := ""
	for i, line := range lines {
		prefix := active
		for _, seq := range sgrSeq.FindAllString(line, -1) {
			if seq == "\x1b[m" || seq == "\x1b[0m" {
				active = ""
			} else {
				active += seq
			}
		}
		if prefix != "" {
			line = prefix + line
		}
		if active != "" {
			line += "\x1b[0m"
		}
		lines[i] = line
	}
	return strings.Join(lines, "\n")
}

// sgrSeq matches ANSI color/style sequences such as "\x1b[38;5;245m".
var sgrSeq = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// wrapAtSpaces word-wraps at spaces only. The x/ansi wrappers also break
// after "-", which split flags such as --execute. A word longer than a whole
// line (an ID or URL) is the only thing cut. Widths ignore color codes.
func wrapAtSpaces(text string, limit int) string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if ansi.StringWidth(line) <= limit {
			out = append(out, line)
			continue
		}
		cur, curW := "", 0
		for _, word := range strings.Split(line, " ") {
			w := ansi.StringWidth(word)
			switch {
			case curW == 0 && cur == "":
				cur, curW = word, w
			case curW+1+w <= limit:
				cur, curW = cur+" "+word, curW+1+w
			default:
				out = append(out, cur)
				cur, curW = word, w
			}
			for curW > limit { // a single word wider than the line
				out = append(out, ansi.Truncate(cur, limit, ""))
				cur = ansi.TruncateLeft(cur, limit, "")
				curW = ansi.StringWidth(cur)
			}
		}
		out = append(out, cur)
	}
	return strings.Join(out, "\n")
}
