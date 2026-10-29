package orabbitcli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// maintenanceOperation is the API view of a maintenance operation.
type maintenanceOperation struct {
	ID              string          `json:"id"`
	Kind            string          `json:"kind"`
	JobID           string          `json:"job_id"`
	DryRun          bool            `json:"dry_run"`
	Status          string          `json:"status"`
	TableIdentifier string          `json:"table_identifier"`
	SnapshotID      string          `json:"snapshot_id"`
	RunID           string          `json:"run_id"`
	Report          json.RawMessage `json:"report"`
	ErrorClass      string          `json:"error_class"`
	ErrorMessage    string          `json:"error_message"`
	CreatedAt       string          `json:"created_at"`
}

type maintenanceCandidate struct {
	ObjectKey  string          `json:"object_key"`
	SizeBytes  int64           `json:"size_bytes"`
	AgeSeconds int64           `json:"age_seconds"`
	Decision   string          `json:"decision"`
	Reason     string          `json:"reason"`
	Status     string          `json:"status"`
	Evidence   json.RawMessage `json:"evidence"`
}

// cmdMaintenance routes the maintenance command group.
func cmdMaintenance(ctx context.Context, args []string) int {
	if len(args) == 0 {
		renderMaintenanceGroupHelp(os.Stdout)
		return exitSuccess
	}
	switch strings.TrimSpace(args[0]) {
	case "submit":
		return cmdMaintenanceSubmit(ctx, args[1:])
	case "show":
		return cmdMaintenanceShow(ctx, args[1:])
	case "list":
		return cmdMaintenanceList(ctx, args[1:])
	case "help", "-h", "--help":
		renderMaintenanceGroupHelp(os.Stdout)
		return exitSuccess
	}
	fmt.Fprintf(os.Stderr, "maintenance: unknown command %q\n", args[0])
	renderMaintenanceGroupHelp(os.Stderr)
	return exitUsage
}

func cmdMaintenanceSubmit(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("maintenance submit", flag.ContinueOnError)
	masterHTTP := fs.String("master-http", localHTTPBase(defaultHTTPAddr), "Master HTTP base URL")
	jobID := fs.String("job", "", "Job ID whose Iceberg table to maintain (required)")
	operation := fs.String("operation", "", "orphan_cleanup or compaction (required)")
	execute := fs.Bool("execute", false, "Delete orphans or start the compaction; without it the operation only reports (dry run)")
	minAge := fs.Float64("min-age-hours", 0, "orphan_cleanup: keep files younger than this (default 168, minimum 1)")
	target := fs.Int64("target-file-bytes", 0, "compaction: output file size (default: the table or job target, else 256 MiB)")
	small := fs.Int64("small-file-bytes", 0, "compaction: rewrite files below this size (default 3/4 of the target)")
	minInputs := fs.Int("min-input-files", 0, "compaction: files a partition needs before it is compacted (default 3)")
	maxInputs := fs.Int("max-input-files", 0, "compaction: most files one operation rewrites (default 1000)")
	if handled, code := parseCommandFlags(fs, args, renderMaintenanceSubmitHelp); handled {
		return code
	}
	if strings.TrimSpace(*jobID) == "" || strings.TrimSpace(*operation) == "" || len(fs.Args()) > 0 {
		fmt.Fprintln(os.Stderr, "maintenance submit: --job and --operation are required")
		renderMaintenanceSubmitHelp(os.Stderr, fs)
		return exitUsage
	}
	dryRun := !*execute
	body := map[string]any{"job_id": strings.TrimSpace(*jobID), "operation": strings.TrimSpace(*operation), "dry_run": dryRun}
	switch strings.TrimSpace(*operation) {
	case "orphan_cleanup", "vacuum":
		if *minAge > 0 {
			body["orphan_cleanup"] = map[string]any{"min_age_hours": *minAge}
		}
	case "compaction", "compact":
		c := map[string]any{}
		for k, v := range map[string]int64{"target_file_bytes": *target, "small_file_bytes": *small, "min_input_files": int64(*minInputs), "max_input_files": int64(*maxInputs)} {
			if v > 0 {
				c[k] = v
			}
		}
		if len(c) > 0 {
			body["compaction"] = c
		}
	}
	var resp struct {
		Operation maintenanceOperation `json:"operation"`
	}
	if err := httpJSON(ctx, http.MethodPost, normalizeHTTPBase(*masterHTTP)+apiV1+"/maintenance/submit", body, &resp); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(err)
	}
	mode := "dry run"
	if !resp.Operation.DryRun {
		mode = "execute"
	}
	fmt.Printf("submitted %s %s (%s) for job %s\n", strings.ToLower(resp.Operation.Kind), resp.Operation.ID, mode, resp.Operation.JobID)
	fmt.Printf("follow it with: %s maintenance show %s\n", CLIName, resp.Operation.ID)
	return exitSuccess
}

func cmdMaintenanceShow(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("maintenance show", flag.ContinueOnError)
	masterHTTP := fs.String("master-http", localHTTPBase(defaultHTTPAddr), "Master HTTP base URL")
	asJSON := fs.Bool("json", false, "Print the full operation and report as JSON")
	id, parseArgs := takeLeadingPositionalArg(args)
	if handled, code := parseCommandFlags(fs, parseArgs, renderMaintenanceShowHelp); handled {
		return code
	}
	if id == "" && len(fs.Args()) == 1 {
		id = strings.TrimSpace(fs.Args()[0])
	}
	if id == "" || len(fs.Args()) > 1 {
		fmt.Fprintln(os.Stderr, "maintenance show: expected exactly one operation ID")
		renderMaintenanceShowHelp(os.Stderr, fs)
		return exitUsage
	}
	var resp struct {
		Operation  maintenanceOperation   `json:"operation"`
		Candidates []maintenanceCandidate `json:"candidates"`
	}
	if err := httpJSON(ctx, http.MethodGet, normalizeHTTPBase(*masterHTTP)+apiV1+"/maintenance/"+url.PathEscape(id), nil, &resp); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(err)
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(resp)
		return exitSuccess
	}
	printMaintenanceOperation(os.Stdout, resp.Operation)
	if len(resp.Candidates) > 0 {
		fmt.Println()
		fmt.Printf("%-10s %-14s %-36s %10s %8s  %s\n", "DECISION", "STATUS", "REASON", "BYTES", "AGE_H", "OBJECT")
		for _, c := range resp.Candidates {
			fmt.Printf("%-10s %-14s %-36s %10d %8.1f  %s\n", c.Decision, c.Status, c.Reason, c.SizeBytes, float64(c.AgeSeconds)/3600, c.ObjectKey)
		}
	}
	return exitSuccess
}

func cmdMaintenanceList(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("maintenance list", flag.ContinueOnError)
	masterHTTP := fs.String("master-http", localHTTPBase(defaultHTTPAddr), "Master HTTP base URL")
	jobID := fs.String("job", "", "Only this job's operations")
	if handled, code := parseCommandFlags(fs, args, renderMaintenanceListHelp); handled {
		return code
	}
	u := normalizeHTTPBase(*masterHTTP) + apiV1 + "/maintenance"
	if j := strings.TrimSpace(*jobID); j != "" {
		u += "?job_id=" + url.QueryEscape(j)
	}
	var resp struct {
		Operations []maintenanceOperation `json:"operations"`
	}
	if err := httpJSON(ctx, http.MethodGet, u, nil, &resp); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(err)
	}
	fmt.Printf("%-24s %-15s %-10s %-8s %-24s %s\n", "ID", "KIND", "STATUS", "DRY_RUN", "CREATED", "JOB")
	for _, op := range resp.Operations {
		fmt.Printf("%-24s %-15s %-10s %-8t %-24s %s\n", op.ID, op.Kind, op.Status, op.DryRun, op.CreatedAt, op.JobID)
	}
	return exitSuccess
}

func printMaintenanceOperation(w io.Writer, op maintenanceOperation) {
	fmt.Fprintf(w, "operation:  %s\n", op.ID)
	fmt.Fprintf(w, "kind:       %s (dry_run=%t)\n", op.Kind, op.DryRun)
	fmt.Fprintf(w, "status:     %s\n", op.Status)
	fmt.Fprintf(w, "job:        %s\n", op.JobID)
	if op.TableIdentifier != "" {
		fmt.Fprintf(w, "table:      %s (snapshot %s)\n", op.TableIdentifier, op.SnapshotID)
	}
	if op.RunID != "" {
		fmt.Fprintf(w, "run:        %s\n", op.RunID)
	}
	if op.ErrorClass != "" || op.ErrorMessage != "" {
		fmt.Fprintf(w, "error:      %s %s\n", op.ErrorClass, op.ErrorMessage)
	}
	if len(op.Report) > 0 && string(op.Report) != "{}" {
		fmt.Fprintf(w, "report:     %s\n", string(op.Report))
	}
}

func renderMaintenanceGroupHelp(w io.Writer) {
	tw := newTextBlockWriter(w)
	tw.title(fmt.Sprintf("%s maintenance - Iceberg orphan cleanup and compaction", CLIName))
	tw.blank()
	tw.section("Usage")
	tw.command(fmt.Sprintf("%s maintenance submit --job <job-id> --operation orphan_cleanup", CLIName))
	tw.command(fmt.Sprintf("%s maintenance submit --job <job-id> --operation compaction --execute", CLIName))
	tw.command(fmt.Sprintf("%s maintenance show <operation-id>", CLIName))
	tw.command(fmt.Sprintf("%s maintenance list --job <job-id>", CLIName))
	tw.blank()
	tw.section("Commands")
	tw.item("submit", "Start an orphan cleanup or a compaction (dry run unless --execute)")
	tw.item("show", "Print an operation and its per-object report")
	tw.item("list", "List operations, newest first")
	tw.blank()
	tw.section("Notes")
	tw.bullet("Operations are dry runs by default: cleanup lists orphans, compaction lists the files it would rewrite.")
	tw.bullet("Only one operation per job runs at a time; the active master runs it in the background.")
}

func renderMaintenanceSubmitHelp(w io.Writer, fs *flag.FlagSet) {
	fs.SetOutput(w)
	tw := newTextBlockWriter(w)
	tw.title(fmt.Sprintf("%s maintenance submit - start an orphan cleanup or a compaction", CLIName))
	tw.blank()
	tw.section("Usage")
	tw.command(fmt.Sprintf("%s maintenance submit --job <job-id> --operation orphan_cleanup [--min-age-hours 168] [--execute]", CLIName))
	tw.command(fmt.Sprintf("%s maintenance submit --job <job-id> --operation compaction [--target-file-bytes N] [--execute]", CLIName))
	tw.blank()
	tw.section("Behavior")
	tw.bullet("orphan_cleanup deletes only Parquet files no retained snapshot references, older than the grace period, whose run no longer needs them.")
	tw.bullet("compaction rewrites small files per partition on workers and swaps them in with one atomic snapshot.")
	tw.bullet("Without --execute nothing is deleted or rewritten; inspect the report with `maintenance show`.")
	tw.blank()
	tw.section("Flags")
	tw.flags(fs)
}

func renderMaintenanceShowHelp(w io.Writer, fs *flag.FlagSet) {
	fs.SetOutput(w)
	tw := newTextBlockWriter(w)
	tw.title(fmt.Sprintf("%s maintenance show - print an operation and its report", CLIName))
	tw.blank()
	tw.section("Usage")
	tw.command(fmt.Sprintf("%s maintenance show <operation-id> [--json]", CLIName))
	tw.blank()
	tw.section("Flags")
	tw.flags(fs)
}

func renderMaintenanceListHelp(w io.Writer, fs *flag.FlagSet) {
	fs.SetOutput(w)
	tw := newTextBlockWriter(w)
	tw.title(fmt.Sprintf("%s maintenance list - list maintenance operations", CLIName))
	tw.blank()
	tw.section("Usage")
	tw.command(fmt.Sprintf("%s maintenance list [--job <job-id>]", CLIName))
	tw.blank()
	tw.section("Flags")
	tw.flags(fs)
}
