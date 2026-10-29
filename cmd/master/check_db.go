package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

// runCheckDB implements `orabbit-master check-db`. It inspects a master
// database, typically a restored backup, without changing it: SQLite
// integrity, schema version, how old the copy is, in-flight work, and
// whether ORABBIT_MASTER_KEY decrypts every stored secret. It prints a JSON
// report and exits non-zero when a master should not be started on the copy.
func runCheckDB(args []string) int {
	fs := flag.NewFlagSet("check-db", flag.ContinueOnError)
	dbPath := fs.String("db", loadMasterConfigFromEnv().DBPath, "SQLite DB path")
	maxAge := fs.Duration("max-age", 0, "Fail when the copy's last master activity is older than this (0 disables)")
	requireKey := fs.Bool("require-key", true, "Fail when ORABBIT_MASTER_KEY is not set")
	if err := fs.Parse(args); err != nil {
		return exitConfig
	}

	key, err := crypto.LoadMasterKeyFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, "load ORABBIT_MASTER_KEY:", err)
		return exitConfig
	}
	if key.IsZero() && *requireKey {
		fmt.Fprintln(os.Stderr, "ORABBIT_MASTER_KEY is required to verify stored secrets (or pass -require-key=false)")
		return exitConfig
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	report, err := db.InspectDatabase(ctx, *dbPath, key, time.Now())
	if err != nil {
		fmt.Fprintln(os.Stderr, "check-db:", err)
		return exitFailure
	}
	problems := report.Problems(*maxAge)
	out := struct {
		db.DatabaseReport
		OK       bool     `json:"ok"`
		Problems []string `json:"problems"`
	}{report, len(problems) == 0, problems}
	if out.Problems == nil {
		out.Problems = []string{}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, "write report:", err)
		return exitFailure
	}
	if !out.OK {
		return exitFailure
	}
	return exitOK
}
