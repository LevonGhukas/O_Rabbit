package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

// runRotateMasterKey implements `orabbit-master rotate-master-key`. It
// re-encrypts every stored secret from ORABBIT_MASTER_KEY to
// ORABBIT_NEW_MASTER_KEY in one transaction. It takes the same local
// singleton lock as the master, so it refuses to run while a master is
// serving from the database.
func runRotateMasterKey(args []string) int {
	fs := flag.NewFlagSet("rotate-master-key", flag.ContinueOnError)
	dbPath := fs.String("db", loadMasterConfigFromEnv().DBPath, "SQLite DB path")
	if err := fs.Parse(args); err != nil {
		return exitConfig
	}
	log := newMasterLogger("INFO", "text")

	oldKey, err := crypto.LoadMasterKeyFromEnv()
	if err != nil || oldKey.IsZero() {
		log.Error("ORABBIT_MASTER_KEY must hold the current key")
		return exitConfig
	}
	rawNew := strings.TrimSpace(os.Getenv("ORABBIT_NEW_MASTER_KEY"))
	newKey, err := crypto.ParseKey(rawNew)
	if rawNew == "" || err != nil {
		log.Error("ORABBIT_NEW_MASTER_KEY must hold the new key (openssl rand -base64 32)")
		return exitConfig
	}
	if rawNew == strings.TrimSpace(os.Getenv("ORABBIT_MASTER_KEY")) {
		log.Error("new master key equals the current key")
		return exitConfig
	}

	ctx := context.Background()
	instanceID, err := db.NewMasterInstanceID()
	if err != nil {
		log.Error("create instance identity", slog.String("err", err.Error()))
		return exitFailure
	}
	lock, err := db.AcquireMasterProcessLock(*dbPath, instanceID)
	if err != nil {
		log.Error("stop the master before rotating its key", slog.String("err", err.Error()))
		return exitFailure
	}
	defer lock.Close()

	st, err := db.Open(ctx, db.Config{Path: lock.DatabasePath}, log)
	if err != nil {
		log.Error("open db", slog.String("err", err.Error()))
		return exitFailure
	}
	defer st.Close()

	res, err := st.RotateMasterKey(ctx, oldKey, newKey)
	if err != nil {
		log.Error("master key rotation failed; nothing was changed", slog.String("err", err.Error()))
		return exitFailure
	}
	fmt.Printf("re-encrypted %d values (connections=%d server_credentials=%d config_versions=%d run_registration_configs=%d retry_overrides=%d worker_ca=%d)\n",
		res.Total(), res.ConnectionSecrets, res.ServerCredentials, res.ConfigVersions, res.RunRegistrationConfs, res.RetryOverrides, res.WorkerCAKeys)
	fmt.Println("Now set ORABBIT_MASTER_KEY to the new key, remove ORABBIT_NEW_MASTER_KEY, and start the master.")
	return exitOK
}
