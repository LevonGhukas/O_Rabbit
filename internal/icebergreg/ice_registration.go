package icebergreg

import (
	"context"
	"fmt"
	"log/slog"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// registerIceTable registers files in a table of the ice engine.
//
// The ice CLI only appends. A full refresh of a table that already has a
// snapshot is therefore committed by the master with iceberg-go instead: one
// transaction replaces every live data file with the new files, keeping the
// table, its earlier snapshots (until retention expires them) and the
// operation markers reconciliation relies on. Readers see the previous rows
// or the new rows, never a missing or empty table, and a failed commit leaves
// the previous snapshot current.
//
// Appends, and a full refresh into a table without a snapshot, still go
// through ice insert.
func (m *Manager) registerIceTable(ctx context.Context, tbl *icetable.Table, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, files []string) error {
	if !isFullRefreshRequest(req) || tbl.CurrentSnapshot() == nil {
		return runIceCLIRegister(ctx, m.execCommand, m.iceBinary, reg, table, regS3, files)
	}
	if err := checkIceFullRefreshSupported(tbl, reg, table); err != nil {
		return err
	}
	m.log.Info("iceberg full refresh: replacing ice table files in one snapshot",
		slog.String("run_id", req.RunID),
		slog.String("table", table),
		slog.Int64("previous_snapshot_id", tbl.CurrentSnapshot().SnapshotID),
	)
	return commitRESTGoFiles(ctx, m.log, tbl, req, reg, table, files)
}

// checkIceFullRefreshSupported rejects a full refresh the master cannot
// commit atomically for the ice engine. Ice workers do not split their files
// by partition (ice partitions on insert), so the files cannot be registered
// in a partitioned table without rewriting their rows.
func checkIceFullRefreshSupported(tbl *icetable.Table, reg RunConfig, table string) error {
	spec := tbl.Spec()
	if spec.IsUnpartitioned() && len(reg.PartitionSpec) == 0 {
		return nil
	}
	return failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf(
		"iceberg table %s is partitioned (%s): a full refresh of a partitioned table that already has data is supported only with engine=rest-go, which commits it as one atomic snapshot; switch the registration to engine=rest-go or run it incrementally",
		table, spec))
}
