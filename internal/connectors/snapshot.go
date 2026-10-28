package connectors

import (
	"fmt"
	"regexp"
)

// The snapshot context reaches the worker through task JSON and is spliced
// into SQL (neither SET TRANSACTION SNAPSHOT nor AS OF SCN takes a bind
// parameter), so it is checked against the shape the source exports.
var (
	// pg_export_snapshot(): hex groups joined by '-', e.g. 00000003-0000001B-1.
	postgresSnapshotIDPattern = regexp.MustCompile(`^[0-9A-Fa-f]+(-[0-9A-Fa-f]+)+$`)
	oracleSCNPattern          = regexp.MustCompile(`^[0-9]+$`)
)

func validatePostgresSnapshotID(id string) error {
	if !postgresSnapshotIDPattern.MatchString(id) {
		return fmt.Errorf("invalid postgres snapshot id %q", id)
	}
	return nil
}

func validateOracleSCN(scn string) error {
	if !oracleSCNPattern.MatchString(scn) {
		return fmt.Errorf("invalid oracle snapshot SCN %q", scn)
	}
	return nil
}
