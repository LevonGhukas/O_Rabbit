package jobopts

import (
	"fmt"
	"strings"
)

// A job's write_mode says what a run does to data already published for its
// dataset. It is a job column, not part of options_json, but its rules live
// here next to the other job option rules.
const (
	// WriteModeAppend adds each run's rows to the table. Incremental jobs use it.
	WriteModeAppend = "append"
	// WriteModeOverwrite replaces the table with each run's rows. Full
	// (non-incremental) jobs use it.
	WriteModeOverwrite = "overwrite"
)

// NormalizeWriteMode returns the canonical spelling of a write mode, or "".
func NormalizeWriteMode(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

// DefaultWriteMode is the write mode a job gets when it does not set one.
func DefaultWriteMode(incremental bool) string {
	if incremental {
		return WriteModeAppend
	}
	return WriteModeOverwrite
}

// ValidateWriteMode checks a job's write_mode against its incremental flag.
// Empty means DefaultWriteMode. Two combinations are rejected:
//   - incremental + overwrite: each run reads only rows after the high-water
//     mark, so replacing the table would leave only those rows (and a run
//     with no new rows would empty it);
//   - full + append: appending full exports is not supported yet; today such
//     runs replaced the table, which "append" did not promise.
func ValidateWriteMode(raw string, incremental bool) error {
	mode := NormalizeWriteMode(raw)
	switch mode {
	case "":
		return nil
	case WriteModeAppend, WriteModeOverwrite:
	default:
		return fmt.Errorf("must be %q or %q, got %q", WriteModeAppend, WriteModeOverwrite, raw)
	}
	if incremental && mode == WriteModeOverwrite {
		return fmt.Errorf(`"overwrite" cannot be combined with incremental: each run reads only new rows, so it would replace the table with just those rows; use "append"`)
	}
	if !incremental && mode == WriteModeAppend {
		return fmt.Errorf(`"append" is not supported for full (non-incremental) runs yet; use "overwrite", or make the job incremental`)
	}
	return nil
}

// EffectiveWriteMode is the write mode a run uses: the job's own when set and
// valid, otherwise the default for its incremental flag.
func EffectiveWriteMode(raw string, incremental bool) string {
	switch mode := NormalizeWriteMode(raw); mode {
	case WriteModeAppend, WriteModeOverwrite:
		return mode
	default:
		return DefaultWriteMode(incremental)
	}
}
