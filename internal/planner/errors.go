package planner

import (
	"errors"
	"fmt"
	"strings"
)

var ErrDatasetBusy = errors.New("dataset is busy")

type DatasetBusyError struct {
	DatasetKey   string
	BasePrefix   string
	ActiveRunID  string
	ActiveJobID  string
	ActiveStatus string
}

func (e *DatasetBusyError) Error() string {
	if e == nil {
		return ErrDatasetBusy.Error()
	}
	prefix := strings.TrimSpace(e.BasePrefix)
	switch {
	case strings.TrimSpace(e.ActiveRunID) != "":
		return fmt.Sprintf("dataset is busy: run %s (job %s) is %s for prefix %q", e.ActiveRunID, e.ActiveJobID, e.ActiveStatus, prefix)
	case prefix != "":
		return fmt.Sprintf("dataset is busy for prefix %q", prefix)
	default:
		return ErrDatasetBusy.Error()
	}
}

func (e *DatasetBusyError) Unwrap() error { return ErrDatasetBusy }
