package planner

import (
	"encoding/json"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

func singleSpecForTest() json.RawMessage {
	return partitionSpecSQLCursorSingle("orders", "table", "", "", nil, nil, "id", connectors.CursorDomainInt64, "10", true, "")
}

func decodeSpec(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var spec map[string]any
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	return spec
}

func TestSingleCursorSpecGetsInclusiveUpperWhenReconciling(t *testing.T) {
	o := jobopts.Options{Reconciliation: &jobopts.Reconciliation{Mode: jobopts.ReconciliationModeCount}}
	raw, err := singleCursorSpecWithUpper(o, singleSpecForTest(), " 99 ")
	if err != nil {
		t.Fatal(err)
	}
	spec := decodeSpec(t, raw)
	if spec["upper"] != "99" || spec["upper_inclusive"] != true {
		t.Fatalf("spec=%v", spec)
	}
	if spec["lower"] != "10" || spec["lower_exclusive"] != true || spec["type"] != "sql_cursor_single" {
		t.Fatalf("existing fields lost: %v", spec)
	}
}

func TestSingleCursorSpecHasNoUpperWhenReconciliationOffOrAbsent(t *testing.T) {
	for name, o := range map[string]jobopts.Options{
		"absent": {},
		"off":    {Reconciliation: &jobopts.Reconciliation{Mode: jobopts.ReconciliationModeOff}},
	} {
		raw, err := singleCursorSpecWithUpper(o, singleSpecForTest(), "99")
		if err != nil {
			t.Fatal(err)
		}
		if spec := decodeSpec(t, raw); spec["upper"] != nil || spec["upper_inclusive"] != nil {
			t.Fatalf("%s: unexpected upper in %v", name, spec)
		}
	}
}

func TestSingleCursorSpecSkipsUpperWithoutPlanTimeMax(t *testing.T) {
	o := jobopts.Options{Reconciliation: &jobopts.Reconciliation{Mode: jobopts.ReconciliationModeCount}}
	raw, err := singleCursorSpecWithUpper(o, singleSpecForTest(), "")
	if err != nil {
		t.Fatal(err)
	}
	if spec := decodeSpec(t, raw); spec["upper"] != nil {
		t.Fatalf("spec=%v", spec)
	}
}
