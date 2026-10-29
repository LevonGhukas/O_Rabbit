package grpcapi

import (
	"encoding/json"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/hwmboundary"
)

func setInt64CursorJob(t *testing.T, f *commitFixture) {
	t.Helper()
	job, err := f.st.GetJob(f.ctx, f.jobID)
	if err != nil {
		t.Fatal(err)
	}
	job.OptionsJSON = []byte(`{"table":"dbo.orders","partition_strategy":"ordered_cursor","cursor_column":"id","cursor_domain":"int64"}`)
	if err := f.st.UpdateJob(f.ctx, job); err != nil {
		t.Fatal(err)
	}
}

func committedHWMBoundary(t *testing.T, f *commitFixture) (string, *hwmboundary.Boundary) {
	t.Helper()
	var state struct {
		MaxHWM   string                `json:"max_hwm_value"`
		Boundary *hwmboundary.Boundary `json:"hwm_boundary"`
	}
	if err := json.Unmarshal(f.objects.objects[f.stateKey], &state); err != nil {
		t.Fatal(err)
	}
	return state.MaxHWM, state.Boundary
}

func TestCommitStoresHWMBoundaryOfRowsAtTheNewMark(t *testing.T) {
	f := newCommitFixtureWithHWMBoundary(t, "hwm-boundary-store", false, true, false, []byte(`{"cursor_value":"42","row_hashes":{"aa":2,"bb":1}}`))
	setInt64CursorJob(t, f)
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	hwm, b := committedHWMBoundary(t, f)
	if hwm != "42" || b == nil || b.CursorValue != "42" || b.RowHashes["aa"] != 2 || b.RowHashes["bb"] != 1 || b.Overflow {
		t.Fatalf("state hwm=%q boundary=%+v, want rows at 42", hwm, b)
	}
}

func TestCommitKeepsStoredBoundaryWhenTheMarkDoesNotMove(t *testing.T) {
	f := newCommitFixtureWithHWMBoundary(t, "hwm-boundary-keep", false, true, false, []byte(`{"cursor_value":"42","row_hashes":{"aa":1}}`))
	setInt64CursorJob(t, f)
	f.objects.objects[f.stateKey] = []byte(`{"last_committed_run_id":"run-earlier","committed_at":"2026-07-22T09:00:00Z","max_hwm_value":"100","max_part":0,"hwm_boundary":{"cursor_value":"100","row_hashes":{"zz":3}}}`)
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	hwm, b := committedHWMBoundary(t, f)
	if hwm != "100" || b == nil || b.CursorValue != "100" || b.RowHashes["zz"] != 3 || len(b.RowHashes) != 1 {
		t.Fatalf("state hwm=%q boundary=%+v, want the stored boundary at 100", hwm, b)
	}
}

func TestCommitWritesNoBoundaryForUntrackedTasks(t *testing.T) {
	f := newCommitFixture(t, "hwm-boundary-untracked")
	setInt64CursorJob(t, f)
	f.objects.objects[f.stateKey] = []byte(`{"last_committed_run_id":"run-earlier","committed_at":"2026-07-22T09:00:00Z","max_hwm_value":"10","max_part":0,"hwm_boundary":{"cursor_value":"10","row_hashes":{"zz":1}}}`)
	if err := f.finalize(); err != nil {
		t.Fatal(err)
	}
	if hwm, b := committedHWMBoundary(t, f); hwm != "42" || b != nil {
		t.Fatalf("state hwm=%q boundary=%+v, want no boundary", hwm, b)
	}
}

func TestEncodeReportedHWMBoundary(t *testing.T) {
	if b, err := encodeReportedHWMBoundary(nil); err != nil || b != nil {
		t.Fatalf("untracked: %s %v", b, err)
	}
	got, err := encodeReportedHWMBoundary(&grpcpb.HWMBoundary{CursorValue: "42", RowHashes: map[string]int64{"bb": 1, "aa": 2}})
	if err != nil || string(got) != `{"cursor_value":"42","row_hashes":{"aa":2,"bb":1}}` {
		t.Fatalf("encoded=%s err=%v", got, err)
	}
	empty, err := encodeReportedHWMBoundary(&grpcpb.HWMBoundary{})
	if err != nil || string(empty) != `{"cursor_value":""}` {
		t.Fatalf("empty task: %s %v", empty, err)
	}
	for name, bad := range map[string]*grpcpb.HWMBoundary{
		"non_positive_count":   {CursorValue: "1", RowHashes: map[string]int64{"aa": 0}},
		"hashes_on_overflow":   {CursorValue: "1", Overflow: true, RowHashes: map[string]int64{"aa": 1}},
		"hashes_without_value": {RowHashes: map[string]int64{"aa": 1}},
	} {
		if _, err := encodeReportedHWMBoundary(bad); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	tooMany := map[string]int64{}
	for i := 0; i <= hwmboundary.MaxRowHashes; i++ {
		tooMany[string(rune('a'+i%26))+string(rune(i))] = 1
	}
	if _, err := encodeReportedHWMBoundary(&grpcpb.HWMBoundary{CursorValue: "1", RowHashes: tooMany}); err == nil {
		t.Error("boundary above the row hash limit was accepted")
	}
}
