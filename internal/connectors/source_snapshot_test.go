package connectors

import (
	"context"
	"testing"
)

type fakeCursorReader struct {
	TableReader
}

type fakeQueryCursorReader struct {
	TableReader
	SourceQueryReader
}

type fakeSnapshotCursorReader struct {
	TableReader
	snapshot string
}

func (f *fakeSnapshotCursorReader) ExportSnapshot(context.Context) (string, error) {
	return f.snapshot, nil
}

type fakeSnapshotQueryCursorReader struct {
	TableReader
	SourceQueryReader
	snapshot string
}

func (f *fakeSnapshotQueryCursorReader) ExportSnapshot(context.Context) (string, error) {
	return f.snapshot, nil
}

func TestWrappedCursorReaderForwardsSnapshotExport(t *testing.T) {
	cases := map[string]struct {
		inner     TableReader
		wantQuery bool
	}{
		"table reader": {inner: &fakeSnapshotCursorReader{snapshot: "00000003-0000001B-1"}},
		"query reader": {inner: &fakeSnapshotQueryCursorReader{snapshot: "00000003-0000001B-1"}, wantQuery: true},
	}
	for name, tc := range cases {
		wrapped := wrapCursorReader(tc.inner)
		exporter, ok := wrapped.(SnapshotExporter)
		if !ok {
			t.Fatalf("%s: wrapped reader %T does not expose SnapshotExporter", name, wrapped)
		}
		got, err := exporter.ExportSnapshot(context.Background())
		if err != nil || got != "00000003-0000001B-1" {
			t.Fatalf("%s: snapshot=%q err=%v", name, got, err)
		}
		if _, ok := wrapped.(SourceQueryReader); ok != tc.wantQuery {
			t.Fatalf("%s: SourceQueryReader=%v, want %v", name, ok, tc.wantQuery)
		}
	}
}

func TestWrappedCursorReaderHidesSnapshotExportWhenUnsupported(t *testing.T) {
	cases := map[string]struct {
		inner     TableReader
		wantQuery bool
	}{
		"table reader": {inner: &fakeCursorReader{}},
		"query reader": {inner: &fakeQueryCursorReader{}, wantQuery: true},
	}
	for name, tc := range cases {
		wrapped := wrapCursorReader(tc.inner)
		if _, ok := wrapped.(SnapshotExporter); ok {
			t.Fatalf("%s: wrapped reader %T exposes SnapshotExporter for a source without snapshot support", name, wrapped)
		}
		if _, ok := wrapped.(SourceQueryReader); ok != tc.wantQuery {
			t.Fatalf("%s: SourceQueryReader=%v, want %v", name, ok, tc.wantQuery)
		}
	}
}
