package icebergreg

import (
	"strings"
	"testing"
)

func ptr[T any](v T) *T { return &v }
func baseEvidence() (OperationIdentity, []ExpectedFile, CatalogObservation) {
	op := OperationIdentity{"reg", "run", strings.Repeat("a", 64), strings.Repeat("b", 64), "manifest"}
	files := []ExpectedFile{{"s3://Bucket/a//part-1.parquet", 10, 2, strings.Repeat("c", 64), strings.Repeat("d", 64)}, {"s3://bucket/a/part-2.parquet", 20, 3, strings.Repeat("e", 64), strings.Repeat("f", 64)}}
	obs := CatalogObservation{Backend: "rest-go", TableExists: true, TableIdentifier: "n.t", MetadataStart: "meta-1", MetadataEnd: "meta-1", CurrentSnapshotID: "7", CompleteHistory: true, SchemaCompatible: true, LocationCompatible: true}
	return op, files, obs
}
func TestNormalizeObjectURI(t *testing.T) {
	cases := map[string]string{"S3://Bucket/a//b/../c%20d.parquet": "s3://bucket/a/c%20d.parquet", "s3://bucket///x": "s3://bucket/x"}
	for in, want := range cases {
		got, err := NormalizeObjectURI(in)
		if err != nil || got != want {
			t.Errorf("%q got=%q want=%q err=%v", in, got, want, err)
		}
	}
}
func TestReconciliationProofHierarchy(t *testing.T) {
	op, files, base := baseEvidence()
	props := op.Properties()
	cases := []struct {
		name, want string
		mut        func(*CatalogObservation)
	}{
		{"exact-marker", OutcomeExactlyCommitted, func(o *CatalogObservation) {
			o.Snapshots = []SnapshotEvidence{{ID: "7", Summary: props, Files: []ObservedFile{{files[0].Path, ptr(int64(10)), ptr(int64(2)), "7", "ADDED"}, {files[1].Path, ptr(int64(20)), ptr(int64(3)), "7", "ADDED"}}}}
		}},
		{"exact-files-no-marker", OutcomeInsufficientEvidence, func(o *CatalogObservation) {
			o.Snapshots = []SnapshotEvidence{{ID: "7", Files: []ObservedFile{{files[0].Path, nil, nil, "7", "ADDED"}, {files[1].Path, nil, nil, "7", "ADDED"}}}}
		}},
		{"partial", OutcomePartiallyCommitted, func(o *CatalogObservation) {
			o.Snapshots = []SnapshotEvidence{{ID: "7", Files: []ObservedFile{{files[0].Path, nil, nil, "7", "ADDED"}}}}
		}},
		{"marker-conflict", OutcomeConflictingCommit, func(o *CatalogObservation) {
			bad := op.Properties()
			bad["orabbit.artifact_set_digest"] = "wrong"
			o.Snapshots = []SnapshotEvidence{{ID: "7", Summary: bad}}
		}},
		{"metadata-conflict", OutcomeConflictingCommit, func(o *CatalogObservation) {
			o.Snapshots = []SnapshotEvidence{{ID: "7", Files: []ObservedFile{{files[0].Path, ptr(int64(99)), nil, "7", "ADDED"}}}}
		}},
		{"proven-absence", OutcomeDefinitelyNotCommitted, func(o *CatalogObservation) {}},
		{"unproven-absence", OutcomeInsufficientEvidence, func(o *CatalogObservation) { o.CompleteHistory = false }},
		{"changed-observation", OutcomeInsufficientEvidence, func(o *CatalogObservation) { o.MetadataEnd = "meta-2" }},
		{"table-not-found", OutcomeTableNotFound, func(o *CatalogObservation) { o.TableExists = false }},
		{"schema-conflict", OutcomeConflictingCommit, func(o *CatalogObservation) { o.SchemaCompatible = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := base
			tc.mut(&o)
			got, err := DecideReconciliation(op, append([]ExpectedFile(nil), files...), o)
			if err != nil || got.Outcome != tc.want {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		})
	}
}
func TestOperationPropertiesAreDeterministicAndSafe(t *testing.T) {
	op, _, _ := baseEvidence()
	a, b := op.Properties(), op.Properties()
	if len(a) != 5 || a["orabbit.registration_id"] != b["orabbit.registration_id"] {
		t.Fatal(a)
	}
	for k, v := range a {
		if strings.Contains(strings.ToLower(k+v), "token") || strings.Contains(strings.ToLower(k+v), "secret") {
			t.Fatalf("unsafe %s=%s", k, v)
		}
	}
}
