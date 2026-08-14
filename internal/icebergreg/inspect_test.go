package icebergreg

import (
	"testing"
)

func TestStableCatalogInspectionCompletesHistoryAndProvesAbsence(t *testing.T) {
	op, files, _ := baseEvidence()
	obs := finalizeCatalogObservation(CatalogObservation{
		Backend:            "rest-go",
		TableExists:        true,
		TableIdentifier:    "n.t",
		MetadataStart:      "meta-1",
		SchemaCompatible:   true,
		LocationCompatible: true,
	}, "meta-1")
	if !obs.CompleteHistory {
		t.Fatal("stable fully walked observation did not complete history")
	}
	decision, err := DecideReconciliation(op, files, obs)
	if err != nil || decision.Outcome != OutcomeDefinitelyNotCommitted {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestStableCatalogInspectionRecognizesMatchingOperation(t *testing.T) {
	op, files, _ := baseEvidence()
	obs := finalizeCatalogObservation(CatalogObservation{
		Backend:            "rest-go",
		TableExists:        true,
		TableIdentifier:    "n.t",
		MetadataStart:      "meta-1",
		SchemaCompatible:   true,
		LocationCompatible: true,
		Snapshots: []SnapshotEvidence{{
			ID:      "7",
			Summary: op.Properties(),
			Files: []ObservedFile{
				{Path: files[0].Path, Size: ptr(files[0].Size), Records: ptr(files[0].Records), SnapshotID: "7", Status: "ADDED"},
				{Path: files[1].Path, Size: ptr(files[1].Size), Records: ptr(files[1].Records), SnapshotID: "7", Status: "ADDED"},
			},
		}},
	}, "meta-1")
	decision, err := DecideReconciliation(op, files, obs)
	if err != nil || decision.Outcome != OutcomeExactlyCommitted {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestCatalogInspectionRaceOrWalkFailureCannotProveAbsence(t *testing.T) {
	op, files, _ := baseEvidence()
	tests := []struct {
		name string
		obs  CatalogObservation
	}{
		{
			name: "metadata changed",
			obs: finalizeCatalogObservation(CatalogObservation{
				Backend: "rest-go", TableExists: true, TableIdentifier: "n.t", MetadataStart: "meta-1", SchemaCompatible: true, LocationCompatible: true,
			}, "meta-2"),
		},
		{
			name: "snapshot walk failed",
			obs: CatalogObservation{
				Backend: "rest-go", TableExists: true, TableIdentifier: "n.t", MetadataStart: "meta-1", SchemaCompatible: true, LocationCompatible: true,
			},
		},
		{
			name: "manifest read failed",
			obs: CatalogObservation{
				Backend: "rest-go", TableExists: true, TableIdentifier: "n.t", MetadataStart: "meta-1", SchemaCompatible: true, LocationCompatible: true,
			},
		},
		{
			name: "manifest entry read failed",
			obs: CatalogObservation{
				Backend: "rest-go", TableExists: true, TableIdentifier: "n.t", MetadataStart: "meta-1", SchemaCompatible: true, LocationCompatible: true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.obs.CompleteHistory {
				t.Fatal("incomplete observation marked complete")
			}
			decision, err := DecideReconciliation(op, append([]ExpectedFile(nil), files...), tc.obs)
			if err != nil || decision.Outcome != OutcomeInsufficientEvidence {
				t.Fatalf("decision=%+v err=%v", decision, err)
			}
		})
	}
}

func TestConfirmedAndRacingTableAbsenceRemainDistinct(t *testing.T) {
	op, files, _ := baseEvidence()
	confirmed := CatalogObservation{
		Backend: "rest-go", TableIdentifier: "n.t", MetadataStart: "TABLE_NOT_FOUND", MetadataEnd: "TABLE_NOT_FOUND", CompleteHistory: true, SchemaCompatible: true, LocationCompatible: true,
	}
	decision, err := DecideReconciliation(op, append([]ExpectedFile(nil), files...), confirmed)
	if err != nil || decision.Outcome != OutcomeTableNotFound {
		t.Fatalf("confirmed decision=%+v err=%v", decision, err)
	}
	racing := confirmed
	racing.MetadataEnd = "CHANGED"
	racing.CompleteHistory = false
	decision, err = DecideReconciliation(op, append([]ExpectedFile(nil), files...), racing)
	if err != nil || decision.Outcome != OutcomeInsufficientEvidence {
		t.Fatalf("racing decision=%+v err=%v", decision, err)
	}
}
