package icebergreg

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/iceberg-go"
)

type restMutationCall struct {
	kind       string
	delete     []string
	add        []string
	properties iceberg.Properties
}

type restMutationSpy struct {
	calls      []restMutationCall
	membership map[string]int
}

func newRESTMutationSpy(existing ...string) *restMutationSpy {
	s := &restMutationSpy{membership: map[string]int{}}
	for _, file := range existing {
		s.membership[file]++
	}
	return s
}

func (s *restMutationSpy) AddFiles(_ context.Context, files []string, properties iceberg.Properties, _ bool) error {
	s.calls = append(s.calls, restMutationCall{kind: "append", add: append([]string(nil), files...), properties: cloneProperties(properties)})
	for _, file := range files {
		s.membership[file]++
	}
	return nil
}

func (s *restMutationSpy) ReplaceDataFiles(_ context.Context, filesToDelete, filesToAdd []string, properties iceberg.Properties) error {
	s.calls = append(s.calls, restMutationCall{kind: "replace", delete: append([]string(nil), filesToDelete...), add: append([]string(nil), filesToAdd...), properties: cloneProperties(properties)})
	for _, file := range filesToDelete {
		delete(s.membership, file)
	}
	for _, file := range filesToAdd {
		s.membership[file]++
	}
	return nil
}

func cloneProperties(properties iceberg.Properties) iceberg.Properties {
	out := iceberg.Properties{}
	for key, value := range properties {
		out[key] = value
	}
	return out
}

func TestRESTIncrementalRegistrationUsesOneMarkedAppend(t *testing.T) {
	existing := "s3://bucket/existing.parquet"
	newFiles := []string{"s3://bucket/part-1.parquet", "s3://bucket/part-2.parquet"}
	identity := OperationIdentity{RegistrationID: "registration", RunID: "run", CommitID: "commit", ArtifactSetDigest: "digest", ManifestKey: "manifest"}
	spy := newRESTMutationSpy(existing)

	if err := applyRESTGoFileMutation(context.Background(), spy, false, nil, newFiles, iceberg.Properties(identity.Properties())); err != nil {
		t.Fatal(err)
	}
	if len(spy.calls) != 1 || spy.calls[0].kind != "append" {
		t.Fatalf("mutation calls=%+v", spy.calls)
	}
	if !reflect.DeepEqual(spy.calls[0].add, newFiles) || !reflect.DeepEqual(spy.calls[0].properties, iceberg.Properties(identity.Properties())) {
		t.Fatalf("append call=%+v", spy.calls[0])
	}
	if spy.membership[existing] != 1 {
		t.Fatalf("existing membership=%d want 1", spy.membership[existing])
	}
	for _, file := range newFiles {
		if spy.membership[file] != 1 {
			t.Fatalf("new file %s membership=%d want 1", file, spy.membership[file])
		}
	}
}

func TestRESTFullRefreshRegistrationUsesOneMarkedReplace(t *testing.T) {
	existing := []string{"s3://bucket/old-1.parquet", "s3://bucket/old-2.parquet"}
	newFiles := []string{"s3://bucket/part-1.parquet", "s3://bucket/part-2.parquet"}
	identity := OperationIdentity{RegistrationID: "registration", RunID: "run", CommitID: "commit", ArtifactSetDigest: "digest", ManifestKey: "manifest"}
	spy := newRESTMutationSpy(existing...)

	if err := applyRESTGoFileMutation(context.Background(), spy, true, existing, newFiles, iceberg.Properties(identity.Properties())); err != nil {
		t.Fatal(err)
	}
	if len(spy.calls) != 1 || spy.calls[0].kind != "replace" {
		t.Fatalf("mutation calls=%+v", spy.calls)
	}
	if !reflect.DeepEqual(spy.calls[0].delete, existing) || !reflect.DeepEqual(spy.calls[0].add, newFiles) || !reflect.DeepEqual(spy.calls[0].properties, iceberg.Properties(identity.Properties())) {
		t.Fatalf("replace call=%+v", spy.calls[0])
	}
	for _, file := range existing {
		if spy.membership[file] != 0 {
			t.Fatalf("old file %s still present", file)
		}
	}
	for _, file := range newFiles {
		if spy.membership[file] != 1 {
			t.Fatalf("new file %s membership=%d want 1", file, spy.membership[file])
		}
	}
}

func TestRESTFullRefreshWithNoArtifactsRemovesExistingFiles(t *testing.T) {
	existing := []string{"s3://bucket/old-1.parquet", "s3://bucket/old-2.parquet"}
	spy := newRESTMutationSpy(existing...)
	if err := applyRESTGoFileMutation(context.Background(), spy, true, existing, nil, iceberg.Properties{"orabbit.commit_id": "empty"}); err != nil {
		t.Fatal(err)
	}
	if len(spy.calls) != 1 || spy.calls[0].kind != "replace" || len(spy.calls[0].add) != 0 {
		t.Fatalf("mutation calls=%+v", spy.calls)
	}
	for _, file := range existing {
		if spy.membership[file] != 0 {
			t.Fatalf("stale file %s remains after empty full refresh", file)
		}
	}
}

func TestVerifiedEmptyActionMatrix(t *testing.T) {
	tests := []struct {
		name        string
		tableExists bool
		incremental bool
		writeMode   string
		schema      bool
		want        string
		wantErr     bool
	}{
		{name: "incremental existing", tableExists: true, incremental: true, writeMode: "append", want: "NO_OP"},
		{name: "full existing", tableExists: true, incremental: false, writeMode: "overwrite", want: "REPLACE_EMPTY"},
		{name: "incremental missing", incremental: true, writeMode: "append", schema: true, want: "CREATE_EMPTY"},
		{name: "full missing", writeMode: "overwrite", schema: true, want: "CREATE_EMPTY"},
		{name: "query missing schema", writeMode: "overwrite", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decideVerifiedEmptyAction(tc.tableExists, tc.incremental, tc.writeMode, tc.schema)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("action=%q err=%v want=%q wantErr=%v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
