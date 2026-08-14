package main

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
)

func main() {
	filepath.Walk("internal", func(path string, info fs.FileInfo, err error) error {
		if filepath.Ext(path) != ".go" {
			return nil
		}
		b, _ := os.ReadFile(path)

		reps := [][]byte{
			[]byte("st.AssignNextPendingTaskWithLease(ctx, "), []byte("st.AssignNextPendingTaskWithLease(ctx, \"\", "),
			[]byte("st.AssignNextPendingTaskWithLease(context.Background(), "), []byte("st.AssignNextPendingTaskWithLease(context.Background(), \"\", "),
			[]byte("st.RenewTaskLease(ctx, "), []byte("st.RenewTaskLease(ctx, \"\", "),
			[]byte("st.RenewTaskLease(context.Background(), "), []byte("st.RenewTaskLease(context.Background(), \"\", "),
			[]byte("st.CompleteTaskAttemptAt(ctx, "), []byte("st.CompleteTaskAttemptAt(ctx, \"\", "),
			[]byte("st.CompleteTaskAttemptWithArtifactsAt(ctx, "), []byte("st.CompleteTaskAttemptWithArtifactsAt(ctx, \"\", "),
			[]byte("st.UpdateTaskProgressFencedAt(ctx, "), []byte("st.UpdateTaskProgressFencedAt(ctx, \"\", "),
			[]byte("st.TouchWorkerHeartbeat(ctx, "), []byte("st.TouchWorkerHeartbeat(ctx, \"\", "),
			[]byte("st.TouchWorkerHeartbeat(context.Background(), "), []byte("st.TouchWorkerHeartbeat(context.Background(), \"\", "),
			[]byte("st.AcquireUploadCapacity(ctx, "), []byte("st.AcquireUploadCapacity(ctx, \"\", "),
		}

		for i := 0; i < len(reps); i += 2 {
			b = bytes.ReplaceAll(b, reps[i], reps[i+1])
		}
		os.WriteFile(path, b, 0644)
		return nil
	})
}
