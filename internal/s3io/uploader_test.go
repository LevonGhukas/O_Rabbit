package s3io

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	failBefore       = "FAIL_BEFORE_DURABILITY"
	durableThenError = "DURABLE_THEN_ERROR"
	transientError   = "TRANSIENT_ERROR"
	corruptContent   = "CORRUPTED_CONTENT"
)

type fakeObject struct {
	body     []byte
	meta     map[string]string
	checksum string
	etag     string
}
type fakeFault struct {
	op, key    string
	occurrence int
	mode       string
	part       int32
}
type fakeCall struct {
	op, key  string
	part     int32
	parts    []int32
	checksum string
	meta     map[string]string
	bytes    int
}
type fakeMultipart struct {
	key       string
	meta      map[string]string
	parts     map[int32][]byte
	checksums map[int32]string
}
type fakeS3Client struct {
	mu               sync.Mutex
	objects          map[string]fakeObject
	faults           []fakeFault
	calls            []fakeCall
	counts           map[string]int
	uploads          map[string]*fakeMultipart
	nextUpload       int
	aborts           int
	providerChecksum bool
}

func newFakeS3() *fakeS3Client {
	return &fakeS3Client{objects: map[string]fakeObject{}, counts: map[string]int{}, uploads: map[string]*fakeMultipart{}}
}
func cloneMeta(in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
func (f *fakeS3Client) fault(op, key string, part int32, call fakeCall) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := op + "\x00" + key
	f.counts[id]++
	f.calls = append(f.calls, call)
	for _, plan := range f.faults {
		if plan.op == op && plan.key == key && (plan.occurrence == 0 || plan.occurrence == f.counts[id]) && (plan.part == 0 || plan.part == part) {
			return plan.mode
		}
	}
	return ""
}
func (f *fakeS3Client) count(op, key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[op+"\x00"+key]
}
func apiErr(code string) error { return &smithy.GenericAPIError{Code: code, Message: code} }

func (f *fakeS3Client) HeadObject(ctx context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	key := aws.ToString(in.Key)
	mode := f.fault("HEAD", key, 0, fakeCall{op: "HEAD", key: key})
	if mode == transientError {
		return nil, errors.New("transient HEAD")
	}
	if mode == "CHECKSUM_UNSUPPORTED" && in.ChecksumMode != "" {
		return nil, apiErr("NotImplemented")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[key]
	if !ok {
		return nil, apiErr("NotFound")
	}
	out := &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(o.body))), Metadata: cloneMeta(o.meta), ETag: aws.String(o.etag)}
	if f.providerChecksum {
		out.ChecksumSHA256 = aws.String(o.checksum)
	}
	return out, nil
}
func (f *fakeS3Client) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(in.Key)
	mode := f.fault("GET", key, 0, fakeCall{op: "GET", key: key})
	if mode == transientError {
		return nil, errors.New("transient GET")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	o, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		return nil, apiErr("NoSuchKey")
	}
	body := append([]byte(nil), o.body...)
	if mode == corruptContent && len(body) > 0 {
		body[0] ^= 0xff
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}
func (f *fakeS3Client) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := aws.ToString(in.Key)
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	mode := f.fault("PUT", key, 0, fakeCall{op: "PUT", key: key, checksum: aws.ToString(in.ChecksumSHA256), meta: cloneMeta(in.Metadata), bytes: len(body)})
	if mode == failBefore {
		return nil, errors.New("put failed")
	}
	if mode == "CHECKSUM_UNSUPPORTED" {
		return nil, apiErr("NotImplemented")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	f.mu.Lock()
	f.objects[key] = fakeObject{body: body, meta: cloneMeta(in.Metadata), checksum: base64.StdEncoding.EncodeToString(sum[:]), etag: "etag-not-a-digest"}
	f.mu.Unlock()
	if mode == durableThenError {
		return nil, errors.New("lost PUT response")
	}
	return &s3.PutObjectOutput{ETag: aws.String("etag-not-a-digest"), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(sum[:]))}, nil
}
func (f *fakeS3Client) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	key := aws.ToString(in.Key)
	mode := f.fault("DELETE", key, 0, fakeCall{op: "DELETE", key: key})
	if mode == failBefore {
		return nil, errors.New("delete failed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	delete(f.objects, key)
	f.mu.Unlock()
	if mode == durableThenError {
		return nil, errors.New("lost DELETE response")
	}
	return &s3.DeleteObjectOutput{}, nil
}
func (f *fakeS3Client) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	out := &s3.DeleteObjectsOutput{}
	for _, obj := range in.Delete.Objects {
		if _, err := f.DeleteObject(ctx, &s3.DeleteObjectInput{Key: obj.Key}); err != nil {
			return nil, err
		}
		out.Deleted = append(out.Deleted, types.DeletedObject{Key: obj.Key})
	}
	return out, nil
}
func (f *fakeS3Client) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	key := aws.ToString(in.Key)
	mode := f.fault("CREATE", key, 0, fakeCall{op: "CREATE", key: key, checksum: string(in.ChecksumAlgorithm), meta: cloneMeta(in.Metadata)})
	if mode == failBefore {
		return nil, errors.New("create failed")
	}
	if mode == "CHECKSUM_UNSUPPORTED" {
		return nil, apiErr("NotImplemented")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextUpload++
	id := key + "-upload"
	f.uploads[id] = &fakeMultipart{key: key, meta: cloneMeta(in.Metadata), parts: map[int32][]byte{}, checksums: map[int32]string{}}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(id)}, nil
}
func (f *fakeS3Client) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	key, part := aws.ToString(in.Key), aws.ToInt32(in.PartNumber)
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	mode := f.fault("PART", key, part, fakeCall{op: "PART", key: key, part: part, checksum: aws.ToString(in.ChecksumSHA256), bytes: len(body)})
	if mode == failBefore {
		return nil, errors.New("part failed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	checksum := base64.StdEncoding.EncodeToString(sum[:])
	f.mu.Lock()
	up := f.uploads[aws.ToString(in.UploadId)]
	up.parts[part] = body
	up.checksums[part] = checksum
	f.mu.Unlock()
	return &s3.UploadPartOutput{ETag: aws.String("multipart-etag"), ChecksumSHA256: aws.String(checksum)}, nil
}
func (f *fakeS3Client) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	key := aws.ToString(in.Key)
	completedParts := make([]int32, 0, len(in.MultipartUpload.Parts))
	for _, part := range in.MultipartUpload.Parts {
		completedParts = append(completedParts, aws.ToInt32(part.PartNumber))
	}
	mode := f.fault("COMPLETE", key, 0, fakeCall{op: "COMPLETE", key: key, parts: completedParts})
	if mode == failBefore {
		return nil, errors.New("complete failed")
	}
	f.mu.Lock()
	up := f.uploads[aws.ToString(in.UploadId)]
	nums := make([]int, 0, len(up.parts))
	for n := range up.parts {
		nums = append(nums, int(n))
	}
	sort.Ints(nums)
	var body []byte
	for _, n := range nums {
		body = append(body, up.parts[int32(n)]...)
	}
	sum := sha256.Sum256(body)
	f.objects[key] = fakeObject{body: body, meta: cloneMeta(up.meta), checksum: base64.StdEncoding.EncodeToString(sum[:]), etag: "multipart-etag-3"}
	delete(f.uploads, aws.ToString(in.UploadId))
	f.mu.Unlock()
	if mode == durableThenError {
		return nil, errors.New("lost COMPLETE response")
	}
	return &s3.CompleteMultipartUploadOutput{ETag: aws.String("multipart-etag-3"), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(sum[:]))}, nil
}
func (f *fakeS3Client) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	key := aws.ToString(in.Key)
	mode := f.fault("ABORT", key, 0, fakeCall{op: "ABORT", key: key})
	f.mu.Lock()
	f.aborts++
	delete(f.uploads, aws.ToString(in.UploadId))
	f.mu.Unlock()
	if mode == failBefore {
		return nil, errors.New("abort failed")
	}
	return &s3.AbortMultipartUploadOutput{}, nil
}
func (f *fakeS3Client) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}
func (f *fakeS3Client) ListMultipartUploads(_ context.Context, in *s3.ListMultipartUploadsInput, _ ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := aws.ToString(in.Prefix)
	out := &s3.ListMultipartUploadsOutput{}
	for id, upload := range f.uploads {
		if strings.HasPrefix(upload.key, prefix) {
			out.Uploads = append(out.Uploads, types.MultipartUpload{Key: aws.String(upload.key), UploadId: aws.String(id)})
		}
	}
	return out, nil
}

func testUploader(fake *fakeS3Client, threshold, partSize int64) *Uploader {
	return &Uploader{client: fake, cfg: Config{Bucket: "bucket"}, checksumCapability: ChecksumAuto, smallPutThreshold: threshold, partSize: partSize, maxConcurrency: 2}
}
func testFile(t *testing.T, body []byte) (string, string) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "artifact-*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(body); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	return f.Name(), hex.EncodeToString(sum[:])
}
func integrityMeta() map[string]string {
	return map[string]string{"run_id": "run", "task_id": "task", "attempt_id": "attempt", "attempt_number": "1", "file_index": "000", "row_count": "3", "schema_fingerprint": strings.Repeat("a", 64), "byte_size": "9", "sha256": strings.Repeat("b", 64), "format_version": "1"}
}

func TestSmallUploadChecksumCapabilitiesAndAmbiguity(t *testing.T) {
	body := []byte("123456789")
	path, digest := testFile(t, body)
	key := "small.parquet"
	t.Run("provider", func(t *testing.T) {
		f := newFakeS3()
		f.providerChecksum = true
		u := testUploader(f, 1024, 3)
		got, err := u.UploadFileVerified(context.Background(), key, path, integrityMeta(), int64(len(body)), digest)
		if err != nil {
			t.Fatal(err)
		}
		if got.VerificationMethod != artifact.VerificationProvider {
			t.Fatalf("method=%s", got.VerificationMethod)
		}
		checksumSent := false
		for _, call := range f.calls {
			if call.op == "PUT" && call.checksum != "" {
				checksumSent = true
			}
		}
		if !checksumSent {
			t.Fatal("provider checksum not sent")
		}
	})
	t.Run("unsupported fallback", func(t *testing.T) {
		f := newFakeS3()
		f.faults = []fakeFault{{op: "PUT", key: key, occurrence: 1, mode: "CHECKSUM_UNSUPPORTED"}}
		u := testUploader(f, 1024, 3)
		got, err := u.UploadFileVerified(context.Background(), key, path, integrityMeta(), int64(len(body)), digest)
		if err != nil {
			t.Fatal(err)
		}
		if got.VerificationMethod != artifact.VerificationPortable || f.count("PUT", key) != 2 {
			t.Fatalf("result=%+v puts=%d", got, f.count("PUT", key))
		}
	})
	t.Run("HEAD checksum unsupported fallback", func(t *testing.T) {
		f := newFakeS3()
		f.faults = []fakeFault{{op: "HEAD", key: key, occurrence: 2, mode: "CHECKSUM_UNSUPPORTED"}}
		u := testUploader(f, 1024, 3)
		got, err := u.UploadFileVerified(context.Background(), key, path, integrityMeta(), int64(len(body)), digest)
		if err != nil || got.VerificationMethod != artifact.VerificationPortable {
			t.Fatalf("got=%+v err=%v", got, err)
		}
	})
	t.Run("durable response loss", func(t *testing.T) {
		f := newFakeS3()
		f.faults = []fakeFault{{op: "PUT", key: key, occurrence: 1, mode: durableThenError}}
		u := testUploader(f, 1024, 3)
		_, err := u.UploadFileVerified(context.Background(), key, path, integrityMeta(), int64(len(body)), digest)
		failure, ok := artifact.AsFailure(err)
		if !ok || !failure.Ambiguous {
			t.Fatalf("err=%v", err)
		}
		got, err := u.UploadFileVerified(context.Background(), key, path, integrityMeta(), int64(len(body)), digest)
		if err != nil || !got.Skipped || f.count("PUT", key) != 1 {
			t.Fatalf("got=%+v err=%v puts=%d", got, err, f.count("PUT", key))
		}
	})
	t.Run("fail before durability", func(t *testing.T) {
		f := newFakeS3()
		f.faults = []fakeFault{{op: "PUT", key: key, occurrence: 1, mode: failBefore}}
		u := testUploader(f, 1024, 3)
		if _, err := u.UploadFileVerified(context.Background(), key, path, integrityMeta(), int64(len(body)), digest); err == nil {
			t.Fatal("accepted failed PUT")
		}
		if _, ok := f.objects[key]; ok {
			t.Fatal("object became durable")
		}
	})
}

func TestExistingObjectRequiresExactMetadataAndBytes(t *testing.T) {
	body := []byte("123456789")
	path, digest := testFile(t, body)
	key := "existing.parquet"
	base := integrityMeta()
	newExisting := func() (*fakeS3Client, *Uploader) {
		f := newFakeS3()
		sum := sha256.Sum256(body)
		f.objects[key] = fakeObject{body: append([]byte(nil), body...), meta: cloneMeta(base), checksum: base64.StdEncoding.EncodeToString(sum[:]), etag: "misleading"}
		return f, testUploader(f, 1024, 3)
	}
	t.Run("exact reuse", func(t *testing.T) {
		f, u := newExisting()
		got, err := u.UploadFileVerified(context.Background(), key, path, base, int64(len(body)), digest)
		if err != nil || !got.Skipped || f.count("PUT", key) != 0 {
			t.Fatalf("got=%+v err=%v", got, err)
		}
	})
	for _, field := range []string{"run_id", "task_id", "attempt_id", "attempt_number", "file_index", "row_count", "schema_fingerprint", "format_version"} {
		field := field
		t.Run("conflict "+field, func(t *testing.T) {
			f, u := newExisting()
			f.objects[key].meta[field] = "wrong"
			_, err := u.UploadFileVerified(context.Background(), key, path, base, int64(len(body)), digest)
			failure, ok := artifact.AsFailure(err)
			if !ok || failure.Classification != artifact.FailureExistingObjectConflict {
				t.Fatalf("err=%v", err)
			}
			if f.count("PUT", key) != 0 {
				t.Fatal("conflict overwritten")
			}
		})
	}
	t.Run("same size wrong bytes", func(t *testing.T) {
		f, u := newExisting()
		f.objects[key] = fakeObject{body: []byte("abcdefghi"), meta: cloneMeta(base)}
		_, err := u.UploadFileVerified(context.Background(), key, path, base, int64(len(body)), digest)
		failure, ok := artifact.AsFailure(err)
		if !ok || failure.Classification != artifact.FailureExistingObjectConflict {
			t.Fatalf("err=%v", err)
		}
		if f.count("PUT", key) != 0 {
			t.Fatal("conflict overwritten")
		}
	})
	t.Run("size", func(t *testing.T) {
		f, u := newExisting()
		f.objects[key] = fakeObject{body: []byte("short"), meta: cloneMeta(base)}
		if _, err := u.UploadFileVerified(context.Background(), key, path, base, int64(len(body)), digest); err == nil {
			t.Fatal("size accepted")
		}
	})
	t.Run("provider mismatch", func(t *testing.T) {
		f, u := newExisting()
		f.providerChecksum = true
		o := f.objects[key]
		o.checksum = base64.StdEncoding.EncodeToString(make([]byte, 32))
		f.objects[key] = o
		if _, err := u.UploadFileVerified(context.Background(), key, path, base, int64(len(body)), digest); err == nil {
			t.Fatal("checksum accepted")
		}
	})
}

func TestMultipartLifecycleAndAmbiguity(t *testing.T) {
	body := []byte("abcdefghijkl")
	path, digest := testFile(t, body)
	key := "multi.parquet"
	meta := integrityMeta()
	t.Run("success order checksum no abort", func(t *testing.T) {
		f := newFakeS3()
		f.providerChecksum = true
		u := testUploader(f, 1, 4)
		got, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest)
		if err != nil {
			t.Fatal(err)
		}
		if got.VerificationMethod != artifact.VerificationProvider || f.aborts != 0 {
			t.Fatalf("got=%+v aborts=%d", got, f.aborts)
		}
		var parts []int
		for _, c := range f.calls {
			if c.op == "PART" {
				parts = append(parts, int(c.part))
				if c.checksum == "" {
					t.Fatal("part checksum missing")
				}
			}
		}
		sort.Ints(parts)
		if len(parts) != 3 || parts[0] != 1 || parts[2] != 3 {
			t.Fatalf("parts=%v", parts)
		}
		var completed []int32
		for _, call := range f.calls {
			if call.op == "COMPLETE" {
				completed = call.parts
			}
		}
		if !reflect.DeepEqual(completed, []int32{1, 2, 3}) {
			t.Fatalf("completion parts=%v", completed)
		}
	})
	t.Run("checksum unsupported uses portable fallback", func(t *testing.T) {
		f := newFakeS3()
		f.faults = []fakeFault{{op: "CREATE", key: key, occurrence: 1, mode: "CHECKSUM_UNSUPPORTED"}}
		u := testUploader(f, 1, 4)
		got, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest)
		if err != nil || got.VerificationMethod != artifact.VerificationPortable || f.count("CREATE", key) != 2 {
			t.Fatalf("got=%+v err=%v creates=%d", got, err, f.count("CREATE", key))
		}
	})
	for _, part := range []int32{1, 2, 3} {
		part := part
		t.Run("part failure", func(t *testing.T) {
			f := newFakeS3()
			f.faults = []fakeFault{{op: "PART", key: key, occurrence: 0, part: part, mode: failBefore}}
			u := testUploader(f, 1, 4)
			_, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest)
			failure, ok := artifact.AsFailure(err)
			if !ok || failure.Classification != artifact.FailureMultipartPartFailed || f.aborts != 1 {
				t.Fatalf("err=%v aborts=%d", err, f.aborts)
			}
		})
	}
	t.Run("create failure", func(t *testing.T) {
		f := newFakeS3()
		f.faults = []fakeFault{{op: "CREATE", key: key, occurrence: 1, mode: failBefore}}
		u := testUploader(f, 1, 4)
		if _, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest); err == nil {
			t.Fatal("create failure accepted")
		}
		if f.aborts != 0 {
			t.Fatal("abort without upload id")
		}
	})
	t.Run("completion durable then error reconciles", func(t *testing.T) {
		f := newFakeS3()
		f.faults = []fakeFault{{op: "COMPLETE", key: key, occurrence: 1, mode: durableThenError}}
		u := testUploader(f, 1, 4)
		_, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest)
		failure, ok := artifact.AsFailure(err)
		if !ok || !failure.Ambiguous || f.aborts != 0 {
			t.Fatalf("err=%v aborts=%d", err, f.aborts)
		}
		got, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest)
		if err != nil || !got.Skipped || f.count("CREATE", key) != 1 {
			t.Fatalf("got=%+v err=%v creates=%d", got, err, f.count("CREATE", key))
		}
	})
	t.Run("verification corruption", func(t *testing.T) {
		f := newFakeS3()
		f.faults = []fakeFault{{op: "GET", key: key, occurrence: 1, mode: corruptContent}}
		u := testUploader(f, 1, 4)
		if _, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest); err == nil {
			t.Fatal("corruption accepted")
		}
		if f.aborts != 0 {
			t.Fatal("aborted completed upload")
		}
	})
	t.Run("cancellation aborts before completion", func(t *testing.T) {
		f := newFakeS3()
		u := testUploader(f, 1, 4)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := u.UploadFileVerified(ctx, key, path, meta, int64(len(body)), digest)
		if err == nil {
			t.Fatal("canceled multipart accepted")
		}
		if f.count("COMPLETE", key) != 0 {
			t.Fatal("completed after cancellation")
		}
	})
	t.Run("abort failure preserves original part error", func(t *testing.T) {
		f := newFakeS3()
		f.faults = []fakeFault{{op: "PART", key: key, part: 2, mode: failBefore}, {op: "ABORT", key: key, occurrence: 1, mode: failBefore}}
		u := testUploader(f, 1, 4)
		_, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest)
		if err == nil || !strings.Contains(err.Error(), "part failed") || !strings.Contains(err.Error(), "abort multipart") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("existing conflict no multipart", func(t *testing.T) {
		f := newFakeS3()
		f.objects[key] = fakeObject{body: []byte("xxxxxxxxxxxx"), meta: cloneMeta(meta)}
		u := testUploader(f, 1, 4)
		if _, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest); err == nil {
			t.Fatal("conflict accepted")
		}
		if f.count("CREATE", key) != 0 {
			t.Fatal("multipart started")
		}
	})
}

func TestTrackedMultipartLifecycleAndManagedListing(t *testing.T) {
	body := []byte("abcdefghijkl")
	file, digest := testFile(t, body)
	fake := newFakeS3()
	uploader := testUploader(fake, 1, 4)
	var events []MultipartEvent
	observer := func(_ context.Context, event MultipartEvent) error {
		events = append(events, event)
		return nil
	}
	key := "datasets/run/attempt/file.parquet"
	if _, err := uploader.UploadFileVerifiedTracked(context.Background(), key, file, integrityMeta(), int64(len(body)), digest, 7, observer); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, event := range events {
		names = append(names, event.Event)
		if event.FileIndex != 7 || event.ObjectKey != key || event.SHA256 != digest {
			t.Fatalf("unsafe/incomplete event: %+v", event)
		}
	}
	if strings.Join(names, ",") != "PREPARED,CREATED,COMPLETING,COMPLETED" {
		t.Fatalf("events=%v", names)
	}

	fake.uploads["tracked"] = &fakeMultipart{key: key, parts: map[int32][]byte{}, checksums: map[int32]string{}}
	fake.uploads["outside"] = &fakeMultipart{key: "unmanaged/file", parts: map[int32][]byte{}, checksums: map[int32]string{}}
	items, err := uploader.ListManagedMultipartUploads(context.Background(), "datasets/run/attempt/", 10)
	if err != nil || len(items) != 1 || items[0].UploadID != "tracked" {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}

func TestExactObjectObservationAndSingleDelete(t *testing.T) {
	fake := newFakeS3()
	body := []byte("canceled artifact")
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	key := "managed/run/attempt/file.parquet"
	meta := map[string]string{"run_id": "run", "task_id": "task", "attempt_id": "attempt", "sha256": digest, "byte_size": fmt.Sprint(len(body))}
	fake.objects[key] = fakeObject{body: body, meta: cloneMeta(meta), checksum: base64.StdEncoding.EncodeToString(sum[:]), etag: "etag"}
	uploader := testUploader(fake, 1024, 512)
	observation, err := uploader.ObserveExactObject(context.Background(), key, int64(len(body)), digest, meta)
	if err != nil || !observation.Exists || !observation.Matches || observation.Identity == "" {
		t.Fatalf("observation=%+v err=%v", observation, err)
	}
	if err := uploader.DeleteExactObject(context.Background(), key, observation.VersionID); err != nil {
		t.Fatal(err)
	}
	missing, err := uploader.ObserveExactObject(context.Background(), key, int64(len(body)), digest, meta)
	if err != nil || missing.Exists || fake.count("DELETE", key) != 1 {
		t.Fatalf("missing=%+v deletes=%d err=%v", missing, fake.count("DELETE", key), err)
	}
}

func TestExactObjectDeleteResponseLossReconcilesAsMissing(t *testing.T) {
	fake := newFakeS3()
	key := "managed/run/attempt/ambiguous.parquet"
	fake.objects[key] = fakeObject{body: []byte("x"), meta: map[string]string{}}
	fake.faults = append(fake.faults, fakeFault{op: "DELETE", key: key, occurrence: 1, mode: durableThenError})
	uploader := testUploader(fake, 1024, 512)
	if err := uploader.DeleteExactObject(context.Background(), key, ""); err == nil {
		t.Fatal("expected lost delete response")
	}
	observation, err := uploader.ObserveExactObject(context.Background(), key, 1, strings.Repeat("0", 64), nil)
	if err != nil || observation.Exists {
		t.Fatalf("observation=%+v err=%v", observation, err)
	}
}

func TestVerificationCancellationAndTransientRead(t *testing.T) {
	body := []byte("123456789")
	path, digest := testFile(t, body)
	key := "cancel.parquet"
	meta := integrityMeta()
	f := newFakeS3()
	sum := sha256.Sum256(body)
	f.objects[key] = fakeObject{body: body, meta: cloneMeta(meta), checksum: base64.StdEncoding.EncodeToString(sum[:])}
	u := testUploader(f, 1024, 3)
	f.faults = []fakeFault{{op: "GET", key: key, occurrence: 1, mode: transientError}}
	if _, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest); err == nil {
		t.Fatal("transient read accepted")
	}
	if got, err := u.UploadFileVerified(context.Background(), key, path, meta, int64(len(body)), digest); err != nil || !got.Skipped {
		t.Fatalf("verification retry did not reuse exact object: got=%+v err=%v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := u.UploadFileVerified(ctx, key, path, meta, int64(len(body)), digest); err == nil {
		t.Fatal("canceled verification accepted")
	} else if failure, ok := artifact.AsFailure(err); !ok || failure.Classification != artifact.FailureVerificationCanceled {
		t.Fatalf("cancel err=%v", err)
	}
}
