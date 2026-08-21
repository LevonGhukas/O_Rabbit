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
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type Config struct {
	Endpoint       string
	Region         string
	Bucket         string
	ForcePathStyle bool

	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type objectClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	ListMultipartUploads(context.Context, *s3.ListMultipartUploadsInput, ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error)
}

type ChecksumCapability int

const (
	ChecksumAuto ChecksumCapability = iota
	ProviderSHA256Supported
	ProviderChecksumUnsupported
)

type Uploader struct {
	client             objectClient
	cfg                Config
	capMu              sync.RWMutex
	checksumCapability ChecksumCapability
	smallPutThreshold  int64
	partSize           int64
	maxConcurrency     int
}

type MultipartEvent struct {
	Event, ObjectKey, ProviderUploadID, SHA256, ErrorClass, ErrorMessage string
	FileIndex                                                            int
	Size                                                                 int64
}

type MultipartObserver func(context.Context, MultipartEvent) error

func (u *Uploader) checksumMode() ChecksumCapability {
	u.capMu.RLock()
	defer u.capMu.RUnlock()
	return u.checksumCapability
}
func (u *Uploader) markChecksumUnsupported() {
	u.capMu.Lock()
	u.checksumCapability = ProviderChecksumUnsupported
	u.capMu.Unlock()
}

func New(ctx context.Context, cfg Config) (*Uploader, error) {
	if strings.TrimSpace(cfg.Region) == "" {
		cfg.Region = "us-east-1"
	}
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, fmt.Errorf("missing bucket")
	}

	creds := credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)

	awsCfg := aws.Config{
		Region:      cfg.Region,
		Credentials: aws.NewCredentialsCache(creds),
		HTTPClient:  awshttp.NewBuildableClient(),
	}

	if strings.TrimSpace(cfg.Endpoint) != "" {
		awsCfg.BaseEndpoint = aws.String(cfg.Endpoint)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
	})

	return &Uploader{client: client, cfg: cfg, checksumCapability: ChecksumAuto, smallPutThreshold: 32 * 1024 * 1024, partSize: 16 * 1024 * 1024, maxConcurrency: 4}, nil
}

type UploadResult struct {
	ETag               string
	Bytes              int64
	Skipped            bool
	VerificationMethod string
}

func digestBase64(hexDigest string) (string, error) {
	b, err := hex.DecodeString(hexDigest)
	if err != nil || len(b) != sha256.Size {
		return "", fmt.Errorf("invalid SHA-256 digest")
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (u *Uploader) OpenObject(ctx context.Context, key string) (io.ReadCloser, bool, error) {
	out, err := u.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(u.cfg.Bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return out.Body, true, nil
}

func (u *Uploader) VerifyObject(ctx context.Context, key string, expectedSize int64, expectedSHA256 string, expectedMeta map[string]string) error {
	_, err := u.verifyObject(ctx, key, expectedSize, expectedSHA256, expectedMeta)
	return err
}

func (u *Uploader) verifyObject(ctx context.Context, key string, expectedSize int64, expectedSHA256 string, expectedMeta map[string]string) (string, error) {
	head, err := u.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(u.cfg.Bucket), Key: aws.String(key), ChecksumMode: types.ChecksumModeEnabled})
	if err != nil && isChecksumUnsupported(err) {
		u.markChecksumUnsupported()
		head, err = u.Head(ctx, key)
	}
	if err != nil {
		return "", &artifact.Failure{Classification: artifact.FailureVerificationUnavailable, Retryable: true, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: err}
	}
	if aws.ToInt64(head.ContentLength) != expectedSize {
		return "", &artifact.Failure{Classification: artifact.FailureRemoteSizeMismatch, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: fmt.Errorf("got=%d expected=%d", aws.ToInt64(head.ContentLength), expectedSize)}
	}
	if !metaMatches(head.Metadata, expectedMeta) {
		return "", &artifact.Failure{Classification: artifact.FailureRemoteMetadataMismatch, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: fmt.Errorf("metadata mismatch")}
	}
	method := artifact.VerificationPortable
	if checksum := strings.TrimSpace(aws.ToString(head.ChecksumSHA256)); checksum != "" && !strings.Contains(checksum, "-") {
		expected, encErr := digestBase64(expectedSHA256)
		if encErr != nil {
			return "", encErr
		}
		if checksum != expected {
			return "", &artifact.Failure{Classification: artifact.FailureRemoteChecksumMismatch, ObjectKey: key, VerificationMethod: artifact.VerificationProvider, Err: fmt.Errorf("provider SHA-256 mismatch")}
		}
		method = artifact.VerificationProvider
	}
	body, found, err := u.OpenObject(ctx, key)
	if err != nil {
		class, retryable := artifact.FailureVerificationUnavailable, true
		if ctx.Err() != nil {
			class, retryable = artifact.FailureVerificationCanceled, false
		}
		return "", &artifact.Failure{Classification: class, Retryable: retryable, ObjectKey: key, Err: err}
	}
	if !found {
		return "", &artifact.Failure{Classification: artifact.FailureVerificationUnavailable, Retryable: true, ObjectKey: key, Err: fmt.Errorf("remote artifact missing")}
	}
	defer body.Close()
	if err := artifact.VerifyStream(ctx, body, expectedSize, expectedSHA256); err != nil {
		class := artifact.FailureRemoteChecksumMismatch
		if ctx.Err() != nil {
			class = artifact.FailureVerificationCanceled
		}
		return "", &artifact.Failure{Classification: class, ObjectKey: key, VerificationMethod: method, Err: err}
	}
	return method, nil
}

func (u *Uploader) UploadFileVerified(ctx context.Context, key, path string, meta map[string]string, expectedSize int64, expectedSHA256 string) (UploadResult, error) {
	return u.UploadFileVerifiedTracked(ctx, key, path, meta, expectedSize, expectedSHA256, 0, nil)
}

func (u *Uploader) UploadFileVerifiedTracked(ctx context.Context, key, path string, meta map[string]string, expectedSize int64, expectedSHA256 string, fileIndex int, observer MultipartObserver) (UploadResult, error) {
	if head, err := u.Head(ctx, key); err == nil {
		if !metaMatches(head.Metadata, meta) {
			return UploadResult{}, &artifact.Failure{Classification: artifact.FailureExistingObjectConflict, ObjectKey: key, Err: fmt.Errorf("metadata mismatch")}
		}
		method, verifyErr := u.verifyObject(ctx, key, expectedSize, expectedSHA256, meta)
		if verifyErr != nil {
			if failure, ok := artifact.AsFailure(verifyErr); ok {
				switch failure.Classification {
				case artifact.FailureRemoteSizeMismatch, artifact.FailureRemoteChecksumMismatch, artifact.FailureRemoteMetadataMismatch:
					failure.Classification = artifact.FailureExistingObjectConflict
				}
			}
			return UploadResult{}, verifyErr
		}
		return UploadResult{ETag: aws.ToString(head.ETag), Bytes: expectedSize, Skipped: true, VerificationMethod: method}, nil
	} else if !isNotFound(err) {
		return UploadResult{}, err
	}
	result, err := u.uploadFile(ctx, key, path, meta, expectedSHA256, fileIndex, observer)
	if err != nil {
		return UploadResult{}, err
	}
	method, verifyErr := u.verifyObject(ctx, key, expectedSize, expectedSHA256, meta)
	if verifyErr != nil {
		return UploadResult{}, verifyErr
	}
	result.VerificationMethod = method
	return result, nil
}

func (u *Uploader) Head(ctx context.Context, key string) (*s3.HeadObjectOutput, error) {
	out, err := u.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(u.cfg.Bucket), Key: aws.String(key)})
	return out, err
}

func (u *Uploader) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	p := strings.TrimSpace(prefix)
	var out []string
	var token *string
	for {
		in := &s3.ListObjectsV2Input{Bucket: aws.String(u.cfg.Bucket), Prefix: aws.String(p)}
		if token != nil {
			in.ContinuationToken = token
		}
		resp, err := u.client.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, c := range resp.Contents {
			out = append(out, aws.ToString(c.Key))
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
		if token == nil || aws.ToString(token) == "" {
			break
		}
	}
	return out, nil
}

type MultipartUploadInfo struct {
	Key, UploadID string
	Initiated     time.Time
}

func (u *Uploader) ListManagedMultipartUploads(ctx context.Context, prefix string, limit int) ([]MultipartUploadInfo, error) {
	if strings.TrimSpace(prefix) == "" {
		return nil, errors.New("managed multipart prefix is required")
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var out []MultipartUploadInfo
	var keyMarker, uploadMarker *string
	for len(out) < limit {
		resp, err := u.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(u.cfg.Bucket), Prefix: aws.String(prefix), KeyMarker: keyMarker, UploadIdMarker: uploadMarker, MaxUploads: aws.Int32(int32(limit - len(out)))})
		if err != nil {
			return nil, err
		}
		for _, item := range resp.Uploads {
			out = append(out, MultipartUploadInfo{Key: aws.ToString(item.Key), UploadID: aws.ToString(item.UploadId), Initiated: aws.ToTime(item.Initiated)})
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		keyMarker, uploadMarker = resp.NextKeyMarker, resp.NextUploadIdMarker
		if keyMarker == nil {
			break
		}
	}
	return out, nil
}

func (u *Uploader) MultipartUploadExists(ctx context.Context, prefix, key, uploadID string) (bool, error) {
	items, err := u.ListManagedMultipartUploads(ctx, prefix, 100)
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if item.Key == key && item.UploadID == uploadID {
			return true, nil
		}
	}
	return false, nil
}

func (u *Uploader) AbortTrackedMultipart(ctx context.Context, key, uploadID string) error {
	if strings.TrimSpace(key) == "" || strings.TrimSpace(uploadID) == "" {
		return errors.New("tracked key and upload id are required")
	}
	_, err := u.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(u.cfg.Bucket), Key: aws.String(key), UploadId: aws.String(uploadID)})
	return err
}

func (u *Uploader) VerifyTrackedFinalObject(ctx context.Context, key string, size int64, sha string, meta map[string]string) (bool, error) {
	if _, err := u.Head(ctx, key); err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	_, err := u.verifyObject(ctx, key, size, sha, meta)
	return true, err
}

type ExactObjectObservation struct {
	Exists, Matches     bool
	Identity, VersionID string
}

// ObserveExactObject verifies the immutable artifact identity before destructive
// cleanup. A missing object is a successful observation, never an authorization
// to delete some neighboring key.
func (u *Uploader) ObserveExactObject(ctx context.Context, key string, size int64, sha string, meta map[string]string) (ExactObjectObservation, error) {
	head, err := u.Head(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return ExactObjectObservation{}, nil
		}
		return ExactObjectObservation{}, err
	}
	_, verifyErr := u.verifyObject(ctx, key, size, sha, meta)
	versionID := aws.ToString(head.VersionId)
	identityBytes := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s\x00%s\x00%s", key, size, sha, versionID, aws.ToString(head.ETag))))
	observation := ExactObjectObservation{
		Exists:    true,
		Matches:   verifyErr == nil,
		Identity:  hex.EncodeToString(identityBytes[:]),
		VersionID: versionID,
	}
	return observation, verifyErr
}

// DeleteExactObject deletes exactly one key, and pins the provider version when
// one was observed. It never performs prefix or recursive deletion.
func (u *Uploader) DeleteExactObject(ctx context.Context, key, versionID string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("object key is required")
	}
	in := &s3.DeleteObjectInput{Bucket: aws.String(u.cfg.Bucket), Key: aws.String(key)}
	if strings.TrimSpace(versionID) != "" {
		in.VersionId = aws.String(versionID)
	}
	_, err := u.client.DeleteObject(ctx, in)
	return err
}

func (u *Uploader) UploadFileMultipart(ctx context.Context, key string, path string, meta map[string]string) (UploadResult, error) {
	return u.uploadFile(ctx, key, path, meta, "", 0, nil)
}

func (u *Uploader) uploadFile(ctx context.Context, key string, path string, meta map[string]string, expectedSHA256 string, fileIndex int, observer MultipartObserver) (UploadResult, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return UploadResult{}, err
	}
	size := fi.Size()
	if size < 0 {
		return UploadResult{}, fmt.Errorf("invalid file size")
	}

	// If object exists with matching metadata, skip.
	if head, err := u.Head(ctx, key); err == nil {
		if metaMatches(head.Metadata, meta) {
			return UploadResult{ETag: aws.ToString(head.ETag), Bytes: size, Skipped: true}, nil
		}
		return UploadResult{}, fmt.Errorf("s3 key already exists with different metadata: %s", key)
	} else {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			code := strings.ToLower(strings.TrimSpace(apiErr.ErrorCode()))
			if code != "notfound" && code != "nosuchkey" && code != "" {
				return UploadResult{}, err
			}
		} else {
			return UploadResult{}, err
		}
	}

	f, err := os.Open(path)
	if err != nil {
		return UploadResult{}, err
	}
	defer f.Close()

	// Fast path: small objects are typically faster as a single PutObject.
	threshold := u.smallPutThreshold
	if threshold <= 0 {
		threshold = 32 * 1024 * 1024
	}
	if size <= threshold {
		in := &s3.PutObjectInput{
			Bucket:        aws.String(u.cfg.Bucket),
			Key:           aws.String(key),
			Body:          f,
			ContentLength: aws.Int64(size),
			Metadata:      meta,
		}
		if expectedSHA256 != "" && u.checksumMode() != ProviderChecksumUnsupported {
			checksum, checksumErr := digestBase64(expectedSHA256)
			if checksumErr != nil {
				return UploadResult{}, checksumErr
			}
			in.ChecksumAlgorithm, in.ChecksumSHA256 = types.ChecksumAlgorithmSha256, aws.String(checksum)
		}
		out, err := u.client.PutObject(ctx, in)
		if err != nil && in.ChecksumSHA256 != nil && isChecksumUnsupported(err) {
			u.markChecksumUnsupported()
			if _, seekErr := f.Seek(0, io.SeekStart); seekErr != nil {
				return UploadResult{}, seekErr
			}
			in.Body, in.ChecksumSHA256, in.ChecksumAlgorithm = f, nil, ""
			out, err = u.client.PutObject(ctx, in)
		}
		if err != nil {
			return UploadResult{}, &artifact.Failure{Classification: artifact.FailureUploadFailed, Retryable: true, Ambiguous: true, ReconciliationOK: true, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: err}
		}
		return UploadResult{ETag: aws.ToString(out.ETag), Bytes: size, Skipped: false}, nil
	}
	notify := func(event MultipartEvent) error {
		if observer == nil {
			return nil
		}
		event.ObjectKey, event.FileIndex, event.Size, event.SHA256 = key, fileIndex, size, expectedSHA256
		return observer(ctx, event)
	}
	if err := notify(MultipartEvent{Event: "PREPARED"}); err != nil {
		return UploadResult{}, &artifact.Failure{Classification: artifact.FailureMultipartIntentFailed, Retryable: true, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: err}
	}

	createIn := &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(u.cfg.Bucket),
		Key:      aws.String(key),
		Metadata: meta,
	}
	if expectedSHA256 != "" && u.checksumMode() != ProviderChecksumUnsupported {
		createIn.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
	}
	createOut, err := u.client.CreateMultipartUpload(ctx, createIn)
	if err != nil && createIn.ChecksumAlgorithm != "" && isChecksumUnsupported(err) {
		u.markChecksumUnsupported()
		createIn.ChecksumAlgorithm = ""
		createOut, err = u.client.CreateMultipartUpload(ctx, createIn)
	}
	if err != nil {
		return UploadResult{}, &artifact.Failure{Classification: artifact.FailureUploadFailed, Retryable: true, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: err}
	}

	uploadID := aws.ToString(createOut.UploadId)
	if err := notify(MultipartEvent{Event: "CREATED", ProviderUploadID: uploadID}); err != nil {
		_, _ = u.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: aws.String(u.cfg.Bucket), Key: aws.String(key), UploadId: aws.String(uploadID)})
		return UploadResult{}, &artifact.Failure{Classification: artifact.FailureMultipartTrackingFailed, Retryable: true, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: err}
	}
	abort := func() error {
		_, abortErr := u.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(u.cfg.Bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
		return abortErr
	}

	// Choose part size and concurrency adaptively.
	// - Larger parts reduce overhead for big objects.
	// - Keep parts <= 10,000 (S3 limit).
	const (
		minPartSize      int64 = 5 * 1024 * 1024 // 5 MiB (S3 minimum, except last)
		defaultPartSize1 int64 = 16 * 1024 * 1024
		defaultPartSize2 int64 = 32 * 1024 * 1024
		defaultPartSize3 int64 = 64 * 1024 * 1024
		maxParts               = 10_000
	)

	customPartSize := u.partSize > 0
	partSize := u.partSize
	if partSize <= 0 {
		partSize = defaultPartSize1
	}
	switch {
	case size > 512*1024*1024:
		partSize = defaultPartSize3
	case size > 64*1024*1024:
		partSize = defaultPartSize2
	}

	// Ensure we don't exceed the max parts limit.
	// partSize >= ceil(size/maxParts)
	need := (size + maxParts - 1) / maxParts
	if need > partSize {
		partSize = need
	}
	if partSize < minPartSize && !customPartSize {
		partSize = minPartSize
	}

	partsCount := int((size + partSize - 1) / partSize)
	if partsCount < 1 {
		partsCount = 1
	}

	// NOTE: This uploader reads from a local temp file. Aggressive parallel part uploads can
	// create random access disk reads (pread) and overwhelm local MinIO/Docker setups.
	// Keep defaults conservative; allow higher concurrency via env/config if needed.
	conc := u.maxConcurrency
	if conc <= 0 {
		conc = runtime.NumCPU()
	}
	if conc < 1 {
		conc = 1
	}
	if conc > 4 {
		conc = 4
	}
	if partsCount < conc {
		conc = partsCount
	}

	parts := make([]types.CompletedPart, partsCount)
	g, gctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, conc)

	for i := 0; i < partsCount; i++ {
		i := i
		offset := int64(i) * partSize
		n := partSize
		if offset+n > size {
			n = size - offset
		}
		partNo := int32(i + 1)

		// Gate both read + upload to bound memory usage. Reading in-order avoids heavy random-access reads
		// from disk when multiple parts are uploaded concurrently.
		select {
		case sem <- struct{}{}:
		case <-gctx.Done():
			// errgroup cancels gctx when a worker returns an error. Wait for
			// that worker before returning so its upload error is not replaced
			// by the cancellation used to stop sibling work.
			err := g.Wait()
			_ = abort()
			if err != nil {
				return UploadResult{}, err
			}
			return UploadResult{}, gctx.Err()
		}

		buf := make([]byte, int(n))
		if _, err := io.ReadFull(io.NewSectionReader(f, offset, n), buf); err != nil {
			<-sem
			_ = abort()
			return UploadResult{}, err
		}

		g.Go(func() error {
			defer func() { <-sem }()

			body := bytes.NewReader(buf)
			partIn := &s3.UploadPartInput{
				Bucket:        aws.String(u.cfg.Bucket),
				Key:           aws.String(key),
				UploadId:      aws.String(uploadID),
				PartNumber:    aws.Int32(partNo),
				Body:          body,
				ContentLength: aws.Int64(int64(len(buf))),
			}
			if expectedSHA256 != "" && u.checksumMode() != ProviderChecksumUnsupported {
				sum := sha256.Sum256(buf)
				partIn.ChecksumAlgorithm, partIn.ChecksumSHA256 = types.ChecksumAlgorithmSha256, aws.String(base64.StdEncoding.EncodeToString(sum[:]))
			}
			upOut, err := u.client.UploadPart(gctx, partIn)
			if err != nil {
				return &artifact.Failure{Classification: artifact.FailureMultipartPartFailed, Retryable: true, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: err}
			}
			parts[i] = types.CompletedPart{ETag: upOut.ETag, PartNumber: aws.Int32(partNo), ChecksumSHA256: upOut.ChecksumSHA256}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		_ = notify(MultipartEvent{Event: "ABORT_PENDING", ProviderUploadID: uploadID, ErrorClass: string(artifact.FailureMultipartPartFailed), ErrorMessage: err.Error()})
		if abortErr := abort(); abortErr != nil {
			return UploadResult{}, fmt.Errorf("%w; abort multipart: %v", err, abortErr)
		}
		if expectedSHA256 != "" && u.checksumMode() != ProviderChecksumUnsupported && isChecksumUnsupported(err) {
			u.markChecksumUnsupported()
			return u.uploadFile(ctx, key, path, meta, expectedSHA256, fileIndex, observer)
		}
		return UploadResult{}, err
	}

	if err := notify(MultipartEvent{Event: "COMPLETING", ProviderUploadID: uploadID}); err != nil {
		return UploadResult{}, &artifact.Failure{Classification: artifact.FailureMultipartTrackingFailed, Retryable: true, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: err}
	}
	compOut, err := u.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.cfg.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		_ = notify(MultipartEvent{Event: "COMPLETION_AMBIGUOUS", ProviderUploadID: uploadID, ErrorClass: string(artifact.FailureMultipartCompleteAmbiguous), ErrorMessage: err.Error()})
		return UploadResult{}, &artifact.Failure{Classification: artifact.FailureMultipartCompleteAmbiguous, Retryable: true, Ambiguous: true, ReconciliationOK: true, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: err}
	}
	if err := notify(MultipartEvent{Event: "COMPLETED", ProviderUploadID: uploadID}); err != nil {
		return UploadResult{}, &artifact.Failure{Classification: artifact.FailureMultipartTrackingFailed, Retryable: true, Ambiguous: true, ReconciliationOK: true, ObjectKey: key, VerificationMethod: artifact.VerificationPortable, Err: err}
	}

	return UploadResult{ETag: aws.ToString(compOut.ETag), Bytes: size, Skipped: false}, nil
}

func metaMatches(got map[string]string, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	for k, v := range want {
		gv, ok := got[strings.ToLower(k)]
		if !ok {
			// AWS SDK returns metadata keys in their original case for some providers.
			gv, ok = got[k]
		}
		if !ok || gv != v {
			return false
		}
	}
	return true
}

func isChecksumUnsupported(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(apiErr.ErrorCode())) {
	case "notimplemented", "unsupported", "invalidrequest", "invalidargument":
		return true
	default:
		return false
	}
}

func (u *Uploader) PutObjectBytes(ctx context.Context, key string, b []byte, contentType string, meta map[string]string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("missing key")
	}
	ct := strings.TrimSpace(contentType)
	if ct == "" {
		ct = "application/octet-stream"
	}
	body := bytes.NewReader(b)
	_, err := u.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(u.cfg.Bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(int64(len(b))),
		ContentType:   aws.String(ct),
		Metadata:      meta,
	})
	return err
}

func (u *Uploader) GetObjectBytes(ctx context.Context, key string) ([]byte, bool, error) {
	if strings.TrimSpace(key) == "" {
		return nil, false, fmt.Errorf("missing key")
	}
	out, err := u.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(u.cfg.Bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// DeleteObjects bulk-deletes a list of S3 object keys from the configured bucket.
// Keys are processed in batches of 1000 (the S3 API limit per request).
// It returns the total count of successfully deleted objects.
// Individual key-level errors are ignored (e.g. key already missing); only
// request-level transport errors are propagated.
func (u *Uploader) DeleteObjects(ctx context.Context, keys []string) (int, error) {
	const batchSize = 1000
	deleted := 0
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]
		objs := make([]types.ObjectIdentifier, len(batch))
		for j, k := range batch {
			k := k // capture
			objs[j] = types.ObjectIdentifier{Key: aws.String(k)}
		}
		out, err := u.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(u.cfg.Bucket),
			Delete: &types.Delete{
				Objects: objs,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return deleted, err
		}
		// Quiet mode suppresses individual deletion confirmations.
		// Any key-level errors surfaced in Errors are best-effort; count only what the API
		// reports as deleted (total batch minus errors).
		deleted += len(batch) - len(out.Errors)
	}
	return deleted, nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := strings.ToLower(strings.TrimSpace(apiErr.ErrorCode()))
		return code == "nosuchkey" || code == "notfound" || code == "404"
	}
	return strings.Contains(strings.ToLower(err.Error()), "nosuchkey") || strings.Contains(strings.ToLower(err.Error()), "not found")
}
