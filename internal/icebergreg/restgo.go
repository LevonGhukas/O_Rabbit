package icebergreg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	iceberg "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	restcatalog "github.com/apache/iceberg-go/catalog/rest"
	icetable "github.com/apache/iceberg-go/table"
)

func openRESTCatalog(ctx context.Context, req RunRequest, reg RunConfig, regS3 s3io.Config, table string) (*restcatalog.Catalog, icetable.Identifier, error) {
	cat, ident, err := newRESTCatalog(ctx, req.DatasetS3.Bucket, reg, regS3, table)
	if err != nil {
		return nil, nil, err
	}
	ns := ident[:len(ident)-1]
	if exists, err := cat.CheckNamespaceExists(ctx, ns); err != nil {
		return nil, nil, err
	} else if !exists {
		if err := cat.CreateNamespace(ctx, ns, iceberg.Properties{}); err != nil {
			return nil, nil, err
		}
	}
	return cat, ident, nil
}

// newRESTCatalog connects to the registration's REST catalog without changing
// it, and parses table into an identifier.
func newRESTCatalog(ctx context.Context, bucket string, reg RunConfig, regS3 s3io.Config, table string) (*restcatalog.Catalog, icetable.Identifier, error) {
	uri := normalizeLocalhost(reg.URI)
	if uri == "" {
		return nil, nil, fmt.Errorf("missing iceberg rest uri in persisted run registration config")
	}
	if parsed, err := url.Parse(uri); err == nil {
		parsed.Path = strings.TrimSuffix(strings.TrimSuffix(parsed.Path, "/v1"), "/")
		uri = parsed.String()
	}
	cat, err := restcatalog.NewCatalog(ctx, "rest", uri,
		restcatalog.WithOAuthToken(strings.TrimSpace(reg.BearerToken)),
		restcatalog.WithWarehouseLocation("s3://"+bucket),
		restcatalog.WithAdditionalProps(icebergRegistrationS3Props(regS3, reg.CredentialVending.Required)),
	)
	if err != nil {
		return nil, nil, err
	}
	parts := strings.Split(table, ".")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	if len(parts) < 2 {
		return nil, nil, fmt.Errorf("invalid iceberg table %q (expected namespace.table)", table)
	}
	return cat, icetable.Identifier(parts), nil
}

func normalizeLocalhost(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if strings.EqualFold(parsed.Hostname(), "localhost") {
		parsed.Host = strings.Replace(parsed.Host, "localhost", "127.0.0.1", 1)
		return parsed.String()
	}
	return raw
}

func icebergRegistrationS3Props(regS3 s3io.Config, credentialVending bool) iceberg.Properties {
	props := iceberg.Properties{}
	if ep := strings.TrimSuffix(strings.TrimSpace(regS3.Endpoint), "/"); ep != "" {
		props["s3.endpoint"] = normalizeLocalhost(ep)
	}
	if region := strings.TrimSpace(regS3.Region); region != "" {
		props["s3.region"] = region
	}
	if !credentialVending {
		if accessKey := strings.TrimSpace(regS3.AccessKeyID); accessKey != "" {
			props["s3.access-key-id"] = accessKey
		}
		if secretKey := strings.TrimSpace(regS3.SecretAccessKey); secretKey != "" {
			props["s3.secret-access-key"] = secretKey
		}
	}
	if regS3.ForcePathStyle {
		props["s3.force-virtual-addressing"] = "false"
	} else {
		props["s3.force-virtual-addressing"] = "true"
	}
	return props
}

func prepareRESTGoTable(ctx context.Context, log *slog.Logger, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, basePrefix string) (*icetable.Table, error) {
	if strings.TrimSpace(os.Getenv("AWS_EC2_METADATA_DISABLED")) == "" {
		_ = os.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	}
	cat, ident, err := openRESTCatalog(ctx, req, reg, regS3, table)
	if err != nil {
		return nil, err
	}
	return loadOrCreateRESTGoTable(ctx, log, cat, ident, req, reg, table, basePrefix)
}

// isFullRefreshRequest reports whether the run replaces the table's rows
// instead of appending to them.
func isFullRefreshRequest(req RunRequest) bool {
	return !req.Incremental || strings.EqualFold(req.WriteMode, "overwrite")
}

// objectURIs returns the S3 URIs of the run's verified objects.
func objectURIs(bucket string, objs []icebergObj) []string {
	out := make([]string, 0, len(objs))
	for _, obj := range objs {
		out = append(out, fmt.Sprintf("s3://%s/%s", bucket, obj.key))
	}
	return out
}

func runRESTGoRegister(ctx context.Context, log *slog.Logger, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, basePrefix string, objs []icebergObj) error {
	tbl, err := prepareRESTGoTable(ctx, log, req, reg, table, regS3, basePrefix)
	if err != nil {
		return err
	}
	return commitRESTGoFiles(ctx, log, tbl, req, reg, table, objectURIs(req.DatasetS3.Bucket, objs))
}

// commitRESTGoFiles registers newFiles in tbl with one transaction, so the
// catalog publishes them in a single snapshot marked with the operation
// identity. A full refresh replaces every live data file in that snapshot:
// readers see the previous rows or the new rows, never an empty table, and a
// failed commit leaves the previous snapshot current.
func commitRESTGoFiles(ctx context.Context, log *slog.Logger, tbl *icetable.Table, req RunRequest, reg RunConfig, table string, newFiles []string) error {
	isFullRefresh := isFullRefreshRequest(req)

	log.Info("iceberg registration files",
		slog.String("run_id", req.RunID),
		slog.String("table", table),
		slog.Int("new_objects", len(newFiles)),
		slog.Bool("full_refresh", isFullRefresh),
	)

	tx := tbl.NewTransaction()
	currentSpec := tbl.Spec()
	if err := applyPartitionSpec(tx, &currentSpec, reg.PartitionSpec, tbl.Schema()); err != nil {
		return err
	}
	if err := tx.SetProperties(tableOptionProperties(reg)); err != nil {
		return err
	}
	if err := applyMetadataRetention(tx, reg.MetadataRetention); err != nil {
		return err
	}
	identity := OperationIdentity{RegistrationID: req.RegistrationID, RunID: req.RunID, CommitID: req.CommitID, ArtifactSetDigest: req.ArtifactSetDigest, ManifestKey: req.ManifestKey}
	snapshotProperties := iceberg.Properties(identity.Properties())
	if err := applyUpsertDelete(ctx, tx, tbl, reg, isFullRefresh, newFiles, snapshotProperties); err != nil {
		return err
	}
	var existing []iceberg.DataFile
	if isFullRefresh {
		// A full refresh atomically replaces every live data file with the new
		// files in one snapshot (true snapshot overwrite).
		var err error
		existing, err = currentDataFiles(ctx, tbl)
		if err != nil {
			return fmt.Errorf("iceberg full refresh: %w", err)
		}

		log.Info("iceberg registration full refresh",
			slog.String("run_id", req.RunID),
			slog.String("table", table),
			slog.Int("files_to_delete", len(existing)),
			slog.Int("files_to_add", len(newFiles)),
		)
	}
	partitionedTable := !currentSpec.IsUnpartitioned() || len(reg.PartitionSpec) > 0
	if partitionedTable {
		// Workers wrote one partition tuple per file, so the verified files are
		// registered as they are; the master never reads or rewrites their rows.
		if err := registerPartitionedFiles(ctx, tx, reg, table, existing, isFullRefresh, newFiles, snapshotProperties); err != nil {
			return err
		}
	} else if err := registerUnpartitionedFiles(ctx, tx, existing, isFullRefresh, newFiles, snapshotProperties); err != nil {
		return err
	}
	_, err := tx.Commit(ctx)
	return err
}

type restGoFileMutation interface {
	AddFiles(context.Context, []string, iceberg.Properties, bool) error
	ReplaceDataFiles(context.Context, []string, []string, iceberg.Properties) error
}

func applyRESTGoFileMutation(ctx context.Context, tx restGoFileMutation, fullRefresh bool, existingFiles, newFiles []string, properties iceberg.Properties) error {
	if fullRefresh {
		if err := tx.ReplaceDataFiles(ctx, existingFiles, newFiles, properties); err != nil {
			return fmt.Errorf("iceberg full refresh replace: %w", err)
		}
		return nil
	}
	if err := tx.AddFiles(ctx, newFiles, properties, false); err != nil {
		return fmt.Errorf("iceberg incremental append: %w", err)
	}
	return nil
}

func loadOrCreateRESTGoTable(ctx context.Context, log *slog.Logger, cat *restcatalog.Catalog, ident icetable.Identifier, req RunRequest, reg RunConfig, table, basePrefix string) (*icetable.Table, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err == nil {
		if reg.Upsert.Enabled && tbl.Metadata().Version() < 2 {
			return nil, fmt.Errorf("upsert requires Iceberg format version 2 or newer")
		}
		var sourceSchema *iceberg.Schema
		if reg.SchemaEvolution == "additive" {
			sourceSchema, err = inferRunIcebergSchema(ctx, req, table)
			if err != nil {
				return nil, err
			}
		}
		schemaTx := tbl.NewTransaction()
		if err := applySchemaOptions(schemaTx, tbl.Schema(), sourceSchema, reg); err != nil {
			return nil, err
		}
		tbl, err = schemaTx.Commit(ctx)
		if err != nil {
			return nil, err
		}
		return applySortOrder(ctx, cat, ident, tbl, reg.SortOrder)
	}
	// Checked before ErrNoSuchTable: a catalog may report missing metadata
	// as 404, which iceberg-go also maps to ErrNoSuchTable, but the table
	// still exists. Dropping and recreating it would erase its snapshots and
	// properties, so the table is left as it is for an operator.
	tableLoc := restGoTableLocation(req.DatasetS3.Bucket, basePrefix)
	if isBrokenRESTMetadataErr(err, tableLoc) {
		log.Warn("iceberg registration cannot read table metadata; table left unchanged",
			slog.String("run_id", req.RunID),
			slog.String("table", table),
			slog.String("location", tableLoc),
		)
		return nil, failure.NewFailure(failure.FailureTableMetadataUnreadable, false, true, fmt.Errorf(
			"iceberg table %s exists but its metadata cannot be read: %w; the table was left unchanged. "+
				"Check that the metadata files under %s/metadata/ exist and are readable (restore them from a backup if needed), "+
				"then retry with POST /api/v1/runs/%s/registration/retry", table, err, tableLoc, req.RunID))
	}
	if errors.Is(err, icecatalog.ErrNoSuchTable) {
		return createRESTGoTable(ctx, cat, ident, req, reg, basePrefix)
	}
	return nil, classifyTableLoadError(table, err)
}

// classifyTableLoadError marks a failed table load as a definite rejection:
// nothing was written, so the registration can safely run again. Without it,
// a load that fails after the external-commit boundary would be reported as an
// ambiguous commit. Catalog outages and timeouts are retried automatically.
func classifyTableLoadError(table string, err error) error {
	err = fmt.Errorf("load iceberg table %s: %w", table, err)
	var ne net.Error
	var ue *url.Error
	switch {
	case errors.Is(err, context.Canceled):
		return err
	case errors.Is(err, context.DeadlineExceeded):
		return failure.NewFailure(failure.FailureCatalogTimeout, true, true, err)
	case errors.Is(err, restcatalog.ErrServiceUnavailable), errors.Is(err, restcatalog.ErrServerError), errors.As(err, &ne), errors.As(err, &ue):
		return failure.NewFailure(failure.FailureCatalogUnavailable, true, true, err)
	case errors.Is(err, restcatalog.ErrUnauthorized), errors.Is(err, restcatalog.ErrAuthorizationExpired):
		return failure.NewFailure(failure.FailureAuthentication, false, true, err)
	case errors.Is(err, restcatalog.ErrForbidden):
		return failure.NewFailure(failure.FailureAuthorization, false, true, err)
	default:
		return failure.NewFailure(failure.FailureUnknownPermanent, false, true, err)
	}
}

func createRESTGoTable(ctx context.Context, cat *restcatalog.Catalog, ident icetable.Identifier, req RunRequest, reg RunConfig, basePrefix string) (*icetable.Table, error) {
	iceSchema, err := inferRunIcebergSchema(ctx, req, strings.Join(ident, "."))
	if err != nil {
		return nil, err
	}

	if reg.Upsert.Enabled {
		iceSchema, err = schemaWithIdentifierFields(iceSchema, reg.Upsert.Keys)
		if err != nil {
			return nil, err
		}
	}
	spec, err := buildPartitionSpec(iceSchema, reg.PartitionSpec)
	if err != nil {
		return nil, err
	}
	order, err := buildSortOrder(iceSchema, reg.SortOrder, icetable.InitialSortOrderID)
	if err != nil {
		return nil, err
	}
	props := tableOptionProperties(reg)
	props["format-version"] = "2"
	loc := restGoTableLocation(req.DatasetS3.Bucket, basePrefix)
	return cat.CreateTable(ctx, ident, iceSchema,
		icecatalog.WithLocation(loc),
		icecatalog.WithPartitionSpec(&spec),
		icecatalog.WithSortOrder(order),
		icecatalog.WithProperties(props),
	)
}

func restGoTableLocation(bucket, basePrefix string) string {
	return fmt.Sprintf("s3://%s/%s", strings.TrimSpace(bucket), strings.Trim(strings.TrimSpace(basePrefix), "/"))
}

func isBrokenRESTMetadataErr(err error, tableLoc string) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if !strings.Contains(msg, "location does not exist:") || !strings.Contains(msg, "/metadata/") {
		return false
	}
	tableLoc = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(tableLoc), "/"))
	if tableLoc == "" {
		return false
	}
	return strings.Contains(msg, tableLoc+"/metadata/")
}
