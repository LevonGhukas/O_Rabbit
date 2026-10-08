package icebergreg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	iceberg "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	restcatalog "github.com/apache/iceberg-go/catalog/rest"
	icetable "github.com/apache/iceberg-go/table"
)

func openRESTCatalog(ctx context.Context, req RunRequest, reg RunConfig, regS3 s3io.Config, table string) (*restcatalog.Catalog, icetable.Identifier, error) {
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
		restcatalog.WithWarehouseLocation("s3://"+req.DatasetS3.Bucket),
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
	ident := icetable.Identifier(parts)
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

// dropAndRecreateCatalogTable drops the Iceberg table via the REST catalog and
// immediately recreates it empty. Used during Full Refresh for the ice engine
// so that the subsequent ice insert appends into a clean snapshot.
func dropAndRecreateCatalogTable(ctx context.Context, log *slog.Logger, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, basePrefix string) error {
	cat, ident, err := openRESTCatalog(ctx, req, reg, regS3, table)
	if err != nil {
		return err
	}
	if dropErr := cat.DropTable(ctx, ident); dropErr != nil && !errors.Is(dropErr, icecatalog.ErrNoSuchTable) {
		return fmt.Errorf("drop iceberg table %s: %w", table, dropErr)
	}
	log.Info("iceberg full refresh: table dropped, recreating",
		slog.String("run_id", req.RunID),
		slog.String("table", table),
	)
	_, err = createRESTGoTable(ctx, cat, ident, req, reg, basePrefix)
	return err
}

func runRESTGoRegister(ctx context.Context, log *slog.Logger, req RunRequest, reg RunConfig, table string, regS3 s3io.Config, basePrefix string, objs []icebergObj) error {
	tbl, err := prepareRESTGoTable(ctx, log, req, reg, table, regS3, basePrefix)
	if err != nil {
		return err
	}

	newFiles := make([]string, 0, len(objs))
	for _, obj := range objs {
		newFiles = append(newFiles, fmt.Sprintf("s3://%s/%s", req.DatasetS3.Bucket, obj.key))
	}

	isFullRefresh := !req.Incremental || strings.EqualFold(req.WriteMode, "overwrite")

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
	if reg.Upsert.Enabled && !isFullRefresh && tbl.CurrentSnapshot() != nil {
		filter, err := buildUpsertDeleteFilter(ctx, tbl, newFiles, reg.Upsert.Keys)
		if err != nil {
			return err
		}
		if !filter.Equals(iceberg.AlwaysFalse{}) {
			if err := tx.Delete(ctx, filter, snapshotProperties); err != nil {
				return fmt.Errorf("iceberg upsert delete existing rows: %w", err)
			}
		}
	}
	var existingFiles []string
	if isFullRefresh {
		// Collect all existing data files from the current snapshot so we can
		// atomically replace them with the new files (true snapshot overwrite).
		if snap := tbl.CurrentSnapshot(); snap != nil {
			fs, fsErr := tbl.FS(ctx)
			if fsErr != nil {
				return fmt.Errorf("iceberg full refresh: open table fs: %w", fsErr)
			}
			manifests, mErr := snap.Manifests(fs)
			if mErr != nil {
				return fmt.Errorf("iceberg full refresh: list manifests: %w", mErr)
			}
			for _, mf := range manifests {
				entries, eErr := mf.FetchEntries(fs, true)
				if eErr != nil {
					return fmt.Errorf("iceberg full refresh: fetch manifest entries: %w", eErr)
				}
				for _, e := range entries {
					existingFiles = append(existingFiles, e.DataFile().FilePath())
				}
			}
		}

		log.Info("iceberg registration full refresh",
			slog.String("run_id", req.RunID),
			slog.String("table", table),
			slog.Int("files_to_delete", len(existingFiles)),
			slog.Int("files_to_add", len(newFiles)),
		)
	}
	partitionedTable := !currentSpec.IsUnpartitioned() || len(reg.PartitionSpec) > 0
	if partitionedTable {
		fs, err := tbl.FS(ctx)
		if err != nil {
			return err
		}
		reader, err := newParquetRecordReader(ctx, fs, newFiles)
		if err != nil {
			return err
		}
		defer reader.Release()
		if isFullRefresh {
			if err := tx.Overwrite(ctx, reader, snapshotProperties); err != nil {
				return fmt.Errorf("iceberg partitioned full refresh: %w", err)
			}
		} else if err := tx.Append(ctx, reader, snapshotProperties); err != nil {
			return fmt.Errorf("iceberg partitioned append: %w", err)
		}
	} else if err := applyRESTGoFileMutation(ctx, tx, isFullRefresh, existingFiles, newFiles, snapshotProperties); err != nil {
		return err
	}
	_, err = tx.Commit(ctx)
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
	if errors.Is(err, icecatalog.ErrNoSuchTable) {
		return createRESTGoTable(ctx, cat, ident, req, reg, basePrefix)
	}

	tableLoc := restGoTableLocation(req.DatasetS3.Bucket, basePrefix)
	if !isBrokenRESTMetadataErr(err, tableLoc) {
		return nil, err
	}

	log.Warn("iceberg registration found broken table metadata; recreating table",
		slog.String("run_id", req.RunID),
		slog.String("table", table),
		slog.String("location", tableLoc),
	)
	if dropErr := cat.DropTable(ctx, ident); dropErr != nil && !errors.Is(dropErr, icecatalog.ErrNoSuchTable) {
		return nil, fmt.Errorf("drop broken iceberg table %s: %w", table, dropErr)
	}
	return createRESTGoTable(ctx, cat, ident, req, reg, basePrefix)
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
