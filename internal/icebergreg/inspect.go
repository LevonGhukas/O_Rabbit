package icebergreg

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	icecatalog "github.com/apache/iceberg-go/catalog"
	restcatalog "github.com/apache/iceberg-go/catalog/rest"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
)

type InspectionRequest struct {
	Registration  RunConfig
	Table         string
	DatasetBucket string
	DatasetPrefix string
	DatasetS3     s3io.Config
}

func (m *Manager) InspectCatalog(ctx context.Context, req InspectionRequest) (CatalogObservation, error) {
	reg := req.Registration.Normalize()
	if reg.Engine != "rest-go" && reg.Engine != "ice" {
		return CatalogObservation{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("unsupported inspector backend %q", reg.Engine))
	}
	uri := strings.TrimSuffix(strings.TrimSuffix(reg.URI, "/v1"), "/")
	if uri == "" {
		return CatalogObservation{}, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("missing catalog uri"))
	}
	cfg := req.DatasetS3
	if reg.S3.Endpoint != "" {
		cfg.Endpoint = reg.S3.Endpoint
	}
	if reg.S3.Region != "" {
		cfg.Region = reg.S3.Region
	}
	if reg.S3.AccessKeyID != "" {
		cfg.AccessKeyID = reg.S3.AccessKeyID
	}
	if reg.S3.SecretAccessKey != "" {
		cfg.SecretAccessKey = reg.S3.SecretAccessKey
	}
	cfg.ForcePathStyle = reg.S3.PathStyleAccess
	cat, err := restcatalog.NewCatalog(ctx, "rest", uri, restcatalog.WithOAuthToken(reg.BearerToken), restcatalog.WithWarehouseLocation("s3://"+req.DatasetBucket), restcatalog.WithAdditionalProps(icebergRegistrationS3Props(cfg)))
	if err != nil {
		return CatalogObservation{}, err
	}
	parts := strings.Split(req.Table, ".")
	ident := icetable.Identifier(parts)
	tbl, err := cat.LoadTable(ctx, ident)
	if errors.Is(err, icecatalog.ErrNoSuchTable) {
		_, confirmErr := cat.LoadTable(ctx, ident)
		if !errors.Is(confirmErr, icecatalog.ErrNoSuchTable) {
			if confirmErr == nil {
				return CatalogObservation{Backend: "rest-go", TableIdentifier: req.Table, MetadataStart: "TABLE_NOT_FOUND", MetadataEnd: "CHANGED"}, nil
			}
			return CatalogObservation{}, confirmErr
		}
		return CatalogObservation{Backend: "rest-go", TableExists: false, TableIdentifier: req.Table, MetadataStart: "TABLE_NOT_FOUND", MetadataEnd: "TABLE_NOT_FOUND", CompleteHistory: true, SchemaCompatible: true, LocationCompatible: true}, nil
	}
	if err != nil {
		return CatalogObservation{}, err
	}
	obs := CatalogObservation{Backend: "rest-go", TableExists: true, TableIdentifier: req.Table, MetadataStart: tbl.MetadataLocation(), CurrentSnapshotID: "", CompleteHistory: false, SchemaCompatible: true, LocationCompatible: strings.TrimSuffix(tbl.Location(), "/") == strings.TrimSuffix("s3://"+req.DatasetBucket+"/"+req.DatasetPrefix, "/")}
	if cur := tbl.CurrentSnapshot(); cur != nil {
		obs.CurrentSnapshotID = strconv.FormatInt(cur.SnapshotID, 10)
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return obs, err
	}
	for _, snap := range tbl.Metadata().Snapshots() {
		se := SnapshotEvidence{ID: strconv.FormatInt(snap.SnapshotID, 10), Summary: map[string]string{}}
		if snap.Summary != nil {
			for k, v := range snap.Summary.Properties {
				se.Summary[k] = v
			}
		}
		manifests, e := snap.Manifests(fs)
		if e != nil {
			return obs, e
		}
		for _, manifest := range manifests {
			entries, e := manifest.FetchEntries(fs, false)
			if e != nil {
				return obs, e
			}
			for _, entry := range entries {
				df := entry.DataFile()
				size, records := df.FileSizeBytes(), df.Count()
				se.Files = append(se.Files, ObservedFile{Path: df.FilePath(), Size: &size, Records: &records, SnapshotID: se.ID, Status: fmt.Sprint(entry.Status())})
			}
		}
		obs.Snapshots = append(obs.Snapshots, se)
	}
	fresh, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return obs, err
	}
	return finalizeCatalogObservation(obs, fresh.MetadataLocation()), nil
}

func finalizeCatalogObservation(obs CatalogObservation, metadataEnd string) CatalogObservation {
	obs.MetadataEnd = metadataEnd
	obs.CompleteHistory = obs.TableExists &&
		obs.MetadataStart != "" &&
		obs.MetadataStart == obs.MetadataEnd
	return obs
}
