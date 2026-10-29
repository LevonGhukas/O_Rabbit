package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	icetable "github.com/apache/iceberg-go/table"
)

// catalogReader reads tables as a downstream engine would: table metadata
// from the REST catalog, then manifests and data files from object storage.
// It talks to the catalog directly, never through the fault proxy.
type catalogReader struct {
	catalogURL string
	// s3Props point at MinIO's host port. They replace any storage config
	// the catalog returns, which names the in-network minio:9000 endpoint.
	s3Props map[string]string
	http    *http.Client
}

// tableView is one loaded version of a table.
type tableView struct {
	metadataLocation string
	tbl              *icetable.Table
}

// exportedRow is one source row as read back from Iceberg.
type exportedRow struct {
	ID        int64
	Name      string
	CreatedAt time.Time
}

func (c *catalogReader) load(ctx context.Context, namespace, name string) (tableView, error) {
	url := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s", c.catalogURL, namespace, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return tableView{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return tableView{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return tableView{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return tableView{}, fmt.Errorf("load table %s.%s: %s: %s", namespace, name, resp.Status, strings.TrimSpace(string(body)))
	}
	var out struct {
		MetadataLocation string          `json:"metadata-location"`
		Metadata         json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return tableView{}, fmt.Errorf("decode load-table response: %w", err)
	}
	meta, err := icetable.ParseMetadataBytes(out.Metadata)
	if err != nil {
		return tableView{}, fmt.Errorf("parse table metadata: %w", err)
	}
	fsF := func(ctx context.Context) (iceio.IO, error) {
		return iceio.LoadFS(ctx, c.s3Props, out.MetadataLocation)
	}
	tbl := icetable.New(icetable.Identifier{namespace, name}, meta, out.MetadataLocation, fsF, nil)
	return tableView{metadataLocation: out.MetadataLocation, tbl: tbl}, nil
}

// rows reads every live row of the current snapshot, sorted by id.
func (v tableView) rows(ctx context.Context) ([]exportedRow, error) {
	tbl, err := v.tbl.Scan(icetable.WithSelectedFields("id", "name", "created_at")).ToArrowTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan table: %w", err)
	}
	defer tbl.Release()

	out := make([]exportedRow, 0, tbl.NumRows())
	reader := array.NewTableReader(tbl, 4096)
	defer reader.Release()
	for reader.Next() {
		rec := reader.RecordBatch()
		ids, ok := rec.Column(0).(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("id is %s, want int64", rec.Column(0).DataType())
		}
		names, ok := rec.Column(1).(*array.String)
		if !ok {
			return nil, fmt.Errorf("name is %s, want string", rec.Column(1).DataType())
		}
		created, ok := rec.Column(2).(*array.Timestamp)
		if !ok {
			return nil, fmt.Errorf("created_at is %s, want timestamp", rec.Column(2).DataType())
		}
		unit := created.DataType().(*arrow.TimestampType).Unit
		for i := 0; i < int(rec.NumRows()); i++ {
			out = append(out, exportedRow{
				ID:        ids.Value(i),
				Name:      names.Value(i),
				CreatedAt: created.Value(i).ToTime(unit).UTC(),
			})
		}
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("read table: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// dataFile is one live data file of the current snapshot.
type dataFile struct {
	path    string
	records int64
}

// dataFiles lists the live data files of the current snapshot. The test
// tables never have delete files, so a scan task with one is an error.
func (v tableView) dataFiles(ctx context.Context) ([]dataFile, error) {
	tasks, err := v.tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("plan files: %w", err)
	}
	out := make([]dataFile, 0, len(tasks))
	for _, task := range tasks {
		if len(task.DeleteFiles) > 0 {
			return nil, fmt.Errorf("data file %s has %d delete files", task.File.FilePath(), len(task.DeleteFiles))
		}
		out = append(out, dataFile{path: task.File.FilePath(), records: task.File.Count()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, nil
}

// snapshotsByRun counts the snapshots each O_Rabbit run committed, using the
// run ID O_Rabbit writes into every snapshot summary.
func (v tableView) snapshotsByRun() map[string]int {
	out := map[string]int{}
	for _, snap := range v.tbl.Metadata().Snapshots() {
		runID := ""
		if snap.Summary != nil {
			runID = snap.Summary.Properties["orabbit.run_id"]
		}
		out[runID]++
	}
	return out
}

func (v tableView) currentRunID() string {
	snap := v.tbl.CurrentSnapshot()
	if snap == nil || snap.Summary == nil {
		return ""
	}
	return snap.Summary.Properties["orabbit.run_id"]
}
