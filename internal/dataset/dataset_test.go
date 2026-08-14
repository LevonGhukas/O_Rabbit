package dataset

import "testing"

func TestTableNameIncludesAllSegments(t *testing.T) {
	got := TableName("[SalesDB].[dbo].[Orders]")
	want := "SalesDB__dbo__Orders"
	if got != want {
		t.Fatalf("TableName()=%q want=%q", got, want)
	}
}

func TestPrefixCanonicalizesOverride(t *testing.T) {
	got := Prefix(" /raw//orders/ ", "mssql", "dbo.Orders")
	want := "raw/orders"
	if got != want {
		t.Fatalf("Prefix()=%q want=%q", got, want)
	}
}

func TestStorageKeyDefaultsEndpoint(t *testing.T) {
	got := StorageKey("", "bucket1", "/mssql/orders/")
	want := "http://localhost:9000|bucket1|mssql/orders"
	if got != want {
		t.Fatalf("StorageKey()=%q want=%q", got, want)
	}
}

func TestIcebergTableFromStoragePrefixStripsFinalMetadataSegment(t *testing.T) {
	got, ok := IcebergTableFromStoragePrefix("postgres/public__demo_orders/metadata/")
	if !ok {
		t.Fatal("IcebergTableFromStoragePrefix() ok=false")
	}
	want := "postgres.public__demo_orders"
	if got != want {
		t.Fatalf("IcebergTableFromStoragePrefix()=%q want=%q", got, want)
	}
}

func TestIcebergTableFromStoragePrefixKeepsMetadataInsideTableName(t *testing.T) {
	tests := map[string]string{
		"postgres/public__metadata_orders/metadata": "postgres.public__metadata_orders",
		"postgres/metadata_orders":                  "postgres.metadata_orders",
		"postgres/metadata/public__demo_orders":     "postgres.metadata.public__demo_orders",
		"postgres/public__demo_orders/METADATA":     "postgres.public__demo_orders.METADATA",
	}
	for in, want := range tests {
		got, ok := IcebergTableFromStoragePrefix(in)
		if !ok {
			t.Fatalf("IcebergTableFromStoragePrefix(%q) ok=false", in)
		}
		if got != want {
			t.Fatalf("IcebergTableFromStoragePrefix(%q)=%q want=%q", in, got, want)
		}
	}
}

func TestIcebergTableFromStoragePrefixIgnoresMetadataFiles(t *testing.T) {
	if got, ok := IcebergTableFromStoragePrefix("postgres/public__demo_orders/metadata/00001.metadata.json"); ok {
		t.Fatalf("IcebergTableFromStoragePrefix()=%q, ok=true; want ok=false for metadata file", got)
	}
}
