package connectors

import "testing"

func TestQuoteClickHouseMultipartIdent(t *testing.T) {
	got, err := quoteClickHouseMultipartIdent("analytics.events")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "`analytics`.`events`" {
		t.Fatalf("unexpected quoted ident: %q", got)
	}

	got, err = quoteClickHouseMultipartIdent("analytics.`event-log`")
	if err != nil || got != "`analytics`.`event-log`" {
		t.Fatalf("quoted unusual identifier = %q, %v", got, err)
	}
}

func TestSplitClickHouseTableIdent(t *testing.T) {
	tests := []struct {
		in        string
		wantDB    string
		wantTable string
	}{
		{in: "analytics.events", wantDB: "analytics", wantTable: "events"},
		{in: "`analytics`.`events`", wantDB: "analytics", wantTable: "events"},
		{in: "events", wantDB: "", wantTable: "events"},
	}
	for _, tc := range tests {
		db, table := splitClickHouseTableIdent(tc.in)
		if db != tc.wantDB || table != tc.wantTable {
			t.Fatalf("splitClickHouseTableIdent(%q)=(%q,%q), want (%q,%q)", tc.in, db, table, tc.wantDB, tc.wantTable)
		}
	}
}

func TestClickHouseDSNIsLocal(t *testing.T) {
	local := []string{
		"clickhouse://localhost:19000/default?username=myuser&password=mypassword",
		"clickhouse://127.0.0.1:19000/default",
		"http://localhost:18123/?username=myuser&password=mypassword",
	}
	for _, dsn := range local {
		if !clickhouseDSNIsLocal(dsn) {
			t.Fatalf("expected local dsn: %q", dsn)
		}
	}

	remote := []string{
		"clickhouse://clickhouse.internal:9000/default?username=myuser&password=mypassword",
		"https://clickhouse.internal:8443/?username=myuser&password=mypassword",
	}
	for _, dsn := range remote {
		if clickhouseDSNIsLocal(dsn) {
			t.Fatalf("expected non-local dsn: %q", dsn)
		}
	}
}

func TestClickHouseSortingKeyMentionsColumn(t *testing.T) {
	if !clickhouseSortingKeyMentionsColumn("tuple(event_date, user_id)", "user_id") {
		t.Fatalf("expected sorting key to mention user_id")
	}
	if clickhouseSortingKeyMentionsColumn("tuple(event_date, session_id)", "user_id") {
		t.Fatalf("did not expect sorting key to mention user_id")
	}
}
