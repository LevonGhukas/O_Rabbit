package connectors

import (
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

func TestParseCassandraDSN(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		want    cassandraDSN
		wantErr bool
	}{
		{
			name: "basic",
			dsn:  "cassandra://localhost/my_keyspace",
			want: cassandraDSN{
				hosts:          []string{"localhost"},
				keyspace:       "my_keyspace",
				consistency:    gocql.LocalQuorum,
				timeout:        30 * time.Second,
				connectTimeout: 5 * time.Second,
			},
		},
		{
			name: "multiple hosts with port",
			dsn:  "cassandra://host1:9042,host2:9042,host3/keyspace123",
			want: cassandraDSN{
				hosts:          []string{"host1:9042", "host2:9042", "host3"},
				keyspace:       "keyspace123",
				consistency:    gocql.LocalQuorum,
				timeout:        30 * time.Second,
				connectTimeout: 5 * time.Second,
			},
		},
		{
			name: "with auth",
			dsn:  "cassandra://user:pass@host/ks",
			want: cassandraDSN{
				hosts:          []string{"host"},
				keyspace:       "ks",
				username:       "user",
				password:       "pass",
				consistency:    gocql.LocalQuorum,
				timeout:        30 * time.Second,
				connectTimeout: 5 * time.Second,
			},
		},
		{
			name: "with queries",
			dsn:  "cassandra://host/ks?consistency=one&timeout=10s&connect_timeout=2s",
			want: cassandraDSN{
				hosts:          []string{"host"},
				keyspace:       "ks",
				consistency:    gocql.One,
				timeout:        10 * time.Second,
				connectTimeout: 2 * time.Second,
			},
		},
		{
			name:    "empty",
			dsn:     "",
			wantErr: true,
		},
		{
			name:    "no scheme",
			dsn:     "host/ks",
			wantErr: true,
		},
		{
			name:    "no keyspace",
			dsn:     "cassandra://host",
			wantErr: true,
		},
		{
			name:    "invalid consistency",
			dsn:     "cassandra://host/ks?consistency=invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCassandraDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseCassandraDSN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseCassandraDSN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQuoteCassandraIdent(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"normal", "my_table", `"my_table"`, false},
		{"uppercase", "Table1", `"Table1"`, false},
		{"already quoted", `"my_table"`, `"my_table"`, false},
		{"spaces", "my table", `"my table"`, false},
		{"embedded quote", `O"Rayelly`, `"O""Rayelly"`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := quoteCassandraIdent(tt.in)
			if (err != nil) != tt.wantErr {
				t.Errorf("quoteCassandraIdent(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("quoteCassandraIdent(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestSplitCassandraTableIdent(t *testing.T) {
	tests := []struct {
		in           string
		defaultKS    string
		wantKeyspace string
		wantTable    string
		wantErr      bool
	}{
		{"my_table", "def_ks", "def_ks", "my_table", false},
		{"my_ks.my_table", "def_ks", "my_ks", "my_table", false},
		{`"my_ks"."my_table"`, "def_ks", "my_ks", "my_table", false},
		{"invalid.ks.table", "def_ks", "", "", true},
		{"bad table", "def_ks", "def_ks", "bad table", false},
		{"my_ks.bad table", "def_ks", "my_ks", "bad table", false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			k, tb, err := splitCassandraTableIdent(tt.defaultKS, tt.in)
			if (err != nil) != tt.wantErr {
				t.Errorf("split(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
				return
			}
			if k != tt.wantKeyspace || tb != tt.wantTable {
				t.Errorf("split(%q) = (%q, %q), want (%q, %q)", tt.in, k, tb, tt.wantKeyspace, tt.wantTable)
			}
		})
	}
}

func TestClassifyCassandraCursorType(t *testing.T) {
	tests := []struct {
		cqlType    string
		wantDomain CursorDomain
		wantOrder  bool
		wantRange  bool
	}{
		{"int", CursorDomainInt64, true, true},
		{"bigint", CursorDomainInt64, true, true},
		{"varint", CursorDomainInt64, true, true},
		{"timestamp", CursorDomainTimestamp, true, true},
		{"date", CursorDomainDate, true, true},
		{"text", CursorDomainString, true, false},
		{"varchar", CursorDomainString, true, false},
		{"uuid", CursorDomainUUID, true, false},
		{"timeuuid", CursorDomainUUID, true, false},
		{"decimal", CursorDomainDecimal, true, false},
		{"list<text>", CursorDomainUnknown, false, false},
		{"frozen<map<int, text>>", CursorDomainUnknown, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.cqlType, func(t *testing.T) {
			got := classifyCassandraCursorType(tt.cqlType)
			if got.Domain != tt.wantDomain || got.Orderable != tt.wantOrder || got.RangeCapable != tt.wantRange {
				t.Errorf("classify(%q) = %+v, want Domain=%v Orderable=%v RangeCapable=%v",
					tt.cqlType, got, tt.wantDomain, tt.wantOrder, tt.wantRange)
			}
		})
	}
}
