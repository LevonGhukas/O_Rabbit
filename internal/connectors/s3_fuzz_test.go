package connectors

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// fuzzMaxFileBytes bounds one fuzzed S3 object. The readers stream, so a
// larger body only repeats work the fuzzer already covers.
const fuzzMaxFileBytes = 64 << 10

// fuzzReadDeadline fails an input whose records do not drain in time: a
// reader that stops consuming input would otherwise spin forever.
const fuzzReadDeadline = 10 * time.Second

func FuzzS3CSVReader(f *testing.F) {
	for _, seed := range []string{
		"", "a,b\n1,2\n", "a,b\n1\n1,2,3\n", "\"a\"\"b\",c\n\"x\ny\",z\n",
		"a\n\"unterminated", "ü,✓\n€,\x00\n", "a,b\r\n1,2\r\n",
		strings.Repeat(",", 4096), "a\n" + strings.Repeat("\"", 4096),
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzMaxFileBytes {
			t.Skip()
		}
		it, err := newS3CSVIterator(bytes.NewReader(data))
		if err != nil {
			return
		}
		drainWithDeadline(t, it, len(data))
		if fields := it.FieldOrder(); len(fields) == 0 {
			t.Fatalf("csv iterator without a header row for %q", data)
		}
	})
}

func FuzzS3JSONReader(f *testing.F) {
	for _, seed := range []struct{ data, path string }{
		{"", ""},
		{`[{"a":1},{"b":"x"}]`, ""},
		{`{"data":{"rows":[{"a":1}]}}`, "/data/rows"},
		{`{"a/b":{"c~d":[{"x":1}]}}`, "a~1b/c~0d"},
		{`{"skip":[[{"z":[1,{"y":[]}]}]],"rows":[{"a":1}]}`, "rows"},
		{`[1, "x", null]`, ""},
		{`{"rows":[{"a":1}`, "rows"},
		{`{"rows":{}}`, "rows"},
		{`[{"ü":"✓","big":123456789012345678901234567890}]`, ""},
		{strings.Repeat("[", 4096), ""},
		{`{"k":` + strings.Repeat(`{"k":`, 4096), "x"},
		{`[{"a":1}]`, "~2"},
	} {
		f.Add([]byte(seed.data), seed.path)
	}
	f.Fuzz(func(t *testing.T, data []byte, recordPath string) {
		if len(data) > fuzzMaxFileBytes || len(recordPath) > fuzzMaxFileBytes {
			t.Skip()
		}
		drainWithDeadline(t, newS3JSONIterator(bytes.NewReader(data), recordPath), len(data))
	})
}

func FuzzS3XMLReader(f *testing.F) {
	for _, seed := range []string{
		"", "<rows><row><a>1</a></row></rows>",
		`<rows><row id="1" x="y"><a k="v">t</a><b/></row></rows>`,
		"<rows><row><a>1</b></row></rows>", "<rows><row>",
		"<?xml version=\"1.0\"?><!DOCTYPE r [<!ENTITY e \"x\">]><r><row>&e;</row></r>",
		"<r><row><a><![CDATA[<x>]]></a></row></r>", "<r><ü>✓</ü></r>",
		strings.Repeat("<a>", 4096),
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzMaxFileBytes {
			t.Skip()
		}
		drainWithDeadline(t, newS3XMLIterator(bytes.NewReader(data)), len(data))
	})
}

// FuzzParseJSONRecordPath checks that every accepted record_path round-trips:
// re-escaping its segments ("~" as "~0", "/" as "~1") parses to the same path.
func FuzzParseJSONRecordPath(f *testing.F) {
	for _, seed := range []string{"", "/", "a", "/a/b", "a~0b/c~1d", "~", "~2", "a//b", " /x ", "ü/✓"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > fuzzMaxSQLBytes {
			t.Skip()
		}
		path, err := parseJSONRecordPath(raw)
		if err != nil || len(path) == 0 {
			return
		}
		escaped := make([]string, len(path))
		for i, part := range path {
			escaped[i] = strings.NewReplacer("~", "~0", "/", "~1").Replace(part)
		}
		again, err := parseJSONRecordPath("/" + strings.Join(escaped, "/"))
		if err != nil || !reflect.DeepEqual(again, path) {
			t.Fatalf("record_path %q parsed to %q, re-escaped to %q (%v)", raw, path, again, err)
		}
	})
}

func TestS3JSONReaderRejectsDeepNestingWithoutCrashing(t *testing.T) {
	const depth = 1_000_000
	data := `{"skip":` + strings.Repeat("[", depth) + strings.Repeat("]", depth) + `,"rows":[{"a":1}]}`
	it := newS3JSONIterator(strings.NewReader(data), "rows")
	if it.Next(context.Background()) {
		t.Fatal("deeply nested JSON was accepted")
	}
	if !errors.Is(it.Err(), errJSONTooDeep) {
		t.Fatalf("err=%v, want %v", it.Err(), errJSONTooDeep)
	}
}

// drainWithDeadline reads every record. Each record consumes at least one
// input byte, so more than inputLen+1 records means the reader is stuck.
func drainWithDeadline(t *testing.T, it DocumentIterator, inputLen int) {
	t.Helper()
	done := make(chan string, 1)
	go func() {
		defer it.Close()
		ctx := context.Background()
		for n := 0; it.Next(ctx); n++ {
			if n > inputLen {
				done <- "iterator produced more records than input bytes"
				return
			}
			if _, err := it.Decode(); err != nil {
				break
			}
		}
		_ = it.Err()
		done <- ""
	}()
	select {
	case msg := <-done:
		if msg != "" {
			t.Fatal(msg)
		}
	case <-time.After(fuzzReadDeadline):
		t.Fatalf("iterator did not finish within %s", fuzzReadDeadline)
	}
}
