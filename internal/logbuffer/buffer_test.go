package logbuffer

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func newLogger(capacity int, level slog.Level) (*slog.Logger, *Buffer, *bytes.Buffer) {
	var out bytes.Buffer
	buf := New(capacity)
	return slog.New(NewHandler(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: level}), buf)), buf, &out
}

func TestHandlerTeesAndMasks(t *testing.T) {
	log, buf, out := newLogger(10, slog.LevelInfo)
	log.With("component", "grpc").WithGroup("req").Info("connect postgres://u:pw@db/x", "token", "abc123", "rows", 5)
	log.Debug("hidden below level")

	if !strings.Contains(out.String(), "connect") {
		t.Fatal("record must still reach stdout")
	}
	items, total := buf.Query(Filter{})
	if total != 1 || len(items) != 1 {
		t.Fatalf("got %d items (total %d), want 1: debug must be filtered by the handler level", len(items), total)
	}
	e := items[0]
	if strings.Contains(e.Message, "pw") || e.Attrs["req.token"] != "***" {
		t.Fatalf("not masked: %+v", e)
	}
	if e.Attrs["component"] != "grpc" || e.Attrs["req.rows"] != int64(5) || e.Level != "INFO" {
		t.Fatalf("attrs/level wrong: %+v", e)
	}
}

func TestRingKeepsNewestAndPages(t *testing.T) {
	log, buf, _ := newLogger(3, slog.LevelDebug)
	for _, m := range []string{"one", "two", "three", "four", "five"} {
		log.Info(m)
	}
	items, total := buf.Query(Filter{})
	if total != 3 || items[0].Message != "five" || items[2].Message != "three" {
		t.Fatalf("ring must hold the newest 3, newest first: %+v", items)
	}
	older, _ := buf.Query(Filter{Before: items[0].Seq, Limit: 1})
	if len(older) != 1 || older[0].Message != "four" {
		t.Fatalf("older page: %+v", older)
	}
	tail, _ := buf.Query(Filter{After: items[2].Seq})
	if len(tail) != 2 || tail[0].Message != "four" || tail[1].Message != "five" {
		t.Fatalf("tail must be oldest first: %+v", tail)
	}
}

func TestFilters(t *testing.T) {
	log, buf, _ := newLogger(10, slog.LevelDebug)
	log.Warn("disk slow", "worker_id", "w1")
	log.Error("commit failed")
	log.Info("ok")
	if items, _ := buf.Query(Filter{Levels: []string{"warning", "error"}}); len(items) != 2 {
		t.Fatalf("levels: %+v", items)
	}
	if items, _ := buf.Query(Filter{Query: "W1"}); len(items) != 1 || items[0].Message != "disk slow" {
		t.Fatalf("query in attrs: %+v", items)
	}
	if items, _ := buf.Query(Filter{Since: time.Now().Add(time.Hour)}); len(items) != 0 {
		t.Fatalf("since: %+v", items)
	}
}

func TestSubscribeNeverBlocks(t *testing.T) {
	log, buf, _ := newLogger(10, slog.LevelDebug)
	ch, cancel := buf.Subscribe()
	defer cancel()
	for i := 0; i < 1000; i++ { // more than the channel holds
		log.Info("spam")
	}
	if e := <-ch; e.Message != "spam" {
		t.Fatalf("got %+v", e)
	}
}
