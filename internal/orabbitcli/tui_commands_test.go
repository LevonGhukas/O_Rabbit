package orabbitcli

import (
	"errors"
	"reflect"
	"testing"
)

func TestSplitTUIArgs(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"spaces only", "   \t ", nil},
		{"plain words", "/watch run-1", []string{"/watch", "run-1"}},
		{"collapses whitespace", "  a \t  b  ", []string{"a", "b"}},
		{"double quotes", `/recover r1 --reason "stale commit"`, []string{"/recover", "r1", "--reason", "stale commit"}},
		{"single quotes keep backslash", `'a\b c'`, []string{`a\b c`}},
		{"escaped space", `a\ b`, []string{"a b"}},
		{"empty quoted arg", `--reason ""`, []string{"--reason", ""}},
		{"quote inside word", `--reason="two words"`, []string{"--reason=two words"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitTUIArgs(tt.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("splitTUIArgs(%q) = %#v, want %#v", tt.in, got, tt.want)
			}
		})
	}
}

func TestSplitTUIArgsUnterminated(t *testing.T) {
	for _, in := range []string{`"open`, `'open`, `trailing\`} {
		if _, err := splitTUIArgs(in); !errors.Is(err, errUnterminatedQuote) {
			t.Fatalf("splitTUIArgs(%q) error = %v, want errUnterminatedQuote", in, err)
		}
	}
}

func TestParseTUIInput(t *testing.T) {
	name, args, err := parseTUIInput("  /WATCH run-1 ")
	if err != nil || name != "watch" || !reflect.DeepEqual(args, []string{"run-1"}) {
		t.Fatalf("got name=%q args=%v err=%v", name, args, err)
	}
	// The slash is optional.
	name, _, _ = parseTUIInput("status")
	if name != "status" {
		t.Fatalf("name without slash = %q, want status", name)
	}
	name, args, err = parseTUIInput("")
	if name != "" || args != nil || err != nil {
		t.Fatalf("empty input: name=%q args=%v err=%v", name, args, err)
	}
}

func TestLookupTUICommandAliases(t *testing.T) {
	for in, want := range map[string]string{"quit": "exit", "q": "exit", "?": "help", "RUNS": "runs"} {
		c, ok := lookupTUICommand(in)
		if !ok || c.Name != want {
			t.Fatalf("lookupTUICommand(%q) = %q, %v; want %q", in, c.Name, ok, want)
		}
	}
	if _, ok := lookupTUICommand("nope"); ok {
		t.Fatal("unknown command resolved")
	}
}

func TestSuggestTUICommand(t *testing.T) {
	for in, want := range map[string]string{"wach": "watch", "stauts": "status", "cancle": "cancel", "exti": "exit"} {
		got, ok := suggestTUICommand(in)
		if !ok || got != want {
			t.Errorf("suggestTUICommand(%q) = %q, %v; want %q", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "completely-different"} {
		if got, ok := suggestTUICommand(in); ok {
			t.Errorf("suggestTUICommand(%q) = %q, want no suggestion", in, got)
		}
	}
}

func TestLevenshtein(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"abc", "", 3},
		{"", "abc", 3},
		{"watch", "watch", 0},
		{"wach", "watch", 1},
		{"kitten", "sitting", 3},
	}
	for _, tt := range tests {
		if got := levenshtein(tt.a, tt.b); got != tt.want {
			t.Errorf("levenshtein(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestHasFlag(t *testing.T) {
	tests := []struct {
		args []string
		want bool
	}{
		{[]string{"--master-http", "x"}, true},
		{[]string{"-master-http=x"}, true},
		{[]string{"--master-http-extra"}, false},
		{[]string{"master-http"}, false}, // a positional value, not a flag
		{nil, false},
	}
	for _, tt := range tests {
		if got := hasFlag(tt.args, "master-http"); got != tt.want {
			t.Errorf("hasFlag(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}
