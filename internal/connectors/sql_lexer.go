package connectors

import (
	"errors"
	"strings"
)

// sqlDialect is one engine family's lexical reading of SQL text: which quotes
// take backslash escapes, which comment forms exist, and which other literal
// forms can hide a ';' or a comment. The validators do not know the source
// engine, so they accept text only when every dialect that can lex it sees the
// same harmless shape: one engine's quoting must not hide a statement or a
// comment that another engine executes.
type sqlDialect struct {
	name               string
	backslashQuotes    string // quote characters whose contents take \ escapes
	backticks          bool   // `ident` quoting (MySQL, ClickHouse)
	hashComments       bool   // '#' starts a line comment (MySQL, ClickHouse)
	slashComments      bool   // '//' starts a line comment (Cassandra)
	dashNeedsSpace     bool   // '--' is a comment only before whitespace (MySQL)
	nestedComments     bool   // block comments nest (Postgres, SQL Server)
	executableComments bool   // '/*!...*/' runs as SQL (MySQL, MariaDB)
	dollarQuotes       bool   // $tag$...$tag$ literals (Postgres, Cassandra)
	escapeStrings      bool   // E'...' takes \ escapes (Postgres)
	bracketIdents      bool   // [ident] with ']]' escape (SQL Server)
	qQuotes            bool   // q'[...]' literals (Oracle)
}

var sqlDialects = []sqlDialect{
	{name: "ansi"},
	{name: "postgres", nestedComments: true, dollarQuotes: true, escapeStrings: true},
	{name: "mysql", backslashQuotes: `'"`, backticks: true, hashComments: true, dashNeedsSpace: true, executableComments: true},
	{name: "mysql-ansi-quotes", backslashQuotes: `'`, backticks: true, hashComments: true, dashNeedsSpace: true, executableComments: true},
	{name: "mysql-no-backslash", backticks: true, hashComments: true, dashNeedsSpace: true, executableComments: true},
	{name: "clickhouse", backslashQuotes: "'\"`", backticks: true, hashComments: true},
	{name: "sqlserver", nestedComments: true, bracketIdents: true},
	{name: "oracle", qQuotes: true},
	{name: "cassandra", slashComments: true, dollarQuotes: true},
}

var (
	errSQLUnterminatedString     = errors.New("unterminated string literal")
	errSQLUnterminatedIdentifier = errors.New("unterminated quoted identifier")
	errSQLUnterminatedComment    = errors.New("unterminated block comment")
	errSQLUnterminatedDollar     = errors.New("unterminated dollar-quoted string")
	errSQLUnterminatedBracket    = errors.New("unterminated bracket identifier")
)

// sqlLexResult is what one dialect sees in the text, up to the end or the
// first lexing error.
type sqlLexResult struct {
	firstWord    string // first bare word outside literals and comments
	lastWord     string // last such word
	keyword      string // first data-changing keyword among those words, upper-cased
	statementEnd int    // offset of the first top-level ';', or -1
	trailing     bool   // something besides whitespace or comments follows statementEnd
	comments     bool   // the text contains a comment
	lineComment  bool   // the text ends inside a line comment
	unbalanced   bool   // a ')' or ']' closes nothing, or one is left open
	forbidden    string // a construct the dialect executes but the lexer cannot follow
}

type sqlLexer struct {
	s           string
	i           int
	d           sqlDialect
	parens      int
	brackets    int
	nextBracket int // offset of the next ']' at or after i, or len(s) when none is left
	res         sqlLexResult
}

func lexSQL(s string, d sqlDialect) (sqlLexResult, error) {
	l := &sqlLexer{s: s, d: d, nextBracket: -1, res: sqlLexResult{statementEnd: -1}}
	for l.i < len(s) {
		if err := l.step(); err != nil {
			return l.res, err
		}
	}
	if l.parens != 0 || l.brackets != 0 {
		l.res.unbalanced = true
	}
	return l.res, nil
}

func (l *sqlLexer) step() error {
	s, i := l.s, l.i
	switch c := s[i]; {
	case isSQLSpace(c):
		l.i++
		return nil
	case l.lineCommentAt(i):
		l.skipLineComment()
		return nil
	case strings.HasPrefix(s[i:], "/*"):
		return l.skipBlockComment()
	}
	if l.res.statementEnd >= 0 {
		l.res.trailing = true
	}
	switch c := s[i]; {
	case c == ';':
		if l.res.statementEnd < 0 {
			l.res.statementEnd = i
		}
		l.i++
	case c == '(':
		l.parens++
		l.i++
	case c == ')':
		l.parens--
		l.res.unbalanced = l.res.unbalanced || l.parens < 0
		l.i++
	case c == '\'':
		return l.skipQuoted(i+1, c, strings.IndexByte(l.d.backslashQuotes, c) >= 0, errSQLUnterminatedString)
	case c == '"' || (c == '`' && l.d.backticks):
		return l.skipQuoted(i+1, c, strings.IndexByte(l.d.backslashQuotes, c) >= 0, errSQLUnterminatedIdentifier)
	case c == '[' || c == ']':
		return l.bracket(c)
	case c == '$' && l.d.dollarQuotes:
		return l.dollar()
	case isSQLIdentStart(c):
		return l.word()
	default:
		l.i++
	}
	return nil
}

func (l *sqlLexer) lineCommentAt(i int) bool {
	s := l.s
	switch {
	case strings.HasPrefix(s[i:], "--"):
		return !l.d.dashNeedsSpace || i+2 >= len(s) || s[i+2] <= ' '
	case s[i] == '#':
		return l.d.hashComments
	case strings.HasPrefix(s[i:], "//"):
		return l.d.slashComments
	}
	return false
}

func (l *sqlLexer) skipLineComment() {
	l.res.comments = true
	end := strings.IndexAny(l.s[l.i:], "\r\n")
	if end < 0 {
		l.res.lineComment = true
		l.i = len(l.s)
		return
	}
	l.i += end
}

func (l *sqlLexer) skipBlockComment() error {
	l.res.comments = true
	if l.d.executableComments && (strings.HasPrefix(l.s[l.i:], "/*!") || strings.HasPrefix(l.s[l.i:], "/*M!")) {
		l.res.forbidden = "executable comments"
	}
	depth := 0
	for i := l.i; i+1 < len(l.s); i++ {
		switch {
		case l.s[i] == '/' && l.s[i+1] == '*' && (depth == 0 || l.d.nestedComments):
			depth++
			i++
		case l.s[i] == '*' && l.s[i+1] == '/':
			i++
			if depth--; depth == 0 {
				l.i = i + 1
				return nil
			}
		}
	}
	l.i = len(l.s)
	return errSQLUnterminatedComment
}

// skipQuoted moves past a literal whose body starts at i. A doubled quote is
// an escaped quote; with backslash, so is any character after '\'.
func (l *sqlLexer) skipQuoted(i int, quote byte, backslash bool, unterminated error) error {
	for i < len(l.s) {
		switch {
		case backslash && l.s[i] == '\\':
			i += 2
		case l.s[i] != quote:
			i++
		case i+1 < len(l.s) && l.s[i+1] == quote:
			i += 2
		default:
			l.i = i + 1
			return nil
		}
	}
	l.i = len(l.s)
	return unterminated
}

// bracket reads '[' and ']'. SQL Server quotes identifiers with them. Other
// dialects use them as subscripts; there a bracket holding only name
// characters is skipped whole so "[Update]" is not read as a keyword.
func (l *sqlLexer) bracket(c byte) error {
	if c == '[' && l.d.bracketIdents {
		for i := l.i + 1; i < len(l.s); i++ {
			if l.s[i] != ']' {
				continue
			}
			if i+1 < len(l.s) && l.s[i+1] == ']' {
				i++
				continue
			}
			l.i = i + 1
			return nil
		}
		l.i = len(l.s)
		return errSQLUnterminatedBracket
	}
	if c == '[' {
		if end := l.closingBracket(); end < len(l.s) && isPlainSQLName(l.s[l.i+1:end]) {
			l.i = end + 1
			return nil
		}
		l.brackets++
	} else {
		l.brackets--
		l.res.unbalanced = l.res.unbalanced || l.brackets < 0
	}
	l.i++
	return nil
}

// closingBracket returns the offset of the first ']' after i. The offset is
// cached so a run of '[' without a ']' is scanned once, not once per '['.
func (l *sqlLexer) closingBracket() int {
	if l.nextBracket <= l.i && l.nextBracket < len(l.s) {
		end := strings.IndexByte(l.s[l.i+1:], ']')
		if end < 0 {
			l.nextBracket = len(l.s)
		} else {
			l.nextBracket = l.i + 1 + end
		}
	}
	return l.nextBracket
}

// dollar reads '$': a positional parameter ($1) or a dollar-quoted literal.
func (l *sqlLexer) dollar() error {
	j := l.i + 1
	for j < len(l.s) && (isSQLIdentStart(l.s[j]) || l.s[j] >= 0x80 || (j > l.i+1 && l.s[j] >= '0' && l.s[j] <= '9')) {
		j++
	}
	if j >= len(l.s) || l.s[j] != '$' {
		l.i++
		return nil
	}
	delim := l.s[l.i : j+1]
	end := strings.Index(l.s[j+1:], delim)
	if end < 0 {
		l.i = len(l.s)
		return errSQLUnterminatedDollar
	}
	l.i = j + 1 + end + len(delim)
	return nil
}

// word reads a bare word, or the prefix of E'...' and q'[...]' literals.
func (l *sqlLexer) word() error {
	j := l.i + 1
	for j < len(l.s) && isSQLIdentPart(l.s[j]) {
		j++
	}
	word := l.s[l.i:j]
	if j < len(l.s) && l.s[j] == '\'' {
		switch {
		case l.d.escapeStrings && strings.EqualFold(word, "E"):
			return l.skipQuoted(j+1, '\'', true, errSQLUnterminatedString)
		case l.d.qQuotes && (strings.EqualFold(word, "Q") || strings.EqualFold(word, "NQ")):
			return l.qQuote(j + 1)
		}
	}
	// Subscript contents are expressions, never statements, and SQL Server
	// bracket names such as [Load-Date] reach here in other dialects.
	if l.brackets == 0 {
		if l.res.firstWord == "" {
			l.res.firstWord = word
		}
		l.res.lastWord = word
		if l.res.keyword == "" && destructiveSQLKeyword(word) {
			l.res.keyword = strings.ToUpper(word)
		}
	}
	l.i = j
	return nil
}

// qQuote reads Oracle's q'<open>...<close>' literal whose delimiter is at i.
func (l *sqlLexer) qQuote(i int) error {
	if i >= len(l.s) {
		l.i = len(l.s)
		return errSQLUnterminatedString
	}
	closing := l.s[i]
	if pos := strings.IndexByte("[{(<", closing); pos >= 0 {
		closing = "]})>"[pos]
	}
	end := strings.Index(l.s[i+1:], string(closing)+"'")
	if end < 0 {
		l.i = len(l.s)
		return errSQLUnterminatedString
	}
	l.i = i + 1 + end + 2
	return nil
}

// isPlainSQLName reports whether s holds only characters that no dialect
// reads as the start of a literal, comment or statement separator.
func isPlainSQLName(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; !isSQLIdentStart(c) && !(c >= '0' && c <= '9') && c != ' ' && c != '.' && c < 0x80 {
			return false
		}
	}
	return true
}

// sqlLexErrorIsSkippable reports whether a dialect that hit err can be left
// out of the safety checks because it would fail to parse the text. Appended
// cursor SQL quotes column names with '"', '`' or '[...]', so an unterminated
// quoted name could be closed by it and is never skippable.
func sqlLexErrorIsSkippable(err error) bool {
	return !errors.Is(err, errSQLUnterminatedIdentifier) && !errors.Is(err, errSQLUnterminatedBracket)
}

// sqlEndsInLineComment reports whether any dialect reads the end of query as
// inside a line comment, which would swallow text appended after it.
func sqlEndsInLineComment(query string) bool {
	for _, dialect := range sqlDialects {
		if res, _ := lexSQL(query, dialect); res.lineComment {
			return true
		}
	}
	return false
}
