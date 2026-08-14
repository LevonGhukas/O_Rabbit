package orabbitcli

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/term"
)

const (
	defaultWrapWidth = 80
	minWrapWidth     = 40
)

type textBlockWriter struct {
	w     io.Writer
	width int
}

func newTextBlockWriter(w io.Writer) textBlockWriter {
	return textBlockWriter{
		w:     w,
		width: wrapWidthForWriter(w),
	}
}

func wrapWidthForWriter(w io.Writer) int {
	if raw, ok := os.LookupEnv("COLUMNS"); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && n > 0 {
			return clampWrapWidth(n)
		}
	}
	if f, ok := w.(*os.File); ok {
		if width, _, err := term.GetSize(int(f.Fd())); err == nil && width > 0 {
			return clampWrapWidth(width)
		}
	}
	return defaultWrapWidth
}

func clampWrapWidth(width int) int {
	if width < minWrapWidth {
		return minWrapWidth
	}
	return width
}

func wrapText(text string, width int, firstIndent, restIndent string) []string {
	width = clampWrapWidth(width)
	paragraphs := strings.Split(text, "\n")
	lines := make([]string, 0, len(paragraphs))
	for i, paragraph := range paragraphs {
		if i > 0 {
			lines = append(lines, "")
		}
		words := strings.Fields(paragraph)
		if len(words) == 0 {
			continue
		}
		currentIndent := firstIndent
		current := currentIndent
		currentWidth := runeLen(current)
		indentWidth := currentWidth
		for _, word := range words {
			sep := ""
			if currentWidth > indentWidth {
				sep = " "
			}
			wordWidth := runeLen(word)
			if currentWidth+len(sep)+wordWidth <= width || currentWidth == indentWidth {
				current += sep + word
				currentWidth += len(sep) + wordWidth
				continue
			}
			lines = append(lines, current)
			currentIndent = restIndent
			current = currentIndent + word
			currentWidth = runeLen(current)
			indentWidth = runeLen(currentIndent)
		}
		lines = append(lines, current)
	}
	return lines
}

func runeLen(s string) int {
	return utf8.RuneCountInString(s)
}

func writeWrapped(w io.Writer, width int, firstIndent, restIndent, text string) {
	for _, line := range wrapText(text, width, firstIndent, restIndent) {
		fmt.Fprintln(w, line)
	}
}

func (tw textBlockWriter) blank() {
	fmt.Fprintln(tw.w)
}

func (tw textBlockWriter) title(text string) {
	writeWrapped(tw.w, tw.width, "", "", text)
}

func (tw textBlockWriter) section(name string) {
	fmt.Fprintf(tw.w, "%s:\n", name)
}

func (tw textBlockWriter) bullet(text string) {
	writeWrapped(tw.w, tw.width, "  - ", "    ", text)
}

func (tw textBlockWriter) item(name, text string) {
	firstIndent := fmt.Sprintf("  %-12s ", name)
	writeWrapped(tw.w, tw.width, firstIndent, strings.Repeat(" ", runeLen(firstIndent)), text)
}

func (tw textBlockWriter) command(line string) {
	fmt.Fprintf(tw.w, "  %s\n", line)
}

func (tw textBlockWriter) flags(fs *flag.FlagSet) {
	fs.VisitAll(func(f *flag.Flag) {
		fmt.Fprintf(tw.w, "  %s\n", formatFlagSynopsis(f))
		writeWrapped(tw.w, tw.width, "      ", "      ", f.Usage)
		if def := formatFlagDefault(f); def != "" {
			writeWrapped(tw.w, tw.width, "      ", "      ", "Default: "+def)
		}
	})
}

func formatFlagSynopsis(f *flag.Flag) string {
	name, _ := flag.UnquoteUsage(f)
	if name == "" {
		if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			return "-" + f.Name
		}
		name = "value"
	}
	return fmt.Sprintf("-%s %s", f.Name, name)
}

func formatFlagDefault(f *flag.Flag) string {
	def := strings.TrimSpace(f.DefValue)
	if def == "" {
		return ""
	}
	if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() && def == "false" {
		return ""
	}
	if def == "0" {
		return ""
	}
	if strings.ContainsAny(def, " \t") {
		return strconv.Quote(def)
	}
	return def
}
