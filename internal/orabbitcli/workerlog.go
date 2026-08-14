package orabbitcli

import (
	"fmt"
	"strings"
)

func normalizeWorkerLogLevel(raw string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "":
		return "", nil
	case "DEBUG":
		return "DEBUG", nil
	case "INFO":
		return "INFO", nil
	case "WARN", "WARNING":
		return "WARN", nil
	case "ERROR":
		return "ERROR", nil
	default:
		return "", fmt.Errorf("invalid worker log level %q: use DEBUG, INFO, WARN, or ERROR", strings.TrimSpace(raw))
	}
}

func normalizeWorkerLogFormat(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return "", nil
	case "json":
		return "json", nil
	case "text":
		return "text", nil
	default:
		return "", fmt.Errorf("invalid worker log format %q: use json or text", strings.TrimSpace(raw))
	}
}

func validateWorkerLogSettings(levelRaw, formatRaw string) error {
	if _, err := normalizeWorkerLogLevel(levelRaw); err != nil {
		return err
	}
	if _, err := normalizeWorkerLogFormat(formatRaw); err != nil {
		return err
	}
	return nil
}

func appendWorkerLogArgs(args []string, levelRaw, formatRaw string) ([]string, error) {
	level, err := normalizeWorkerLogLevel(levelRaw)
	if err != nil {
		return nil, err
	}
	format, err := normalizeWorkerLogFormat(formatRaw)
	if err != nil {
		return nil, err
	}
	if level != "" {
		args = append(args, "-log-level", level)
	}
	if format != "" {
		args = append(args, "-log-format", format)
	}
	return args, nil
}
