package typesystem

import (
	"strings"
	"time"
)

func convertDate(value any, target LogicalType) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, time.UTC), nil
	case string:
		text := strings.TrimSpace(v)
		if err := rejectTemporalSpecialValue(text, target, value); err != nil {
			return time.Time{}, err
		}
		parsed, err := time.Parse("2006-01-02", text)
		if err != nil {
			return time.Time{}, conversionError(target, value, "invalid date %q", v)
		}
		return parsed, nil
	case []byte:
		return convertDate(string(v), target)
	default:
		return time.Time{}, conversionError(target, value, "date source must be time.Time, string, or []byte")
	}
}

func convertTime(value any, target LogicalType) (time.Duration, error) {
	switch v := value.(type) {
	case time.Duration:
		if v < 0 || v >= 24*time.Hour {
			return 0, conversionError(target, value, "time-of-day must be within a day")
		}
		return v, nil
	case time.Time:
		return time.Duration(v.Hour())*time.Hour + time.Duration(v.Minute())*time.Minute + time.Duration(v.Second())*time.Second + time.Duration(v.Nanosecond()), nil
	case string:
		return parseTimeOfDay(v, target, value)
	case []byte:
		return parseTimeOfDay(string(v), target, value)
	default:
		return 0, conversionError(target, value, "time source must be time.Time, time.Duration, string, or []byte")
	}
}

func parseTimeOfDay(text string, target LogicalType, original any) (time.Duration, error) {
	text = strings.TrimSpace(text)
	layouts := []string{"15:04", "15:04:05", "15:04:05.999999999"}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, text); err == nil {
			return time.Duration(parsed.Hour())*time.Hour + time.Duration(parsed.Minute())*time.Minute + time.Duration(parsed.Second())*time.Second + time.Duration(parsed.Nanosecond()), nil
		}
	}
	return 0, conversionError(target, original, "invalid time-of-day %q", text)
}

func convertTimestamp(value any, target LogicalType) (time.Time, error) {
	var result time.Time
	switch v := value.(type) {
	case time.Time:
		result = v
	case string:
		text := strings.TrimSpace(v)
		if err := rejectTemporalSpecialValue(text, target, value); err != nil {
			return time.Time{}, err
		}
		parsed, ok := parseTimestamp(text)
		if !ok {
			return time.Time{}, conversionError(target, value, "invalid timestamp %q", v)
		}
		result = parsed
	case []byte:
		return convertTimestamp(string(v), target)
	default:
		return time.Time{}, conversionError(target, value, "timestamp source must be time.Time, string, or []byte")
	}
	if target.Kind == KindTimestampTZ {
		return result.UTC(), nil
	}
	return result, nil
}

func rejectTemporalSpecialValue(text string, target LogicalType, value any) error {
	if strings.EqualFold(text, "infinity") || strings.EqualFold(text, "-infinity") {
		return conversionError(target, value, "PostgreSQL infinity value %q is not representable as a native %s; override this column to string to preserve it losslessly", text, target.String())
	}
	return nil
}

// parseTimestamp accepts RFC3339Nano and the database timestamp forms already
// recognized by the legacy Arrow path. Offset-free forms use time.Parse and
// therefore have UTC as their explicitly defined location.
func parseTimestamp(text string) (time.Time, bool) {
	if text == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return parsed, true
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}
