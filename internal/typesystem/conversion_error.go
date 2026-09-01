package typesystem

import (
	"fmt"
	"reflect"
)

// ConversionError describes a failed conversion without hiding the source or
// destination involved. Callers can use errors.As to inspect it.
type ConversionError struct {
	Target LogicalType
	Value  any
	Reason string
}

func (e *ConversionError) Error() string {
	source := "<nil>"
	if e.Value != nil {
		source = reflect.TypeOf(e.Value).String()
	}
	return fmt.Sprintf("cannot convert %s to %s: %s", source, e.Target.String(), e.Reason)
}

func conversionError(target LogicalType, value any, format string, args ...any) error {
	return &ConversionError{
		Target: target,
		Value:  value,
		Reason: fmt.Sprintf(format, args...),
	}
}
