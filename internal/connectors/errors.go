package connectors

import "github.com/LevonGhukas/O_Rabbit/internal/failure"

// ClassifyConnectorError maps driver-specific errors (like pq or pgx errors)
// to a typed failure.Failure. The rules live in failure.Classify so that
// connectors, workers and the master classify errors identically.
func ClassifyConnectorError(err error) error {
	if f := failure.Classify(err); f != nil {
		return f
	}
	return nil
}
