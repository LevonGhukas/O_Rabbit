package grpcapi

import "github.com/LevonGhukas/O_Rabbit/internal/telemetry"

// SetCatalogWorkLimit bounds registration and reconciliation calls across all
// master-owned executors. It is configured before serving requests.
func (s *Server) SetCatalogWorkLimit(limit int) {
	if limit > 0 {
		s.catalogWorkSlots = make(chan struct{}, limit)
		telemetry.SetCatalogWorkCapacity(limit)
	}
}

// tryAcquireCatalogWork takes a catalog work slot without waiting. Slot use
// and refusals are exported, so a saturated catalog is visible without a
// per-run label.
func (s *Server) tryAcquireCatalogWork() (func(), bool) {
	select {
	case s.catalogWorkSlots <- struct{}{}:
		telemetry.ObserveCatalogWork(true)
		return func() {
			<-s.catalogWorkSlots
			telemetry.ReleaseCatalogWork()
		}, true
	default:
		telemetry.ObserveCatalogWork(false)
		return nil, false
	}
}
