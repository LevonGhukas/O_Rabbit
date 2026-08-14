package grpcapi

// SetCatalogWorkLimit bounds registration and reconciliation calls across all
// master-owned executors. It is configured before serving requests.
func (s *Server) SetCatalogWorkLimit(limit int) {
	if limit > 0 {
		s.catalogWorkSlots = make(chan struct{}, limit)
	}
}

func (s *Server) tryAcquireCatalogWork() (func(), bool) {
	select {
	case s.catalogWorkSlots <- struct{}{}:
		return func() { <-s.catalogWorkSlots }, true
	default:
		return nil, false
	}
}
