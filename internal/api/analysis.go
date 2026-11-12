package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

func (s *Server) analyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.RLock()
	service := s.analysis
	s.mu.RUnlock()
	if service == nil {
		http.Error(w, "analysis service is not configured", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	result, err := service.Analyze(ctx)
	status := http.StatusOK
	if err != nil { status = http.StatusBadGateway }
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if encodeErr := json.NewEncoder(w).Encode(result); encodeErr != nil {
		// The response may already be partially written; log rather than attempt
		// a second status code or hide the original analysis outcome.
		return
	}
}
