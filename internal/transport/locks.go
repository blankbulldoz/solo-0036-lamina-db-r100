package transport

import (
	"lamina/internal/datum"
	"lamina/internal/engine"
	"net/http"
)

func (s *Server) acquireLock(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Relation string      `json:"relation"`
		Key      datum.Value `json:"key"`
		WaitMS   int         `json:"wait_ms"`
		LeaseMS  int         `json:"lease_ms"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.AcquireLock(r.Context(), r.PathValue("tx"), in.Relation, in.Key, in.WaitMS, in.LeaseMS)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
func (s *Server) acquireLocks(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Rows    []engine.LockRequest `json:"rows"`
		WaitMS  int                  `json:"wait_ms"`
		LeaseMS int                  `json:"lease_ms"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.AcquireLocks(r.Context(), r.PathValue("tx"), in.Rows, in.WaitMS, in.LeaseMS)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]any{"locks": out})
}
func (s *Server) releaseLock(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Relation string      `json:"relation"`
		Key      datum.Value `json:"key"`
	}
	if !decode(w, r, &in) {
		return
	}
	released, err := s.db.ReleaseLock(r.PathValue("tx"), in.Relation, in.Key, false)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]any{"released": released})
}
func (s *Server) releaseLocks(w http.ResponseWriter, r *http.Request) {
	var in struct{}
	if !decode(w, r, &in) {
		return
	}
	released, err := s.db.ReleaseLock(r.PathValue("tx"), "", datum.Value{}, true)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]any{"released": released})
}
func (s *Server) cancelLockWait(w http.ResponseWriter, r *http.Request) {
	var in struct{}
	if !decode(w, r, &in) {
		return
	}
	canceled, err := s.db.CancelLockWait(r.PathValue("tx"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]any{"canceled": canceled})
}
func (s *Server) locks(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.Locks()
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
