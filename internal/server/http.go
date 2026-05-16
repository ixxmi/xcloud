package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"xcloud/internal/syncmodel"
)

type Server struct {
	store *Store
	token string
	log   *slog.Logger
}

func New(store *Store, token string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{store: store, token: token, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /v1/files", s.auth(s.listFiles))
	mux.HandleFunc("GET /v1/events", s.auth(s.events))
	mux.HandleFunc("POST /v1/chunks/check", s.auth(s.checkChunks))
	mux.HandleFunc("PUT /v1/chunks/", s.auth(s.putChunk))
	mux.HandleFunc("GET /v1/chunks/", s.auth(s.getChunk))
	mux.HandleFunc("POST /v1/commit", s.auth(s.commit))
	mux.HandleFunc("POST /v1/delete", s.auth(s.delete))
	return mux
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.token != "" {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if got != s.token {
				writeJSON(w, http.StatusUnauthorized, syncmodel.ErrorResponse{Error: "unauthorized"})
				return
			}
		}
		next(w, r)
	}
}

func (s *Server) listFiles(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, syncmodel.ListResponse{Files: s.store.ListFiles()})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	after := int64(0)
	if v := r.URL.Query().Get("after"); v != "" {
		_, _ = fmtSscanf(v, &after)
	}
	writeJSON(w, http.StatusOK, syncmodel.EventsResponse{Events: s.store.EventsAfter(after)})
}

func (s *Server) checkChunks(w http.ResponseWriter, r *http.Request) {
	var req syncmodel.CheckChunksRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	writeJSON(w, http.StatusOK, syncmodel.CheckChunksResponse{Missing: s.store.CheckChunks(req.Chunks)})
}

func (s *Server) putChunk(w http.ResponseWriter, r *http.Request) {
	hash := strings.TrimPrefix(r.URL.Path, "/v1/chunks/")
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 512<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	if err := s.store.PutChunk(hash, data); err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getChunk(w http.ResponseWriter, r *http.Request) {
	hash := strings.TrimPrefix(r.URL.Path, "/v1/chunks/")
	p, err := s.store.ChunkPath(hash)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	http.ServeFile(w, r, p)
}

func (s *Server) commit(w http.ResponseWriter, r *http.Request) {
	var req syncmodel.CommitRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	resp, err := s.store.Commit(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	var req syncmodel.DeleteRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	resp, err := s.store.Delete(req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, syncmodel.ErrorResponse{Error: err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func fmtSscanf(v string, out *int64) (int, error) {
	var n int64
	sign := int64(1)
	for i, r := range v {
		if i == 0 && r == '-' {
			sign = -1
			continue
		}
		if r < '0' || r > '9' {
			return 0, nil
		}
		n = n*10 + int64(r-'0')
	}
	*out = n * sign
	return 1, nil
}
