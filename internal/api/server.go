package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"graph-platform/internal/query"
)

// WithAuth wraps h with static bearer-token authentication. An empty token
// disables auth entirely (open mode for local development — the caller should
// log loudly when that happens). /health stays unauthenticated so load
// balancers and uptime probes work without credentials.
//
// A static shared token is deliberately minimal: it keeps the org-wide code
// index off the open network today, while the eventual repo-permission-aware
// access control (per-user tokens mapped to Git permissions) slots in here
// without touching handlers.
func WithAuth(h http.Handler, token string) http.Handler {
	if token == "" {
		return h
	}
	expected := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			h.ServeHTTP(w, r)
			return
		}
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, expected) != 1 {
			writeErr(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		h.ServeHTTP(w, r)
	})
}

type Server struct {
	svc *query.Service
}

func NewServer(svc *query.Service) *Server {
	return &Server{svc: svc}
}

// Routes returns the HTTP handler with all read-only query routes mounted.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	// existing routes
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /search", s.search)
	mux.HandleFunc("GET /symbol/{name}", s.findSymbol)
	mux.HandleFunc("GET /callers/{symbol}", s.findCallers)
	mux.HandleFunc("GET /callees/{symbol}", s.findCallees)
	mux.HandleFunc("GET /blast-radius/{symbol}", s.blastRadius)
	mux.HandleFunc("GET /path", s.shortestPath)

	// repository onboarding
	mux.HandleFunc("GET /overview/{repo}", s.repositoryOverview)

	// extractor-backed routes
	mux.HandleFunc("GET /dependencies/{repo}", s.findDependencies)
	mux.HandleFunc("GET /dependents/{dep}", s.findDependents)
	mux.HandleFunc("GET /routes", s.findRoutes)
	mux.HandleFunc("GET /kafka/topic/{name}", s.findKafkaTopic)
	mux.HandleFunc("GET /sql/object", s.findSQLObject)
	mux.HandleFunc("GET /glue/jobs", s.findGlueJobs)
	return mux
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		writeErr(w, http.StatusBadRequest, "missing query parameter q")
		return
	}
	results, err := s.svc.Search(r.Context(), q)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, results)
}

func (s *Server) findSymbol(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	results, err := s.svc.FindSymbol(r.Context(), name)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, results)
}

func (s *Server) findCallers(w http.ResponseWriter, r *http.Request) {
	sym := r.PathValue("symbol")
	edges, err := s.svc.FindCallers(r.Context(), sym)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, edges)
}

func (s *Server) findCallees(w http.ResponseWriter, r *http.Request) {
	sym := r.PathValue("symbol")
	edges, err := s.svc.FindCallees(r.Context(), sym)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, edges)
}

func (s *Server) blastRadius(w http.ResponseWriter, r *http.Request) {
	sym := r.PathValue("symbol")
	depth := 0
	if d := r.URL.Query().Get("depth"); d != "" {
		parsed, err := strconv.Atoi(d)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "depth must be an integer")
			return
		}
		depth = parsed
	}
	nodes, err := s.svc.BlastRadius(r.Context(), sym, depth)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nodes)
}

func (s *Server) shortestPath(w http.ResponseWriter, r *http.Request) {
	src := r.URL.Query().Get("src")
	dst := r.URL.Query().Get("dst")
	if src == "" || dst == "" {
		writeErr(w, http.StatusBadRequest, "missing src or dst")
		return
	}
	path, err := s.svc.ShortestPath(r.Context(), src, dst)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, path)
}

func (s *Server) repositoryOverview(w http.ResponseWriter, r *http.Request) {
	repo := r.PathValue("repo")
	if repo == "" {
		writeErr(w, http.StatusBadRequest, "missing repo")
		return
	}
	out, err := s.svc.RepositoryOverview(r.Context(), repo)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) findDependencies(w http.ResponseWriter, r *http.Request) {
	repo := r.PathValue("repo")
	scope := r.URL.Query().Get("scope")
	out, err := s.svc.FindDependencies(r.Context(), repo, scope)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) findDependents(w http.ResponseWriter, r *http.Request) {
	dep := r.PathValue("dep")
	out, err := s.svc.FindDependents(r.Context(), dep)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) findRoutes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	out, err := s.svc.FindRoutes(r.Context(), q.Get("method"), q.Get("path"), q.Get("repo"))
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) findKafkaTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	out, err := s.svc.FindKafkaTopic(r.Context(), name)
	if err != nil {
		serverError(w, err)
		return
	}
	if out == nil {
		writeErr(w, http.StatusNotFound, "topic not found")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) findSQLObject(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	out, err := s.svc.FindSQLObject(r.Context(), q.Get("schema"), q.Get("name"))
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) findGlueJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	out, err := s.svc.FindGlueJobs(r.Context(), q.Get("source"), q.Get("target"))
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json: %v", err)
	}
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func serverError(w http.ResponseWriter, err error) {
	if errors.Is(err, query.ErrNotImplemented) {
		writeErr(w, http.StatusNotImplemented, err.Error())
		return
	}
	log.Printf("query error: %v", err)
	writeErr(w, http.StatusInternalServerError, "internal error")
}
