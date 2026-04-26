package http

import (
	"encoding/json"
	"log"
	nethttp "net/http"
	"time"

	"github.com/StepanK17/LogBroker/internal/broker"
)

type Server struct {
	logger *log.Logger
	svc    *broker.Service
	mux    *nethttp.ServeMux
}

func NewServer(logger *log.Logger, svc *broker.Service) nethttp.Handler {
	server := &Server{
		logger: logger,
		svc:    svc,
		mux:    nethttp.NewServeMux(),
	}

	server.routes()

	return server.loggingMiddleware(server.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/topics", s.handleTopics)

}

func (s *Server) loggingMiddleware(next nethttp.Handler) nethttp.Handler {
	return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.logger.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func writeJSON(w nethttp.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(payload)
}
