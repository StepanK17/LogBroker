package http

import (
	"encoding/json"
	nethttp "net/http"

	"time"

	"github.com/StepanK17/LogBroker/internal/metadata"
)

type healthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}
type topicsResponse struct {
	Topics []metadata.Topic `json:"topics"`
}
type topicRequest struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

func (s *Server) handleHealth(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
		return
	}

	writeJSON(w, nethttp.StatusOK, healthResponse{
		Status:    "ok",
		Timestamp: time.Now().UTC().Format(nethttp.TimeFormat),
	})
}

func (s *Server) handleTopics(w nethttp.ResponseWriter, r *nethttp.Request) {
	switch r.Method {
	case nethttp.MethodGet:
		writeJSON(w, nethttp.StatusOK, topicsResponse{
			Topics: s.svc.ListTopics(),
		})

	case nethttp.MethodPost:
		var req topicRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, nethttp.StatusBadRequest, map[string]string{
				"error": "invalid json",
			})
			return
		}

		err := s.svc.CreateTopic(req.Name, req.Partitions)
		if err != nil {
			writeJSON(w, nethttp.StatusBadRequest, map[string]string{
				"error": err.Error(),
			})
			return
		}

		writeJSON(w, nethttp.StatusCreated, map[string]string{
			"status": "created",
		})

	default:
		writeJSON(w, nethttp.StatusMethodNotAllowed, map[string]string{
			"error": "method not allowed",
		})
	}
}
