package http

import (
	"encoding/json"
	"io"
	"log"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/StepanK17/LogBroker/internal/broker"
)

func TestHealthEndpoint(t *testing.T) {
	handler, cleanup := newTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest(nethttp.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != nethttp.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var payload healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if payload.Status != "ok" {
		t.Fatalf("expected status ok, got %q", payload.Status)
	}
}

func TestTopicsEndpoints(t *testing.T) {
	handler, cleanup := newTestHandler(t)
	defer cleanup()

	createReq := httptest.NewRequest(nethttp.MethodPost, "/topics", strings.NewReader(`{"name":"orders","partitions":1}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	handler.ServeHTTP(createRec, createReq)

	if createRec.Code != nethttp.StatusCreated {
		t.Fatalf("expected create status 201, got %d", createRec.Code)
	}

	listReq := httptest.NewRequest(nethttp.MethodGet, "/topics", nil)
	listRec := httptest.NewRecorder()
	handler.ServeHTTP(listRec, listReq)

	if listRec.Code != nethttp.StatusOK {
		t.Fatalf("expected list status 200, got %d", listRec.Code)
	}

	var payload topicsResponse
	if err := json.Unmarshal(listRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode topics response: %v", err)
	}
	if len(payload.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(payload.Topics))
	}
	if payload.Topics[0].Name != "orders" {
		t.Fatalf("expected topic orders, got %q", payload.Topics[0].Name)
	}
}

func newTestHandler(t *testing.T) (nethttp.Handler, func()) {
	t.Helper()

	svc, err := broker.OpenService(t.TempDir())
	if err != nil {
		t.Fatalf("open broker service: %v", err)
	}

	logger := log.New(io.Discard, "", 0)
	return NewServer(logger, svc), func() {
		_ = svc.Close()
	}
}
