package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigIncludesGRPCAndStorageSettings(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "broker.json")

	data := []byte(`{
  "grpc": {
    "address": ":9191"
  },
  "http": {
    "address": ":8181"
  },
  "storage": {
    "data_dir": "./custom-data",
    "segment_max_bytes": 4096
  }
}`)

	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.GRPC.Address != ":9191" {
		t.Fatalf("expected grpc address :9191, got %q", cfg.GRPC.Address)
	}
	if cfg.HTTP.Address != ":8181" {
		t.Fatalf("expected http address :8181, got %q", cfg.HTTP.Address)
	}
	if cfg.Storage.DataDir != "./custom-data" {
		t.Fatalf("expected data dir ./custom-data, got %q", cfg.Storage.DataDir)
	}
	if cfg.Storage.SegmentMaxBytes != 4096 {
		t.Fatalf("expected segment max bytes 4096, got %d", cfg.Storage.SegmentMaxBytes)
	}
}
