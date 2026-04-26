package broker

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestServiceUsesConfiguredSegmentMaxBytes(t *testing.T) {
	baseDir := t.TempDir()

	svc, err := OpenServiceWithOptions(baseDir, 128)
	if err != nil {
		t.Fatalf("open service: %v", err)
	}
	defer svc.Close()

	if err := svc.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	value := bytes.Repeat([]byte("v"), 128)

	if _, err := svc.Produce("orders", 0, []byte("k1"), value); err != nil {
		t.Fatalf("produce first record: %v", err)
	}
	if _, err := svc.Produce("orders", 0, []byte("k2"), value); err != nil {
		t.Fatalf("produce second record: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(baseDir, "topics", "orders", "0"))
	if err != nil {
		t.Fatalf("read partition directory: %v", err)
	}

	var logFiles int
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".log" {
			logFiles++
		}
	}
	if logFiles < 2 {
		t.Fatalf("expected segment rotation with configured segment max bytes, got %d log files", logFiles)
	}
}
