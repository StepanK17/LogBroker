package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestPartitionLogApplyRetentionDeletesOldSegments(t *testing.T) {
	log, err := OpenPartitionLog(t.TempDir())
	if err != nil {
		t.Fatalf("open partition log: %v", err)
	}
	defer log.Close()

	log.segmentMaxBytes = 80
	log.retentionBytes = 250

	value := bytes.Repeat([]byte("v"), 64)

	if _, err := log.Append([]byte("k1"), value); err != nil {
		t.Fatalf("append first record: %v", err)
	}
	if _, err := log.Append([]byte("k2"), value); err != nil {
		t.Fatalf("append second record: %v", err)
	}

	if len(log.segments) < 2 {
		t.Fatalf("expected at least 2 segments after rotation, got %d", len(log.segments))
	}

	firstBaseOffset := log.segments[0].baseOffset

	if _, err := log.Append([]byte("k3"), value); err != nil {
		t.Fatalf("append third record: %v", err)
	}

	if len(log.segments) != 2 {
		t.Fatalf("expected retention to keep 2 segments, got %d", len(log.segments))
	}
	if log.segments[0].baseOffset == firstBaseOffset {
		t.Fatalf("expected oldest segment with base offset %d to be deleted", firstBaseOffset)
	}
	if log.activeSegment != log.segments[len(log.segments)-1] {
		t.Fatalf("expected active segment to remain the last segment")
	}

	records, err := log.ReadAll()
	if err != nil {
		t.Fatalf("read all after retention: %v", err)
	}
	if len(records) == 0 {
		t.Fatalf("expected records to remain readable after retention")
	}
}

func TestPartitionLogRecoversAfterIndexDeletion(t *testing.T) {
	dir := t.TempDir()

	log, err := OpenPartitionLog(dir)
	if err != nil {
		t.Fatalf("open partition log: %v", err)
	}

	first, err := log.Append([]byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatalf("append first record: %v", err)
	}
	second, err := log.Append([]byte("k2"), []byte("v2"))
	if err != nil {
		t.Fatalf("append second record: %v", err)
	}

	activeBaseOffset := log.activeSegment.baseOffset
	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	segmentIndexPath := filepath.Join(dir, formatIndexName(activeBaseOffset))
	if err := os.Remove(segmentIndexPath); err != nil {
		t.Fatalf("remove index file: %v", err)
	}

	reopened, err := OpenPartitionLog(dir)
	if err != nil {
		t.Fatalf("reopen partition log: %v", err)
	}
	defer reopened.Close()

	records, err := reopened.ReadAll()
	if err != nil {
		t.Fatalf("read all after reopen: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records after recovery, got %d", len(records))
	}
	if records[0].Offset != first.Offset || records[1].Offset != second.Offset {
		t.Fatalf("unexpected offsets after recovery: got [%d %d]", records[0].Offset, records[1].Offset)
	}

	third, err := reopened.Append([]byte("k3"), []byte("v3"))
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if third.Offset != 2 {
		t.Fatalf("expected next offset 2 after recovery, got %d", third.Offset)
	}
}

func TestPartitionLogReadFromOffsetNRespectsLimit(t *testing.T) {
	log, err := OpenPartitionLog(t.TempDir())
	if err != nil {
		t.Fatalf("open partition log: %v", err)
	}
	defer log.Close()

	log.segmentMaxBytes = 80
	value := bytes.Repeat([]byte("v"), 64)

	for i := 0; i < 3; i++ {
		if _, err := log.Append([]byte("k"), value); err != nil {
			t.Fatalf("append record %d: %v", i, err)
		}
	}

	records, err := log.ReadFromOffsetN(0, 2)
	if err != nil {
		t.Fatalf("read with limit: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].Offset != 0 || records[1].Offset != 1 {
		t.Fatalf("unexpected offsets: got [%d %d]", records[0].Offset, records[1].Offset)
	}
}

func TestPartitionLogRecoversAfterInactiveSegmentIndexDeletion(t *testing.T) {
	dir := t.TempDir()

	log, err := OpenPartitionLog(dir)
	if err != nil {
		t.Fatalf("open partition log: %v", err)
	}

	log.segmentMaxBytes = 80
	value := bytes.Repeat([]byte("v"), 64)

	for i := 0; i < 3; i++ {
		if _, err := log.Append([]byte("k"), value); err != nil {
			t.Fatalf("append record %d: %v", i, err)
		}
	}

	if len(log.segments) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(log.segments))
	}

	inactiveBaseOffset := log.segments[0].baseOffset
	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	inactiveIndexPath := filepath.Join(dir, formatIndexName(inactiveBaseOffset))
	if err := os.Remove(inactiveIndexPath); err != nil {
		t.Fatalf("remove inactive index: %v", err)
	}

	reopened, err := OpenPartitionLog(dir)
	if err != nil {
		t.Fatalf("reopen partition log: %v", err)
	}
	defer reopened.Close()

	records, err := reopened.ReadFromOffset(0)
	if err != nil {
		t.Fatalf("read from offset after reopen: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records after inactive index recovery, got %d", len(records))
	}
	if records[0].Offset != 0 {
		t.Fatalf("expected first recovered offset 0, got %d", records[0].Offset)
	}
}

func formatIndexName(baseOffset uint64) string {
	return formatSegmentName(baseOffset, ".index")
}

func formatSegmentName(baseOffset uint64, suffix string) string {
	return fmt.Sprintf("%020d%s", baseOffset, suffix)
}
