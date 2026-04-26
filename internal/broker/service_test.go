package broker

import "testing"

func TestServiceHappyPath(t *testing.T) {
	baseDir := t.TempDir()

	svc, err := OpenService(baseDir)
	if err != nil {
		t.Fatalf("open service: %v", err)
	}
	defer svc.Close()

	if err := svc.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	first, err := svc.Produce("orders", 0, []byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatalf("produce first record: %v", err)
	}
	second, err := svc.Produce("orders", 0, []byte("k2"), []byte("v2"))
	if err != nil {
		t.Fatalf("produce second record: %v", err)
	}

	if first.Offset != 0 || second.Offset != 1 {
		t.Fatalf("unexpected offsets: got %d and %d", first.Offset, second.Offset)
	}

	records, err := svc.Fetch("orders", 0, 0, 100)
	if err != nil {
		t.Fatalf("fetch records: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	if err := svc.CommitOffset("group-a", "orders", 0, second.Offset); err != nil {
		t.Fatalf("commit offset: %v", err)
	}

	offset, found, err := svc.GetCommittedOffset("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("get committed offset: %v", err)
	}
	if !found {
		t.Fatalf("expected committed offset to be found")
	}
	if offset != second.Offset {
		t.Fatalf("expected committed offset %d, got %d", second.Offset, offset)
	}
}

func TestServiceCommitOffsetPersistsAcrossRestart(t *testing.T) {
	baseDir := t.TempDir()

	svc, err := OpenService(baseDir)
	if err != nil {
		t.Fatalf("open service: %v", err)
	}

	if err := svc.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	if err := svc.CommitOffset("group-a", "orders", 0, 7); err != nil {
		t.Fatalf("commit offset: %v", err)
	}

	offset, found, err := svc.GetCommittedOffset("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("get committed offset: %v", err)
	}
	if !found {
		t.Fatalf("expected committed offset to be found")
	}
	if offset != 7 {
		t.Fatalf("expected offset 7, got %d", offset)
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("close service: %v", err)
	}

	reopened, err := OpenService(baseDir)
	if err != nil {
		t.Fatalf("reopen service: %v", err)
	}
	defer reopened.Close()

	offset, found, err = reopened.GetCommittedOffset("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("get committed offset after reopen: %v", err)
	}
	if !found {
		t.Fatalf("expected committed offset to be found after reopen")
	}
	if offset != 7 {
		t.Fatalf("expected offset 7 after reopen, got %d", offset)
	}
}

func TestServiceFetchIsBounded(t *testing.T) {
	baseDir := t.TempDir()

	svc, err := OpenService(baseDir)
	if err != nil {
		t.Fatalf("open service: %v", err)
	}
	defer svc.Close()

	if err := svc.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	for i := 0; i < defaultFetchMaxRecords+5; i++ {
		if _, err := svc.Produce("orders", 0, []byte("k"), []byte("v")); err != nil {
			t.Fatalf("produce record %d: %v", i, err)
		}
	}

	firstBatch, err := svc.Fetch("orders", 0, 0, uint32(defaultFetchMaxRecords))
	if err != nil {
		t.Fatalf("fetch first batch: %v", err)
	}
	if len(firstBatch) != defaultFetchMaxRecords {
		t.Fatalf("expected %d records in first batch, got %d", defaultFetchMaxRecords, len(firstBatch))
	}

	secondBatch, err := svc.Fetch("orders", 0, uint64(defaultFetchMaxRecords), uint32(defaultFetchMaxRecords))
	if err != nil {
		t.Fatalf("fetch second batch: %v", err)
	}
	if len(secondBatch) != 5 {
		t.Fatalf("expected 5 records in second batch, got %d", len(secondBatch))
	}
}
