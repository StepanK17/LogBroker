package grpc

import (
	"context"
	"testing"

	"github.com/StepanK17/LogBroker/api/proto/brokerv1"
	"github.com/StepanK17/LogBroker/internal/broker"
)

func TestServerHappyPath(t *testing.T) {
	svc, err := broker.OpenService(t.TempDir())
	if err != nil {
		t.Fatalf("open broker service: %v", err)
	}
	defer svc.Close()

	server := NewServer(svc)
	ctx := context.Background()

	if _, err := server.CreateTopic(ctx, &brokerv1.CreateTopicRequest{
		Name:       "orders",
		Partitions: 1,
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	first, err := server.Produce(ctx, &brokerv1.ProduceRequest{
		Topic:     "orders",
		Partition: 0,
		Key:       []byte("k1"),
		Value:     []byte("v1"),
	})
	if err != nil {
		t.Fatalf("produce first record: %v", err)
	}

	second, err := server.Produce(ctx, &brokerv1.ProduceRequest{
		Topic:     "orders",
		Partition: 0,
		Key:       []byte("k2"),
		Value:     []byte("v2"),
	})
	if err != nil {
		t.Fatalf("produce second record: %v", err)
	}

	if first.Offset != 0 || second.Offset != 1 {
		t.Fatalf("unexpected produce offsets: got %d and %d", first.Offset, second.Offset)
	}

	fetchResp, err := server.Fetch(ctx, &brokerv1.FetchRequest{
		Topic:      "orders",
		Partition:  0,
		Offset:     0,
		MaxRecords: 100,
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(fetchResp.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(fetchResp.Records))
	}
	if fetchResp.NextOffset != 2 {
		t.Fatalf("expected next offset 2, got %d", fetchResp.NextOffset)
	}

	if _, err := server.CommitOffset(ctx, &brokerv1.CommitOffsetRequest{
		Group:     "group-a",
		Topic:     "orders",
		Partition: 0,
		Offset:    1,
	}); err != nil {
		t.Fatalf("commit offset: %v", err)
	}

	getResp, err := server.GetCommittedOffset(ctx, &brokerv1.GetCommittedOffsetRequest{
		Group:     "group-a",
		Topic:     "orders",
		Partition: 0,
	})
	if err != nil {
		t.Fatalf("get committed offset: %v", err)
	}
	if !getResp.Found {
		t.Fatalf("expected committed offset to be found")
	}
	if getResp.Offset != 1 {
		t.Fatalf("expected committed offset 1, got %d", getResp.Offset)
	}
}

func TestServerFetchIsBounded(t *testing.T) {
	svc, err := broker.OpenService(t.TempDir())
	if err != nil {
		t.Fatalf("open broker service: %v", err)
	}
	defer svc.Close()

	server := NewServer(svc)
	ctx := context.Background()

	if _, err := server.CreateTopic(ctx, &brokerv1.CreateTopicRequest{
		Name:       "orders",
		Partitions: 1,
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	for i := 0; i < 105; i++ {
		if _, err := server.Produce(ctx, &brokerv1.ProduceRequest{
			Topic:     "orders",
			Partition: 0,
			Key:       []byte("k"),
			Value:     []byte("v"),
		}); err != nil {
			t.Fatalf("produce record %d: %v", i, err)
		}
	}

	firstBatch, err := server.Fetch(ctx, &brokerv1.FetchRequest{
		Topic:      "orders",
		Partition:  0,
		Offset:     0,
		MaxRecords: 100,
	})
	if err != nil {
		t.Fatalf("fetch first batch: %v", err)
	}
	if len(firstBatch.Records) != 100 {
		t.Fatalf("expected 100 records in first batch, got %d", len(firstBatch.Records))
	}
	if firstBatch.NextOffset != 100 {
		t.Fatalf("expected next offset 100, got %d", firstBatch.NextOffset)
	}

	secondBatch, err := server.Fetch(ctx, &brokerv1.FetchRequest{
		Topic:      "orders",
		Partition:  0,
		Offset:     100,
		MaxRecords: 100,
	})
	if err != nil {
		t.Fatalf("fetch second batch: %v", err)
	}
	if len(secondBatch.Records) != 5 {
		t.Fatalf("expected 5 records in second batch, got %d", len(secondBatch.Records))
	}
	if secondBatch.NextOffset != 105 {
		t.Fatalf("expected next offset 105, got %d", secondBatch.NextOffset)
	}
}
