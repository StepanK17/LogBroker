package grpc

import (
	"context"

	"github.com/StepanK17/LogBroker/api/proto/brokerv1"
	"github.com/StepanK17/LogBroker/internal/broker"
)

type Server struct {
	brokerv1.UnimplementedBrokerServiceServer
	svc *broker.Service
}

func (s *Server) Produce(ctx context.Context, req *brokerv1.ProduceRequest) (*brokerv1.ProduceResponse, error) {
	record, err := s.svc.Produce(req.Topic, int(req.Partition), req.Key, req.Value)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &brokerv1.ProduceResponse{
		Offset:            record.Offset,
		TimestampUnixNano: record.Timestamp,
	}, nil
}

func (s *Server) Fetch(ctx context.Context, req *brokerv1.FetchRequest) (*brokerv1.FetchResponse, error) {
	records, err := s.svc.Fetch(req.Topic, int(req.Partition), req.Offset, req.MaxRecords)
	if err != nil {
		return nil, toStatusError(err)
	}

	var pbRecords []*brokerv1.Record
	nextOffset := req.Offset
	for _, record := range records {
		pbRecords = append(pbRecords, &brokerv1.Record{
			Offset:            record.Offset,
			TimestampUnixNano: record.Timestamp,
			Key:               record.Key,
			Value:             record.Value,
		})
		nextOffset = record.Offset + 1
	}

	return &brokerv1.FetchResponse{
		Records:    pbRecords,
		NextOffset: nextOffset,
	}, nil
}

func (s *Server) CreateTopic(ctx context.Context, req *brokerv1.CreateTopicRequest) (*brokerv1.CreateTopicResponse, error) {
	err := s.svc.CreateTopic(req.Name, int(req.Partitions))
	if err != nil {
		return nil, toStatusError(err)
	}
	return &brokerv1.CreateTopicResponse{}, nil
}

func (s *Server) CommitOffset(ctx context.Context, req *brokerv1.CommitOffsetRequest) (*brokerv1.CommitOffsetResponse, error) {
	if err := s.svc.CommitOffset(req.Group, req.Topic, int(req.Partition), req.Offset); err != nil {
		return nil, toStatusError(err)
	}
	return &brokerv1.CommitOffsetResponse{}, nil
}

func (s *Server) GetCommittedOffset(ctx context.Context, req *brokerv1.GetCommittedOffsetRequest) (*brokerv1.GetCommittedOffsetResponse, error) {
	offset, found, err := s.svc.GetCommittedOffset(req.Group, req.Topic, int(req.Partition))
	if err != nil {
		return nil, toStatusError(err)
	}
	return &brokerv1.GetCommittedOffsetResponse{
		Offset: offset,
		Found:  found,
	}, nil
}

func NewServer(svc *broker.Service) *Server {
	return &Server{
		svc: svc,
	}
}
