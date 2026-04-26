package grpc

import (
	"errors"

	"github.com/StepanK17/LogBroker/internal/broker"
	"github.com/StepanK17/LogBroker/internal/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toStatusError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, metadata.ErrTopicNameEmpty):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, metadata.ErrInvalidTopicName):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, metadata.ErrInvalidPartitions):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, metadata.ErrTopicAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, broker.ErrTopicNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, broker.ErrInvalidPartition):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, broker.ErrGroupEmpty):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
