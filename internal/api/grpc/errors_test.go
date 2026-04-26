package grpc

import (
	"fmt"
	"testing"

	"github.com/StepanK17/LogBroker/internal/broker"
	"github.com/StepanK17/LogBroker/internal/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToStatusError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{
			name: "invalid topic name",
			err:  fmt.Errorf("%w: %q", metadata.ErrInvalidTopicName, "../orders"),
			code: codes.InvalidArgument,
		},
		{
			name: "topic already exists",
			err:  fmt.Errorf("%w: %q", metadata.ErrTopicAlreadyExists, "orders"),
			code: codes.AlreadyExists,
		},
		{
			name: "topic not found",
			err:  fmt.Errorf("%w: %q", broker.ErrTopicNotFound, "orders"),
			code: codes.NotFound,
		},
		{
			name: "invalid partition",
			err:  broker.ErrInvalidPartition,
			code: codes.InvalidArgument,
		},
		{
			name: "unexpected",
			err:  fmt.Errorf("disk failure"),
			code: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toStatusError(tt.err)
			if status.Code(got) != tt.code {
				t.Fatalf("expected code %s, got %s", tt.code, status.Code(got))
			}
		})
	}
}
