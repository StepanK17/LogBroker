package broker

import "errors"

var (
	ErrTopicNotFound    = errors.New("topic not found")
	ErrInvalidPartition = errors.New("invalid partition")
	ErrGroupEmpty       = errors.New("group is empty")
)
