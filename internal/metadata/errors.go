package metadata

import "errors"

var (
	ErrTopicNameEmpty     = errors.New("topic name is empty")
	ErrInvalidTopicName   = errors.New("topic name contains invalid characters")
	ErrInvalidPartitions  = errors.New("partitions must be > 0")
	ErrTopicAlreadyExists = errors.New("topic already exists")
)
