package broker

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/StepanK17/LogBroker/internal/group"
	"github.com/StepanK17/LogBroker/internal/metadata"
	"github.com/StepanK17/LogBroker/internal/storage"
)

const defaultFetchMaxRecords = 100

type Service struct {
	baseDir         string
	segmentMaxBytes int64
	meta            *metadata.Store
	groups          *group.Store
	logsMu          sync.Mutex
	logs            map[partitionKey]*storage.PartitionLog
}
type partitionKey struct {
	topic     string
	partition int
}

func OpenService(baseDir string) (*Service, error) {
	return OpenServiceWithOptions(baseDir, 0)
}

func OpenServiceWithOptions(baseDir string, segmentMaxBytes int64) (*Service, error) {
	metadataStore, err := metadata.OpenStore(baseDir)
	if err != nil {
		return nil, err
	}

	groupStore, err := group.OpenStore(baseDir)
	if err != nil {
		_ = metadataStore.Close()
		return nil, err
	}

	service := &Service{
		baseDir:         baseDir,
		segmentMaxBytes: segmentMaxBytes,
		meta:            metadataStore,
		groups:          groupStore,
		logs:            make(map[partitionKey]*storage.PartitionLog),
	}
	return service, nil
}

func (s *Service) CreateTopic(name string, partitions int) error {
	return s.meta.CreateTopic(name, partitions)
}

func (s *Service) Produce(topic string, partition int, key, value []byte) (storage.Record, error) {
	if err := s.validateTopicPartition(topic, partition); err != nil {
		return storage.Record{}, err
	}

	log, err := s.getPartitionLog(topic, partition)
	if err != nil {
		return storage.Record{}, err
	}
	record, err := log.Append(key, value)
	if err != nil {
		return storage.Record{}, err
	}

	return record, nil
}

func (s *Service) Fetch(topic string, partition int, offset uint64, maxRecords uint32) ([]storage.Record, error) {
	if err := s.validateTopicPartition(topic, partition); err != nil {
		return []storage.Record{}, err
	}

	log, err := s.getPartitionLog(topic, partition)
	if err != nil {
		return []storage.Record{}, err
	}

	limit := defaultFetchMaxRecords
	if maxRecords > 0 {
		limit = int(maxRecords)
	}

	records, err := log.ReadFromOffsetN(offset, limit)
	if err != nil {
		return []storage.Record{}, err
	}
	return records, nil

}

func (s *Service) CommitOffset(group, topic string, partition int, offset uint64) error {
	if group == "" {
		return ErrGroupEmpty
	}
	if err := s.validateTopicPartition(topic, partition); err != nil {
		return err
	}
	return s.groups.Commit(group, topic, partition, offset)
}

func (s *Service) GetCommittedOffset(group, topic string, partition int) (uint64, bool, error) {
	if group == "" {
		return 0, false, ErrGroupEmpty
	}
	if err := s.validateTopicPartition(topic, partition); err != nil {
		return 0, false, err
	}
	return s.groups.Get(group, topic, partition)
}

func (s *Service) Close() error {
	s.logsMu.Lock()
	logs := make([]*storage.PartitionLog, 0, len(s.logs))
	for _, log := range s.logs {
		logs = append(logs, log)
	}
	s.logs = make(map[partitionKey]*storage.PartitionLog)
	s.logsMu.Unlock()

	var closeErr error
	for _, log := range logs {
		closeErr = errors.Join(closeErr, log.Close())
	}

	return errors.Join(closeErr, s.groups.Close(), s.meta.Close())
}

func (s *Service) ListTopics() []metadata.Topic {
	return s.meta.ListTopics()
}

func (s *Service) validateTopicPartition(topic string, partition int) error {
	metaTopic, ok := s.meta.GetTopic(topic)
	if !ok {
		return fmt.Errorf("%w: %q", ErrTopicNotFound, topic)
	}
	if partition < 0 || partition >= metaTopic.PartitionsCount {
		return ErrInvalidPartition
	}
	return nil
}

func (s *Service) getPartitionLog(topic string, partition int) (*storage.PartitionLog, error) {
	key := partitionKey{
		topic:     topic,
		partition: partition,
	}

	s.logsMu.Lock()
	defer s.logsMu.Unlock()

	if log, ok := s.logs[key]; ok {
		return log, nil
	}

	partitionPath := filepath.Join(s.baseDir, "topics", topic, strconv.Itoa(partition))
	log, err := storage.OpenPartitionLogWithOptions(partitionPath, storage.PartitionLogOptions{
		SegmentMaxBytes: s.segmentMaxBytes,
	})
	if err != nil {
		return nil, err
	}

	s.logs[key] = log
	return log, nil
}
