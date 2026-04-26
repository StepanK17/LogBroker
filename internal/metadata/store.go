package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
)

var topicNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type Store struct {
	baseDir      string
	metadataPath string
	topics       map[string]Topic
	mu           sync.RWMutex
}

func OpenStore(baseDir string) (*Store, error) {
	metadataDir := filepath.Join(baseDir, "metadata")
	metadataPath := filepath.Join(metadataDir, "topics.json")

	err := os.MkdirAll(metadataDir, 0755)
	if err != nil {
		return nil, err
	}

	store := &Store{
		baseDir:      baseDir,
		metadataPath: metadataPath,
		topics:       make(map[string]Topic),
	}

	err = store.load()
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (s *Store) CreateTopic(name string, partitions int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if name == "" {
		return ErrTopicNameEmpty
	}
	if !isValidTopicName(name) {
		return fmt.Errorf("%w: %q", ErrInvalidTopicName, name)
	}
	if partitions <= 0 {
		return ErrInvalidPartitions
	}
	if _, exists := s.topics[name]; exists {
		return fmt.Errorf("%w: %q", ErrTopicAlreadyExists, name)
	}

	topicDir := filepath.Join(s.baseDir, "topics", name)
	err := os.MkdirAll(topicDir, 0755)
	if err != nil {
		return err
	}

	for i := 0; i < partitions; i++ {
		partitionDir := filepath.Join(topicDir, strconv.Itoa(i))
		err = os.MkdirAll(partitionDir, 0755)
		if err != nil {
			return err
		}
	}

	topic := Topic{
		Name:            name,
		PartitionsCount: partitions,
	}
	s.topics[name] = topic

	err = s.save()
	if err != nil {
		delete(s.topics, name)
		return err
	}

	return nil

}

func (s *Store) GetTopic(name string) (Topic, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topic, ok := s.topics[name]
	if !ok {
		return Topic{}, false
	}
	return topic, true
}

func (s *Store) ListTopics() []Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var topics []Topic
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})

	return topics

}

func (s *Store) Close() error {
	return nil
}

func (s *Store) load() error {
	_, err := os.Stat(s.metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	data, err := os.ReadFile(s.metadataPath)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	var topics []Topic
	err = json.Unmarshal(data, &topics)
	if err != nil {
		return err
	}

	for _, topic := range topics {
		s.topics[topic.Name] = topic
	}

	return nil
}

func (s *Store) save() error {

	var topics []Topic
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})
	data, err := json.Marshal(topics)
	if err != nil {
		return err
	}

	tmpPath := s.metadataPath + ".tmp"
	err = os.WriteFile(tmpPath, data, 0644)
	if err != nil {
		return err
	}

	return os.Rename(tmpPath, s.metadataPath)

}

func isValidTopicName(name string) bool {
	if name == "." || name == ".." {
		return false
	}

	return topicNamePattern.MatchString(name)
}
