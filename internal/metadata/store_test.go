package metadata

import (
	"errors"
	"testing"
)

func TestStoreCreateTopicRejectsInvalidNames(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	invalidNames := []string{
		".",
		"..",
		"orders/payments",
		`orders\payments`,
		"orders payments",
	}

	for _, name := range invalidNames {
		t.Run(name, func(t *testing.T) {
			err := store.CreateTopic(name, 1)
			if !errors.Is(err, ErrInvalidTopicName) {
				t.Fatalf("expected ErrInvalidTopicName for %q, got %v", name, err)
			}
		})
	}
}

func TestStoreCreateTopicAcceptsSafeNames(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	validNames := []string{
		"orders",
		"orders.v1",
		"orders_v1",
		"orders-v1",
	}

	for _, name := range validNames {
		t.Run(name, func(t *testing.T) {
			if err := store.CreateTopic(name, 1); err != nil {
				t.Fatalf("create topic %q: %v", name, err)
			}
		})
	}
}
