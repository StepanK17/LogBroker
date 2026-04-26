package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	GRPC    GRPCConfig    `json:"grpc"`
	HTTP    HTTPConfig    `json:"http"`
	Storage StorageConfig `json:"storage"`
}

type GRPCConfig struct {
	Address string `json:"address"`
}

type HTTPConfig struct {
	Address string `json:"address"`
}

type StorageConfig struct {
	DataDir         string `json:"data_dir"`
	SegmentMaxBytes int64  `json:"segment_max_bytes"`
}

func Default() Config {
	return Config{
		GRPC: GRPCConfig{
			Address: ":9090",
		},
		HTTP: HTTPConfig{
			Address: ":8080",
		},
		Storage: StorageConfig{
			DataDir:         "./data",
			SegmentMaxBytes: 1 << 20,
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config file: %w", err)
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config file: %w", err)
	}

	return cfg, nil
}
