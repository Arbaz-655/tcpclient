package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	RedisConfig  RedisConfig  `yaml:"redis"`
	ClientConfig ClientConfig `yaml:"client"`
}

type RedisConfig struct {
	Address  string `yaml:"address"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type ClientConfig struct {
	MaxConnections int            `yaml:"max_connections"`
	MaxRequests    int            `yaml:"max_requests"`
	TPS            map[string]int `yaml:"tps"`
	Address        string         `yaml:"address"`
	Ports          []int
}

func Load(filename string) (*Config, error) {
	// Read the YAML file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	// Create a Config struct to hold the parsed data
	var config Config

	// Unmarshal the YAML into the Config struct
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("error parsing config file: %v", err)
	}

	// Validate the configuration
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %v", err)
	}

	return &config, nil
}

func validateConfig(config *Config) error {
	// Validate Redis configuration
	if config.RedisConfig.Address == "" {
		return fmt.Errorf("redis address is required")
	}
	// Validate Client configuration
	if config.ClientConfig.MaxConnections <= 0 {
		return fmt.Errorf("max_connections must be greater than 0")
	}
	if config.ClientConfig.MaxRequests <= 0 {
		return fmt.Errorf("max_requests must be greater than 0")
	}
	if len(config.ClientConfig.TPS) == 0 {
		return fmt.Errorf("at least one TPS configuration is required")
	}
	// Validate Server configuration
	if config.ClientConfig.Address == "" {
		return fmt.Errorf("server address is required")
	}
	if len(config.ClientConfig.Ports) == 0 {
		return fmt.Errorf("at least one server port is required")
	}

	return nil
}
