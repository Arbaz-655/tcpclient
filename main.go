package main

import (
	"log"
	"os"
	"sync"

	"github.com/Arbaz/tcp/client"
	"github.com/Arbaz/tcp/config"
	"github.com/Arbaz/tcp/redis"
)

/*
The main function setup TCP client.
It loads the configuration, initializes the Redis client, runs the client */

func main() {

	// Create or open the log file
	logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()
	// Set log output to the log file
	log.SetOutput(logFile)

	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize Redis client
	redisClient, err := redis.NewClient(cfg.RedisConfig)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	// Simulate initial data in Redis with progress logging and error handling
	totalRecords := 500000
	batchSize := 10000
	for i := 0; i < totalRecords; i += batchSize {
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}
		err = redisClient.SimulateData(i, end)
		if err != nil {
			log.Printf("Failed to simulate data batch %d-%d: %v", i, end, err)
			continue
		}
	}

	log.Printf("Simulated %d records", totalRecords)

	// Check if any records were actually inserted
	count, err := redisClient.GetRecordCount()
	if err != nil {
		log.Fatalf("Failed to get record count: %v", err)
	}
	if count == 0 {
		log.Fatalf("No records were inserted during simulation")
	}

	// Initialize TCP client
	client := client.NewClient(cfg.ClientConfig, redisClient)

	// Use a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Start client operations
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := client.Run(); err != nil {
			log.Printf("Client error: %v", err)
		}
	}()

	// Wait for all operations to complete
	wg.Wait()
}
