package redis

import (
	"fmt"
	"log"
	"time"

	"github.com/Arbaz/tcp/config"
	"github.com/go-redis/redis"
)

// Redis Client
type Client struct {
	*redis.Client
	globalCursor uint64 // Added global cursor to maintain state across calls
}

func NewClient(cfg config.RedisConfig) (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:        cfg.Address,
		DialTimeout: 5 * time.Second,
		MaxRetries:  3,
	})

	// Attempt to ping the Redis server with retries
	var err error
	for i := 0; i < 3; i++ {
		_, err = client.Ping().Result()
		if err == nil {
			break
		}
		time.Sleep(time.Second * 2)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis at %s after 3 attempts: %v", cfg.Address, err)
	}

	// Flush the Redis database upon successful connection
	if err := client.FlushDB().Err(); err != nil {
		return nil, fmt.Errorf("failed to flush Redis database: %v", err)
	}

	// Print the number of records in the Redis client
	recordCount, err := client.DBSize().Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get record count: %v", err)
	}
	log.Println("Connected to redis client")
	log.Printf("Number of records in Redis client: %d", recordCount)

	return &Client{Client: client}, nil
}

func (c *Client) SimulateData(start, end int) error {
	// To Batch Multiple Commands
	pipe := c.Pipeline()

	for i := start; i < end; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("data-%d", i)
		pipe.Set(key, value, 0)
	}

	// Execute all the batched commands
	_, err := pipe.Exec()
	if err != nil {
		return fmt.Errorf("failed to simulate data: %v", err)
	}

	return nil
}

// Update Redis Records
func (c *Client) UpdateRecord(key, value string) error {
	err := c.Set(key, value, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to update record %s: %v", key, err)
	}
	return nil
}

func (c *Client) GetRecords(count int) ([]string, error) {
	var keys []string

	// Use the global cursor to fetch records sequentially
	for len(keys) < count {
		// Scan for keys matching the pattern "key:*"
		scanKeys, nextCursor, err := c.Scan(c.globalCursor, "key*", int64(count)).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan keys: %v", err)
		}
		keys = append(keys, scanKeys...) // Append the scanned keys to the keys slice
		c.globalCursor = nextCursor      // Update the global cursor for the next iteration
		if c.globalCursor == 0 {         // Break if there are no more keys to scan
			break
		}
	}

	// Limit the number of keys to the requested count
	if len(keys) > count {
		keys = keys[:count]
	}

	// Fetch the values for the keys using a pipeline
	pipe := c.Client.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.Get(key) // Queue the GET commands for the keys
	}
	if _, err := pipe.Exec(); err != nil {
		return nil, fmt.Errorf("failed to get records: %v", err)
	}
	// Collect results
	records := make([]string, len(keys))
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			log.Printf("Error fetching key %s: %v", keys[i], err)
			continue
		}
		records[i] = cmd.Val() // Store the value in the records slice
	}

	return records, nil // Return the collected records
}

// Get the count of all the records in Redis
func (c *Client) GetRecordCount() (int64, error) {
	return c.DBSize().Result()
}
