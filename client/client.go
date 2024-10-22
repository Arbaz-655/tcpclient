package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Arbaz/tcp/config"
	"github.com/Arbaz/tcp/redis"
)

/*
Reads the TPS config, pumps the corresponding number of records from the Redis store every second.
Establishes multiple TCP socket connections to a server (up to 10 connections max).
Ensures each connection sends a maximum of 100 requests.
The TCP client application should map each response received from the server back to the correct record in Redis.
Implement concurrency using Goroutines, worker pools, and channels to handle rate limiting and processing.
*/

/*
The Client struct manages the client's configuration and state.
*/
type Client struct {
	cfg         config.ClientConfig
	redisClient *redis.Client
	connections []*net.TCPConn
	workerPool  chan struct{}
}

/*
The NewClient function initializes a Client instance with the provided configuration and Redis client.
*/
func NewClient(cfg config.ClientConfig, redisClient *redis.Client) *Client {
	return &Client{
		cfg:         cfg,
		redisClient: redisClient,
		connections: make([]*net.TCPConn, 0, cfg.MaxConnections),
		workerPool:  make(chan struct{}, cfg.MaxConnections),
	}
}

/*
GetCurrentTPS retrieves the TPS value for the given second.
The second parameter should be in the format "s1", "s2", etc.
*/
func GetCurrentTPS(cfg config.ClientConfig, second int) (int, error) {
	key := fmt.Sprintf("s%d", second+1) // Create the key based on the second
	tps, exists := cfg.TPS[key]
	if !exists {
		return 0, fmt.Errorf("TPS configuration for %s not found", key)
	}
	return tps, nil
}

/*
The Run method establishes multiple TCP connections (up to the configured maximum)
and starts a worker goroutine for each connection.
*/
func (c *Client) Run() error {
	const maxWorkers = 10
	var wg sync.WaitGroup

	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Establish TCP connections
	for i := 0; i < c.cfg.MaxConnections; i++ {
		addr := fmt.Sprintf("%s:%d", c.cfg.Address, c.cfg.Ports[i%len(c.cfg.Ports)]) // Use address and port from config
		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			log.Fatalf("Failed to resolve address: %v", err)
		}

		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			log.Fatalf("Failed to connect to server: %v", err)
		}
		c.connections = append(c.connections, conn)
		log.Printf("Connected successfully to server %s:%d", c.cfg.Address, c.cfg.Ports[i%len(c.cfg.Ports)])
	}

	// Create a channel to send records to workers
	recordChan := make(chan string, 10000) // Since max 1000 request sent per second
	stopChan := make(chan struct{})        // Channel to signal when to stop workers

	// Start worker goroutines
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.worker(workerID, recordChan, stopChan)
		}(i)
	}

	// Get the total number of records in Redis
	totalRecords, err := c.redisClient.GetRecordCount()
	if err != nil {
		log.Printf("Error getting total record count: %v", err)
		return err
	}
	log.Println("Total Records in Redis", totalRecords)

	fetchedRecords := 0         // Track the number of fetched records
	lastFetchTime := time.Now() // Track the last fetch time

	// Main loop to fetch records from Redis and send to workers
	for {
		currentTime := time.Now()
		if currentTime.Sub(lastFetchTime) >= time.Second { // Check if at least one second has passed
			second := currentTime.Second()
			// Get the count of records to fetch based on TPS
			tps, err := GetCurrentTPS(cfg.ClientConfig, second)
			if err != nil {
				log.Printf("Error getting TPS for second %d: %v", second, err)
				break
			}
			log.Printf("Current TPS for second %d: %d", second, tps)

			// Fetch records from Redis based on TPS
			// Calculate the number of records to fetch based on TPS or the remaining records
			recordsToFetch := tps
			if remainingRecords := int(totalRecords) - fetchedRecords; remainingRecords < tps {
				recordsToFetch = remainingRecords
			}

			records, err := c.redisClient.GetRecords(recordsToFetch) // Fetch only the number of records specified by TPS
			if err != nil {
				log.Printf("Error getting records: %v", err)
				break
			}
			log.Printf("Fetched %d records from Redis", len(records))
			lastFetchTime = currentTime

			// Send records to the worker channel
			for _, record := range records {
				recordChan <- record // Send record to the channel
			}

			fetchedRecords += len(records)

			// Check if all records have been fetched
			if fetchedRecords >= int(totalRecords) { // Convert totalRecords to int
				log.Printf("All records have been fetched from Redis.")
				break
			}
		}
	}

	// Check if the record channel is empty
	for {
		if len(recordChan) == 0 {
			time.Sleep(1 * time.Minute)
			//Adding delay of 1 Minute to process response
			if len(recordChan) == 0 {
				break
			}
			continue
		}
		time.Sleep(5 * time.Second)
	}

	close(recordChan)
	// Close the stop channel to signal workers to stop
	close(stopChan)

	// Close all connections
	for _, conn := range c.connections {
		conn.Close()
	}

	return nil
}

// Add other necessary methods

type Request struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Response struct {
	ID    uint64 `json:"id"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

/*
The RateLimiter struct and its Wait method implement rate limiting based on the TPS configuration.
It uses the current second as a key to determine the appropriate rate.
*/
type RateLimiter struct {
	tps    int
	ticker *time.Ticker
	tokens chan struct{}
}

/*
NewRateLimiter initializes a RateLimiter with a specified TPS.
*/
func NewRateLimiter(tps int) *RateLimiter {
	rl := &RateLimiter{
		tps:    tps,
		ticker: time.NewTicker(time.Second),
		tokens: make(chan struct{}, tps), // Allow up to tps tokens
	}
	go rl.generateTokens()
	return rl
}

func (rl *RateLimiter) generateTokens() {
	for range rl.ticker.C {
		for i := 0; i < rl.tps; i++ {
			select {
			case rl.tokens <- struct{}{}:
			default:
			}
		}
	}
}

func (rl *RateLimiter) Wait() {
	<-rl.tokens
}

/*
The worker method is a worker goroutine that sends requests to the server and updates Redis.
It handles rate limiting, connection management, and request/response processing.
*/
func (c *Client) worker(id int, recordChan chan string, stopChan chan struct{}) {
	conn := c.connections[id]
	defer conn.Close()

	// Initialize a rate limiter for this worker
	workerRateLimiter := NewRateLimiter(100) // Each worker can send 100 requests per second
	scanner := bufio.NewScanner(conn)

	requestsSent := 0         // Counter for requests sent
	responsesReceived := 0    // Counter for responses received
	recordChanClosed := false // Flag to track if recordChan is closed

	for {
		select {
		case record, ok := <-recordChan:
			if !ok {
				log.Printf("Worker %d: Record channel closed.\n", id)
				recordChanClosed = true // Set the flag when the record channel is closed
				continue                // Continue to check for responses
			}

			// Wait for a token to ensure rate limiting for this worker
			workerRateLimiter.Wait() // Ensure only 100 requests per second for this worker

			// Prepare request
			req := Request{
				Key:   fmt.Sprintf("record:%s", record),
				Value: record,
			}

			// Send request to server
			reqJSON, err := json.Marshal(req)
			if err != nil {
				log.Printf("Worker %d: Failed to marshal request: %v\n", id, err)
				return
			}

			_, err = conn.Write(append(reqJSON, '\n'))
			if err != nil {
				log.Printf("Worker %d: Failed to send request: %v\n", id, err)
				return
			}

			log.Printf("Worker %d: Successfully sent request %s\n", id, req.Key)
			requestsSent++ // Increment the requests sent counter

			// Check for responses immediately after sending the request
			if scanner.Scan() {
				rawResponse := scanner.Bytes() // Get the raw response
				if len(rawResponse) == 0 {
					log.Printf("Worker %d: Received empty response\n", id)
					continue
				}

				var resp Response
				if err := json.Unmarshal(rawResponse, &resp); err != nil {
					log.Printf("Worker %d: Failed to unmarshal response: %v\n", id, err)
					return
				}

				// Verify the response matches the request
				if resp.Key != req.Key {
					log.Printf("Worker %d: Response key mismatch. Expected: %s, Got: %s\n", id, req.Key, resp.Key)
					return
				}

				// Update Redis with the response
				updatedValue := fmt.Sprintf("%s:ID:%d", record, resp.ID)
				err = c.redisClient.UpdateRecord(req.Key, updatedValue)
				if err != nil {
					log.Printf("Worker %d: Failed to update record %s with response: %v\n", id, req.Key, err)
				} else {
					log.Printf("Worker %d: Successfully updated %s with ID %d\n", id, req.Key, resp.ID)
				}

				responsesReceived++ // Increment the responses received counter
			}

		case <-stopChan:
			log.Printf("Worker %d: Stop signal received.\n", id)
			// If the stop channel is closed, check if we should exit
			if recordChanClosed && responsesReceived == requestsSent {
				log.Printf("Worker %d: All responses received and channels closed, exiting.\n", id)
				return // Exit when all responses have been received and channels are closed
			}
		}
	}
}
