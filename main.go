package main

import (
	"context"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	// Set up Kafka reader configuration

	// Define Kafka topics to consume messages from
	topics := []string{"topic1", "AD_DEV_LogEvent", "DEV_LogEvent"} // Add your topics here

	// Create Kafka readers for the given topics
	readers := make([]*kafka.Reader, len(topics))
	for i, topic := range topics {
		config := kafka.ReaderConfig{
			Brokers:  []string{"localhost:9092"},
			GroupID:  "go-good-kafka",
			MinBytes: 1,   // 1KB
			MaxBytes: 1e6, // 1MB
		}

		config.Topic = topic
		r := kafka.NewReader(config)
		r.SetOffset(kafka.FirstOffset)
		readers[i] = r
		defer readers[i].Close()
	}

	// Handle OS signals for graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	//out := make(chan *kafka.Message)

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background()) //make them get cancelled by the same func and wait on the wg

	for _, reader := range readers {
		wg.Add(1)

		go consumeMe(&ctx, cancel, &wg, reader)

	}
	// Consume messages
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	cancel()
	wg.Wait()
}

func consumeMe(ctx *context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, reader *kafka.Reader) {
	var processed int = 0
	defer wg.Done()
	defer cancel()
	for {
		msg, err := reader.FetchMessage(*ctx)
		if err != nil {
			if err.Error() == "context canceled" || err.Error() == "EOF" {
				break
			}
			log.Printf("Error fetching message: %v", err)
			continue
		}
		fmt.Printf("Received message from topic %s: %s\n", msg.Topic, string(msg.Value))
		processed++
		err = reader.CommitMessages(*ctx, msg)
		if err != nil {
			log.Printf("Error committing message: %v", err)
		}
		time.Sleep(10000 * time.Millisecond)
	}
	defer log.Printf("Processed: %d messages\n", processed)
}
