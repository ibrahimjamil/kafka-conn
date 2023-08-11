package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	c "github.com/kafka/consumer"
	pp "github.com/kafka/producer"
)

func main() {
	topic := "HVSE"
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "foo",
		"acks":              "all"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	// listen consumer with goroutine
	c.Consumer()

	// send producer with messages
	op := pp.NewOrderPlacer(p, topic)
	for i := 0; i < 1000; i++ {
		err = op.PlaceOrder("1 billion", i+1)
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(time.Second * 1)
	}
}
