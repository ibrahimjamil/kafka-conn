package consumer

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Consumer() {
	topic := "HVSE"
	go func() {
		// consumer code
		// seperate groupID for multiple consumer readings from producer
		consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"group.id":          "foo",
			"auto.offset.reset": "smallest"})

		err := consumer.SubscribeTopics([]string{topic}, nil)
		if err != nil {
			fmt.Printf("Failed to subscribe to topic: %s\n", err)
			os.Exit(1)
		}

		for {
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}()
	return
}
