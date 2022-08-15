package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

func main() {
	conf := kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "main-topic-2",
		GroupID:  "g1",
		MaxBytes: 100}

	reader := kafka.NewReader(conf)

	for {
		m, err := reader.ReadMessage(context.Background())

		if err != nil {
			println("Some errors are seen in reading messages process in consumer", err)
			continue
		}

		fmt.Println("Message is: ", string(m.Value))
	}
}
