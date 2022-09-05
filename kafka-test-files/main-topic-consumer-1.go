package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafka-error-triggerer/utils"
)

func main() {
	conf := kafka.ReaderConfig{
		Brokers:  []string{utils.Configuration{}.Kafka.Brokers},
		Topic:    "earth.international-test-target",
		GroupID:  "g1",
		MaxBytes: 100}

	reader := kafka.NewReader(conf)

	for {
		m, err := reader.ReadMessage(context.Background())

		if err != nil {
			println("Some errors are seen in reading messages process in consumer", err)
			continue
		}

		fmt.Println("Message is: ", string(m.Headers[2].Value))
	}
}
