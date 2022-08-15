package main

func main() {

	/*	topic := "my-test-topic-2"

		w := &kafka.Writer{
			Addr: kafka.TCP("localhost:9092"),
			// NOTE: When Topic is not defined here, each Message must define it instead.
			Balancer: &kafka.LeastBytes{},
		}

		message1, err := json.Marshal(models.MessageModel{
			Offset:    "2222",
			Partition: 0,
			Timestamp: time.Now(),
			Key:       "whnd223jfn",
			Value: models.Value{
				ReproduceCount: 0,
				ConsumerId:     2,
				TopicName:      topic,
			},
		})

		if err != nil {
			println("Marshal message error in producer-2", err.Error())
		}

		message2, err := json.Marshal(models.MessageModel{
			Offset:    "2222222",
			Partition: 1,
			Timestamp: time.Now(),
			Key:       "2fkjwnkdj439",
			Value: models.Value{
				ReproduceCount: 0,
				ConsumerId:     2,
				TopicName:      topic,
			},
		})

		if err != nil {
			println("Marshal message error in producer-2", err.Error())
		}

		err = w.WriteMessages(context.Background(),
			// NOTE: Each Message has Topic defined, otherwise an error is returned.
			kafka.Message{
				Topic: topic,
				Key:   []byte("Key-A"),
				Value: message1,
			},
		)

		if err != nil {
			log.Fatal("failed to write messages:", err.Error())
		}

		err = w.WriteMessages(context.Background(),
			// NOTE: Each Message has Topic defined, otherwise an error is returned.
			kafka.Message{
				Topic: topic,
				Key:   []byte("Key-A"),
				Value: message2,
			},
		)

		if err := w.Close(); err != nil {
			log.Fatal("failed to close writer:", err.Error())
		}*/

}
