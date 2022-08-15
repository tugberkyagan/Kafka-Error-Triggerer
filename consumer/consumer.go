package consumer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafka-error-triggerer/pkg/models"
	"kafka-error-triggerer/pkg/repositories"
	"kafka-error-triggerer/utils"
)

func StartKafkaConsumer(messageRepo repositories.MessageRepositoryInterface, consumerConfig utils.ConsumerConfig) {

	conf := kafka.ReaderConfig{
		Brokers:  []string{consumerConfig.Broker},
		Topic:    consumerConfig.TopicName,
		GroupID:  consumerConfig.GroupID,
		MaxBytes: consumerConfig.MaxBytes,
	}

	reader := kafka.NewReader(conf)

	for {
		m, err := reader.ReadMessage(context.Background())

		if err != nil {
			println("Some errors are seen in reading messages process in consumer: ", err)
			continue
		}

		consumedMessage := models.MessageModel{}

		consumedMessage.Topic = m.Topic
		consumedMessage.Offset = m.Offset
		consumedMessage.Partition = m.Partition

		//decodedMessageValue, _ := base64.StdEncoding.DecodeString(string(m.Value))

		consumedMessage.Value = string(m.Value)

		//decodedMessageKey, _ := base64.StdEncoding.DecodeString(string(m.Key))

		consumedMessage.Key = string(m.Key)

		myHeaders := make([]models.MyHeader, 0)

		isReproducedBefore := false
		hasConsumerID := false

		for _, element := range m.Headers {

			marshalledValue := string(m.Value)

			newHeader := models.MyHeader{
				Key:   element.Key,
				Value: marshalledValue,
			}

			if newHeader.Key == "ReproduceCount" {
				isReproducedBefore = true
			}

			if newHeader.Key == "ConsumerID" {
				hasConsumerID = true
			}

			myHeaders = append(myHeaders, newHeader)
		}

		if !isReproducedBefore {
			newHeader := models.MyHeader{
				Key:   "ReproduceCount",
				Value: 0,
			}

			myHeaders = append(myHeaders, newHeader)
		}

		if !hasConsumerID {
			newHeader := models.MyHeader{
				Key:   "ConsumerID",
				Value: consumerConfig.ID,
			}

			myHeaders = append(myHeaders, newHeader)
		}

		consumedMessage.Headers = myHeaders

		fmt.Println(consumedMessage)

		err = messageRepo.UpsertMessage(consumedMessage)

		if err != nil {
			println("An error occurs at upsert function in consumer: ", err)
		}
	}
}
