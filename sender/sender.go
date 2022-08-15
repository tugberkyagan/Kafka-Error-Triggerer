package sender

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"kafka-error-triggerer/pkg/models"
	"kafka-error-triggerer/utils"
	"log"
)

func SendMessageToAnotherTopic(reproducedMessage models.MessageModel, consumerConfig utils.ConsumerConfig) {

	w := &kafka.Writer{
		Addr: kafka.TCP(consumerConfig.Broker),
		// NOTE: When Topic is not defined here, each Message must define it instead.
		Balancer: &kafka.LeastBytes{},
	}

	reproducedMessageKey, err := json.Marshal(reproducedMessage.Key)

	if err != nil {
		println("Key Converter Error in Sender", err.Error())
	}

	reproducedMessageValue, err := json.Marshal(reproducedMessage.Value)

	if err != nil {
		println("Value Converter Error in Sender", err.Error())
	}

	headersList := make([]kafka.Header, 0)

	for _, element := range reproducedMessage.Headers {

		messageHeadersValue, _ := json.Marshal(element.Value)

		if err != nil {
			println("Marshalled message error on value in sender")
		}

		newHeader := kafka.Header{
			Key:   element.Key,
			Value: messageHeadersValue,
		}

		headersList = append(headersList, newHeader)
	}

	err = w.WriteMessages(context.Background(),
		// NOTE: Each Message has Topic defined, otherwise an error is returned.
		kafka.Message{
			Topic:     consumerConfig.TargetTopic.TopicName,
			Partition: reproducedMessage.Partition,
			Key:       reproducedMessageKey,
			Value:     reproducedMessageValue,
			Headers:   headersList,
		},
	)

	if err != nil {
		log.Println("failed to write messages in sender:", err.Error())
	}

	if err := w.Close(); err != nil {
		log.Println("failed to close writer in sender:", err.Error())
	}

}
