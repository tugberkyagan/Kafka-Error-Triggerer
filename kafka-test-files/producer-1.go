package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"kafka-error-triggerer/pkg/models"
	"kafka-error-triggerer/utils"
	"log"
	"time"
)

func main() {
	topic := "earth.international-test"

	config := utils.NewConfiguration()

	w := &kafka.Writer{
		Addr:     kafka.TCP(config.Kafka.Brokers),
		Balancer: &kafka.LeastBytes{},
	}

	kafkaMessage1ValueSample := models.KafkaMessageValueModel{
		Xyz: 14,
		Abc: "abc",
	}

	marshalledKafkaMessage1ValueSample, err := json.Marshal(kafkaMessage1ValueSample)

	if err != nil {
		println("Marshall error on marshalledKafkaMessage1ValueSample in producer-1: ", err)
	}

	message1Key := []byte("zzzzzzzz")

	headerList := make([]kafka.Header, 0)

	header1Value, err := json.Marshal(11)
	if err != nil {
		println(err)
	}

	headerList = append(headerList, kafka.Header{
		Key:   "Deneme header key1",
		Value: header1Value,
	})

	header2Value := []byte("denemeee")

	headerList = append(headerList, kafka.Header{
		Key:   "denemeHeaderKey2",
		Value: header2Value,
	})

	message1 := models.KafkaMessageModel{
		Topic:         topic,
		Partition:     0,
		Offset:        11,
		HighWaterMark: 1,
		Key:           message1Key,
		Value:         marshalledKafkaMessage1ValueSample,
		Headers:       headerList,
		Time:          time.Now(),
	}

	//------------------
	kafkaMessage2ValueSample := models.KafkaMessageValueModel{
		Xyz: 14,
		Abc: "abc",
	}

	marshalledKafkaMessage2ValueSample, err := json.Marshal(kafkaMessage2ValueSample)
	if err != nil {
		println(err)
	}

	message2Key := []byte("1test2")

	headerList2 := make([]kafka.Header, 0)

	header3Value, err := json.Marshal(22)

	if err != nil {
		println(err.Error())
	}

	headerList2 = append(headerList2, kafka.Header{
		Key:   "denemeHeaderKey3",
		Value: header3Value,
	})

	header4Value := []byte("denemeee")

	headerList2 = append(headerList2, kafka.Header{
		Key:   "denemeHeaderKey4",
		Value: header4Value,
	})

	message2 := models.KafkaMessageModel{
		Topic:         topic,
		Partition:     0,
		Offset:        1111,
		HighWaterMark: 1,
		Key:           message2Key,
		Value:         marshalledKafkaMessage2ValueSample,
		Headers:       headerList2,
		Time:          time.Now(),
	}

	err = w.WriteMessages(context.Background(),
		// NOTE: Each Message has Topic defined, otherwise an error is returned.
		kafka.Message{
			Topic:     topic,
			Partition: message1.Partition,
			Key:       message1.Key,
			Value:     message1.Value,
			Headers:   message1.Headers,
			Time:      message1.Time,
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err.Error())
	}

	err = w.WriteMessages(context.Background(),
		// NOTE: Each Message has Topic defined, otherwise an error is returned.
		kafka.Message{
			Topic:     topic,
			Partition: message2.Partition,
			Key:       message2.Key,
			Value:     message2.Value,
			Headers:   message2.Headers,
			Time:      message2.Time,
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err.Error())
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err.Error())
	}
}
