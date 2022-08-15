package models

import (
	"github.com/segmentio/kafka-go"
	"time"
)

type KafkaMessageModel struct {
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Headers       []kafka.Header
	Time          time.Time
}

type KafkaMessageValueModel struct {
	Xyz int
	Abc string
}
