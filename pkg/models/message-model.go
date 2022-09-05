package models

import "time"

type MessageModel struct {
	Topic       string `json:"topic"`
	TargetTopic string `json:"targetTopic"`
	Partition   int    `json:"partition"`
	Offset      int64  `json:"offset"`
	Key         string `json:"key"`
	Value       string `json:"value"`
	Headers     []MyHeader
	Time        time.Time `json:"time"`
}

type MyHeader struct {
	Key   string
	Value interface{}
}
