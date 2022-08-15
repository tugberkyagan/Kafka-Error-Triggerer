package utils

import (
	"fmt"
	"github.com/spf13/viper"
)

type ConfigStruct struct {
	Couchbase struct {
		Address  string `json:"address" mapstucture:"address"`
		Port     string `json:"port"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"couchbase" mapstucture:"couchbase"`
	Server struct {
		Port string `json:"port"`
	} `json:"server"`
	Consumers []ConsumerConfig `json:"consumers"`
}

type ConsumerConfig struct {
	ID          int    `json:"id"`
	Broker      string `json:"broker"`
	TopicName   string `json:"topicName"`
	GroupID     string `json:"groupId"`
	MaxBytes    int    `json:"maxBytes"`
	TargetTopic struct {
		Network   string `json:"network"`
		Address   string `json:"address"`
		TopicName string `json:"topicName"`
	} `json:"targetTopic"`
}

var vp *viper.Viper

var Config ConfigStruct

func LoadConfig() (ConfigStruct, error) {
	vp = viper.New()

	vp.AddConfigPath("./utils")
	vp.SetConfigName("config")
	vp.SetConfigType("json")
	err := vp.ReadInConfig()

	if err != nil {
		return ConfigStruct{}, err
	}

	err = vp.Unmarshal(&Config)

	if err != nil {
		return ConfigStruct{}, err
	}

	fmt.Println(Config)

	return Config, nil
}
