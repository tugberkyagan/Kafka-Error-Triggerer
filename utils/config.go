package utils

import (
	"github.com/spf13/viper"
	"log"
	"os"
)

type Configuration struct {
	Couchbase CouchbaseConfig `yaml:"couchbase"`
	Kafka     KafkaConfig     `yaml:"kafka"`
	Server    ServerConfig    `yaml:"server"`
	Consumers []Consumer      `yaml:"consumers"`
}
type CouchbaseConfig struct {
	Address        string `yaml:"address"`
	UserName       string `yaml:"username"`
	Password       string `yaml:"password"`
	BucketName     string `yaml:"bucketName"`
	ScopeName      string `yaml:"scopeName"`
	CollectionName string `yaml:"collectionName"`
}
type KafkaConfig struct {
	Brokers string `yaml:"brokers"`
}
type ServerConfig struct {
	Port string `yaml:"port"`
}
type Consumer struct {
	TopicName       string `yaml:"topicName"`
	TargetTopicName string `yaml:"targetTopicName"`
}

var vp *viper.Viper

var Config Configuration

func NewConfiguration() *Configuration {
	var configuration Configuration
	env := getEnv()

	viper := viper.New()

	viper.AddConfigPath("utils")
	viper.SetConfigType("yaml")
	viper.SetConfigName("config")

	err := viper.ReadInConfig()
	v := viper.Sub(env)

	if err != nil {
		log.Panic(err.Error())
	}

	err = v.Unmarshal(&configuration)

	return &configuration
}

func getEnv() string {
	env := os.Getenv("ACTIVE_PROFILE")
	if env != "" {
		return env
	}
	return "stage"
}
