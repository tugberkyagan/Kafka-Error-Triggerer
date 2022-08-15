package consumer

import (
	"kafka-error-triggerer/pkg/repositories"
	"kafka-error-triggerer/utils"
)

func ConsumerGenerator(messageRepo repositories.MessageRepositoryInterface) {

	for _, consumerConfig := range utils.Config.Consumers {
		go StartKafkaConsumer(messageRepo, consumerConfig)
	}

}
