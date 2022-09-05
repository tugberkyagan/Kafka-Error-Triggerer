package consumer

import (
	"kafka-error-triggerer/pkg/repositories"
	"kafka-error-triggerer/utils"
)

func ConsumerGenerator(messageRepo repositories.MessageRepositoryInterface, config utils.Configuration) {

	for _, consumer := range config.Consumers {
		go StartKafkaConsumer(messageRepo, config, consumer)
	}

}
