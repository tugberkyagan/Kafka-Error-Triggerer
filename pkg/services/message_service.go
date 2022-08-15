package services

import (
	"fmt"
	"kafka-error-triggerer/pkg/models"
	"kafka-error-triggerer/pkg/repositories"
	"kafka-error-triggerer/sender"
	"kafka-error-triggerer/utils"
	"log"
)

type MessageServiceInterface interface {
	GetAllMessagesService() ([]models.MessageModel, error)
	GetMessageByKey(key string) (models.MessageModel, error)
	ReproduceMessage(key string) error
	UpsertMessage(newMessage models.MessageModel) error
	GetMessagesByTopic(topicName string) ([]map[string]interface{}, error)
}

type MessageServiceStruct struct {
	Repository repositories.MessageRepositoryInterface
}

func (s MessageServiceStruct) GetAllMessagesService() ([]models.MessageModel, error) {

	messages, err := s.Repository.GetAllMessages()

	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (s MessageServiceStruct) GetMessageByKey(key string) (models.MessageModel, error) {

	message, err := s.Repository.GetMessageByKey(key)

	if err != nil {
		println(err)
	}

	return message, nil
}

func (s MessageServiceStruct) UpsertMessage(newMessage models.MessageModel) error {

	newMessages := s.Repository.UpsertMessage(newMessage)

	return newMessages
}

func (s MessageServiceStruct) ReproduceMessage(key string) error {

	message, err := s.Repository.GetMessageByKey(key)

	if err != nil {
		println("Message couldn't get from DB")
		return err
	}

	for _, consumerConfig := range utils.Config.Consumers {

		newHeaderList := make([]models.MyHeader, 0)

		for _, header := range message.Headers {

			if header.Key == "ReproduceCount" {
				reproduceCount, ok := header.Value.(int)

				if !ok {
					println("An error occurs on type converting from interface to int in sender")
				}

				reproduceCount += 1

				header.Value = float64(reproduceCount)
			}

			newHeaderList = append(newHeaderList, header)

		}

		message.Headers = newHeaderList

		sender.SendMessageToAnotherTopic(message, consumerConfig)

		err := s.Repository.UpsertMessage(message)

		if err != nil {
			log.Println(err)
		}
	}

	return err
}

func (s MessageServiceStruct) GetMessagesByTopic(topicName string) ([]map[string]interface{}, error) {

	messages, err := s.Repository.GetMessagesByTopic(topicName)

	if err != nil {
		println(err)
	}

	fmt.Println(messages)

	return messages, nil
}
