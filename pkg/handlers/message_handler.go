package handlers

import (
	"errors"
	"github.com/gofiber/fiber/v2"
	"kafka-error-triggerer/pkg/models"
	"kafka-error-triggerer/pkg/services"
)

type MessageHandlerInterface interface {
	GetAllMessagesHandler(c *fiber.Ctx) error
	GetMessageByKey(c *fiber.Ctx) error
	ReproduceMessage(c *fiber.Ctx) error
	InsertMessage(c *fiber.Ctx) error
	GetMessagesByTopic(c *fiber.Ctx) error
}

type MessageHandlerStruct struct {
	Service services.MessageServiceInterface
}

func (h MessageHandlerStruct) GetAllMessagesHandler(c *fiber.Ctx) error {

	messages, err := h.Service.GetAllMessagesService()

	if err != nil {
		return err
	}
	return c.JSON(messages)
}

func (h MessageHandlerStruct) GetMessageByKey(c *fiber.Ctx) error {

	key := c.Params("key")

	message, err := h.Service.GetMessageByKey(key)

	if err != nil {
		return err
	}

	return c.JSON(message)
}

func (h MessageHandlerStruct) UpsertMessage(c *fiber.Ctx) error {

	newMessage := &models.MessageModel{}

	if err := c.BodyParser(newMessage); err != nil {
		return err
	}

	response := h.Service.UpsertMessage(*newMessage)

	return response
}

func (h MessageHandlerStruct) ReproduceMessage(c *fiber.Ctx) error {

	key := c.Params("key")

	if key == "" {
		return errors.New("id cannot be empty")
	}

	err := h.Service.ReproduceMessage(key)

	return err
}

func (h MessageHandlerStruct) GetMessagesByTopic(c *fiber.Ctx) error {

	topicName := c.Params("topicName")

	if topicName == "" {
		return errors.New("topicName cannot be empty")
	}

	messages, err := h.Service.GetMessagesByTopic(topicName)

	if err != nil {
		return errors.New("topicName cannot be empty")
	}

	return c.JSON(messages)
}
