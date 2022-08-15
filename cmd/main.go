package main

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"kafka-error-triggerer/consumer"
	"kafka-error-triggerer/pkg/db"
	"kafka-error-triggerer/pkg/handlers"
	"kafka-error-triggerer/pkg/repositories"
	"kafka-error-triggerer/pkg/services"
	"kafka-error-triggerer/utils"
	"log"
)

func main() {

	config, err := utils.LoadConfig()

	if err != nil {
		println("An error occurred on config process in main.go file")
	}

	cluster, collection := db.Start()

	messageRepo := repositories.NewMessageRepository(cluster, collection)

	consumer.ConsumerGenerator(messageRepo)

	app := fiber.New()

	messageServices := services.MessageServiceStruct{
		Repository: messageRepo,
	}

	messageHandler := handlers.MessageHandlerStruct{
		Service: messageServices,
	}

	app.Use(cors.New(cors.Config{
		Next:             nil,
		AllowOrigins:     "*",
		AllowMethods:     "GET,POST,HEAD,PUT,DELETE,PATCH",
		AllowHeaders:     "",
		AllowCredentials: false,
		ExposeHeaders:    "",
		MaxAge:           0,
	}))

	app.Get("/message/all", messageHandler.GetAllMessagesHandler)

	app.Get("/message/:key", messageHandler.GetMessageByKey)

	app.Post("/message/insert", messageHandler.UpsertMessage)

	app.Post("/message/reproduce/:key", messageHandler.ReproduceMessage)

	app.Get("/message/by/:topicName", messageHandler.GetMessagesByTopic)

	err = app.Listen(config.Server.Port)

	if err != nil {
		log.Println(err)
	}
}
