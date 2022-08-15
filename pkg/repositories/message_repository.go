package repositories

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"kafka-error-triggerer/pkg/models"
)

type MessageRepositoryInterface interface {
	GetAllMessages() ([]models.MessageModel, error)
	GetMessageByKey(key string) (models.MessageModel, error)
	UpsertMessage(newMessage models.MessageModel) error
	DeleteMessage(key string) error
	GetMessagesByTopic(topicName string) ([]map[string]interface{}, error)
	// TextSearch(text string) (*gocb.SearchResult, error)
}

type messageRepoStruct struct {
	cluster    *gocb.Cluster
	collection *gocb.Collection
	options    *gocb.GetOptions
}

func NewMessageRepository(cluster *gocb.Cluster, collection *gocb.Collection) MessageRepositoryInterface {
	return messageRepoStruct{cluster: cluster, collection: collection, options: &gocb.GetOptions{}}
}

func (r messageRepoStruct) GetAllMessages() ([]models.MessageModel, error) {
	queryResult, err := r.cluster.Query(
		fmt.Sprintf("SELECT * FROM %s", "errors._default._default"), nil,
	)

	if err != nil {
		return nil, err
	}

	var resultList []models.MessageModel // []map[string]interface{}

	for queryResult.Next() {

		result := map[string]models.MessageModel{} // map[string]interface{}
		err := queryResult.Row(&result)

		if err != nil {
			return nil, err
		}

		resultList = append(resultList, result["_default"])
	}

	return resultList, nil
}

func (r messageRepoStruct) GetMessageByKey(key string) (models.MessageModel, error) {

	result := models.MessageModel{}

	queryResult, err := r.collection.Get(key, r.options)

	if err != nil {
		println("An error occurs while 'GetMessageByKey' processing on DB", err)
	}

	err = queryResult.Content(&result)

	if err != nil {
		println("An error occurs while 'GetMessageByKey' mapping process on repository", err)
		return result, err
	}

	return result, err
}

func (r messageRepoStruct) UpsertMessage(message models.MessageModel) error {

	_, err := r.collection.Upsert(message.Key, message, nil)

	if err != nil {
		fmt.Println("Upsert error", err)

		return err
	}

	return nil
}

func (r messageRepoStruct) DeleteMessage(key string) error {

	_, err := r.collection.Remove(key, nil)

	if err != nil {
		fmt.Println("Delete error", err)

		return err
	}

	return nil
}

func (r messageRepoStruct) GetMessagesByTopic(topicName string) ([]map[string]interface{}, error) {

	queryResult, err := r.cluster.Query(
		fmt.Sprintf("SELECT * FROM %s WHERE TopicName = '%s'", "errors._default._default", topicName), nil,
	)

	if err != nil {
		return nil, err
	}

	var resultList []map[string]interface{}

	for queryResult.Next() {

		var result map[string]interface{}
		err := queryResult.Row(&result)

		if err != nil {
			return nil, err
		}

		resultList = append(resultList, result)
	}

	return resultList, nil
}

/* func (r messageRepoStruct) TextSearch(text string) (*gocb.SearchResult, error) {
	matchResult, err := r.cluster.SearchQuery(
		"Value",
		search.NewMatchQuery(text),
		&gocb.SearchOptions{
			//Limit:  10,
			//Fields: []string{"description"},
		},
	)
	if err != nil {
		println("An error occurred in searching process")
	}

	println(matchResult)

	return matchResult, err
}*/
