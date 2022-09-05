package db

import (
	"kafka-error-triggerer/utils"
	"log"
	"time"

	"github.com/couchbase/gocb/v2"
)

func Start(config utils.Configuration) (*gocb.Cluster, *gocb.Collection) {

	username := config.Couchbase.UserName
	password := config.Couchbase.Password

	// Initialize the Connection
	cluster, err := gocb.Connect(config.Couchbase.Address, gocb.ClusterOptions{

		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
		TimeoutsConfig: gocb.TimeoutsConfig{
			KVTimeout: time.Duration(10) * time.Second,
		},
		SecurityConfig: gocb.SecurityConfig{
			TLSSkipVerify: true,
		},
	})

	if err != nil {
		log.Fatal(err)
	}

	collection := cluster.Bucket(config.Couchbase.BucketName).Scope(config.Couchbase.ScopeName).Collection(config.Couchbase.CollectionName)

	return cluster, collection
}
