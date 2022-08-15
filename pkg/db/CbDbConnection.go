package db

import (
	"kafka-error-triggerer/utils"
	"log"
	"time"

	"github.com/couchbase/gocb/v2"
)

func Start() (*gocb.Cluster, *gocb.Collection) {

	username := utils.Config.Couchbase.Username
	password := utils.Config.Couchbase.Password

	// Initialize the Connection
	cluster, err := gocb.Connect(utils.Config.Couchbase.Address, gocb.ClusterOptions{

		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
		TimeoutsConfig: gocb.TimeoutsConfig{
			KVTimeout: time.Duration(5) * time.Second,
		},
		SecurityConfig: gocb.SecurityConfig{
			TLSSkipVerify: true,
		},
	})

	if err != nil {
		log.Fatal(err)
	}

	collection := cluster.Bucket("errors").Scope("_default").Collection("_default")

	return cluster, collection
}
