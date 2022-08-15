## *KAFKA ERROR TRIGGERER*

### Summary
##### In order to scale our systems, we communicate via async(messaging systems) as much as possible, instead of sync(http request). For the messaging system, we actively use Kafka. For more information on Kafka please visit the [documentation](https://kafka.apache.org/documentation/). We listen to messages over topics. If an error occurs while reading or processing a message, we send that message to the error topic. Within the scope of this project, we need to see messages in the error topics and in some cases in order to be able to reprocess the messages in the error topic, we need to transfer these messages back to the main topic. Within the scope of this project, we will be able to move the messages back to the main topic.

<hr/>

### How you can run the project?
##### After cloning the project to your local, you can write as many 'consumer' as the number of consumers you want to create in the 'config.json' file. Then, you can write the topics that you want these consumers to listen to and the topic to which the message to be sent to another target topic will be sent to the 'target topic' field. Again on the same file, you need to specify the port where you want the project to run and the port to be connected to your database. Then, in order for the 'Kafka', 'ZooKeeper' and 'Kowl' programs in the project to run, you should run the 'docker-compose up -d' command from your terminal on the project directory. Then again, you can start the consumers to listen to the topics specified in the 'config.json' file by typing and running the 'go run cmd/main.go' command in the terminal. If a message comes to the determined topic, it will both write this message to CouchbaseDB and reflect it on the screen in the UI section.

<hr/>

### Which technologies were used while developing the project?
##### - Golang 
##### - CouchbaseDB
##### - Kafka
##### - Zookeeper
##### - Kowl
##### - React

<hr/>

### Future software developments
##### -Unit tests:
###### It will be useful to write unit tests in order to test all functions in the project at the smallest level and to minimize internal errors.
##### -Full text search
###### While the number of messages listened to from the consumer is low, it will not be difficult to find the specifically sought message among them. However, as the number of messages increases, it will be harder to find a specific message. In order to overcome this situation, 'full text search' development would be very useful for the project.
##### -TTL (Time to live)
###### Messages received by consumers or consumers over various topics are recorded in the database. However, these records can overwhelm the database with too much data after a long period of time. In order to overcome this, the 'Time to live' feature can be added to the project and the messages that remain in the system for a certain period of time can be deleted from the database.

<hr/>

### Developers of the project
##### Trendyol Product International Team

