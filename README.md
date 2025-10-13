# Spring Kafka with Confluent

## Setup sample Kafka cluster and broker
All of Confluent Kafka config is inside `cp-all-in-one` folder. Run:
```
docker compose -f ./cp-all-in-one/docker-compose.yml up -d
```

## Start Spring Boot app
```
./gradlew clean build
java -jar build/libs/demo-0.0.1-SNAPSHOT.jar
```

## View offset in topic partitions
Go inside `broker` container (image `confluentinc/cp-server`), inspect consumer group:
```
/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 \
	--describe --group myConsumer
```
Offsets:

* `LOG-END-OFFSET`: This is the offset of the last message written to a partition in a Kafka topic, plus one (i.e., it points to the position where the next message will be written). It represents the total number of messages in the partition at a given time
* `CURRENT-OFFSET`: This is the offset of the last message consumed (or committed) by a consumer in a consumer group for a specific partition. It indicates the position up to which the consumer group has processed messages
* `LAG`: difference of `LOG-END-OFFSET - CURRENT-OFFSET`, indicates how many messages have been lagged behind (aka not consumed yet)

To reset offset to earliest, run:
```
/usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 \
	--group myConsumer \
	--topic hello \
	--reset-offsets --to-earliest --execute
```

## APIs
Check these endpoints below to understand how Kafka works

The application exposes a small set of HTTP endpoints under the base path `/kafka`.

All endpoints are defined in `src/main/java/com/example/demo/controllers/SampleController.java`.

### POST /kafka/book/produce

- Description: produce/send a message of `Book` Object type to a Kafka `books` topic via the app's `bookProducer`.
- Request headers: `Content-Type: application/json`
- Request body (JSON):

	```json
	{
		"topic": "books",
		"messageKey": "key",
		"message": { "title": "title", "author": "trung vo" }
	}
	```

- Response: plain text message confirming the topic, e.g. `Message sent to Kafka topic: books` (HTTP 200).

- Example curl:

	```bash
	curl -X POST http://localhost:8080/kafka/book/produce \
		-H "Content-Type: application/json" \
		-d '{"topic":"books","messageKey":"key-1","message":{"title": "title", "author": "trung vo"}}'
	```

### POST /kafka/string/produce

- Description: produce/send a message of string type to a Kafka `hello` topic via the app's `stringProducer`.
- Request headers: `Content-Type: application/json`
- Request body (JSON):

	```json
	{
		"topic": "hello",
		"messageKey": "key",
		"message": "hello world..."
	}
	```

### GET /kafka/consumer/listeners

- Description: list all registered Kafka consumer listeners.
- Response: JSON array of ConsumerListenerModel objects.

- ConsumerListenerModel fields:
	- `listenerId` (string)
	- `groupId` (string)
	- `isRunning` (boolean)
	- `topicPartitions` (object) — map of topic -> list of PartitionInfoDTO

- PartitionInfoDTO fields:
	- `topic` (string)
	- `partition` (number)

- Example curl:

	```bash
	curl http://localhost:8080/kafka/consumer/listeners
	```

### GET /kafka/consumer/{groupId}/members

- Description: get members (consumers) for a consumer group id.
- Path parameter:
	- `groupId` — the consumer group id to inspect.
- Response: JSON array of ConsumerModel objects.

- ConsumerModel fields:
	- `clientId` (string)
	- `groupId` (string)
	- `consumerMemberId` (string)
	- `assignedPartitions` (array of PartitionInfoDTO)

- Example curl:

	```bash
	curl http://localhost:8080/kafka/consumer/my-group/members
	```

### GET /kafka/consumer/{consumerListenerId}/start

- Description: start a specific consumer listener (by listener id).
- Path parameter: `consumerListenerId` — the listener id to start.
- Response: HTTP 200 on success (no response body).

- Example curl:

	```bash
	curl http://localhost:8080/kafka/consumer/my-listener-id/start
	```

### GET /kafka/consumer/{consumerListenerId}/stop

- Description: stop a specific consumer listener (by listener id).
- Response: HTTP 200 on success (no response body).

- Example curl:

	```bash
	curl http://localhost:8080/kafka/consumer/my-listener-id/stop
	```

### GET /kafka/consumer/{consumerListenerId}/pause

- Description: pause consumption for a specific listener.
- Response: HTTP 200 on success (no response body).

- Example curl:

	```bash
	curl http://localhost:8080/kafka/consumer/my-listener-id/pause
	```

### GET /kafka/consumer/{consumerListenerId}/resume

- Description: resume consumption for a specific listener.
- Response: HTTP 200 on success (no response body).

- Example curl:

	```bash
	curl http://localhost:8080/kafka/consumer/my-listener-id/resume
	```

Notes:
- The app uses Spring Boot's default port 8080 unless overridden in `application.yml`.
- The produce endpoint accepts an arbitrary JSON `message` payload — by default the producer will convert it to string using `toString()` and serialize it as configured in the application (check `Producer` implementation).
- The start/stop/pause/resume endpoints perform control actions on in-process listeners; they do not modify Kafka broker state.


