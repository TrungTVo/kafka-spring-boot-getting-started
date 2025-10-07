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
