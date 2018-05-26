# go-rabbitmq

### Requirements
A running RabbitMQ instance (https://www.rabbitmq.com/download.html)

### Configuration
The consumerClient and the producerClient reads a json configuration from the users home directory.
```json
{
  "amqpURI": "amqp://guest:guest@localhost:5672/",
  "exchangeName": "test-exchange",
  "exchangeType": "direct",
  "queue": "test-queue",
  "consumerTag": "simple-consumer",
  "consumerLifetime": 0,
  "key": "test-key",
  "reliable": true
}
```
Name the file `.rabbitMQConfig.json`.

### Installation
1. Run `go install go-rabbitmq/cmd/consumerClient`
2. Run `go install go-rabbitmq/cmd/producerClient`
3. Run `$GOPATH/bin/consumerClient` to start the consumer
4. Run `$GOPATH/bin/producerClient` to produce a message

