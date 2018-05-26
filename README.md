# go-rabbitmq

### Consists of a consumer and a producer.


Requires a running RabbitMQ instance (https://www.rabbitmq.com/download.html)

1. Clone the repo to your `$GOPATH/src`
2. Run `go install go-rabbitmq/cmd/consumerClient`
3. Run `go install go-rabbitmq/cmd/producerClient`
4. Run `$GOPATH/bin/consumerClient` to start the consumer
5. Run `$GOPATH/bin/producerClient` to produce a message
