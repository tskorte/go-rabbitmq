# go-rabbitmq

Consists of a consumer and a producer.

Start by installing RabbitMQ (https://www.rabbitmq.com/download.html)

Clone the repo to your `$GOPATH/src`
Run `go install go-rabbitmq/cmd/consumerClient`
Run `go install go-rabbitmq/cmd/producerClient`
Run `$GOPATH/bin/consumerClient` to start the consumer
Run `$GOPATH/bin/producerClient` to produce a message
