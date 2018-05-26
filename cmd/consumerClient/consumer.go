package main

import (
	"flag"
	"fmt"
	"go-rabbitmq/consumer"
	"log"
	"time"

	"github.com/happierall/l"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	queue        = flag.String("queue", "test-queue", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("key", "test-key", "AMQP binding key")
	consumerTag  = flag.String("consumer-tag", "simple-consumer", "AMQP consumer tag (should not be blank)")
	lifetime     = flag.Duration("lifetime", 0, "lifetime of process before shutdown (0s=infinite)")
)

func init() {
	flag.Parse()
}

func main() {
	c, err := consumer.NewConsumer(
		*uri,
		*exchange,
		*exchangeType,
		*queue,
		*bindingKey,
		*consumerTag,
	)
	if err != nil {
		l.Error(err)
	}

	if *lifetime > 0 {
		l.Logf(fmt.Sprintf("Consumer running for %s", *lifetime))
		time.Sleep(*lifetime)
	} else {
		l.Logf("Consumer running forever")
		select {}
	}

	if err := c.Shutdown(); err != nil {
		log.Fatalf("Error during shutdown: %s", err)
	}
}
