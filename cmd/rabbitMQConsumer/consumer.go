package main

import (
	"flag"
	"fmt"
	"log"
	"rabbitmqtest/utils"
	"time"

	"github.com/streadway/amqp"
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
	c, err := NewConsumer(
		*uri,
		*exchange,
		*exchangeType,
		*queue,
		*bindingKey,
		*consumerTag,
	)
	if err != nil {
		utils.LogError(err.Error())
	}

	if *lifetime > 0 {
		utils.LogWorking(fmt.Sprintf("Consumer running for %s", *lifetime))
		time.Sleep(*lifetime)
	} else {
		utils.LogWorking("Consumer runnning forever")
		select {}
	}

	utils.LogWorking("Shutting down")
	if err := c.Shutdown(); err != nil {
		log.Fatalf("Error during shutdown: %s", err)
	}
}

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func NewConsumer(
	amqpURI,
	exchange,
	exchangeType,
	queueName,
	key,
	ctag string,
) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
	}

	var err error
	utils.LogWorking(fmt.Sprintf("Dialling %q", amqpURI))
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, err
	}

	go func() {
		fmt.Printf(" [⬇️] Closing: %s ", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	utils.LogSuccess("Got connection")
	utils.LogWorking("Getting channel")

	c.channel, err = c.conn.Channel()
	if err != nil {
		utils.LogError(fmt.Sprintf("Channel: %s", err))
	}

	utils.LogSuccess("Got channel")
	utils.LogWorking(fmt.Sprintf("Declaring Exchange (%q)", exchange))

	if err = c.channel.ExchangeDeclare(
		exchange,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	utils.LogSuccess("Declared exchange")
	utils.LogWorking(fmt.Sprintf("Declaring queue %q", queueName))
	queue, err := c.channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("Queue declare: %s", err)
	}

	utils.LogSuccess(
		fmt.Sprintf(
			"Declared queue (%q, %d messages, %d consumers)",
			queue.Name,
			queue.Messages,
			queue.Consumers,
		),
	)

	utils.LogSuccess(
		fmt.Sprintf(
			"Binding to Exchange key %q",
			key,
		),
	)

	if err = c.channel.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	); err != nil {
		return nil, fmt.Errorf("Queue bind: %s", err)
	}
	utils.LogSuccess("Queue bound to exchange")
	utils.LogWorking(
		fmt.Sprintf("Starting consume (consumer tag %q)", c.tag),
	)
	deliveries, err := c.channel.Consume(
		queue.Name,
		c.tag,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("Queue consume: %s", err)
	}

	go handle(deliveries, c.done)
	return c, nil
}

func (c *Consumer) Shutdown() error {
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer utils.LogSuccess("AMQP Shutdown OK")
	return <-c.done
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		LogReceivedMessage(d)
		d.Ack(false)
	}
	done <- nil
}

func LogReceivedMessage(delivery amqp.Delivery) {
	log.Printf(
		" [▶️] %dB - [%v] %q",
		len(delivery.Body),
		delivery.DeliveryTag,
		delivery.Body,
	)
}
