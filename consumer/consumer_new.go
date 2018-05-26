package consumer

import (
	"fmt"
	"log"

	"github.com/happierall/l"
	"github.com/streadway/amqp"
)

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
	l.Debugf(fmt.Sprintf("Dialling %q", amqpURI))
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, err
	}

	go func() {
		fmt.Printf(" [⬇️] Closing: %s ", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	l.Debugf("Got connection")
	l.Debugf("Getting channel")

	c.channel, err = c.conn.Channel()
	if err != nil {
		fmt.Printf("Channel: %s", err)
	}

	l.Debugf("Got channel")
	l.Debugf(fmt.Sprintf("Declaring Exchange (%q)", exchange))

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

	l.Debugf("Declared exchange")
	l.Debugf(fmt.Sprintf("Declaring queue %q", queueName))
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

	l.Debugf(
		fmt.Sprintf(
			"Declared queue (%q, %d messages, %d consumers)",
			queue.Name,
			queue.Messages,
			queue.Consumers,
		),
	)

	l.Debugf(
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
	l.Debugf("Queue bound to exchange")
	l.Debugf(
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

	defer l.Debugf("AMQP Shutdown OK")
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
