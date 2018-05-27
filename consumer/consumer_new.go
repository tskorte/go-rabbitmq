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

func (c *Consumer) dialAMQP(amqpURI string) {
	var err error
	l.Debugf(fmt.Sprintf("Dialling %q", amqpURI))
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		log.Fatalf("Unable to dial AMQP: %s", err)
	}
	l.Debugf("Got connection")
}

func (c *Consumer) setupChannel() {
	l.Debugf("Getting channel")
	var err error
	c.channel, err = c.conn.Channel()
	if err != nil {
		fmt.Printf("Channel: %s", err)
	}

	l.Debug("Got channel")
}

func (c *Consumer) declareExchange(en string, et string) {
	l.Debugf(fmt.Sprintf("Declaring Exchange (%q)", en))
	var err error
	err = c.channel.ExchangeDeclare(
		en,
		et,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Exchange Declare: %s", err)
	}

	l.Debugf("Declared exchange")
}

func (c *Consumer) declareQueue(qn string) {
	l.Debugf(fmt.Sprintf("Declaring queue %q", qn))
	queue, err := c.channel.QueueDeclare(
		qn,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Queue declare: %s", err)
	}

	l.Debugf(
		fmt.Sprintf(
			"Declared queue (%q, %d messages, %d consumers)",
			queue.Name,
			queue.Messages,
			queue.Consumers,
		),
	)
}

func (c *Consumer) bindQueue(qn string, key string, en string) {
	l.Debugf(
		fmt.Sprintf(
			"Binding to Exchange key %q",
			key,
		),
	)

	err := c.channel.QueueBind(
		qn,
		key,
		en,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Queue bind: %s", err)
	}
	l.Debugf("Queue bound to exchange")
}

func (c *Consumer) startConsuming(qn string) <-chan amqp.Delivery {
	l.Debugf(
		fmt.Sprintf("Starting consume (consumer tag %q)", c.tag),
	)
	deliveries, err := c.channel.Consume(
		qn,
		c.tag,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		l.Errorf("Queue consume: %s", err)
	}
	return deliveries
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

	c.dialAMQP(amqpURI)
	go func() {
		fmt.Printf(" [⬇️] Closing: %s ", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()
	c.setupChannel()
	c.declareExchange(exchange, exchangeType)
	c.declareQueue(queueName)
	c.bindQueue(queueName, key, exchange)
	deliveries := c.startConsuming(queueName)
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
	l.Debugf(
		" [▶️] %dB - [%v] %q",
		len(delivery.Body),
		delivery.DeliveryTag,
		delivery.Body,
	)
}
