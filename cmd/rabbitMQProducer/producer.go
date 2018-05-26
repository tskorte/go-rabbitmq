package main

import (
	"flag"
	"fmt"
	"log"
	"rabbitmqtest/utils"

	"github.com/streadway/amqp"
)

var (
	uri = flag.String(
		"uri",
		"amqp://guest:guest@localhost:5672/",
		"AMQP URI",
	)
	exchangeName = flag.String(
		"exchange",
		"text-exchange",
		"Durable AMQP exchange name",
	)
	exchangeType = flag.String(
		"exchangeType",
		"direct",
		"Exchange type - direct|fanout|topic|x-custom",
	)
	routingKey = flag.String(
		"key",
		"test-key",
		"AMQP routing key",
	)
	body = flag.String(
		"body",
		"foobar",
		"Body of message",
	)
	reliable = flag.Bool(
		"reliable",
		true,
		"Wait for the publisher confirmation before exiting",
	)
)

func init() {
	flag.Parse()
}

func main() {
	if err := publish(
		*uri,
		*exchangeName,
		*exchangeType,
		*routingKey,
		*body,
		*reliable,
	); err != nil {
		log.Fatalf(" [🛑] %s", err)
	}
	log.Printf(" [✅] Published %dB OK", len(*body))
}

func publish(
	amqpURI,
	exchange,
	exchangeType,
	routingKey,
	body string,
	reliable bool,
) error {
	log.Printf(" [🔄] Dialing %q", amqpURI)
	connection, err := amqp.Dial(amqpURI)

	if err != nil {
		return fmt.Errorf(" [🛑] Dial: %s", err)
	}

	defer connection.Close()

	log.Printf(" [✅] Got connection")
	log.Printf(" [🔄] Getting channel")
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf(" [🛑] Channel: %s", err)
	}

	log.Printf(
		" [✅] Got channel, declaring %q Exchange (%q)",
		exchangeType,
		exchange,
	)

	if err := channel.ExchangeDeclare(
		exchange,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf(" [🛑] Exchange declare: %s", err)
	}

	if reliable {
		log.Printf(" [✅] Enabling publishing confirms")
		if err := channel.Confirm(false); err != nil {
			return fmt.Errorf(
				" [🛑] Channel could not be put in confirm mode: %s",
				err,
			)
		}
		confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
		defer confirmOne(confirms)
	}

	utils.LogSuccess("Declared exchange")
	log.Printf(
		" Publishing %d body (%q)",
		len(body),
		body,
	)
	if err = channel.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient,
			Priority:        0,
		},
	); err != nil {
		return fmt.Errorf("Exchange publish: %s", err)
	}
	return nil
}

func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf(" [🔄] Waiting for confirmation of one publishing")
	if confirmed := <-confirms; confirmed.Ack {
		log.Printf(
			" [✅] Confirmed delivery with delivery tag: %d",
			confirmed.DeliveryTag,
		)
	} else {
		log.Printf(
			" [🛑] Failed delivery of delivery tag: %d",
			confirmed.DeliveryTag,
		)
	}
}
