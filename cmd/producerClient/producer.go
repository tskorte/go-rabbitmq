package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os/user"

	"github.com/happierall/l"
	"github.com/streadway/amqp"
)

const (
	jsonconfigfile = ".rabbitMQConfig.json"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchangeName = flag.String("exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	queue        = flag.String("queue", "test-queue", "Ephemeral AMQP queue name")
	lifetime     = flag.Duration("lifetime", 0, "lifetime of process before shutdown (0s=infinite)")
	routingKey   = flag.String(
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

type config struct {
	AmqpURI      string
	ExchangeName string
	ExchangeType string
	Queue        string
	RoutingKey   string
	Reliable     bool
}

func init() {
	flag.Parse()
}

func main() {
	config := parseConfigFile()
	if err := publish(
		config.AmqpURI,
		config.ExchangeName,
		config.ExchangeType,
		config.RoutingKey,
		*body,
		config.Reliable,
	); err != nil {
		log.Fatalf(" [🛑] %s", err)
	}
	log.Printf(" [✅] Published %dB OK", len(*body))
}

func parseConfigFile() *config {
	usr, err := user.Current()
	if err != nil {
		log.Fatalf("Error reading current user %s", err)
	}
	configPath := fmt.Sprintf("%s/%s", usr.HomeDir, jsonconfigfile)
	configFileBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Error reading config file %s", err)
	}
	var config config
	err = json.Unmarshal(configFileBytes, &config)
	if err != nil {
		log.Fatalf("Error reading config file %s", err)
	}
	return &config
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

	l.Debug("Declared exchange")
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
