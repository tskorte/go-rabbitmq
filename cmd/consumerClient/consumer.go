package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go-rabbitmq/consumer"
	"io/ioutil"
	"log"
	"os/user"
	"time"

	"github.com/happierall/l"
)

const (
	jsonconfigfile = ".rabbitMQConfig.json"
)

type config struct {
	AmqpURI      string
	ExchangeName string
	ExchangeType string
	Queue        string
	Key          string
	ConsumerTag  string
	LifeTime     int
}

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
	config := parseConfigFile()
	c, err := consumer.NewConsumer(
		config.AmqpURI,
		config.ExchangeName,
		config.ExchangeType,
		config.Queue,
		config.Key,
		config.ConsumerTag,
	)
	if err != nil {
		l.Error(err)
	}

	if config.LifeTime > 0 {
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
