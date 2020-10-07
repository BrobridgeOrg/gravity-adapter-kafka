package eventbus

import (
	//"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	log "github.com/sirupsen/logrus"
)

type Options struct {
	ClientName		string
	GroupId         string
}

type EventBusHandler struct {
	Reconnect  func()
	Disconnect func()
}

type EventBus struct {
	connection *kafka.Consumer
	host       string
	handler    *EventBusHandler
	options    *Options
}

func NewEventBus(host string, options Options) *EventBus {
	return &EventBus{
		connection: nil,
		host:       host,
		options:    &options,
	}
}

func (eb *EventBus) NewConsumer() error {

	log.WithFields(log.Fields{
		"host":            eb.host,
		"Group ID":        eb.options.GroupId,
	}).Info("Connecting to Kafka server")
	
	conn, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": eb.host,
		"group.id":          eb.options.GroupId,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Warn(err)
	}
	eb.connection = conn

	return nil
}

func (eb *EventBus) Close() {
	eb.connection.Close()
}

func (eb *EventBus) GetConnection() *kafka.Consumer {
	return eb.connection
}
