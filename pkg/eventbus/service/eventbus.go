package eventbus

import (
	//"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Options struct {
	ClientName string
	GroupId    string
}

type EventBusHandler struct {
	Reconnect  func()
	Disconnect func()
}

type EventBus struct {
	connection *kafka.Consumer
	hosts      string
	handler    *EventBusHandler
	options    *Options
}

func NewEventBus(hosts string, options Options) *EventBus {
	return &EventBus{
		connection: nil,
		hosts:      hosts,
		options:    &options,
	}
}

func (eb *EventBus) NewConsumer() error {

	log.WithFields(log.Fields{
		"host":     eb.hosts,
		"Group ID": eb.options.GroupId,
	}).Info("Connecting to Kafka server")

	conn, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": eb.hosts,
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
