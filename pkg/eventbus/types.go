package eventbus

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type EventBus interface {
	Connect() error
	Close()
	GetConnection() *kafka.Consumer
}