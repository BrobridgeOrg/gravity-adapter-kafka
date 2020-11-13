package adapter

import (
	"context"
	"fmt"
	"sync"
	"time"

	eventbus "github.com/BrobridgeOrg/gravity-adapter-kafka/pkg/eventbus/service"
	dsa "github.com/BrobridgeOrg/gravity-api/service/dsa"
	log "github.com/sirupsen/logrus"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Default settings
var DefaultWorkerCount int = 8

type Packet struct {
	EventName string      `json:"event"`
	Payload   interface{} `json:"payload"`
}

type Source struct {
	adapter     *Adapter
	workerCount int
	incoming    chan *kafka.Message
	eventBus    *eventbus.EventBus
	name        string
	host        string
	port        int
	topic       string
	groupId     string
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &dsa.PublishRequest{}
	},
}

func NewSource(adapter *Adapter, name string, sourceInfo *SourceInfo) *Source {

	// required Group ID
	if len(sourceInfo.GroupId) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required Group ID")

		return nil
	}

	// required topic
	if len(sourceInfo.Topic) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required topic")

		return nil
	}

	info := sourceInfo

	if info.WorkerCount == nil {
		info.WorkerCount = &DefaultWorkerCount
	}

	return &Source{
		adapter:     adapter,
		workerCount: *info.WorkerCount,
		incoming:    make(chan *kafka.Message, 4096),
		name:        name,
		host:        info.Host,
		port:        info.Port,
		topic:       info.Topic,
		groupId:     info.GroupId,
	}
}

func (source *Source) InitSubscription() error {

	c := source.eventBus.GetConnection()
	c.SubscribeTopics([]string{source.topic}, nil)

	go func() {
		for {
			msg, err := c.ReadMessage(-1)
			if err == nil {
				source.incoming <- msg
			} else {
				// The client will automatically try to recover from all errors.
				log.Warn("Consumer error: ", err)
			}
		}
	}()

	return nil
}

func (source *Source) Init() error {

	address := fmt.Sprintf("%s:%d", source.host, source.port)

	log.WithFields(log.Fields{
		"source":      source.name,
		"address":     address,
		"client_name": source.adapter.clientID + "-" + source.name,
		"topic":       source.topic,
	}).Info("Initializing source connector")

	options := eventbus.Options{
		ClientName: source.adapter.clientID + "-" + source.name,
		GroupId:    source.groupId,
	}

	source.eventBus = eventbus.NewEventBus(
		address,
		options,
	)

	err := source.eventBus.NewConsumer()
	if err != nil {
		return err
	}

	source.InitWorkers()

	return source.InitSubscription()
}

func (source *Source) InitWorkers() {

	log.WithFields(log.Fields{
		"source":      source.name,
		"client_name": source.adapter.clientID + "-" + source.name,
		"topic":       source.topic,
		"count":       source.workerCount,
	}).Info("Initializing workers ...")

	// Multiplexing
	for i := 0; i < source.workerCount; i++ {
		go func() {
			for {
				select {
				case msg := <-source.incoming:
					go source.HandleMessage(msg)
				}
			}
		}()
	}
}

func (source *Source) HandleMessage(m *kafka.Message) {

	var packet Packet

	// Parse JSON
	err := json.Unmarshal(m.Value, &packet)
	if err != nil {
		log.Warn(err)
		return
	}

	log.WithFields(log.Fields{
		"event":     packet.EventName,
		"partition": m.TopicPartition,
	}).Info("Received event")

	// Convert payload to JSON string
	payload, err := json.Marshal(packet.Payload)
	if err != nil {
		return
	}

	// Preparing request
	request := requestPool.Get().(*dsa.PublishRequest)
	request.EventName = packet.EventName
	request.Payload = payload

	// Getting connection from pool
	conn, err := source.adapter.app.GetGRPCPool().Get()
	if err != nil {
		log.Error("Failed to get connection: ", err)
		return
	}
	client := dsa.NewDataSourceAdapterClient(conn)

	// Preparing context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// Publish
	resp, err := client.Publish(ctx, request)
	requestPool.Put(request)
	if err != nil {
		log.Error("did not connect: ", err)
		return
	}
	log.Warn(resp)
	if resp.Success == false {
		log.Error("Failed to push message to data source adapter")
		return
	}
}
