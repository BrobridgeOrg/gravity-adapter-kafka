package adapter

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	eventbus "github.com/BrobridgeOrg/gravity-adapter-kafka/pkg/eventbus/service"
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
)

// Default settings
var DefaultWorkerCount int = 8

type Packet struct {
	EventName string
	Payload   []byte
}

type Source struct {
	adapter     *Adapter
	workerCount int
	incoming    chan []byte
	eventBus    *eventbus.EventBus
	name        string
	host        string
	port        int
	topic       string
	groupId     string
	parser      *parallel_chunked_flow.ParallelChunkedFlow
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &Packet{}
	},
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
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

	// Initialize parapllel chunked flow
	pcfOpts := parallel_chunked_flow.Options{
		BufferSize: 204800,
		ChunkSize:  512,
		ChunkCount: 512,
		Handler: func(data interface{}, output func(interface{})) {
			/*
				id := atomic.AddUint64((*uint64)(&counter), 1)
				if id%1000 == 0 {
					log.Info(id)
				}
			*/
			eventName := jsoniter.Get(data.([]byte), "event").ToString()
			payload := jsoniter.Get(data.([]byte), "payload").ToString()

			// Preparing request
			request := requestPool.Get().(*Packet)
			request.EventName = eventName
			request.Payload = StrToBytes(payload)

			output(request)
		},
	}

	return &Source{
		adapter:     adapter,
		workerCount: *info.WorkerCount,
		incoming:    make(chan []byte, 204800),
		name:        name,
		host:        info.Host,
		port:        info.Port,
		topic:       info.Topic,
		groupId:     info.GroupId,
		parser:      parallel_chunked_flow.NewParallelChunkedFlow(&pcfOpts),
	}
}

func (source *Source) InitSubscription() error {

	c := source.eventBus.GetConnection()
	c.SubscribeTopics([]string{source.topic}, nil)

	go func() {
		for {
			msg, err := c.ReadMessage(-1)
			if err == nil {

				log.WithFields(log.Fields{
					"partition": msg.TopicPartition,
				}).Info("Received event")

				source.incoming <- msg.Value
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
		"client_name": source.adapter.clientName + "-" + source.name,
		"topic":       source.topic,
	}).Info("Initializing source connector")

	options := eventbus.Options{
		ClientName: source.adapter.clientName + "-" + source.name,
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

	go source.eventReceiver()
	go source.requestHandler()

	return source.InitSubscription()
}

func (source *Source) eventReceiver() {

	log.WithFields(log.Fields{
		"source":      source.name,
		"client_name": source.adapter.clientName + "-" + source.name,
		"count":       source.workerCount,
	}).Info("Initializing event receiver ...")

	for {
		select {
		case msg := <-source.incoming:
			source.parser.Push(msg)
		}
	}
}

func (source *Source) requestHandler() {

	for {
		select {
		case req := <-source.parser.Output():
			source.HandleRequest(req.(*Packet))
			requestPool.Put(req)
		}
	}
}

func (source *Source) HandleRequest(request *Packet) {

	for {
		connector := source.adapter.app.GetAdapterConnector()
		err := connector.Publish(request.EventName, request.Payload, nil)
		if err != nil {
			log.Error(err)
			time.Sleep(time.Second)
			continue
		}

		break
	}
}
