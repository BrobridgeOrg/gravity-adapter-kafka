package instance

import (
	adapter_service "github.com/BrobridgeOrg/gravity-adapter-kafka/pkg/adapter/service"
	pool "github.com/cfsghost/grpc-connection-pool"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done     chan bool
	grpcPool *pool.GRPCPool
	adapter  *adapter_service.Adapter
}

func NewAppInstance() *AppInstance {

	a := &AppInstance{
		done: make(chan bool),
	}

	a.adapter = adapter_service.NewAdapter(a)

	return a
}

func (a *AppInstance) Init() error {

	log.Info("Starting application")

	// Initializing gRPC pool
	err := a.initGRPCPool()
	if err != nil {
		return err
	}
	
	err = a.adapter.Init()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	<-a.done

	return nil
}