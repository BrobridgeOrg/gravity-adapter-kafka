package app

import (
	pool "github.com/cfsghost/grpc-connection-pool"
)

type App interface {
	GetGRPCPool() *pool.GRPCPool
}