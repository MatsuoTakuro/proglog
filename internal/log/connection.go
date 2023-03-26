package log

type ConnectionType uint8

const (
	GRPC ConnectionType = iota
	RaftRPC
)
