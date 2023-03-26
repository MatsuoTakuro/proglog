package log

type RequestType uint8

const (
	AppendRequestType RequestType = iota + RequestType(GRPC)
)
