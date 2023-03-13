package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	// define a status
	st := status.New(
		codes.OutOfRange,
		fmt.Sprintf("offset out of range: %d", e.Offset),
	)

	// define a message as details other than status
	msg := fmt.Sprintf(
		"The requested offset is outside the log's range: %d",
		e.Offset,
	)
	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	std, err := st.WithDetails(d)
	if err != nil {
		return st
		// panic(fmt.Sprintf("Unexpected error attaching metadata: %v", err))
	}

	return std
}

// by this method, ErrOffsetOutOfRange implements Error interface
func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
