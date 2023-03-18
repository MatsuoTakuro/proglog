package middleware

import (
	"context"

	"github.com/MatsuoTakuro/proglog/internal/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Authenticate, as middleware, captures a subject from context of grpc(, which is called a 'peer' containing auth info of grpc)
// and add it to context with its key.
// This func implements AuthFunc of grpc middleware(, which is also called an interceptor)
// Refer to https://pkg.go.dev/github.com/grpc-ecosystem/go-grpc-middleware@v1.4.0/auth#AuthFunc
func Authenticate(ctx context.Context) (context.Context, error) {

	// capture peer from context of grpc transport
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, auth.SubjectContextKey(), ""), nil
	}

	// gets tls info from peer, followed by getting a subject from it
	tls := peer.AuthInfo.(credentials.TLSInfo)
	subject := tls.State.VerifiedChains[0][0].Subject.CommonName
	// set subject to context with its key
	ctx = context.WithValue(ctx, auth.SubjectContextKey(), subject)

	return ctx, nil
}
