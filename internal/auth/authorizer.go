package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type authorizer struct {
	enforcer *casbin.Enforcer
}

func New(model, policy string) *authorizer {
	// model refers to the authentication mechanism, in this case an ACL file.
	// policy is also a csv file containing the ACL table.
	enforcer := casbin.NewEnforcer(model, policy)
	return &authorizer{
		enforcer: enforcer,
	}
}

// Authorize makes authorization based on ALC.
// it determines whether or not which 'subject' is allowed to perform which 'action' on which 'object'
func (a *authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject, object, action,
		)
		st := status.New(codes.PermissionDenied, msg) ///
		return st.Err()
	}
	return nil
}
