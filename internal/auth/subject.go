package auth

import "context"

type subjectContextKey struct{}

func SubjectContextKey() subjectContextKey {
	return subjectContextKey{}
}

// Subject returns value which is as a 'subject' in ACL auth mechanism
func Subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}
