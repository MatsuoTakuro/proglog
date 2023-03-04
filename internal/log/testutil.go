package log

import (
	"log"
	"os"
	"testing"
)

// print content of file for debugging
func printFile(t *testing.T, name string) {
	t.Helper()

	b, err := os.ReadFile(name)
	if err != nil {
		t.Error(err)
	}
	log.Println(string(b))
}
