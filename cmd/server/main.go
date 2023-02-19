package main

import (
	"fmt"
	"log"

	"github.com/MatsuoTakuro/proglog/internal/server"
)

func main() {
	srv := server.NewHttpServer(":18080")
	fmt.Printf("server starting at %s\n", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}
