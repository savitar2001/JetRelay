package main

import (
	"log"
	"net/http"
	"time"

	"app/internal/config"
	appmcp "app/internal/mcp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	natsConfig, err := config.LoadNATS()
	if err != nil {
		log.Fatal("load NATS config", err)
	}

	nc, err := nats.Connect(natsConfig.URL)

	if err != nil {
		log.Fatal("connect to NATS fail", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal("create JetStream client: ", err)
	}

	mcpServer := appmcp.NewServer()

	appmcp.RegisterTools(mcpServer, js) // 註冊mcp工具

	mux := http.NewServeMux() // 分派路由器
	mux.Handle("/mcp", appmcp.NewHTTPHandler(mcpServer))

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Println("MCP HTTP server listening on :8080")
	log.Fatal(server.ListenAndServe())
}
