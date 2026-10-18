package main

import (
	"log"
	"app/internal/config"
	"github.com/nats-io/nats.go"
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

	payload := []byte(`{"mission_id":"A-1001","force_fail":false}`)
	if err := nc.Publish("mission.created", payload); err != nil {
		log.Fatal("publish mission event", err)
	}

	if err := nc.Flush(); err != nil {
		log.Fatal("flush NATS publish", err)
	}

	log.Println("published mission.created")
	defer nc.Close()
}
