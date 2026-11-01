package main

import (
	"context"
	"log"
	"time"

	"app/internal/config"
	"app/internal/messaging"
	"app/internal/task"
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ack, err := messaging.PublishTask(ctx, js, task.Task{
		ID:     "A-1001",
		Prompt: "今天天氣真好",
	}, task.PolicyDecision)
	if err != nil {
		log.Fatal("publish task: ", err)
	}
	log.Printf("published task to %s seq=%d", ack.Stream, ack.Sequence)
}
