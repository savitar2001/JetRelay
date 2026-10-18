package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"app/internal/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	missionStreamName    = "MISSION"
	missionWorkerName    = "MISSION_WORKER"
	missionCreateSubject = "mission.created"
)

type missionCreatedEvent struct {
	MissionID string `json:"mission_id"`
	ForceFail bool   `json:"force_fail"`
}

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

	setupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.Stream(setupCtx, missionStreamName)
	if err != nil {
		log.Fatal("get MISSION stream: ", err)
	}

	consumer, err := stream.Consumer(setupCtx, missionWorkerName)
	if err != nil {
		log.Fatal("get MISSION_WORKER consumer: ", err)
	}

	shutdownCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	consumerCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		if err := processMission(msg); err != nil {
			slog.Error(
				"process mission failed; message remains unacknowledged",
				"error", err,
				"subject", msg.Subject(),
			)

			return
		}

		if err := msg.Ack(); err != nil {
			slog.Error(
				"ack mission message",
				"error", err,
				"subject", msg.Subject(),
			)
			return
		}

		slog.Info(
			"nission processed and acknowlede",
			"subject", msg.Subject(),
		)
	})

	if err != nil {
		log.Fatal("start consumer:", err)
	}

	defer consumerCtx.Stop()

	slog.Info("MISSION worker started", "consumer", missionWorkerName)
	<-shutdownCtx.Done()
	slog.Info("MISSION worker stopped")
}

func processMission(msg jetstream.Msg) error {
	var event missionCreatedEvent

	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		return fmt.Errorf("decode mission payload: %w", err)
	}

	if event.MissionID == "" {
		return errors.New("mission_id is required")
	}

	if event.ForceFail {
		return errors.New("forced mission processing failure")
	}

	slog.Info("processing mission", "mission_id", event.MissionID)

	return nil
}
