package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	"app/internal/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	missionStreamName = "MISSION"
	missionStreamSubject = "mission.>"
	missionWorkerName = "MISSION_WORKER"
	missionCreatedSubject = "mission.created"

	missionDLQStreamName = "MISSION_DLQ"
	missionDLQStreamSubject = "dlq.mission.>"

	missionMaxDeliveryEventsStreamName = "MISSION_MAX_DELIVERY_EVENTS"
	missionMaxDeliveryEventsStreamSubject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.MISSION.MISSION_WORKER"
)

func main() {
	natsConfig, err := config.LoadNATS()
	if err != nil {
		log.Fatal("load NATS config: ", err)
	}

	nc, err := connectWithRetry(natsConfig.URL, 10, time.Second)
	if err != nil {
		log.Fatal("connect to NATS: ", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal("create JetStream client: ", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	stream, err := ensureMissionStream(ctx, js)

	if err != nil {
		log.Fatal("ensure MISSION stream: ", err)
	}

	if _, err := ensureMissionDLQStream(ctx, js); err != nil {
		log.Fatal("ensure MISSION_DLQ stream: ", err)
	}

	if _, err := ensureMissionMaxDeliveryEventsStream(ctx, js); err != nil {
		log.Fatal("ensure MISSION_MAX_DELIVERY_EVENTS stream: ", err)
	}

	if err := ensureMissionWorker(ctx, stream, natsConfig.MissionConsumer); err != nil {
		log.Fatal("ensure MISSION_WORKER consumer: ", err)
	}

	slog.Info(
		"JetStream bootstrap completed",
		"mission_stream", missionStreamName,
		"dlq_stream", missionDLQStreamName,
		"max_delivery_events_stream", missionMaxDeliveryEventsStreamName,
		"consumer", missionWorkerName,
	)
}

func connectWithRetry(url string, attempts int, retryDelay time.Duration) (*nats.Conn, error) {
	var lastErr error

	if attempts < 1 {
		return nil, errors.New("attempts must be greater than zero")
	}

	for i := attempts; i > 0; i-- {
		nc, err := nats.Connect(url)
		if err == nil {
			return nc, nil
		}
		slog.Warn(
			"NATS is not ready",
			"attempt", i,
			"attempts", attempts,
			"error", err,
		)

		if i != 1 {
			time.Sleep(retryDelay)
		}

		lastErr = err
	}

	slog.Error("connect to NATS fail", "error", lastErr)
	return nil, lastErr
}


func ensureMissionStream(
	ctx context.Context,
	js jetstream.JetStream,
) (jetstream.Stream, error) {
	return ensureStream(ctx, js, jetstream.StreamConfig{
		Name: missionStreamName,
		Subjects: []string{missionStreamSubject},
		Storage: jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	})
}

func ensureMissionDLQStream(
	ctx context.Context,
	js jetstream.JetStream,
) (jetstream.Stream, error) {
	return ensureStream(ctx, js, jetstream.StreamConfig{
		Name: missionDLQStreamName,
		Subjects: []string{missionDLQStreamSubject},
		Storage: jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	})
}

func ensureMissionMaxDeliveryEventsStream(
	ctx context.Context,
	js jetstream.JetStream,
) (jetstream.Stream, error) {
	return ensureStream(ctx, js, jetstream.StreamConfig{
		Name:      missionMaxDeliveryEventsStreamName,
		Subjects:  []string{missionMaxDeliveryEventsStreamSubject},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	})
}

func ensureMissionWorker(ctx context.Context, stream jetstream.Stream, consumerConfig config.MissionConsumerConfig) error {
	_, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: missionWorkerName,
		FilterSubject: missionCreatedSubject,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: consumerConfig.AckWait,
		MaxDeliver: consumerConfig.MaxDeliver,
	})

	if err != nil {
		return fmt.Errorf("create or update consumer: %w", err)
	}

	return nil
}

func ensureStream(
	ctx context.Context,
	js jetstream.JetStream,
	streamConfig jetstream.StreamConfig,
) (jetstream.Stream, error) {
	stream, err := js.Stream(ctx, streamConfig.Name)
	if err == nil {
		return stream, nil
	}

	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, fmt.Errorf(
			"look up stream %q: %w",
			streamConfig.Name,
			err,
		)
	}

	stream, err = js.CreateStream(ctx, streamConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"create stream %q: %w",
			streamConfig.Name,
			err,
		)
	}

	return stream, nil
}
