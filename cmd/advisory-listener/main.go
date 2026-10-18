package main

import (
	"context"
	"encoding/base64"
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
	missionDLQStreamName = "MISSION_DLQ"

	missionMaxDeliveryEventsStreamName= "MISSION_MAX_DELIVERY_EVENTS"
	missionDLQCollectorName = "MISSION_DLQ_COLLECTOR"

	collectorAckWait = 30 * time.Second
	operationTimeout = 10 * time.Second
)

type maxDeliverAdvisory struct {
	Stream string `json:"stream"`
	Consumer string `json:"consumer"`
	StreamSeq uint64 `json:"stream_seq"`
	Deliveries uint64 `json:"deliveries"`
	Timestamp time.Time `json:"timestamp"`
}

type quarantinedMessage struct {
	QuarantineID string `json:"quarantine_id"`
	QuarantineAt time.Time `json:"quarantine_at"`

	Source quarantinedSource `json:"source"`

	PayloadBase64 string `json:"payload_base64"`
}

type quarantinedSource struct {
	Stream string `json:"stream"`
	Consumer string `json:"consumer"`
	Subject string `json:"subject"`
	StreamSeq uint64 `json:"stream_seq"`
	Deliveries uint64 `json:"deliveries"`
	PublishedAt time.Time `json:"published_at"`
	FailedAt time.Time `json:"failed_at"`
	Headers map[string][]string `json:"header,omitempty"`
}

func main() {
	natsConfig, err := config.LoadNATS()
	if err != nil {
		log.Fatal("load NATS config: ", err)
	}

	nc, err := nats.Connect(natsConfig.URL)
	if err != nil {
		log.Fatal("connect to NATS: ", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal("create JetStream client", err)
	}

	setupCtx, cancelSetup := context.WithTimeout(
		context.Background(),
		operationTimeout,
	)
	defer cancelSetup()

	eventStream, err := js.Stream(
		setupCtx,
		missionMaxDeliveryEventsStreamName,
	)
	if err != nil {
		log.Fatal("get max-delivery events stream: ", err)
	}

	collector, err := eventStream.CreateOrUpdateConsumer(
		setupCtx,
		jetstream.ConsumerConfig{
			Durable: missionDLQCollectorName,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckPolicy: jetstream.AckExplicitPolicy,
			AckWait: collectorAckWait,
			MaxAckPending: 1,
		},
	)

	if err != nil {
		log.Fatal("create or update DLQ collector consumer: ", err)
	}

	consumerCtx, err := collector.Consume(func(eventMsg jetstream.Msg) {
		quarantineID, err := quarantineMessage(js, eventMsg)
		if err != nil {
			slog.Error(
					"quarantine max-delivery event failed; advisory remains unacknowledged",
					"error", err,
			)
			return
		}
		
		ackCtx, cancelAck := context.WithTimeout(
			context.Background(),
			operationTimeout,
		)
		defer cancelAck()

		if err := eventMsg.DoubleAck(ackCtx); err != nil {
			slog.Error(
				"ack max-delivery event",
				"error", err,
				"quarantine_id", quarantineID,
			)
			return
		}

		slog.Info(
			"message quarantined",
			"quarantine_id", quarantineID,
		)
	})
	if err != nil {
		log.Fatal("start DLQ collector consumer: ", err)
	}
	defer consumerCtx.Stop()

	shutdownCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	slog.Info(
		"advisory listener started",
		"event_stream", missionMaxDeliveryEventsStreamName,
		"consumer", missionDLQCollectorName,
		"dlq_stream", missionDLQStreamName,
	)

	<-shutdownCtx.Done()

	slog.Info("advisory listener stopped")
}
func quarantineMessage(
	js jetstream.JetStream,
	eventMsg jetstream.Msg,
) (string, error) {
	var advisory maxDeliverAdvisory

	if err := json.Unmarshal(eventMsg.Data(), &advisory); err != nil {
		return "", fmt.Errorf("decode max=delivery adviosry: %w", err)
	}

	if advisory.Stream == "" {
		return "", errors.New("max-delivery advisory stream is required")
	}

	if advisory.Consumer == "" {
		return "", errors.New("max-delivery advisory consumer is required")
	}

	if advisory.StreamSeq == 0 {
		return "", errors.New("max-delivery advisory stream_seq is required")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		operationTimeout,
	)
	defer cancel()

	sourceStream, err := js.Stream(ctx, advisory.Stream)
	if err != nil {
		return "", fmt.Errorf(
			"get source stream %q: %w",
			advisory.Stream,
			err,
		)
	}

	originalMsg, err := sourceStream.GetMsg(ctx, advisory.StreamSeq)
	if err != nil {
		return "", fmt.Errorf(
			"get source meddage %q sequence %d: %w",
			advisory.Stream,
			advisory.StreamSeq,
			err,
		)
	}

	quarantineID := fmt.Sprintf(
		"quarantine:%s:%s:%d",
		advisory.Stream,
		advisory.Consumer,
		advisory.StreamSeq,
	)

	envelope := quarantinedMessage{
		QuarantineID: quarantineID,
		QuarantineAt: time.Now().UTC(),
		Source: quarantinedSource{
			Stream:      advisory.Stream,
			Consumer:    advisory.Consumer,
			Subject:     originalMsg.Subject,
			StreamSeq:   advisory.StreamSeq,
			Deliveries:  advisory.Deliveries,
			PublishedAt: originalMsg.Time,
			FailedAt:    advisory.Timestamp,
			Headers:     cloneHeaders(originalMsg.Header),
		},
		PayloadBase64: base64.StdEncoding.EncodeToString(originalMsg.Data),
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("encode DLQ envelope: %w", err)
	}

	dlqMsg := nats.NewMsg("dlq." + originalMsg.Subject)
	dlqMsg.Data = payload

	dlqMsg.Header.Set("Nats-Msg-Id", quarantineID)

	if _, err := js.PublishMsg(ctx, dlqMsg); err != nil {
		return "", fmt.Errorf("publish message to DLQ: %w", err)
	}

	return quarantineID, nil
}

func cloneHeaders(headers nats.Header) map[string][]string {
	if len(headers) == 0 {
		return nil
	}

	cloned := make(map[string][]string, len(headers))

	for key, values := range headers {
		cloned[key] = append([]string(nil), values...)
	}

	return cloned
}
