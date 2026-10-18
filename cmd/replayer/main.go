package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"time"

	"app/internal/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	missionDLQStreamName = "MISSION_DLQ"
	operationTimeout = 10 * time.Second
)

type dlqEnvelope struct {
	QuarantineID string `json:"quarantine_id"`
	Source dlqSource `json:"source"`
	PayloadBase64 string `json:"payload_base64"`
}

type dlqSource struct {
	Subject string `json:"subject"`
	Headers map[string][]string `json:"header,omitempty"`
}

func main() {
	dlqSequence := flag.Uint64(
		"sequence",
		0,
		"DLQ stram sequence to replay",
	)

	replayRequestID := flag.String(
		"request-id",
		"",
		"stable identifier for this replay request",
	)

	flag.Parse()

	if *dlqSequence == 0{
		log.Fatal("-sequence must be greater than zero")
	}

	if strings.TrimSpace(*replayRequestID) == "" {
		log.Fatal("-request-id is required")
	}

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
		log.Fatal("create JetStream client: ", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		operationTimeout,
	)
	defer cancel()

	dlqStream, err := js.Stream(ctx, missionDLQStreamName)
	if err != nil {
		log.Fatal("get DLQ stram: ", err)
	}

	dlqMsg, err := dlqStream.GetMsg(ctx, *dlqSequence)
	if err != nil {
		log.Fatal("get DLQ message: ", err)
	}

	var envelope dlqEnvelope
	if err := json.Unmarshal(dlqMsg.Data, &envelope); err != nil {
		log.Fatal("decode DLQ envelope: ", err)
	}
	if err := validateReplayEnvelope(envelope); err != nil {
		log.Fatal("validate DLQ envelope: ", err)
	}

	if envelope.Source.Subject == "" {
		log.Fatal("DLQ envelope source.subject is required")
	}

	payload, err := base64.StdEncoding.DecodeString(envelope.PayloadBase64)
	if err != nil {
		log.Fatal("decode DLQ payload: ", err)
	}

	replayMessageID := fmt.Sprintf(
		"replay:%s:%s",
		*replayRequestID,
		envelope.QuarantineID,
	)

	replayMsg := nats.NewMsg(envelope.Source.Subject)
	replayMsg.Data = payload
	replayMsg.Header = cloneHeaders(envelope.Source.Headers)

	replayMsg.Header.Set("Nats-Msg-Id", replayMessageID)
	replayMsg.Header.Set(
		"X-JetRescue-Replay-Id",
		*replayRequestID,
	)
	replayMsg.Header.Set(
		"X-JetRescue-Quarantine-Id",
		envelope.QuarantineID,
	)

	ack, err := js.PublishMsg(ctx, replayMsg)
	if err != nil {
		log.Fatal("publish replay message: ", err)
	}

	slog.Info(
		"DLQ message replayed",
		"dlq_sequence", *dlqSequence,
		"quarantine_id", envelope.QuarantineID,
		"subject", envelope.Source.Subject,
		"replay_request_id", *replayRequestID,
		"replay_message_id", replayMessageID,
		"target_stream", ack.Stream,
		"target_sequence", ack.Sequence,
	)
}

func cloneHeaders(headers map[string][]string) nats.Header {
	cloned := make(nats.Header, len(headers))

	for key, values := range headers {
		cloned[key] = append([]string(nil), values...)
	}

	return cloned
}

func validateReplayEnvelope(envelope dlqEnvelope) error {
	if envelope.QuarantineID == "" {
		return errors.New("quarantine_id is required")
	}

	if envelope.Source.Subject == "" {
		return errors.New("source.subject is required")
	}

	if envelope.PayloadBase64 == "" {
		return errors.New("payload_base64 is required")
	}

	return nil
}