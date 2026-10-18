package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type NATSConfig struct {
	URL string
	MissionConsumer MissionConsumerConfig
}

type MissionConsumerConfig struct {
	AckWait time.Duration
	MaxDeliver int
}

func LoadNATS() (NATSConfig, error) {
	natsURL, exist := os.LookupEnv("NATS_URL")
	if !exist || strings.TrimSpace(natsURL) == "" {
		return NATSConfig{}, errors.New("NATS_URL is required")
	}

	ackWait := 30 * time.Second
	maxDeliver := 3

	if raw, found := os.LookupEnv("MISSION_ACK_WAIT"); found {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return NATSConfig{}, fmt.Errorf(
				"MISSION_ACK_WAIT must be a positive duration: %q",
				raw,
			)
		}
		ackWait = parsed
	}

	if raw, found := os.LookupEnv("MISSION_MAX_DELIVER"); found {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return NATSConfig{}, fmt.Errorf(
				"MISSION_MAX_DELIVER must be a positive integer: %q", 
				raw,
			)
		}
		maxDeliver = parsed
	}

	return NATSConfig{
		URL: natsURL,
		MissionConsumer: MissionConsumerConfig{
			AckWait: ackWait,
			MaxDeliver: maxDeliver,
		},
	}, nil
}
