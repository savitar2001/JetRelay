FROM golang:1.26.5 AS base

WORKDIR /usr/src/app

RUN curl -fsSL https://claude.ai/install.sh | bash

COPY go.mod go.sum ./
RUN go mod download

ENV PATH="/root/.local/bin:${PATH}"