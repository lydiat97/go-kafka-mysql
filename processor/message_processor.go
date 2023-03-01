package processor

import (
	"context"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConsumeFunc[T any] func(context.Context, T) error

type MessageProcessor interface {
	ProcessMessage(context.Context, kafka.Message) error
}

type messageProcessor[T any] struct {
	consumeFunc ConsumeFunc[T]
}

func NewMessageProcessor[T any](consumeFunc ConsumeFunc[T]) *messageProcessor[T] {
	return &messageProcessor[T]{
		consumeFunc: consumeFunc,
	}
}

func (m *messageProcessor[T]) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	var t T

	if err := json.Unmarshal(msg.Value, &t); err != nil {
		return err
	}

	return m.consumeFunc(ctx, t)
}
