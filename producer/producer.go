package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	JSONKey = "json"
)

type Producer struct {
	kafka *kafka.Producer
}

type Options struct {
	kafka *kafka.Producer
}

func NewProducer(options *Options) (*Producer, error) {
	if options.kafka == nil {
		return nil, fmt.Errorf("no kafka client provided")
	}

	return &Producer{kafka: options.kafka}, nil
}

func (p *Producer) Produce(ctx context.Context, stream string, entry interface{}) error {
	data, err := json.Marshal(&entry)
	if err != nil {
		return fmt.Errorf("marshal err: %w", err)
	}
	topic := "topic"
	p.kafka.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(JSONKey),
		Value:          data,
	}, nil)

	return nil
}
