package consumer

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"kafka-mysql/processor"
	"os"
	"sync"
	"time"
)

type Consumer struct {
	kafka  *kafka.Consumer
	topic  string
	server string

	errorHandler   func(error)
	processors     map[string]processor.MessageProcessor
	internalCancel context.CancelFunc
	blockDuration  time.Duration
	mux            sync.Mutex
	errorsChan     chan error
	stopChan       chan struct{}
}

type Options struct {
	Topic         string
	Server        string
	BlockDuration time.Duration
	Kafka         *kafka.Consumer
}

func NewConsumer(options *Options) (*Consumer, error) {
	if options.Topic == "" {
		return nil, fmt.Errorf("must provide topic")
	}
	if options.Server == "" {
		options.Server = "127.0.0.1:9092"
	}

	if options.BlockDuration == 0 {
		options.BlockDuration = 10 * time.Second
	}

	if options.Kafka == nil {
		return nil, fmt.Errorf("no consumer client provided")
	}

	errorsChan := make(chan error)

	return &Consumer{
		processors:    make(map[string]processor.MessageProcessor),
		kafka:         options.Kafka,
		topic:         options.Topic,
		server:        options.Server,
		blockDuration: options.BlockDuration,
		errorsChan:    errorsChan,
		stopChan:      make(chan struct{}),
		errorHandler:  func(e error) {},
	}, nil
}

func (c *Consumer) ProcessStream(messageProcessor processor.MessageProcessor, streams ...string) {
	c.mux.Lock()
	defer c.mux.Unlock()
	for _, stream := range streams {
		c.processors[stream] = messageProcessor
	}
}

func (c *Consumer) AddErrorHandler(errorFunc func(error)) {
	c.errorHandler = errorFunc
}

func (c *Consumer) Consume(ctx context.Context) {
	cancelContext, cancelFunc := context.WithCancel(ctx)
	c.internalCancel = cancelFunc

	shutdownChan := make(chan struct{})
	defer close(shutdownChan)
	go c.processError(shutdownChan)

	if len(c.processors) == 0 {
		c.errorsChan <- fmt.Errorf("no processors")
		return
	}

	go c.poll(cancelContext)

	<-c.stopChan
}

func (c *Consumer) processError(stop <-chan struct{}) {
	for {
		select {
		case err := <-c.errorsChan:
			c.errorHandler(err)
		case <-stop:
			return
		}
	}
}

func (c *Consumer) poll(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.stopChan <- struct{}{}
			return
		default:
			ev := c.kafka.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				_, err := c.kafka.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n",
						e.TopicPartition)
				}
			}
		}
	}
}