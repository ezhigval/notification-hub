package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ezhigval/notification-hub/internal/model"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	writers map[string]*kafka.Writer
}

func NewPublisher(brokers []string, topics ...string) *Publisher {
	writers := make(map[string]*kafka.Writer, len(topics))
	for _, topic := range topics {
		writers[topic] = &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}
	}
	return &Publisher{writers: writers}
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg model.KafkaMessage) error {
	w, ok := p.writers[topic]
	if !ok {
		return fmt.Errorf("unknown topic: %s", topic)
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return w.WriteMessages(ctx, kafka.Message{Value: data})
}

func (p *Publisher) Close() error {
	var first error
	for _, w := range p.writers {
		if err := w.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

type ConsumerPool struct {
	readers []*kafka.Reader
}

func NewConsumerPool(brokers []string, group string, topics []string) *ConsumerPool {
	readers := make([]*kafka.Reader, 0, len(topics))
	for _, topic := range topics {
		readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  group,
			MinBytes: 1,
			MaxBytes: 10e6,
		}))
	}
	return &ConsumerPool{readers: readers}
}

func (p *ConsumerPool) Readers() []*kafka.Reader {
	return p.readers
}

func (p *ConsumerPool) Close() error {
	var first error
	for _, r := range p.readers {
		if err := r.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
