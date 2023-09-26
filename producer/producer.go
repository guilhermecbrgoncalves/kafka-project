package producer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

const (
	network   = "tcp"
	address   = "localhost:9092"
	topicTest = "topic_test"
	partition = 0
)

type Writer struct {
	Writer *kafka.Writer
}

func NewKafkaWriter() *Writer {
	return &Writer{
		Writer: &kafka.Writer{
			Addr:  kafka.TCP(address),
			Topic: topicTest,
		}}
}

func (k *Writer) WriteMessages(ctx context.Context, messages []string) error {
	for _, m := range messages {
		err := k.Writer.WriteMessages(ctx, kafka.Message{
			Value: []byte(m),
		})
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	return nil
}
