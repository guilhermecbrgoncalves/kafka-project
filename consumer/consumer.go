package consumer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

const (
	network   = "tcp"
	address   = "localhost:9092"
	topicTest = "topic_test"
	partition = 0
)

type Reader struct {
	Reader *kafka.Reader
}

func NewKafkaReader() *Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{address},
		Topic:   topicTest,
		GroupID: "group_2",
	})

	return &Reader{Reader: reader}
}

func (k *Reader) FetchMessages(ctx context.Context, messages chan<- kafka.Message) error {
	for {
		message, err := k.Reader.FetchMessage(ctx)
		if err != nil {
			fmt.Println(err)
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case messages <- message:
			log.Printf("Message fetched and sent to a channel: %v \n", string(message.Value))
		}
	}
}

func (k *Reader) CommitMessages(ctx context.Context, messageCommitChan <-chan kafka.Message) error {
	for {
		select {
		case <-ctx.Done():
		case msg := <-messageCommitChan:
			err := k.Reader.CommitMessages(ctx, msg)
			if err != nil {
				fmt.Println(err)
				return err
			}
			log.Printf("Committed a messagee: %s \n", string(msg.Value))
		}

	}
}
