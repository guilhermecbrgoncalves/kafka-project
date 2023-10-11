package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
	sameConsumerImport "kafka-project/consumer"
	theConsumerImport "kafka-project/consumer"
	"kafka-project/producer"
	"log"
)

func main() {
	reader := theConsumerImport.NewKafkaReader()
	writer := producer.NewKafkaWriter()
	_ = sameConsumerImport.NewKafkaReader()
	ctx := context.Background()
	messages := make(chan kafka.Message, 1000)

	g, ctx := errgroup.WithContext(ctx)

	err := writer.WriteMessages(ctx, []string{"message 1", "message 2", "message 3", "message 4", "message 5"}, "something", "something else")
	if err != nil {
		log.Fatal(err)
	}

	g.Go(func() error {
		return reader.FetchMessages(ctx, messages)
	})

	g.Go(func() error {
		return reader.CommitMessages(ctx, messages)
	})

	err = g.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
