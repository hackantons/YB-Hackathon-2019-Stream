package main

import (
	"flag"
	"fmt"

	"github.com/a8m/kinesis-producer"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func ProducerInit(stream string, region string) *producer.Producer {
	flag.Parse()

	// client
	var client = kinesis.New(session.Must(session.NewSession(
		aws.NewConfig().
			WithRegion(region),
	)))

	ybProducer := producer.New(&producer.Config{
		StreamName:   stream,
		BacklogCount: 2000,
		Client:       client,
	})

	ybProducer.Start()

	// Handle failures
	go func() {
		for r := range ybProducer.NotifyFailures() {
			// r contains `Data`, `PartitionKey` and `Error()`
			fmt.Println(r)
		}
	}()

	return ybProducer
}
