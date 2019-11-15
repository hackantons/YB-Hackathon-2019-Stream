package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	consumer "github.com/harlow/kinesis-consumer"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

func main() {

	var (
		stream    = flag.String("stream", "Foo", "Stream name")
		awsRegion = flag.String("region", "eu-central-1", "AWS Region")
	)

	producer := ProducerInit(*stream, *awsRegion)


	hub := newHub()
	go hub.run()

	// client
	var client = kinesis.New(session.Must(session.NewSession(
		aws.NewConfig().
			WithRegion(*awsRegion),
	)))


	var shardId = "shardId-000000000000"
	var streamName = "Foo"
	var iteratorType = kinesis.ShardIteratorTypeTrimHorizon

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {

		enableCors(&w)

		switch r.Method {
		case "GET":
			wsClient, _ := serveWs(hub, w, r)

			shardIterator, err := client.GetShardIterator(&kinesis.GetShardIteratorInput{ShardId: &shardId, StreamName: &streamName, ShardIteratorType: &iteratorType})

			data, err := client.GetRecords(&kinesis.GetRecordsInput{ShardIterator: shardIterator.ShardIterator})

			if err != nil {
				log.Fatalf("error %v", err)
			}

			records := data.Records
			for i := 0; i < len(records); i++ {
				dataString := cleanString(string(records[i].Data))
				fmt.Println(dataString)
				wsClient.send <- []byte(dataString)
			}
		case "POST":
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(string(body))
			producer.Put(body, "Foo")
		}
	})

	go func() {
		http.ListenAndServe(":80", nil)
	}()

	flag.Parse()


	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithClient(client),
	)

	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}
	err = c.ScanShard(context.Background(), "shardId-000000000000", func(r *consumer.Record) error {
		data := string(r.Data)
		data = cleanString(data)
		hub.broadcast <- []byte(data)
		return nil
	})
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}
	fmt.Println("test")
	// Note: If you need to aggregate based on a specific shard
	// the `ScanShard` function should be used instead.

}

// Just for testing purposes
func enableCors(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
}

func cleanString(str string) string {
	var data = str
	lastIndex := strings.LastIndex(data, "}")
	data = data[0:lastIndex+1]
	firstIndex := strings.Index(data, "{")
	if firstIndex == -1 {
		return data
	}
	data = data[firstIndex:len(data)]
	return data
}
