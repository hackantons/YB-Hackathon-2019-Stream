package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/comprehend"
	"github.com/aws/aws-sdk-go/service/kinesis"
	consumer "github.com/harlow/kinesis-consumer"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"unicode/utf8"
)

type Message struct {
	Message string `json:"message"`
	Sentiment string `json:"sentiment"`
	Label string `json:"label"`
	Group string `json:"group"`
	Time string `json:"time"`
	Type string `json:"type"`
}

func main() {

	fmt.Println("V: 1.1")


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
			str := string(body)
			str = ToValidUTF8(str)

			var message Message
			json.Unmarshal([]byte(str), &message)

			if message.Type == "text" {
				sess := session.Must(session.NewSession(&aws.Config{
					Region: aws.String("eu-central-1"),
				}))

				// Create a Comprehend client from just a session.
				client := comprehend.New(sess)


				params := comprehend.DetectSentimentInput{}
				params.SetLanguageCode("de")
				params.SetText(message.Message)

				req, resp := client.DetectSentimentRequest(&params)

				err = req.Send()
				if err == nil { // resp is now filled
					fmt.Println(resp.SentimentScore)
					if *resp.SentimentScore.Negative > *resp.SentimentScore.Neutral &&
					 *resp.SentimentScore.Negative > *resp.SentimentScore.Positive {
						message.Sentiment = "Negative"
					} else if *resp.SentimentScore.Positive > *resp.SentimentScore.Negative &&
						*resp.SentimentScore.Positive > *resp.SentimentScore.Neutral {
						message.Sentiment = "Positive"
					} else {
						message.Sentiment = "Neutral"
					}

					data, _ := json.Marshal(message)
					producer.Put(data, "Foo")
				} else {
					fmt.Println(err)
				}
			} else {
				fmt.Println("WIthout sentimentation")
				producer.Put([]byte(str), "Foo")
			}
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

}

// Just for testing purposes
func enableCors(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
	(*w).Header().Set("Access-Control-Allow-Headers", "*")
}

func cleanString(str string) string {
	var data = str
	data = ToValidUTF8(data)
	lastIndex := strings.LastIndex(data, "}")
	data = data[0:lastIndex+1]
	firstIndex := strings.Index(data, "{")
	if firstIndex == -1 {
		return data
	}
	data = data[firstIndex:len(data)]
	data = strings.ReplaceAll(data, "\x11", "")
	data = strings.ReplaceAll(data, "\xb3", "")
	data = strings.ReplaceAll(data, "\x6d\nxd2", "")
	return data
}

func ToValidUTF8(s string) string {
	if !utf8.ValidString(s) {
		v := make([]rune, 0, len(s))
		for i, r := range s {
			if r == utf8.RuneError {
				_, size := utf8.DecodeRuneInString(s[i:])
				if size == 1 {
					continue
				}
			}
			v = append(v, r)
		}
		s = string(v)
	}
	return s
}
