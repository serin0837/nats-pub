package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
)

func setup() *nats.Conn {
	natsURL := "nats://localhost:4222"

	opts := nats.Options{
		AllowReconnect: true,
		MaxReconnect:   5,
		ReconnectWait:  5 * time.Second,
		Timeout:        3 * time.Second,
		Url:            natsURL,
	}

	conn, err := opts.Connect()
	fmt.Errorf("v", err)

	return conn
}

func GetContext() nats.JetStreamContext {
	conn := setup()

	js, err := conn.JetStream()
	fmt.Errorf("v", err)

	return js
}

const (
	streamName     = "ORDERS"
	streamSubjects = "ORDERS.*"
)

//in our stream save every subject with ORDERS
func createStream(js nats.JetStreamContext) {

	// Check if the stream already exists; if not, create it.
	stream, _ := js.StreamInfo(streamName)

	if stream == nil {
		log.Printf("creating stream %q and subjects %q", streamName, streamSubjects)

		_, err := js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects},
			MaxAge:   0, //0 mean keep forever
			Storage:  nats.FileStorage,
		})

		fmt.Errorf("v", err)
	}

	strinfo, err := js.StreamInfo(streamName)
	fmt.Errorf("v", err)

	prettyPrint(strinfo)

}

func prettyPrint(x interface{}) {
	b, err := json.MarshalIndent(x, "", "  ")
	fmt.Errorf("v", err)

	fmt.Println(string(b))
}

func main() {
	fmt.Println("SECTION: Setup Nats JS Manager")
	js := GetContext()
	createStream(js)
	createOrder(js)
}

const (
	subjectName = "ORDERS.received"
)

type Order struct {
	OrderId    int
	CustomerId string
	Status     string
}

//createOrder pin;osjed stream of events
func createOrder(js nats.JetStreamContext) error {
	for i := 1; i <= 10; i++ {
		order := Order{
			OrderId:    i,
			CustomerId: "cust-" + strconv.Itoa(i),
			Status:     "created",
		}
		orderJSON, _ := json.Marshal(order)
		_, err := js.Publish(subjectName, orderJSON)
		if err != nil {
			return err
		}
		log.Printf("Order with OrderID: %d has been published\n", i)
	}
	return nil
}
