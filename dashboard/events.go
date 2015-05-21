package main

import (
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

type Event struct {
	Topic      string `json:"topic"`
	Partition  string `json:"partition"`
	ConsumerId string `json:"consumerId"`
	EventName  string `json:"eventName"`
	Second     uint64 `json:"second"`
	Operation  string `json:"operation"`
	Value      uint64 `json:"value"`
	Count      uint64 `json:"count"`
}

type EventFetcher struct {
	events     chan *Event
	connection *gocql.Session
	lastSeen   uint64
}

func NewEventFetcher() *EventFetcher {
	var err error
	fetcher := new(EventFetcher)
	fetcher.events = make(chan *Event)
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "spark_analysis"
	fetcher.connection, err = cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	fetcher.lastSeen = 0
	return fetcher
}

func (this *EventFetcher) Close() {
	this.connection.Close()
}

func (this *EventFetcher) FetchEvents() {
	var operations = [...]string{"avg10sec", "avg30sec", "avg1min", "avg5min", "avg10min", "avg15min"}

	for i := 0; i < 4; i++ {
		for _, operation := range operations {
			consumerId := strconv.Itoa(i + 1)
			eventName := "generated-consumed"
			event := &Event{
				Operation:  operation,
				EventName:  eventName,
				ConsumerId: consumerId,
				Value:      uint64(rand.Intn(9999)),
				Second:     this.lastSeen + 1,
			}
			this.events <- event
		}
	}
	this.lastSeen += 1
}

func (this *EventFetcher) EventHistory() []Event {
	data := this.connection.Query("SELECT * FROM events;").Iter()
	var events []Event
	var topic string
	var partition string
	var consumerId string
	var eventName string
	var second uint64
	var operation string
	var value uint64
	for data.Scan(&topic, &second, &partition, &consumerId, &eventName, &operation, &value) {
		event := Event{
			Topic:      topic,
			Second:     second,
			Partition:  partition,
			ConsumerId: consumerId,
			EventName:  eventName,
			Operation:  operation,
			Value:      value,
		}
		events = append(events, event)
		this.lastSeen = event.Second
	}
	return events
}

func (this *EventFetcher) startFetch() {
	for _ = range time.Tick(1 * time.Second) {
		this.FetchEvents()
	}
}