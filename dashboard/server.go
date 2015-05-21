package main

import (
	"encoding/json"
	"log"
	"net/http"
	"fmt"
	"github.com/gorilla/websocket"
	"os"
)

func (this *App) setHandlers() {
	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))
	http.HandleFunc("/events", this.eventsHandler)
	http.HandleFunc("/event_history", this.eventHistoryHandler)
	http.HandleFunc("/", this.dashboardHandler)
}

func (this *App) eventHistoryHandler(w http.ResponseWriter, req *http.Request) {
	history := this.eventFetcher.EventHistory()
	jsonData, err := json.Marshal(history)
	if err != nil {
		log.Fatal(err)
	}
	renderJSON(w, jsonData)
}

func renderJSON(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (this *App) dashboardHandler(w http.ResponseWriter, req *http.Request) {
	http.ServeFile(w, req, "static/index.html")
}

func (this *App) eventsHandler(w http.ResponseWriter, req *http.Request) {
	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	conn, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		log.Println(err)
		return
	}
	this.connections[conn] = make(chan *Event)
	for {
		message := <-this.connections[conn]
		err := conn.WriteJSON(message)
		if err != nil {
			delete(this.connections, conn)
			conn.Close()
			return
		}
	}
}

func (this *App) eventSender() {
	for {
		if len(this.connections) == 0 {
			<-this.eventFetcher.events
		} else {
			message := <-this.eventFetcher.events
			this.sendToAll(message)
		}
	}
}

func (this *App) sendToAll(message *Event) {
	for _, events := range this.connections {
		events <- message
	}
}

func startWebServer(port int) {
	hostName, _ := os.Hostname()
	err := http.ListenAndServe(fmt.Sprintf("%s:%d", hostName, port), nil)
	if err != nil {
		log.Fatal(err)
	}
}
