package main

import "github.com/gorilla/websocket"

type App struct {
	eventFetcher *EventFetcher
	connections  map[*websocket.Conn]chan *Event
}

func NewApp() *App {
	app := new(App)
	app.eventFetcher = NewEventFetcher()
	app.connections = make(map[*websocket.Conn]chan *Event)
	return app
}

func main() {
	app := NewApp()
	defer app.eventFetcher.Close()
	go app.eventFetcher.startFetch()
	app.setHandlers()
	go app.eventSender()
	startWebServer()
}
