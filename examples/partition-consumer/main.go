package main

import (
	"bytes"
	"encoding/json"
	"github.com/erezlevip/event-listener"
	"github.com/erezlevip/event-listener/types"
	"log"
	"strconv"
)

func main() {
	listenerConfig := make(map[string]interface{})
	listenerConfig[event_listener.CONSUMER_RETURN_ERRORS] = strconv.FormatBool(true)
	listenerConfig[event_listener.TOPICS] = ""
	listenerConfig[event_listener.ZOOKEEPER_CONNECTION_STRING] = ""
	listenerConfig[event_listener.CONSUMER_GROUP] = "group1"
	listenerConfig[event_listener.MAX_BUFFER_SIZE] = "0"

	jsonConfig, _ := json.Marshal(listenerConfig)

	el, err := event_listener.NewKafkaEventListener(bytes.NewReader(jsonConfig))

	if err != nil {
		log.Panic(err)
	}

	out, errOut := el.Listen()

	for _, ch := range out {
		go func() {
			for msg := range ch {
				process(msg)
				msg.Ack()
			}
		}()
	}
	go func() {
		for err := range errOut {
			log.Println(err)
		}
	}()

	<-make(chan bool)
}

func process(e *types.WrappedEvent) {
	data, err := e.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	var msg map[string]string
	err = json.Unmarshal(data, &msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("this is a new event, consumed from topic", e.Topic)
	log.Println(msg)
}
