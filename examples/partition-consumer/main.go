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
	listenerConfig[event_listener.TOPICS] = "comments"
	listenerConfig[event_listener.ZOOKEEPER_CONNECTION_STRING] = ""

	jsonConfig,_ := json.Marshal(listenerConfig)

	el,err := event_listener.NewKafkaEventListener(bytes.NewReader(jsonConfig))

	if err != nil{
		log.Panic(err)
	}

	topicsOutChannels := make(map[string]chan *types.WrappedEvent)
	errOut :=make( chan error)

	go func() {
		topicsOutChannels,errOut = el.Listen()
	}()

	go func() {
		for t,c := range topicsOutChannels{
			go processTopic(t,c)
		}
	}()

	go func() {
		for err := range errOut{
			log.Println(err)
		}
	}()

	<- make(chan bool)
}


func processTopic(topic string,events chan *types.WrappedEvent){
	for e := range events{
		data,err := e.ReadAll()
		if err != nil{
			log.Fatal(err)
		}
		var msg map[string]string
		err = json.Unmarshal(data,&msg)
		if err != nil{
			log.Fatal(err)
		}
		log.Println("this is a new event, consumed from topic",topic)
		log.Println(msg)
	}
}

