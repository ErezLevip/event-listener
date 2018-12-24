package event_listener

import (
	"github.com/erezlevip/event-listener/types"
)

type ListenerConfig map[string]interface{}

type EventListener interface {
	Listen() (out chan *types.WrappedEvent,errors chan error)
}
