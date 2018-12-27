package event_listener

import (
	"github.com/erezlevip/event-listener/types"
)

type ListenerConfig map[string]interface{}

type EventListener interface {
	Listen() (out map[string]<- chan *types.WrappedEvent, errors map[string]<- chan error)
}
