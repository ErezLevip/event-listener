package event_listener

import (
	"github.com/erezlevip/event-listener/types"
)

type ListenerConfig map[string]interface{}

type EventListener interface {
	Listen() (topicsOutChannels map[string]chan *types.WrappedEvent,errors chan error)
}
