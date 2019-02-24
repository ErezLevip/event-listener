package event_listener

type EventListener interface {
	Listen() (out map[string]<- chan *WrappedEvent, errors map[string]<- chan error,error error)
}
