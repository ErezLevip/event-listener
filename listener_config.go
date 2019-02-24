package event_listener

import (
	"github.com/bsm/sarama-cluster"
	"time"
)

type ListenerConfig struct {
	Connection []string
	Topics []string
	MaxBufferSize int
	ConsumerGroup string
	InitialOffset int64
	ProcessingTimeout time.Duration
	ReturnErrors bool
	ReturnNotifications bool
}

func (lc *ListenerConfig) ToSaramaConfig() *cluster.Config {
	var config = cluster.NewConfig()
	config.Group.Return.Notifications = lc.ReturnNotifications
	config.Consumer.Return.Errors = lc.ReturnErrors
	config.Consumer.MaxProcessingTime = lc.ProcessingTimeout
	config.Consumer.Offsets.Initial = lc.InitialOffset
	config.ChannelBufferSize = lc.MaxBufferSize
	return config
}