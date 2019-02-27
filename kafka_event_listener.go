package event_listener

import (
	"bytes"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"strconv"
	"time"
)

const METADATA_KEY_PARTITION = "partition"
const METADATA_KEY_OFFSET = "offset"

type KafkaEventListener struct {
	config *ListenerConfig
	logger Logger
}

func NewKafkaEventListener(config *ListenerConfig, logger Logger) EventListener {
	err := validateAndApplyConfigDefaults(config)
	if err != nil {
		logger.Fatal(err)
		return nil
	}

	return &KafkaEventListener{
		config: config,
		logger: logger,
	}
}

func validateAndApplyConfigDefaults(config *ListenerConfig) error {
	if config == nil {
		return fmt.Errorf("config cant be nil")
	}

	if config.ProcessingTimeout == 0 {
		config.ProcessingTimeout = 10 * time.Second
	}

	if config.InitialOffset == 0 {
		config.InitialOffset = sarama.OffsetOldest
	}

	if len(config.Topics) == 0 {
		return fmt.Errorf("topics are missing")
	}

	if len(config.Connection) == 0 {
		return fmt.Errorf("connection is missing")
	}
	return nil
}

func (l *KafkaEventListener) Listen() (map[string]<-chan *WrappedEvent, map[string]<-chan error, error) {
	outMap := make(map[string]<-chan *WrappedEvent, len(l.config.Topics))
	errors := make(map[string]<-chan error, len(l.config.Topics))

	for _, t := range l.config.Topics {
		cg, err := cluster.NewConsumer(l.config.Connection, l.config.ConsumerGroup, []string{t}, l.config.ToSaramaConfig())
		if err != nil {
			return nil, nil, err
		}

		l.logger.Info(fmt.Sprintf("listening on %s", l.config.Topics))
		outMap[t] = l.consume(l.config.Topics, cg)
		errors[t] = cg.Errors()
	}

	return outMap, errors, nil
}

func (l *KafkaEventListener) consume(topics []string, cg *cluster.Consumer) chan *WrappedEvent {
	out := make(chan *WrappedEvent)
	go func() {
		defer close(out)
		l.logger.Info("waiting for messages")
		for msg := range cg.Messages() {
			l.logger.Debug("message inbound", "key", string(msg.Key))

			valid := false
			for _, t := range topics {
				if t == msg.Topic {
					valid = true
					break
				}
			}

			if !valid {
				continue
			}

			out <- &WrappedEvent{
				Value: bytes.NewReader(msg.Value),
				Topic: msg.Topic,
				Metadata: map[string]string{
					METADATA_KEY_PARTITION: strconv.Itoa(int(msg.Partition)),
					METADATA_KEY_OFFSET:    strconv.FormatInt(msg.Offset, 10),
				},
				Ack: func() error {
					l.logger.Debug(fmt.Sprintf("committing partition %v, offset %v", msg.Partition, msg.Offset))
					cg.MarkOffset(msg,"")
					return cg.CommitOffsets()
				},
			}
			l.logger.Debug(fmt.Sprintf("written to %s", msg.Topic))
		}
	}()

	go func() {
		for n := range cg.Notifications() {
			l.logger.Debug(n.Type.String())
		}
	}()
	return out
}
