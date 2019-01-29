package event_listener

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/erezlevip/event-listener/acceptable_interfaces"
	"github.com/erezlevip/event-listener/types"
	"io"
	"strconv"
	"strings"
	"time"
)

const PROCESSING_TIMEOUT = "processing_timeout"
const INITIAL_OFFSET = "initial_offset"
const ZOOKEEPER_CONNECTION_STRING = "zookeeper_connection_string"
const KAFKA_BROKERS_CONNECTION_STRING = "kafka_brokers_connection_string"
const TOPICS = "topics"
const CONSUMER_RETURN_ERRORS = "consumer_return_errors"
const CONSUMER_GROUP = "consumer_group"
const MAX_BUFFER_SIZE = "max_buffer_size"

const METADATA_KEY_PARTITION = "partition"
const METADATA_KEY_OFFSET = "offset"

type KafkaEventListener struct {
	saramaConfig   *sarama.Config
	brokers        []string
	topics         []string
	zookeeper      []string
	group          string
	maxBufferSize  int64
	consumerGroups map[string]*cluster.Consumer
	config         map[string]string
	logger         acceptable_interfaces.Logger
}

func NewKafkaEventListenerWithSaramaConfig(scfg *sarama.Config, cfg io.Reader, logger acceptable_interfaces.Logger) (EventListener, error) {
	serializedConfig, err := serializeConfig(cfg)
	if err != nil {
		return nil, err
	}

	zookeeper := strings.Split(serializedConfig[ZOOKEEPER_CONNECTION_STRING], ",")
	brokers := strings.Split(serializedConfig[KAFKA_BROKERS_CONNECTION_STRING], ",")

	topics := strings.Split(serializedConfig[TOPICS], ",")

	maxBufferSize, err := strconv.ParseInt(serializedConfig[MAX_BUFFER_SIZE], 10, 64)
	if err != nil {
		return nil, err
	}

	return &KafkaEventListener{
		saramaConfig:  scfg,
		topics:        topics,
		zookeeper:     zookeeper,
		brokers:       brokers,
		group:         serializedConfig[CONSUMER_GROUP],
		maxBufferSize: maxBufferSize,
		logger:        logger,
	}, nil
}

func NewKafkaEventListener(config io.Reader, logger acceptable_interfaces.Logger) (EventListener, error) {
	serializedConfig, err := serializeConfig(config)
	if err != nil {
		return nil, err
	}

	kafkaConfig, err := serializeSaramaConfig(serializedConfig)
	if err != nil {
		return nil, err
	}

	zookeeper := strings.Split(serializedConfig[ZOOKEEPER_CONNECTION_STRING], ",")
	brokers := strings.Split(serializedConfig[KAFKA_BROKERS_CONNECTION_STRING], ",")

	topics := strings.Split(serializedConfig[TOPICS], ",")

	maxBufferSize, err := strconv.ParseInt(serializedConfig[MAX_BUFFER_SIZE], 10, 64)
	if err != nil {
		return nil, err
	}

	return &KafkaEventListener{
		saramaConfig:  kafkaConfig,
		brokers:       brokers,
		topics:        topics,
		zookeeper:     zookeeper,
		group:         serializedConfig[CONSUMER_GROUP],
		maxBufferSize: maxBufferSize,
		config:        serializedConfig,
		logger:        logger,
	}, nil
}

func (l *KafkaEventListener) Listen() (map[string]<-chan *types.WrappedEvent, map[string]<-chan error) {

	cgs, err := l.initConsumer(l.topics, l.group, l.brokers, l.maxBufferSize)
	if err != nil {
		l.logger.Fatal(err)
	}

	outMap := make(map[string]<-chan *types.WrappedEvent, len(l.topics))
	errors := make(map[string]<-chan error, len(l.topics))

	for t, cg := range cgs {
		l.logger.Info(fmt.Sprintf("listening on %s", l.topics))
		outMap[t] = l.consume(l.topics, cg)
		errors[t] = cg.Errors()
	}

	return outMap, errors
}

func (l *KafkaEventListener) initConsumer(topics []string, cgroup string, brokers []string, maxBufferSize int64) (map[string]*cluster.Consumer, error) {
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Return.Errors = true

	initialOffset, err := getInitialOffset(l.config)
	if err != nil {
		return nil, err
	}
	config.Consumer.Offsets.Initial = initialOffset
	processingTimeout, err := getProcessingTimeout(l.config)
	if err != nil {
		return nil, err
	}
	config.Consumer.MaxProcessingTime = processingTimeout

	if maxBufferSize > 0 {
		config.ChannelBufferSize = int(maxBufferSize)
	}

	l.consumerGroups = make(map[string]*cluster.Consumer, len(topics))

	for _, t := range topics {
		// join to consumer group
		consumer, err := cluster.NewConsumer(brokers, cgroup, topics, config)
		if err != nil {
			return nil, err
		}
		l.consumerGroups[t] = consumer
	}

	return l.consumerGroups, nil
}

func (l *KafkaEventListener) consume(topics []string, cg *cluster.Consumer) chan *types.WrappedEvent {
	out := make(chan *types.WrappedEvent)
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

			out <- &types.WrappedEvent{
				Value: bytes.NewReader(msg.Value),
				Topic: msg.Topic,
				Metadata: map[string]string{
					METADATA_KEY_PARTITION: strconv.Itoa(int(msg.Partition)),
					METADATA_KEY_OFFSET:    strconv.FormatInt(msg.Offset, 10),
				},
				Ack: func() error {
					l.logger.Debug(fmt.Sprintf("committing partition %v, offset %v", msg.Partition, msg.Offset))
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

func serializeConfig(reader io.Reader) (map[string]string, error) {
	buff := make([]byte, 4)
	total := make([]byte, 0)
	var config map[string]string
	for {
		n, err := reader.Read(buff)
		if err != nil {
			if err == io.EOF {
				total = append(total, buff[:n]...)
				break
			}
			return nil, err
		}
		total = append(total, buff[:n]...)
	}

	err := json.Unmarshal(total, &config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func serializeSaramaConfig(consumerConfig map[string]string) (*sarama.Config, error) {
	var config = sarama.NewConfig()

	if val, exists := consumerConfig[CONSUMER_RETURN_ERRORS]; exists {
		v, err := strconv.ParseBool(val)

		if err != nil {
			return nil, err
		}

		if v {
			config.Consumer.Return.Errors = true
		}
	}
	return config, nil
}

func getInitialOffset(config map[string]string) (int64, error) {
	if val, ok := config[INITIAL_OFFSET]; ok {
		return strconv.ParseInt(val, 10, 64)
	}
	return sarama.OffsetOldest, nil
}
func getProcessingTimeout(config map[string]string) (time.Duration, error) {
	if val, ok := config[PROCESSING_TIMEOUT]; ok {
		return time.ParseDuration(val)
	}
	return 10 * time.Second, nil
}
