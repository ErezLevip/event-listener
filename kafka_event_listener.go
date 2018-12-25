package event_listener

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/erezlevip/event-listener/types"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

const ZOOKEEPER_CONNECTION_STRING = "zookeeper_connection_string"
const TOPICS = "topics"
const CONSUMER_RETURN_ERRORS = "consumer_return_errors"
const CONSUMER_GROUP = "consumer_group"
const MAX_BUFFER_SIZE = "max_buffer_size"

const METADATA_KEY_PARTITION = "partition"
const METADATA_KEY_OFFSET = "offset"

type KafkaEventListener struct {
	kafkaConfig    *sarama.Config
	topics         []string
	zookeeper      []string
	group          string
	maxBufferSize  int64
	consumerGroups map[string]*consumergroup.ConsumerGroup
}

func (el *KafkaEventListener) Ack(msg *types.WrappedEvent) error {
	partition, err := strconv.Atoi(msg.Metadata[METADATA_KEY_PARTITION])
	if err != nil {
		return err
	}

	offset, err := strconv.ParseInt(msg.Metadata[METADATA_KEY_OFFSET], 10, 64)
	err = el.consumerGroups[msg.Topic].CommitUpto(&sarama.ConsumerMessage{
		Topic:     msg.Topic,
		Partition: int32(partition),
		Offset:    offset,
	})

	if err != nil {
		fmt.Println("Error commit zookeeper: ", err.Error())
	}
	return err
}

func NewKafkaEventListener(config io.Reader) (EventListener, error) {
	serializedConfig, err := serializeConfig(config)
	if err != nil {
		return nil, err
	}

	kafkaConfig, err := serializeSaramaConfig(serializedConfig)
	if err != nil {
		return nil, err
	}

	zookeeper := strings.Split(serializedConfig[ZOOKEEPER_CONNECTION_STRING], ",")

	topics := strings.Split(serializedConfig[TOPICS], ",")

	maxBufferSize, err := strconv.ParseInt(serializedConfig[MAX_BUFFER_SIZE], 10, 64)
	if err != nil {
		return nil, err
	}

	return &KafkaEventListener{
		kafkaConfig:   kafkaConfig,
		topics:        topics,
		zookeeper:     zookeeper,
		group:         serializedConfig[CONSUMER_GROUP],
		maxBufferSize: maxBufferSize,
	}, nil
}

func (l *KafkaEventListener) Listen() (map[string]chan *types.WrappedEvent, chan error) {
	errors := make(chan error)
	cgs, err := l.initConsumer(l.topics, l.group, l.zookeeper, l.maxBufferSize)
	if err != nil {
		log.Panic(err)
	}

	outMap := make(map[string]chan *types.WrappedEvent, len(l.topics))

	for t, cg := range cgs {
		log.Println("listening on", l.topics)
		outMap[t] = consume(l.topics, cg)
	}

	return outMap, errors
}

func (l *KafkaEventListener) initConsumer(topics []string, cgroup string, zookeeperConn []string, maxBufferSize int64) (map[string]*consumergroup.ConsumerGroup, error) {
	// consumer config
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	if maxBufferSize > 0 {
		config.ChannelBufferSize = int(maxBufferSize)
	}

	l.consumerGroups = make(map[string]*consumergroup.ConsumerGroup, len(topics))

	for _, t := range topics {
		// join to consumer group
		cg, err := consumergroup.JoinConsumerGroup(cgroup, []string{t}, zookeeperConn, config)
		if err != nil {
			return nil, err
		}
		l.consumerGroups[t] = cg
	}

	return l.consumerGroups, nil
}

func consume(topics []string, cg *consumergroup.ConsumerGroup) chan *types.WrappedEvent {
	out := make(chan *types.WrappedEvent)
	go func() {
		defer close(out)
		log.Println("waiting for messages")
		for msg := range cg.Messages() {
			log.Println("message inbound", string(msg.Key))
			// messages coming through chanel
			// only take messages from subscribed topic

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

			go func() {
				out <- &types.WrappedEvent{
					Value: bytes.NewReader(msg.Value),
					Topic: msg.Topic,
					Metadata: map[string]string{
						METADATA_KEY_PARTITION: strconv.Itoa(int(msg.Partition)),
						METADATA_KEY_OFFSET:    strconv.FormatInt(msg.Offset, 10),
					},
				}
				log.Println("written to", msg.Topic)

				// commit to zookeeper that message is read
				// this prevent read message multiple times after restart
				/*err := cg.CommitUpto(msg)
				if err != nil {
					fmt.Println("Error commit zookeeper: ", err.Error())
				}*/
			}()
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
