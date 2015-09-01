/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"errors"

	"github.com/op/go-logging"
	"github.com/optiopay/kafka"
)

const (
	// The main topic used for coordination. This must be constant across all consumers that
	// you want to coordinate.
	MARSHAL_TOPIC = "__marshal"

	// This is the main timing used to determine how "chatty" the system is and how fast it
	// responds to failures of consumers. THIS VALUE MUST BE THE SAME BETWEEN ALL CONSUMERS
	// as it is critical to coordination.
	HEARTBEAT_INTERVAL = 60 // Measured in seconds.
)

var log = logging.MustGetLogger("PortalMarshal")

func init() {
	logging.SetLevel(logging.WARNING, "PortalMarshal")
}

// NewMarshaler connects to a cluster (given broker addresses) and prepares to handle marshalling
// requests.
// TODO: It might be nice to make the marshaler agnostic of clients and able to support
// requests from N clients/groups. For now, though, we require instantiating a new
// marshaler for every client/group.
func NewMarshaler(clientId, groupId string, brokers []string) (*MarshalState, error) {
	brokerConf := kafka.NewBrokerConf("PortalMarshal")

	kfka, err := kafka.Dial(brokers, brokerConf)
	if err != nil {
		return nil, err
	}
	ws := &MarshalState{
		quit:          new(int32),
		clientId:      clientId,
		groupId:       groupId,
		kafka:         kfka,
		kafkaProducer: kfka.Producer(kafka.NewProducerConf()),
		topics:        make(map[string]int),
		groups:        make(map[string]map[string]*topicState),
	}
	ws.lock.Lock() // Probably not strictly necessary.
	defer ws.lock.Unlock()

	// TODO: This is unfortunate, as this triggers a completely fresh metadata fresh. We did one
	// just moments ago with the Dial...
	md, err := kfka.Metadata()
	if err != nil {
		return nil, err
	}
	foundMarshal := 0
	for _, topic := range md.Topics {
		if topic.Name == MARSHAL_TOPIC {
			foundMarshal = len(topic.Partitions)
		}
		ws.topics[topic.Name] = len(topic.Partitions)
		//log.Debug("Discovered: Topic %s has %d partitions.",
		//	topic.Name, len(topic.Partitions))
	}

	// If there is no marshal topic, then we can't run. The admins must go create the topic
	// before they can use this library. Please see the README.
	if foundMarshal == 0 {
		return nil, errors.New("Marshalling topic not found. Please see the documentation.")
	}

	// Now we start a goroutine to start consuming each of the partitions in the marshal
	// topic. Note that this doesn't handle increasing the partition count on that topic
	// without stopping all consumers.
	for id := 0; id < foundMarshal; id++ {
		go ws.rationalize(id, ws.kafkaConsumerChannel(id))
	}

	return ws, nil
}
