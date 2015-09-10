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
	"fmt"
	"strconv"
	"strings"
)

// TODO: This all uses a dumb string representation format which is very bytes-intensive.
// A binary protocol would be nice.

type msgType int

const (
	msgTypeHeartbeat          msgType = iota
	msgTypeClaimingPartition  msgType = iota
	msgTypeReleasingPartition msgType = iota
	msgTypeClaimingMessages   msgType = iota
)

type message interface {
	Encode() string
	Type() msgType
}

// Decode takes a slice of bytes that should constitute a single message and attempts to
// decode it into one of our message structs.
func Decode(inp []byte) (message, error) {
	parts := strings.Split(string(inp), "/")
	if len(parts) < 6 {
		return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
	}

	// Get out the base message which is always present as it identifies the sender.
	partId, err := strconv.Atoi(parts[5])
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
	}
	ts, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
	}
	base := msgBase{
		Time:     ts,
		ClientId: parts[2],
		GroupId:  parts[3],
		Topic:    parts[4],
		PartId:   partId,
	}

	switch parts[0] {
	case "Heartbeat":
		if len(parts) != 7 {
			return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
		}
		offset, err := strconv.Atoi(parts[6])
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
		}
		return &msgHeartbeat{msgBase: base, LastOffset: offset}, nil
	case "ClaimingPartition":
		if len(parts) != 6 {
			return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
		}
		return &msgClaimingPartition{msgBase: base}, nil
	case "ReleasingPartition":
		if len(parts) != 7 {
			return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
		}
		offset, err := strconv.Atoi(parts[6])
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
		}
		return &msgReleasingPartition{msgBase: base, LastOffset: offset}, nil
	case "ClaimingMessages":
		if len(parts) != 7 {
			return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
		}
		offset, err := strconv.Atoi(parts[6])
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
		}
		return &msgClaimingMessages{msgBase: base, ProposedLastOffset: offset}, nil
	}
	return nil, errors.New(fmt.Sprintf("Invalid message: [%s]", string(inp)))
}

type msgBase struct {
	Time     int
	ClientId string
	GroupId  string
	Topic    string
	PartId   int
}

// Encode returns a string representation of the message.
func (m *msgBase) Encode() string {
	return fmt.Sprintf("%d/%s/%s/%s/%d", m.Time, m.ClientId, m.GroupId, m.Topic, m.PartId)
}

// Type returns the type of this message.
func (m *msgBase) Type() {
	panic("Attempted to type the base message. This should never happen.")
}

// msgHeartbeat is sent regularly by all consumers to re-up their claim to the partition that
// they're consuming.
type msgHeartbeat struct {
	msgBase
	LastOffset int
}

// Encode returns a string representation of the message.
func (m *msgHeartbeat) Encode() string {
	return "Heartbeat/" + m.msgBase.Encode() + fmt.Sprintf("/%d", m.LastOffset)
}

// Type returns the type of this message.
func (m *msgHeartbeat) Type() msgType {
	return msgTypeHeartbeat
}

// msgClaimingPartition is used in the claim flow.
type msgClaimingPartition struct {
	msgBase
}

// Encode returns a string representation of the message.
func (m *msgClaimingPartition) Encode() string {
	return "ClaimingPartition/" + m.msgBase.Encode()
}

// Type returns the type of this message.
func (m *msgClaimingPartition) Type() msgType {
	return msgTypeClaimingPartition
}

// msgReleasingPartition is used in a controlled shutdown to indicate that you are done with
// a partition.
type msgReleasingPartition struct {
	msgBase
	LastOffset int
}

// Encode returns a string representation of the message.
func (m *msgReleasingPartition) Encode() string {
	return "ReleasingPartition/" + m.msgBase.Encode() + fmt.Sprintf("/%d", m.LastOffset)
}

// Type returns the type of this message.
func (m *msgReleasingPartition) Type() msgType {
	return msgTypeReleasingPartition
}

// msgClaimingMessages is used for at-most-once consumption semantics, this is a pre-commit
// advisory message.
type msgClaimingMessages struct {
	msgBase
	ProposedLastOffset int
}

// Encode returns a string representation of the message.
func (m *msgClaimingMessages) Encode() string {
	return "ClaimingMessages/" + m.msgBase.Encode() + fmt.Sprintf("/%d", m.ProposedLastOffset)
}

// Type returns the type of this message.
func (m *msgClaimingMessages) Type() msgType {
	return msgTypeClaimingMessages
}
