/*
 * portal - marshal
 *
 * a library that implements an algorithm for doing consumer coordination within Kafka, rather
 * than using Zookeeper or another external system.
 *
 */

package marshal

import (
	"sync"
	"time"
)

// topicState contains information about a given topic.
type topicState struct {
	// This lock also protects the contenst of the partitions.
	lock       sync.RWMutex
	partitions []PartitionClaim
}

// PartitionClaim contains claim information about a given partition.
type PartitionClaim struct {
	LastHeartbeat int64
	LastOffset    int
	ClientId      string
	GroupId       string

	// Used internally when someone is waiting on this partition to be claimed.
	pendingClaims []chan bool
}

// isClaimed returns a boolean indicating whether or not this structure is indicating a
// still valid claim. Validity is based on the delta between NOW and lastHeartbeat:
//
// delta = 0 .. HEARTBEAT_INTERVAL: claim good.
//         HEARTBEAT_INTERVAL .. 2*HEARTBEAT_INTERVAL-1: claim good.
//         >2xHEARTBEAT_INTERVAL: claim invalid.
//
// This means that the worst case for a "dead consumer" that has failed to heartbeat
// is that a partition will be idle for twice the heartbeat interval.
func (p *PartitionClaim) isClaimed(ts int64) bool {
	// If lastHeartbeat is 0, then the partition is unclaimed
	if p.LastHeartbeat == 0 {
		return false
	}

	// We believe we have claim information, but let's analyze it to determine whether or
	// not the claim is valid. Of course this assumes that our time and the remote's time
	// are roughly in sync.
	now := ts
	if ts == 0 {
		now = time.Now().Unix()
	}

	delta := now - p.LastHeartbeat
	switch {
	case 0 <= delta && delta <= HEARTBEAT_INTERVAL:
		// Fresh claim - all good
		return true
	case HEARTBEAT_INTERVAL < delta && delta < 2*HEARTBEAT_INTERVAL:
		// Aging claim - missed/delayed heartbeat, but still in tolerance
		return true
	default:
		// Stale claim - no longer valid
		return false
	}
}
