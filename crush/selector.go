package crush

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
	"lukechampine.com/blake3"
	"lukechampine.com/uint128"
)

type Selector interface {
	Select(input uint64, round uint64) Node
}

// A selector for use with Crush that chooses nodes based on rendezvous hashing.
// See https://en.wikipedia.org/wiki/Rendezvous_hashing .
type RendezvousHashSelector struct {
	nodes     []Node
	node2Hash map[Node]uint64
}

func NewRendezvousHashSelector(nodes []Node) *RendezvousHashSelector {
	node2Hash := make(map[Node]uint64)
	for _, node := range nodes {
		// The ids are very similar to eachother so using a cryptographic
		// hash yields much more fair distributions.
		h := blake3.Sum256([]byte(node.GetId()))
		node2Hash[node] = binary.BigEndian.Uint64(h[:8])
	}
	return &RendezvousHashSelector{
		nodes:     nodes,
		node2Hash: node2Hash,
	}
}

func (s *RendezvousHashSelector) Select(input uint64, round uint64) Node {
	var buf [8 * 3]byte
	var selected Node
	var largestWeightedHash uint128.Uint128
	for _, node := range s.nodes {
		binary.BigEndian.PutUint64(buf[0:8], input)
		binary.BigEndian.PutUint64(buf[8:16], round)
		binary.BigEndian.PutUint64(buf[16:24], s.node2Hash[node])
		hash := uint128.From64(xxhash.Sum64(buf[:]))
		weightedHash := hash.Mul64(node.GetWeight())

		if selected == nil {
			selected = node
			largestWeightedHash = weightedHash
			continue
		}

		switch largestWeightedHash.Cmp(weightedHash) {
		case -1:
			selected = node
			largestWeightedHash = weightedHash
		case 0:
			if node.GetId() > selected.GetId() {
				selected = node
				largestWeightedHash = weightedHash
			}
		default:
			// Nothing to do.
		}
	}
	return selected
}
