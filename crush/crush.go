package crush

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/google/shlex"
	"github.com/xlab/treeprint"
)

type Node interface {
	GetId() string
	GetTypeIdx() int
	GetChildren() []Node
	GetWeight() uint64
	IsDefunct() bool
	GetParent() Node
	IsLeaf() bool
	Select(input uint64, round uint64) Node
}

type Location []string

func (l Location) Equals(other Location) bool {
	if len(l) != len(other) {
		return false
	}
	// Reverse order should exit faster since most
	// locations actually have a unique end.
	for i := len(l) - 1; i >= 0; i -= 1 {
		if l[i] != other[i] {
			return false
		}
	}
	return true
}

func (l Location) String() string {
	return fmt.Sprintf("%v", []string(l))
}

type StorageNodeInfo struct {
	Location Location
	Defunct  bool
	Weight   uint64
}

type StorageNode struct {
	StorageNodeInfo
	Id      string
	TypeIdx int
	Parent  Node
}

func (n *StorageNode) GetChildren() []Node {
	return []Node{}
}

func (n *StorageNode) IsDefunct() bool {
	return n.Defunct
}

func (n *StorageNode) GetWeight() uint64 {
	return n.Weight
}

func (n *StorageNode) GetTypeIdx() int {
	return n.TypeIdx
}

func (n *StorageNode) GetId() string {
	return n.Id
}

func (n *StorageNode) GetParent() Node {
	return n.Parent
}

func (n *StorageNode) IsLeaf() bool {
	return true
}

func (n *StorageNode) Select(input uint64, round uint64) Node {
	return nil
}

type InternalNode struct {
	Id          string
	TypeIdx     int
	Parent      Node
	Children    []Node
	ChildNames  []string
	NameToChild map[string]Node
	Defunct     bool
	UsedSpace   int64
	Weight      uint64
	Selector    Selector
}

func (n *InternalNode) GetChildren() []Node {
	return n.Children
}

func (n *InternalNode) IsDefunct() bool {
	return n.Defunct
}

func (n *InternalNode) GetWeight() uint64 {
	return n.Weight
}

func (n *InternalNode) GetId() string {
	return n.Id
}

func (n *InternalNode) GetTypeIdx() int {
	return n.TypeIdx
}

func (n *InternalNode) GetParent() Node {
	return n.Parent
}

func (n *InternalNode) IsLeaf() bool {
	return len(n.Children) == 0
}

func (n *InternalNode) Select(input uint64, round uint64) Node {
	return n.Selector.Select(input, round)
}

type StorageHierarchy struct {
	Types        []string
	TypeToIdx    map[string]int
	IdxToType    map[int]string
	StorageNodes []*StorageNode
	Root         *InternalNode
}

func NewStorageHierarchyFromSchema(s string) (*StorageHierarchy, error) {

	types, err := shlex.Split(s)
	if err != nil {
		return nil, fmt.Errorf("unable to split types: %s", err)
	}

	h := &StorageHierarchy{
		Types:     types,
		TypeToIdx: make(map[string]int),
		IdxToType: make(map[int]string),
		Root: &InternalNode{
			NameToChild: make(map[string]Node),
		},
	}
	for i, t := range h.Types {
		if _, ok := h.TypeToIdx[t]; ok {
			return nil, fmt.Errorf("duplicate type %s in schema", t)
		}
		h.TypeToIdx[t] = i
		h.IdxToType[i] = t
	}
	return h, nil
}

func (h *StorageHierarchy) ContainsStorageNodeAtLocation(location Location) bool {
	if len(h.Types) != len(location) {
		return false
	}
	n := h.Root
	for _, name := range location {
		child, ok := n.NameToChild[name]
		if !ok {
			return false
		}
		switch child := child.(type) {
		case *StorageNode:
			return true
		case *InternalNode:
			n = child
		}
	}
	return false
}

func (h *StorageHierarchy) AddStorageNode(ni *StorageNodeInfo) error {

	// Validate compatible.
	if len(ni.Location) != len(h.Types) {
		return fmt.Errorf(
			"location %v is not compatible with type schema %v, expected %d members",
			ni.Location,
			h.Types,
			len(h.Types),
		)
	}

	n := &StorageNode{
		StorageNodeInfo: *ni,
	}

	idBuf := bytes.Buffer{}

	node := h.Root
	for i, loc := range n.Location {
		ty := h.Types[i]

		_, err := idBuf.WriteString(loc)
		if err != nil {
			panic(err)
		}
		if i != 0 {
			_, err := idBuf.WriteString("\x00")
			if err != nil {
				panic(err)
			}
		}

		if i != len(n.Location)-1 {
			if _, ok := node.NameToChild[loc]; !ok {
				node.NameToChild[loc] = &InternalNode{
					Id:          idBuf.String(),
					TypeIdx:     h.TypeToIdx[ty],
					NameToChild: make(map[string]Node),
				}
			}
			node = node.NameToChild[loc].(*InternalNode)
		} else {
			if _, ok := node.NameToChild[loc]; ok {
				return fmt.Errorf(
					"location '%s' has already been added to storage heirarchy",
					n.Location,
				)
			}
			n.Id = idBuf.String()
			n.TypeIdx = h.TypeToIdx[ty]
			node.NameToChild[loc] = n
		}
	}

	h.StorageNodes = append(h.StorageNodes, n)

	return nil
}

func (h *StorageHierarchy) Finish() error {

	if len(h.StorageNodes) == 0 {
		return fmt.Errorf("expected at least one storage node")
	}

	var recurse func(n *InternalNode)
	recurse = func(n *InternalNode) {
		n.ChildNames = make([]string, 0, len(n.NameToChild))
		for name, child := range n.NameToChild {
			n.ChildNames = append(n.ChildNames, name)
			switch child := child.(type) {
			case *InternalNode:
				recurse(child)
				n.Weight += child.Weight
			case *StorageNode:
				n.Weight += child.Weight
			}
		}
		sort.Strings(n.ChildNames)
		n.Children = make([]Node, 0, len(n.ChildNames))
		for _, name := range n.ChildNames {
			n.Children = append(n.Children, n.NameToChild[name])
		}

		if len(n.Children) != 0 {
			n.Defunct = true
			for _, child := range n.Children {
				if !child.IsDefunct() {
					n.Defunct = false
				}
			}
		}
		n.Selector = NewRendezvousHashSelector(n.Children)
	}
	recurse(h.Root)

	return nil
}

type CrushSelectPlacement struct {
	Type  string
	Count uint64
}

type PlacementRule interface{}

func (h *StorageHierarchy) Crush(input string, rules []PlacementRule) ([]Location, error) {

	if len(rules) == 0 {
		return nil, errors.New("expected at least one placement rule")
	}

	expectedCount := uint64(1)
	selection := []Node{}
	nodes := []Node{h.Root}
	hashedInput := xxhash.Sum64String(input)

	for _, rule := range rules {
		switch rule := rule.(type) {
		case CrushSelectPlacement:
			nodeType, ok := h.TypeToIdx[rule.Type]
			if !ok {
				return nil, fmt.Errorf("unable to do CRUSH mapping, unknown type '%s'", rule.Type)
			}
			expectedCount *= rule.Count
			nextNodes := make([]Node, 0, uint64(len(nodes))*rule.Count)
			for _, n := range nodes {
				selection = h.doSelect(n, hashedInput, uint64(rule.Count), nodeType, selection[:0])
				nextNodes = append(nextNodes, selection...)
			}
			nodes = nextNodes
		default:
			panic("bug")
		}

	}

	locations := make([]Location, 0, len(nodes))
	for _, node := range nodes {
		storageNode, ok := node.(*StorageNode)
		if !ok {
			return nil, fmt.Errorf("'%s' is a not a storage node", node.GetId())
		}
		locations = append(locations, storageNode.Location)
	}

	if uint64(len(locations)) != expectedCount {
		return locations, errors.New("unable to satisfy crush placement")
	}

	return locations, nil
}

func (h *StorageHierarchy) doSelect(parent Node, input uint64, count uint64, nodeTypeIdx int, results []Node) []Node {
	rPrime := uint64(0)
	for r := uint64(1); r <= count; r++ {
		failure := uint64(0)
		loopbacks := uint64(0)
		escape := false
		retryOrigin := false
		var out Node
		for {
			retryOrigin = false
			in := parent
			retryNode := false
			for {
				retryNode = false
				rPrime = r + failure
				out = in.Select(input, rPrime)
				if out.GetTypeIdx() != nodeTypeIdx {
					in = out
					retryNode = true
				} else {
					if contains(results, out) {
						if !nodesAvailable(in, results) {
							if loopbacks == 150 {
								escape = true
								break
							}
							loopbacks += 1
							retryOrigin = true
						} else {
							retryNode = true
						}
						failure += 1
					} else if out.IsDefunct() {
						failure += 1
						if loopbacks == 150 {
							escape = true
							break
						}
						loopbacks += 1
						retryOrigin = true
					} else {
						break
					}
				}
				if !retryNode {
					break
				}
			}
			if !retryOrigin {
				break
			}
		}
		if escape {
			continue
		}
		results = append(results, out)
	}

	return results
}

func nodesAvailable(parent Node, selected []Node) bool {
	children := parent.GetChildren()
	for _, child := range children {
		if child.IsDefunct() {
			continue
		}
		if contains(selected, child) {
			continue
		}
		return true
	}
	return false
}

func contains(s []Node, n Node) bool {
	for _, a := range s {
		if a == n {
			return true
		}
	}
	return false
}

func (h *StorageHierarchy) AsciiTree() string {
	t := treeprint.New()
	var recurse func(t treeprint.Tree, n *InternalNode)
	recurse = func(t treeprint.Tree, n *InternalNode) {
		for _, name := range n.ChildNames {
			switch child := n.NameToChild[name].(type) {
			case *InternalNode:
				childT := t.AddBranch(name)
				recurse(childT, child)
			case *StorageNode:
				status := "healthy"
				if child.Defunct {
					status = "defunct"
				}
				meta := fmt.Sprintf("%d %s", child.Weight, status)
				t.AddBranch(name).SetMetaValue(meta)
			}
		}
	}
	recurse(t, h.Root)
	return t.String()
}
