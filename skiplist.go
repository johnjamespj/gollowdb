package gollowdb

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-collections/collections/stack"
)

type Skiplist[V any] struct {
	EnhancedIterator[V]
	head       *skiplistNode[V]
	maxHeight  int
	size       uint64
	comparator Comparator[V]
	mu         sync.RWMutex
}

type skiplistNode[V any] struct {
	nodes []*skiplistNode[V]
	value *V
}

func CreateSkiplist[V any](estimateSize int, comparator Comparator[V]) *Skiplist[V] {
	height := int(math.Ceil(math.Log(float64(estimateSize)) / math.Log(2)))
	head := &skiplistNode[V]{value: nil, nodes: make([]*skiplistNode[V], height)}
	skiplist := Skiplist[V]{
		head:       head,
		maxHeight:  height,
		comparator: comparator,
		size:       0}
	skiplist.base = &skiplist

	return &skiplist
}

// Adds item to a skiplist. O(log n)
func (v *Skiplist[V]) Add(value V) {
	v.mu.Lock()
	defer v.mu.Unlock()

	height := calculateRandomHeight(v.maxHeight)
	atomic.AddUint64(&v.size, 1)

	// creates the node
	node := skiplistNode[V]{value: &value, nodes: make([]*skiplistNode[V], height)}
	if v.head.nodes[0] == nil {
		for i := 0; i < height; i++ {
			v.head.nodes[i] = &node
		}
		return
	}

	// find the position
	previousNode := stack.New()
	currentNode := v.head
	level := v.maxHeight - 1
	for level >= 0 {
		if currentNode.nodes[level] == nil {
			previousNode.Push(currentNode)
			level--
		} else if v.comparator(*currentNode.nodes[level].value, value) <= 0 {
			currentNode = currentNode.nodes[level]
		} else {
			previousNode.Push(currentNode)
			level--
		}
	}

	i := 0
	var lastPosition *skiplistNode[V]
	var tempNode *skiplistNode[V]
	for i < height {
		lastPosition = previousNode.Pop().(*skiplistNode[V])
		tempNode = lastPosition.nodes[i]
		lastPosition.nodes[i] = &node
		node.nodes[i] = tempNode
		i++
	}
}

// Returns the first value. O(1)
func (v *Skiplist[V]) First() *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.head.nodes[0] != nil {
		return v.head.nodes[0].value
	}
	return nil
}

// Returns the first value. O(log n)
func (v *Skiplist[V]) Last() *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	return v.FirstNodeWhen(nil, func(a *V, b *V) bool { return true }).value
}

// Returns a key-value mapping associated with the least key greater
// than or equal to the given key, or null if there is no such key. O(log n)
func (v *Skiplist[V]) Ceiling(k V) *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	node := v.FirstNodeWhen(&k, func(a *V, b *V) bool {
		return v.comparator(*a, *b) <= 0
	})

	if node != nil {
		return node.value
	}
	return nil
}

// Returns a key-value mapping associated with the least key
// strictly greater than the given key, or null if there is
// no such key. O(log n)
func (v *Skiplist[V]) Higher(k V) *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	node := v.FirstNodeWhen(&k, func(a *V, b *V) bool {
		return v.comparator(*a, *b) <= 0
	})

	if node != nil {
		return node.nodes[0].value
	}
	return nil
}

// Returns a key-value mapping associated with the greatest key less
// than or equal to the given key, or null if there is no such key.
// O(log n)
func (v *Skiplist[V]) Floor(k V) *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	node := v.FirstNodeWhen(&k, func(a *V, b *V) bool {
		return v.comparator(*a, *b) < 0
	})

	if node != nil {
		return node.nodes[0].value
	}
	return nil
}

// Returns a key-value mapping associated with the greatest key
// strictly less than the given key, or null if there is no such
// key. O(log n)
func (v *Skiplist[V]) Lower(k V) *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	node := v.FirstNodeWhen(&k, func(a *V, b *V) bool {
		return v.comparator(*a, *b) < 0
	})

	if node != nil {
		return node.value
	}
	return nil
}

// Returns a view of the portion of this map whose keys are
// strictly less than toKey. O(log n)
func (v *Skiplist[V]) Tail(fromKey V, inclusive bool) Iterable[V] {
	v.mu.RLock()
	defer v.mu.RUnlock()

	node := v.FirstNodeWhen(&fromKey, func(a *V, b *V) bool {
		if inclusive {
			return v.comparator(*a, *b) < 0
		} else {
			return v.comparator(*a, *b) <= 0
		}
	})

	itr := Iterable[V]{
		recreaterCallback: func() IteratorBase[V] {
			return &SkipListIterator[V]{
				mu:           &v.mu,
				stopCallback: func(a *V) bool { return false },
				currentNode:  node,
			}
		},
	}

	itr.base = itr
	return itr
}

// Returns a view of the portion of this map whose keys are
// greater than or equal to fromKey. O(log n)
func (v *Skiplist[V]) Head(toKey V, inclusive bool) Iterable[V] {
	v.mu.RLock()
	defer v.mu.RUnlock()

	itr := Iterable[V]{
		recreaterCallback: func() IteratorBase[V] {
			return &SkipListIterator[V]{
				mu: &v.mu,
				stopCallback: func(a *V) bool {
					if inclusive {
						return v.comparator(*a, toKey) > 0
					} else {
						return v.comparator(*a, toKey) >= 0
					}
				},
				currentNode: v.head,
			}
		},
	}
	itr.base = itr
	return itr
}

// Returns a view of the portion of this map whose keys range
// from fromKey, inclusive, to toKey, exclusive. O(log n)
func (v *Skiplist[V]) Sub(fromKey V, toKey V, fromInclusive bool, toInclusive bool) Iterable[V] {
	v.mu.RLock()
	defer v.mu.RUnlock()

	node := v.FirstNodeWhen(&fromKey, func(a *V, b *V) bool {
		if fromInclusive {
			return v.comparator(*a, *b) < 0
		} else {
			return v.comparator(*a, *b) <= 0
		}
	})

	itr := Iterable[V]{
		recreaterCallback: func() IteratorBase[V] {
			return &SkipListIterator[V]{
				mu: &v.mu,
				stopCallback: func(a *V) bool {
					if toInclusive {
						return v.comparator(*a, toKey) > 0
					} else {
						return v.comparator(*a, toKey) >= 0
					}
				},
				currentNode: node,
			}
		},
	}
	itr.base = itr
	return itr
}

// Returns all the entry matching the value. O(log n)
func (v *Skiplist[V]) Get(value V) Iterable[V] {
	return v.Sub(value, value, true, true)
}

func (v *Skiplist[V]) GetIterator() IteratorBase[V] {
	return &SkipListIterator[V]{
		stopCallback: func(a *V) bool { return false },
		currentNode:  v.head,
		mu:           &v.mu,
	}
}

type FirstNodeWhenCallback[V any] func(a *V, b *V) bool

func (v *Skiplist[V]) FirstNodeWhen(key *V, callback FirstNodeWhenCallback[V]) *skiplistNode[V] {
	currentNode := v.head
	level := v.maxHeight - 1
	for level >= 0 {
		if currentNode.nodes[level] == nil {
			level--
		} else if callback(currentNode.nodes[level].value, key) {
			currentNode = currentNode.nodes[level]
		} else {
			level--
		}
	}
	return currentNode
}

func calculateRandomHeight(maxHeight int) int {
	rand.Seed(time.Now().UnixNano())
	num := rand.Intn(1 << 30)
	height := 1

	for (num&1) != 0 && height < maxHeight {
		height++
		num >>= 1
	}

	return height
}

type StopCallback[V any] func(a *V) bool

type SkipListIterator[V any] struct {
	currentNode  *skiplistNode[V]
	stopCallback StopCallback[V]
	mu           *sync.RWMutex
}

func (i *SkipListIterator[V]) MoveNext() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.currentNode.nodes[0] != nil && !i.stopCallback(i.currentNode.nodes[0].value) {
		i.currentNode = i.currentNode.nodes[0]
		return true
	}
	return false
}

func (i *SkipListIterator[V]) GetCurrent() V {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.currentNode == nil {
		panic("Iterator: No more items left or the first MoveNext() is called")
	}

	return *i.currentNode.value
}
