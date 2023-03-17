package gollowdb

import (
	"sort"
	"sync"
)

type SortedList[V any] struct {
	EnhancedIterator[V]
	list       []V
	comparator Comparator[V]

	mu sync.RWMutex
}

func NewSortedList[V any](list []V, comparator Comparator[V]) *SortedList[V] {
	sortedList := &SortedList[V]{
		list:       list,
		comparator: comparator,
	}
	sortedList.base = sortedList
	return sortedList
}

// Add adds a value to the list efficiently
func (v *SortedList[V]) Add(value V) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.list = append(v.list, value)
	sort.Slice(v.list, func(i, j int) bool {
		return v.comparator(v.list[i], v.list[j]) < 0
	})
}

// Add all values to the list efficiently
func (v *SortedList[V]) AddAll(values []V) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.list = append(v.list, values...)
	sort.Slice(v.list, func(i, j int) bool {
		return v.comparator(v.list[i], v.list[j]) < 0
	})
}

func (v *SortedList[V]) Merge(other []V) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// TODO: merge two sorted lists
	newList := make([]V, 0)
	i, j := 0, 0
	for i < len(v.list) && j < len(other) {
		if v.comparator(v.list[i], other[j]) < 0 {
			newList = append(newList, v.list[i])
			i++
		} else {
			newList = append(newList, other[j])
			j++
		}
	}

	for i < len(v.list) {
		newList = append(newList, v.list[i])
		i++
	}

	for j < len(other) {
		newList = append(newList, other[j])
		j++
	}

	v.list = newList
}

// Remove removes a value from the list efficiently
func (v *SortedList[V]) Remove(value V) *V {
	v.mu.Lock()
	defer v.mu.Unlock()

	i := sort.Search(len(v.list), func(i int) bool {
		return v.comparator(v.list[i], value) >= 0
	})

	if i >= len(v.list) {
		return nil
	}

	if v.comparator(v.list[i], value) == 0 {
		v.list = append(v.list[:i], v.list[i+1:]...)
	}

	return &value
}

// Remove removes a value from the list efficiently
func (v *SortedList[V]) RemoveWhere(predicate func(V) bool) []V {
	v.mu.Lock()
	defer v.mu.Unlock()

	var removed []V

	for i := 0; i < len(v.list); i++ {
		if predicate(v.list[i]) {
			removed = append(removed, v.list[i])
			v.list = append(v.list[:i], v.list[i+1:]...)
			i--
		}
	}

	return removed
}

func (v *SortedList[V]) Clear() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.list = make([]V, 0)
}

func (v *SortedList[V]) ToList() []V {
	ret := make([]V, len(v.list))
	copy(ret, v.list)
	return ret
}

// Returns the first value. O(1)
func (v *SortedList[V]) First() *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if len(v.list) == 0 {
		return nil
	}

	return &v.list[0]
}

// Returns the last value. O(1)
func (v *SortedList[V]) Last() *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if len(v.list) == 0 {
		return nil
	}

	return &v.list[len(v.list)-1]
}

// Returns a key-value mapping associated with the least key greater
// than or equal to the given key, or null if there is no such key. O(log n)
func (v *SortedList[V]) Floor(k V) *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	i, _ := sort.Find(len(v.list), func(i int) int {
		return v.comparator(k, v.list[i])
	})

	if i >= len(v.list) {
		return &v.list[len(v.list)-1]
	}

	for i >= 0 && v.comparator(v.list[i], k) > 0 {
		i--
	}

	if i < 0 {
		return nil
	}

	return &v.list[i]
}

// Returns a key-value mapping associated with the greatest key less
// than or equal to the given key, or null if there is no such key.
func (v *SortedList[V]) Lower(k V) *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	i, found := sort.Find(len(v.list), func(i int) int {
		return v.comparator(k, v.list[i])
	})

	if found && i > 0 {
		return &v.list[i-1]
	} else if found {
		return nil
	} else if i >= len(v.list) {
		return &v.list[len(v.list)-1]
	}

	return &v.list[i]
}

// Returns a key-value mapping associated with the least key greater
// than or equal to the given key, or null if there is no such key.
func (v *SortedList[V]) Ceiling(k V) *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	i, _ := sort.Find(len(v.list), func(i int) int {
		return v.comparator(k, v.list[i])
	})

	for i+1 < len(v.list) {
		if v.comparator(v.list[i+1], k) > 0 {
			break
		}
		i++
	}

	if i >= len(v.list) || v.comparator(v.list[i], k) < 0 {
		return nil
	}

	return &v.list[i]
}

// Returns a key-value mapping associated with the least key
// strictly greater than the given key, or null if there is
// no such key.
func (v *SortedList[V]) Higher(k V) *V {
	v.mu.RLock()
	defer v.mu.RUnlock()

	i, _ := sort.Find(len(v.list), func(i int) int {
		return v.comparator(k, v.list[i])
	})

	for i+1 < len(v.list) {
		if v.comparator(v.list[i+1], k) > 0 {
			i++
			break
		}
		i++
	}

	if i >= len(v.list) || v.comparator(v.list[i], k) == 0 {
		return nil
	}

	return &v.list[i]
}

// Returns a view of the portion of this map whose keys are
// strictly less than toKey.
func (v *SortedList[V]) Tail(fromKey V, inclusive bool) Iterable[V] {
	v.mu.RLock()
	defer v.mu.RUnlock()

	i, _ := sort.Find(len(v.list), func(i int) int {
		return v.comparator(fromKey, v.list[i])
	})

	for i < len(v.list) && !inclusive {
		if v.comparator(v.list[i+1], fromKey) > 0 {
			i++
			break
		}
		i++
	}

	itr := Iterable[V]{
		recreaterCallback: func() IteratorBase[V] {
			return &SortedListIterator[V]{
				list:  v.list[i:],
				index: -1,
				mu:    &v.mu,
			}
		},
	}

	itr.base = itr
	return itr
}

// Returns a view of the portion of this map whose keys are
// greater than or equal to toKey.
func (v *SortedList[V]) Head(toKey V, inclusive bool) Iterable[V] {
	v.mu.RLock()
	defer v.mu.RUnlock()

	i, found := sort.Find(len(v.list), func(i int) int {
		return v.comparator(toKey, v.list[i])
	})

	if !inclusive {
		if found && i > 0 {
			i--
		} else if found {
			i = -1
		}
	} else {
		for i < len(v.list)-1 && v.comparator(v.list[i+1], toKey) == 0 {
			i++
		}
	}

	itr := Iterable[V]{
		recreaterCallback: func() IteratorBase[V] {
			return &SortedListIterator[V]{
				list:  v.list[:i+1],
				index: -1,
				mu:    &v.mu,
			}
		},
	}

	itr.base = itr
	return itr
}

// Returns a view of the portion of this map whose keys range
// from fromKey, inclusive, to toKey, exclusive.
func (v *SortedList[V]) Sub(fromKey V, toKey V, fromInclusive bool, toInclusive bool) Iterable[V] {
	v.mu.RLock()
	defer v.mu.RUnlock()

	// find the first element that is greater than fromKey
	i, _ := sort.Find(len(v.list), func(i int) int {
		return v.comparator(fromKey, v.list[i])
	})

	for i < len(v.list) && !fromInclusive {
		if v.comparator(v.list[i+1], fromKey) > 0 {
			i++
			break
		}
		i++
	}

	// find the first element that is greater than toKey
	j, _ := sort.Find(len(v.list), func(i int) int {
		return v.comparator(toKey, v.list[i])
	})

	for j < len(v.list) && !toInclusive {
		if v.comparator(v.list[j+1], toKey) > 0 {
			j++
			break
		}
		j++
	}

	var itr Iterable[V]
	if i > j || i >= len(v.list) || j < 0 {
		emptyArray := make([]V, 0)
		itr = Iterable[V]{
			recreaterCallback: func() IteratorBase[V] {
				return &SortedListIterator[V]{
					list:  emptyArray,
					index: -1,
					mu:    &v.mu,
				}
			},
		}
	} else {
		itr = Iterable[V]{
			recreaterCallback: func() IteratorBase[V] {
				return &SortedListIterator[V]{
					list:  v.list[i : j+1],
					index: -1,
					mu:    &v.mu,
				}
			},
		}
	}

	itr.base = itr
	return itr
}

// Returns all the entry matching the value. O(log n)
func (v *SortedList[V]) Get(value V) Iterable[V] {
	v.mu.RLock()
	defer v.mu.RUnlock()

	return v.Sub(value, value, true, true)
}

func (v *SortedList[V]) GetSize() int {
	v.mu.RLock()
	defer v.mu.RUnlock()

	return len(v.list)
}

// returns sortedlist iterator
func (v *SortedList[V]) GetIterator() IteratorBase[V] {
	return &SortedListIterator[V]{
		list:  v.list,
		index: -1,
		mu:    &v.mu,
	}
}

// Iterator for a sortedlist
type SortedListIterator[V any] struct {
	index int
	list  []V
	mu    *sync.RWMutex
}

// move next for SortedListIterator
func (i *SortedListIterator[V]) MoveNext() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.index < len(i.list)-1 {
		i.index++
		return true
	}
	return false
}

// get current for SortedListIterator
func (i *SortedListIterator[V]) GetCurrent() V {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.index >= len(i.list) || i.index < 0 {
		panic("Iterator: No more items left or the first MoveNext() is called")
	}

	return i.list[i.index]
}
