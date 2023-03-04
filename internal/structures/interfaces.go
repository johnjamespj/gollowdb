package structures

type Comparator[V any] func(a V, b V) int

type NavigableList[V any] interface {
	// Returns the first value
	First() *V

	// Returns the last value
	Last() *V

	// Returns a key-value mapping associated with the least key greater
	// than or equal to the given key, or null if there is no such key.
	Ceiling(k V) *V

	// Returns a key-value mapping associated with the least key
	// strictly greater than the given key, or null if there is
	// no such key.
	Higher(k V) *V

	// Returns a key-value mapping associated with the greatest key less
	// than or equal to the given key, or null if there is no such key.
	Floor(k V) *V

	// Returns a key-value mapping associated with the greatest key
	// strictly less than the given key, or null if there is no such
	// key.
	Lower(k V) *V

	// Returns a view of the portion of this map whose keys are
	// strictly less than toKey.
	Tail(fromKey V, inclusive bool) Iterable[V]

	// Returns a view of the portion of this map whose keys are
	// greater than or equal to fromKey.
	Head(toKey V, inclusive bool) Iterable[V]

	// Returns a view of the portion of this map whose keys range
	// from fromKey, inclusive, to toKey, exclusive.
	Sub(fromKey V, toKey V, fromInclusive bool, toInclusive bool) Iterable[V]

	// Returns all the entry matching the value
	Get(value V) Iterable[V]
}

type TableRow struct{}

type AbstractTable interface {
	NavigableList[TableRow]

	Delete(k any)

	Put(k any, v any)

	IsInRange(k any)

	EstimateExistance(K any)
}
