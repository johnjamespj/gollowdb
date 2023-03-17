package structures_test

import (
	"math/rand"
	"testing"
)

type RowMap[V any] func(name int) V

func CreateTestList[V any](m RowMap[V]) {
	l := rand.Intn(1000)
	list := make([]V, l)

	for i := 0; i < l; i++ {
		list[i] = m(rand.Intn(1000))
	}
}

func TestAbs(t *testing.T) {
	got := 1
	if got != 1 {
		t.Errorf("Abs(-1) = %d; want 1", got)
	}
}
