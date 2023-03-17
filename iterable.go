/*
*	Copyright (c) 2023
*	John's Page All rights reserved.
*
*	Redistribution and use in source and binary forms, with or without
*	modification, are permitted provided that the following conditions
*	are met:
*
*	Redistributions of source code must retain the above copyright notice,
*	this list of conditions and the following disclaimer.
*
*	THIS SOFTWARE IS PROVIDED BY [Name of Organization] “AS IS” AND ANY EXPRESS
*	OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
*	OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
*	EVENT SHALL [Name of Organisation] BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
*	SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
*	PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
*	OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
*	IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
*	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
*	OF SUCH DAMAGE.
 */

package gollowdb

import "container/list"

type IteratorBase[V any] interface {
	MoveNext() bool
	GetCurrent() V
}

type IterableInterface[V any] interface {
	GetIterator() IteratorBase[V]
}

type EnhancedIterator[V any] struct {
	base IterableInterface[V]
}

func (v EnhancedIterator[V]) ToList() []V {
	list := list.New()
	itr := v.base.GetIterator()
	for itr.MoveNext() {
		list.PushBack(itr.GetCurrent())
	}

	ary := make([]V, list.Len())
	curr := list.Front()
	i := 0
	for curr != nil {
		ary[i] = curr.Value.(V)
		curr = curr.Next()
		i++
	}

	return ary
}

func (v EnhancedIterator[V]) Where(filter FilterCallback[V]) Iterable[V] {
	itr := Iterable[V]{
		recreaterCallback: func() IteratorBase[V] {
			return &FilterIterator[V]{
				itr:      v.base.GetIterator(),
				callback: filter,
				current:  nil,
			}
		},
	}
	itr.base = itr
	return itr
}

type FilterCallback[V any] func(a V) bool

type FilterIterator[V any] struct {
	itr      IteratorBase[V]
	callback FilterCallback[V]
	current  *V
}

func (i *FilterIterator[V]) MoveNext() bool {
	var cur V
	for i.itr.MoveNext() {
		cur = i.itr.GetCurrent()

		if i.callback(cur) {
			i.current = &cur
			return true
		}
	}
	return false
}

func (i *FilterIterator[V]) GetCurrent() V {
	if i.current == nil {
		panic("Iterator: No more items left or the first MoveNext() is called")
	}
	return *i.current
}

type CreateIteratorCallback[V any] func() IteratorBase[V]

type Iterable[V any] struct {
	EnhancedIterator[V]
	recreaterCallback CreateIteratorCallback[V]
}

func (i Iterable[V]) GetIterator() IteratorBase[V] {
	return i.recreaterCallback()
}
