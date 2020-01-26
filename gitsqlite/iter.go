package gitsqlite

import (
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

// Encoded object iterator.

type ObjectIter struct {
	db    *Storer
	scan  func(plumbing.Hash, []plumbing.Hash) ([]plumbing.Hash, error)
	typ   plumbing.ObjectType
	ptr   int
	set   []plumbing.Hash
	after plumbing.Hash
	done  bool
}

func (iter *ObjectIter) step() error {
	if iter.done {
		return nil
	}
	iter.ptr = 0
	set := iter.set[:0]
	if set == nil {
		set = make([]plumbing.Hash, 0, 32)
	}
	set, err := iter.scan(iter.after, set)
	err = notFound(err, nil)
	if err != nil {
		return err
	} else if len(set) == 0 {
		iter.Close()
		return nil
	}
	iter.set, iter.after = set, set[len(set)-1]
	return nil
}

func (iter *ObjectIter) Next() (plumbing.EncodedObject, error) {
	if iter.done {
		return nil, nil
	}
	if iter.ptr >= len(iter.set) {
		if err := iter.step(); err != nil {
			return nil, err
		}
		if iter.done {
			return nil, io.EOF
		}
	}
	if len(iter.set) == 0 {
		return nil, nil
	}

	next := iter.set[iter.ptr]
	iter.ptr++

	return iter.db.EncodedObject(iter.typ, next)
}

func (iter *ObjectIter) Close() {
	iter.set, iter.done = nil, true
}

func (iter *ObjectIter) ForEach(cb func(plumbing.EncodedObject) error) error {
	return storer.ForEachIterator(iter, cb)
}

// Reference iterator.

type ReferenceIter struct {
	scan  func(plumbing.ReferenceName, []*plumbing.Reference) ([]*plumbing.Reference, error)
	ptr   int
	set   []*plumbing.Reference
	after plumbing.ReferenceName
	done  bool
}

func (iter *ReferenceIter) step() error {
	if iter.done {
		return nil
	}
	iter.ptr = 0
	set := iter.set[:0]
	if set == nil {
		set = make([]*plumbing.Reference, 0, 32)
	}
	set, err := iter.scan(iter.after, set)
	err = notFound(err, nil)
	if err != nil {
		return err
	} else if len(set) == 0 {
		iter.Close()
		return nil
	}
	iter.set, iter.after = set, set[len(set)-1].Name()
	return nil
}

func (iter *ReferenceIter) Next() (*plumbing.Reference, error) {
	if iter.done {
		return nil, io.EOF
	}
	if iter.ptr >= len(iter.set) {
		if err := iter.step(); err != nil {
			return nil, err
		}
		if iter.done {
			return nil, io.EOF
		}
	}
	if len(iter.set) == 0 {
		return nil, nil
	}

	next := iter.set[iter.ptr]
	iter.ptr++
	return next, nil
}

func (iter *ReferenceIter) Close() {
	iter.set, iter.done = nil, true
}

func (iter *ReferenceIter) ForEach(cb func(*plumbing.Reference) error) error {
	defer iter.Close()
	for !iter.done {
		ref, err := iter.Next()
		if err != nil || ref == nil {
			return err
		}
		if err = cb(ref); err != nil {
			return err
		}
	}
	return nil
}
