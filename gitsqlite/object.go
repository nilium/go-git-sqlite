package gitsqlite

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"gopkg.in/src-d/go-git.v4/plumbing"
)

type Object struct {
	typ  plumbing.ObjectType
	h    plumbing.Hash
	body []byte
}

var _ plumbing.EncodedObject = (*Object)(nil)

func (o *Object) Hash() plumbing.Hash {
	if o.h == plumbing.ZeroHash {
		o.h = plumbing.ComputeHash(o.typ, o.body)
	}
	return o.h
}

func (o *Object) Type() plumbing.ObjectType {
	return o.typ
}

func (o *Object) SetType(typ plumbing.ObjectType) {
	o.typ, o.h = typ, plumbing.ZeroHash
}

func (o *Object) Size() int64 {
	return int64(len(o.body))
}

func (o *Object) SetSize(sz int64) {
	n := int64(len(o.body))
	if n < sz {
		if int64(cap(o.body)) >= sz {
			o.body = o.body[0:sz:cap(o.body)]
			tail := o.body[sz:]
			for i := range tail { // memzero
				tail[i] = 0
			}
		} else {
			p := make([]byte, sz)
			copy(p, o.body)
			o.body = p
		}
	}
	o.body = o.body[:sz]
}

func (o *Object) Writer() (io.WriteCloser, error) {
	return o, nil
}

func (o *Object) Reader() (io.ReadCloser, error) {
	if len(o.body) == 0 {
		// FIXME: go-git 4.13.1 doesn't like bytes.Reader because it
		// assumes the first read can't return EOF, but bytes.Reader
		// will return EOF on first read if the object is empty. To work
		// around this, create a dummy reader that does nothing but flip
		// a flag to return EOF after the first read for empty objects.
		// This appears to be fixed in an unreleased version of go-git.
		return &emptyReader{}, nil
	}
	return ioutil.NopCloser(bytes.NewReader(o.body)), nil
}

func (o *Object) Write(p []byte) (n int, err error) {
	n = len(p)
	o.body, o.h = append(o.body, p...), plumbing.ZeroHash
	return n, nil
}

func (o *Object) Close() error {
	return nil
}

type emptyReader struct {
	once bool
}

func (e *emptyReader) Close() error {
	return nil
}

func (e *emptyReader) Read(p []byte) (n int, err error) {
	if e.once {
		return 0, io.EOF
	}
	e.once = true
	return 0, nil
}

func chunks(p []byte, size int) (pcs [][]byte, err error) {
	if size < 1 {
		return nil, fmt.Errorf("invalid chunk size: %d", size)
	}
	for len(p) > size {
		pc := append(make([]byte, 0, size), p[:size]...)
		pcs = append(pcs, pc)
		p = p[size:]
	}
	if len(p) > 0 {
		pc := append(make([]byte, 0, size), p...)
		pcs = append(pcs, pc)
	}
	return pcs, nil
}

func chunkReader(r io.Reader, size int) (rd func([]byte) ([]byte, error), err error) {
	if size < 1 {
		return nil, fmt.Errorf("invalid chunk size: %d", size)
	}

	return func(dest []byte) ([]byte, error) {
		p, err := ioutil.ReadAll(io.LimitReader(r, int64(size)))
		if len(p) == 0 && err == nil {
			return nil, io.EOF
		}
		return p, err
	}, nil
}
