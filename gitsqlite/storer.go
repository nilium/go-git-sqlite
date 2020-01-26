package gitsqlite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"sync"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/format/index"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-git.v4/storage"
)

var (
	ErrClosed      = errors.New("storage is closed")
	ErrNoConn      = errors.New("no connection available")
	ErrCloseModule = errors.New("cannot close submodule storage")
)

type Storer struct {
	name   string
	sub    bool
	once   *sync.Once
	closed chan struct{}
	db     *sqlitex.Pool
}

var (
	_ storage.Storer = (*Storer)(nil)
)

type withFunc func(c *sqlite.Conn) error

func New(name string, db *sqlitex.Pool) (*Storer, error) {
	name = strconv.Quote(name)
	s := &Storer{
		name:   name,
		db:     db,
		once:   new(sync.Once),
		closed: make(chan struct{}),
	}
	if err := s.init(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Storer) Module(name string) (storage.Storer, error) {
	name = s.name + "/" + strconv.Quote(name)
	sub := *s
	sub.name = name
	sub.sub = true
	return &sub, nil
}

func (s *Storer) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *Storer) Close() (err error) {
	if s.isClosed() {
		return ErrClosed
	}
	if s.sub {
		return ErrCloseModule
	}
	err = ErrClosed
	s.once.Do(func() {
		close(s.closed)
		err = nil
	})
	return err
}

func (s *Storer) init() error {
	return s.with(func(c *sqlite.Conn) (err error) {
		run := func(query string) {
			if err != nil {
				return
			}
			err = execTransient(c, query, nil)
			if err != nil {
				err = fmt.Errorf("error executing query %q: %w", query, err)
			}
		}

		// EncodedObject storage.
		run(`-- GitEncodedObjects table.
CREATE TABLE IF NOT EXISTS GitEncodedObjects (
	repo TEXT NOT NULL,
	hash BLOB NOT NULL,
	type TEXT NOT NULL,
	data BLOB NOT NULL,
	PRIMARY KEY (repo, hash)
)`)
		run(`-- GitEncodedObjects hash_type index.
CREATE INDEX IF NOT EXISTS HashType ON GitEncodedObjects (repo, hash, type)
		`)

		// Reference storage.
		run(`-- GitReferences table.
CREATE TABLE IF NOT EXISTS GitReferences (
	repo TEXT NOT NULL,
	name BLOB NOT NULL,
	target BLOB NOT NULL,
	PRIMARY KEY (repo, name)
)`)

		// Shallow hash storage.
		run(`-- GitShallowCommits table.
CREATE TABLE IF NOT EXISTS GitShallowCommits (
	repo TEXT NOT NULL,
	hash BLOB NOT NULL,
	PRIMARY KEY (repo, hash)
)`)

		// KV storage.
		// This is arbitrary storage for data that doesn't yet have its
		// own table and can be expressed as a single key-value pair.
		run(`-- Key-value table.
CREATE TABLE IF NOT EXISTS GitKV (
	repo TEXT NOT NULL,
	name TEXT NOT NULL,
	data BLOB NOT NULL,
	PRIMARY KEY (repo, name)
)`)

		return err
	})
}

func (s *Storer) with(fn withFunc) error {
	if s.isClosed() {
		return ErrClosed
	}
	conn := s.db.Get(nil)
	if conn == nil {
		return ErrNoConn
	}
	defer s.db.Put(conn)
	return fn(conn)
}

func (s *Storer) exec(query string, row rowFunc, params ...interface{}) error {
	return s.with(func(c *sqlite.Conn) error {
		return exec(c, query, row, params...)
	})
}

// EncodedObjectStorer

var _ storer.EncodedObjectStorer = (*Storer)(nil)

func (s *Storer) NewEncodedObject() plumbing.EncodedObject {
	return &Object{}
}

func (s *Storer) SetEncodedObject(o plumbing.EncodedObject) (plumbing.Hash, error) {
	h := o.Hash()

	if s.isClosed() {
		return h, ErrClosed
	}

	typ := o.Type()
	switch typ {
	case plumbing.CommitObject,
		plumbing.TreeObject,
		plumbing.BlobObject,
		plumbing.TagObject:
	default:
		return h, fmt.Errorf("unsupported object type: %v", typ)
	}

	data, err := readObject(o)
	if err != nil {
		return h, fmt.Errorf("unable to get object %v contents: %w", h, err)
	}

	err = s.exec(`INSERT INTO GitEncodedObjects (repo, hash, type, data) VALUES ($repo, $hash, $type, $data)`,
		nil,
		s.name, h, typ, data,
	)
	if err != nil {
		return h, err
	}

	return h, nil
}

func readObject(o plumbing.EncodedObject) ([]byte, error) {
	if o, ok := o.(*Object); ok {
		return o.body, nil
	}
	r, err := o.Reader()
	if err != nil {
		return nil, fmt.Errorf("error getting object reader: %w", err)
	}
	body, err := ioutil.ReadAll(io.LimitReader(r, o.Size()))
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		return nil, fmt.Errorf("error reading object body: %w", err)
	}
	return body, nil
}

func (s *Storer) EncodedObject(typ plumbing.ObjectType, h plumbing.Hash) (obj plumbing.EncodedObject, err error) {
	if s.isClosed() {
		return nil, ErrClosed
	}
	if typ == plumbing.AnyObject {
		obj, err = s.encodedObjectByHash(h)
	} else {
		obj, err = s.encodedObjectByType(typ, h)
	}
	if err != nil {
		return nil, notFound(err, plumbing.ErrObjectNotFound)
	} else if oh := obj.Hash(); h != oh {
		return nil, fmt.Errorf("object with hash key %v has hash %v: storage may be damaged",
			h, oh)
	}
	return obj, nil
}

func (s *Storer) encodedObjectByHash(h plumbing.Hash) (plumbing.EncodedObject, error) {
	obj := &Object{}
	err := s.exec(`SELECT type, data FROM GitEncodedObjects WHERE repo = $repo AND hash = $hash LIMIT 1`,
		scanner(&obj.typ, &obj.body),
		s.name, h,
	)
	return obj, err
}

func (s *Storer) encodedObjectByType(typ plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	obj := &Object{}
	err := s.exec(`SELECT type, data FROM GitEncodedObjects WHERE repo = $repo AND hash = $hash AND type = $type LIMIT 1`,
		scanner(&obj.typ, &obj.body),
		s.name, h, typ,
	)
	return obj, err
}

func (s *Storer) IterEncodedObjects(typ plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	if s.isClosed() {
		return nil, ErrClosed
	}
	return &ObjectIter{
		db:   s,
		typ:  typ,
		scan: s.nextHashesTyped(typ),
	}, nil
}

func (s *Storer) nextHashesUntyped(after plumbing.Hash, dest []plumbing.Hash) ([]plumbing.Hash, error) {
	err := s.exec(`SELECT hash FROM GitEncodedObjects WHERE repo = $repo AND hash > $hash ORDER BY hash ASC LIMIT $cap`,
		func(stmt *sqlite.Stmt) error {
			var h plumbing.Hash
			if err := scan(stmt, 0, &h); err != nil {
				return err
			}
			dest = append(dest, h)
			return nil
		},
		s.name, after, cap(dest),
	)
	return dest, err
}

func (s *Storer) nextHashesTyped(typ plumbing.ObjectType) func(plumbing.Hash, []plumbing.Hash) ([]plumbing.Hash, error) {
	if typ == plumbing.AnyObject {
		return s.nextHashesUntyped
	}
	return func(after plumbing.Hash, dest []plumbing.Hash) ([]plumbing.Hash, error) {
		err := s.exec(`SELECT hash FROM GitEncodedObjects WHERE repo = $repo AND hash > $hash AND type = $type ORDER BY hash ASC LIMIT $cap`,
			func(stmt *sqlite.Stmt) error {
				var h plumbing.Hash
				if err := scan(stmt, 0, &h); err != nil {
					return err
				}
				dest = append(dest, h)
				return nil
			},
			s.name, after, typ, cap(dest),
		)
		return dest, err
	}
}

func (s *Storer) HasEncodedObject(h plumbing.Hash) error {
	err := s.exec(`SELECT 1 FROM GitEncodedObjects WHERE repo = $repo AND hash = $hash LIMIT 1`,
		discardRows,
		s.name, h,
	)
	err = notFound(err, plumbing.ErrObjectNotFound)
	return err
}

func (s *Storer) EncodedObjectSize(h plumbing.Hash) (size int64, err error) {
	err = s.exec(`SELECT length(data) FROM GitEncodedObjects WHERE repo = $repo AND hash = $hash LIMIT 1`,
		scanner(&size),
		s.name, h,
	)
	if err != nil {
		return 0, notFound(err, plumbing.ErrObjectNotFound)
	}
	return size, nil
}

// ReferenceStorer

var _ storer.ReferenceStorer = (*Storer)(nil)

func (s *Storer) setReference(c *sqlite.Conn, r *plumbing.Reference) error {
	if r == nil {
		return nil
	}
	rs := r.Strings()
	name, target := rs[0], rs[1]
	return exec(c, `INSERT OR REPLACE INTO GitReferences (repo, name, target) VALUES ($repo, $name, $target)`,
		nil,
		s.name, name, target,
	)
}

func (s *Storer) SetReference(r *plumbing.Reference) error {
	return s.with(func(c *sqlite.Conn) error { return s.setReference(c, r) })
}

func (s *Storer) CheckAndSetReference(ref, old *plumbing.Reference) error {
	if old == nil {
		return s.SetReference(ref)
	}
	olds := old.Strings()
	return s.with(func(c *sqlite.Conn) (err error) {
		defer sqlitex.Save(c)(&err)

		var oldTarget string
		err = exec(c, `SELECT target FROM GitReferences WHERE repo = $repo AND name = $name LIMIT 1`,
			scanner(&oldTarget),
			s.name, olds[0],
		)
		if err != nil {
			return notFound(err, plumbing.ErrReferenceNotFound)
		}

		if oldTarget != olds[1] {
			return storage.ErrReferenceHasChanged
		}
		return s.setReference(c, ref)
	})
}

func (s *Storer) Reference(name plumbing.ReferenceName) (*plumbing.Reference, error) {
	var target string
	err := s.exec(`SELECT target FROM GitReferences WHERE repo = $repo AND name = $name LIMIT 1`,
		scanner(&target),
		s.name, name,
	)
	if err != nil {
		return nil, notFound(err, plumbing.ErrReferenceNotFound)
	}
	ref := plumbing.NewReferenceFromStrings(string(name), target)
	return ref, nil
}

func (s *Storer) RemoveReference(name plumbing.ReferenceName) error {
	err := s.exec(`DELETE FROM GitReferences WHERE repo = $repo AND name = $name`,
		nil,
		s.name, name,
	)
	return notFound(err, nil)
}

func (s *Storer) CountLooseRefs() (int, error) {
	var n int
	err := s.exec(`SELECT COUNT(*) FROM GitReferences WHERE repo = $repo`,
		scanner(&n),
		s.name,
	)
	return n, err
}

func (s *Storer) IterReferences() (storer.ReferenceIter, error) {
	return &ReferenceIter{
		scan: s.nextReferences,
	}, nil
}

func (s *Storer) nextReferences(after plumbing.ReferenceName, dest []*plumbing.Reference) ([]*plumbing.Reference, error) {
	err := s.exec(`SELECT name, target FROM GitReferences WHERE repo = $repo AND name > $name ORDER BY name ASC LIMIT $cap`,
		func(stmt *sqlite.Stmt) error {
			var name, target string
			if err := scanner(&name, &target)(stmt); err != nil {
				return err
			}
			dest = append(dest, plumbing.NewReferenceFromStrings(name, target))
			return nil
		},
		s.name, after, cap(dest),
	)
	return dest, notFound(err, nil)
}

func (s *Storer) PackRefs() error {
	// nop
	return nil
}

// Shallow storage.

var _ storer.ShallowStorer = (*Storer)(nil)

func (s *Storer) SetShallow(commits []plumbing.Hash) error {
	return s.with(func(c *sqlite.Conn) (err error) {
		defer sqlitex.Save(c)(&err)

		// Truncate shallow commits table.
		err = exec(c, `DELETE FROM GitShallowCommits WHERE repo = $repo`,
			nil,
			s.name,
		)
		if err != nil {
			return err
		}

		for _, h := range commits {
			err = exec(c, `INSERT OR IGNORE INTO GitShallowCommits (repo, hash) VALUES ($repo, $hash)`,
				nil,
				s.name, h,
			)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *Storer) Shallow() (hashes []plumbing.Hash, err error) {
	err = s.with(func(c *sqlite.Conn) (err error) {
		var numRows int
		err = exec(c, `SELECT COUNT(*) FROM GitShallowCommits WHERE repo = $repo`,
			scanner(&numRows),
			s.name,
		)
		if err != nil {
			return err
		}
		hashes = make([]plumbing.Hash, 0, numRows)
		err = exec(c, `SELECT hash FROM GitShallowCommits WHERE repo = $repo`,
			func(stmt *sqlite.Stmt) error {
				var h plumbing.Hash
				if err := scan(stmt, 0, &h); err != nil {
					return err
				}
				hashes = append(hashes, h)
				return nil
			},
			s.name,
		)
		return notFound(err, nil)
	})
	if err != nil {
		return nil, err
	}
	return hashes, nil
}

// KV storage.
// This holds the index and git-config data.

const (
	kvGitIndex  = "index"
	kvGitConfig = "config"
)

func (s *Storer) writeBlob(key string, blob []byte) error {
	return s.with(func(c *sqlite.Conn) (err error) {
		// Start savepoint.
		defer sqlitex.Save(c)(&err)

		stmt, err := c.Prepare(`INSERT OR REPLACE INTO GitKV (repo, name, data) VALUES ($repo, $name, $data)`)
		if err != nil {
			return err
		}
		defer func() {
			// Can deal with somewhat large objects, so clear bindings
			// before resetting.
			cerr := stmt.ClearBindings()
			rerr := stmt.Reset()
			if err == nil {
				err = rerr
			}
			if err == nil {
				err = cerr
			}
		}()

		stmt.BindText(1, s.name)               // $repo
		stmt.BindText(2, key)                  // $name
		stmt.BindZeroBlob(3, int64(len(blob))) // $data
		_, err = stmt.Step()
		if err != nil {
			return fmt.Errorf("error updating blob for key %q: %w", key, err)
		}

		rowID := c.LastInsertRowID()
		b, err := c.OpenBlob("main", "GitKV", "data", rowID, true)
		if err != nil {
			return fmt.Errorf("error opening blob for writing key %q: %w", key, err)
		}
		defer b.Close()

		if _, err := b.Write(blob); err != nil {
			return fmt.Errorf("error writing blob for key %q: %w", key, err)
		}

		return nil
	})
}

func (s *Storer) readBlob(key string, read func(*sqlite.Blob) error) error {
	return s.with(func(c *sqlite.Conn) (err error) {
		var rowID int64
		err = exec(c, `SELECT ROWID FROM GitKV WHERE repo = $repo AND name = $name LIMIT 1`,
			scanner(&rowID),
			s.name, key,
		)
		if err != nil {
			return err
		}

		b, err := c.OpenBlob("main", "GitKV", "data", rowID, false)
		if err != nil {
			return err
		}
		defer b.Close()

		if err = read(b); err != nil {
			return fmt.Errorf("error reading blob %q: %w", key, err)
		}
		return nil
	})
}

var _ storer.IndexStorer = (*Storer)(nil)

func (s *Storer) SetIndex(idx *index.Index) error {
	if s.isClosed() {
		return ErrClosed
	}
	var buf bytes.Buffer
	w := index.NewEncoder(&buf)
	if err := w.Encode(idx); err != nil {
		return err
	}
	return s.writeBlob(kvGitIndex, buf.Bytes())
}

func (s *Storer) Index() (*index.Index, error) {
	if s.isClosed() {
		return nil, ErrClosed
	}

	idx := &index.Index{
		Version: 2,
	}

	err := notFound(s.readBlob(kvGitIndex, func(b *sqlite.Blob) error {
		dec := index.NewDecoder(b)
		return dec.Decode(idx)
	}), nil)

	if err != nil {
		return nil, err
	}

	return idx, nil
}

// Config storage.

var _ config.ConfigStorer = (*Storer)(nil)

func (s *Storer) Config() (*config.Config, error) {
	if s.isClosed() {
		return nil, ErrClosed
	}
	conf := config.NewConfig()
	s.readBlob(kvGitConfig, func(b *sqlite.Blob) error {
		p, err := ioutil.ReadAll(b)
		if err != nil {
			return fmt.Errorf("error reading config blob: %w", err)
		}
		return conf.Unmarshal(p)
	})
	return conf, nil
}

func (s *Storer) SetConfig(conf *config.Config) (err error) {
	if s.isClosed() {
		return ErrClosed
	}
	if conf == nil {
		return errors.New("config is nil")
	} else if err = conf.Validate(); err != nil {
		return fmt.Errorf("error validating config: %w", err)
	}
	p, err := conf.Marshal()
	if err != nil {
		return fmt.Errorf("unable to marshal config: %w", err)
	}
	return s.writeBlob(kvGitConfig, p)
}
