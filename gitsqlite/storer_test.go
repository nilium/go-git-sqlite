package gitsqlite_test

import (
	"testing"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"go.spiff.io/go-git-sqlite/gitsqlite"
	. "gopkg.in/check.v1"
	"gopkg.in/src-d/go-git.v4/storage/test"
)

// Test uses the bizarre check package that go-git uses just so we cna use their
// storage tests.
//
// TODO: Hack around it so that go-git-sqlite doesn't rely on this mess.
func Test(t *testing.T) { TestingT(t) }

type StorageSuite struct {
	pool  *sqlitex.Pool
	store *gitsqlite.Storer
	test.BaseStorageSuite
}

var _ = Suite(&StorageSuite{})

func (s *StorageSuite) SetUpTest(c *C) {
	const flags = sqlite.SQLITE_OPEN_URI |
		sqlite.SQLITE_OPEN_SHAREDCACHE |
		sqlite.SQLITE_OPEN_NOMUTEX |
		sqlite.SQLITE_OPEN_READWRITE

	var err error
	s.pool, err = sqlitex.Open("file::memory:?mode=memory", flags, 2)
	c.Assert(err, IsNil)

	s.store, err = gitsqlite.New("test", s.pool)
	c.Assert(err, IsNil)

	s.BaseStorageSuite = test.NewBaseStorageSuite(s.store)
	s.BaseStorageSuite.SetUpTest(c)
}

func (s *StorageSuite) TearDownTest(c *C) {
	c.Assert(s.store.Close(), IsNil)
	c.Assert(s.store.Close(), Equals, gitsqlite.ErrClosed)
	c.Assert(s.pool.Close(), IsNil)
}
