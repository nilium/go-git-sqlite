go-git-sqlite
=============

This is a rudimentary implementation of a [go-git][] storage layer using SQLite.
The intent behind this is to use it as a single-file repository for things like
CI and GitHub Apps where increasing isolation between the underlying filesystem
and whatever is using the repository is desirable.

[go-git]: https://github.com/src-d/go-git/


Usage
-----

First, create an sqlitex.Pool and then hand it to gitsqlite.New. Afterward, you can
pass the resulting gitsqlite.Storer to git.Clone, git.Open, or other functions.

For example:

```go
pool, err := sqlitex.Open("file:git.db", 0, 2)
if err != nil {
	log.Panic(fmt.Errorf("unable to create sqlite pool: %w", err))
}
defer pool.Close()

// The first parameter is the name of the repo. You can use this to
// differentiate root-level storage inside the same DB if you want.
storage, err := gitsqlite.New("", pool)
if err != nil {
	log.Panic(fmt.Errorf("unable to create storage: %w", err))
}
defer storage.Close()

// Create a gopkg.in/billy.v4/memfs filesystem:
fs := memfs.New()

// Clone the repository into sqlite with a memory filesystem:
repo, err := git.Clone(storage, fs, &git.CloneOptions{
	URL: "https://github.com/nilium/flagenv",
})
if err != nil {
	log.Panic(fmt.Errorf("error cloning repository: %w", err))
}

// Now you have a repo with files in the memory fs.
```


Notes
-----

  - go-git-sqlite does not currently encode the config or index data from go-git
    in any special way, and this is all kept in a GitKV table that just holds
    key-value pairs (where the value is a blob).

  - There is no implementation of packed refs, which as far as I can tell is
    a detail that only a .git-dir implementation needs to care about.

  - Repositories and modules are currently named using quoted strings, with
    modules of the root repository being separated by slashes. There's likely
    a better way to do this, but for now it works. The only reason for quoting
    names is to ensure that the slash separator can't be spoofed by a clever
    module name.

  - This uses [crawshaw.io/sqlite][] (and the sqlitex package) for SQLite. This
    is because there's no reason to support the database/sql package for
    go-git-sqlite and that this package provides a clean interface to the
    standard SQLite library instead of making it look like a database with
    network connections and so on.

[crawshaw.io/sqlite]: https://crawshaw.io/sqlite


License
-------

go-git-sqlite is available under the two-clause BSD license. It can be found in
the COPYING file.
