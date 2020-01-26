package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"go.spiff.io/go-git-sqlite/gitsqlite"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git.v4"
)

func usage() {
	fmt.Fprintln(flag.CommandLine.Output(), "Usage: clone.go REPO FILE")
}

func main() {
	flag.CommandLine.Usage = usage
	flag.Parse()
	if flag.NArg() != 2 {
		flag.CommandLine.Usage()
		os.Exit(2)
	}

	remote, file := flag.Arg(0), flag.Arg(1)

	pool, err := sqlitex.Open("file:git.db", sqlite.SQLITE_OPEN_READWRITE|sqlite.SQLITE_OPEN_CREATE|sqlite.SQLITE_OPEN_URI, 1)
	if err != nil {
		log.Panic(fmt.Errorf("unable to create sqlite pool: %w", err))
	}
	defer func() {
		if err := pool.Close(); err != nil {
			log.Printf("Error closing sqlite pool: %v", err)
		}
		time.Sleep(time.Second)
	}()

	// The first parameter is the name of the repo. You can use this to
	// differentiate root-level storage inside the same DB if you want.
	storage, err := gitsqlite.New("root", pool)
	if err != nil {
		log.Panic(fmt.Errorf("unable to create storage: %w", err))
	}
	defer storage.Close()

	fs := memfs.New()
	repo, err := git.Clone(storage, fs, &git.CloneOptions{
		URL:        remote,
		NoCheckout: false,
		Progress:   os.Stderr,
	})
	if errors.Is(err, git.ErrRepositoryAlreadyExists) {
		repo, err = git.Open(storage, fs)
		if err != nil {
			log.Panic(fmt.Errorf("error opening repository: %w", err))
		}

		wt, err := repo.Worktree()
		if err != nil {
			log.Panic(fmt.Errorf("error geting worktree: %w", err))
		}

		err = wt.Checkout(&git.CheckoutOptions{Force: true})
		if err != nil {
			log.Panic(fmt.Errorf("error checking out master: %w", err))
		}
	} else if err != nil {
		log.Panic(fmt.Errorf("error cloning repository: %w", err))
	}

	f, err := fs.Open(file)
	if err != nil {
		log.Panic(fmt.Errorf("error opening file %q: %w", file, err))
	}

	_, err = io.Copy(os.Stdout, f)
	if err != nil {
		log.Panic(fmt.Errorf("error copying file %q to stdout: %w", file, err))
	}
}
