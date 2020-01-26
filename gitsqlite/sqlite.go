package gitsqlite

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"crawshaw.io/sqlite"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

type rowFunc func(*sqlite.Stmt) error

func discardRows(*sqlite.Stmt) error { return nil }

func scanner(dest ...interface{}) rowFunc {
	return func(stmt *sqlite.Stmt) error {
		for col, dest := range dest {
			if err := scan(stmt, col, dest); err != nil {
				return err
			}
		}
		return nil
	}
}

func scan(stmt *sqlite.Stmt, col int, dest interface{}) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("error scanning column %d (%s): %w", col, stmt.ColumnName(col), err)
		}
	}()
	switch out := dest.(type) {
	case nil:
		// Nop
	case *plumbing.Hash:
		n := stmt.ColumnLen(col)
		if n != len(*out) {
			return fmt.Errorf("scanned hash returned %d bytes, expected %d", n, len(*out))
		}
		stmt.ColumnBytes(col, (*out)[:])
	case *plumbing.ObjectType:
		c := stmt.ColumnText(col)
		typ, err := plumbing.ParseObjectType(c)
		if err != nil {
			return fmt.Errorf("invalid object type %q: %w", c, err)
		}
		*out = typ
	case *plumbing.ReferenceName:
		*out = plumbing.ReferenceName(stmt.ColumnText(col))
	case *[]byte:
		n := stmt.ColumnLen(col)
		buf := make([]byte, n)
		buf = buf[:stmt.ColumnBytes(col, buf)]
		*out = buf
	case *bool:
		n := stmt.ColumnInt64(col)
		*out = n != 0
	case *string:
		*out = stmt.ColumnText(col)
	case *int:
		*out = stmt.ColumnInt(col)
	case *int64:
		*out = stmt.ColumnInt64(col)
	default:
		return fmt.Errorf("unrecognized column type %T", out)
	}
	return nil
}

func bind(stmt *sqlite.Stmt, param int, value interface{}) error {
	// Decompose plumbing types.
	switch v := value.(type) {
	case io.Reader:
		p, err := ioutil.ReadAll(v)
		if err != nil {
			return fmt.Errorf("error reading data for parameter %d: %w", param, err)
		}
		value = p
	case plumbing.Hash:
		value = []byte(v[:])
	case plumbing.ObjectType:
		value = v.String()
	case plumbing.ReferenceName:
		value = string(v)
	case int:
		value = int64(v)
	}

	// Set parameter bindings.
	// TODO: Support blob streaming as an alternative to []byte values.
	switch v := value.(type) {
	case bool:
		stmt.BindBool(param, v)
	case []byte:
		if len(v) == 0 {
			stmt.BindText(param, "")
		} else {
			stmt.BindBytes(param, v[:])
		}
	case string:
		stmt.BindText(param, v)
	case int64:
		stmt.BindInt64(param, v)
	default:
		return fmt.Errorf("unrecognized binding type %T", v)
	}
	return nil
}

// The exec functions below are similar to those in sqlitex, but don't modify
// their error returns and don't rely on reflection to bind parameters, don't
// bind all types of parameters, and do bind some plumbing types. This is mostly
// to keep the code for parameter binding stable (in the behavior-not-changing
// sense, not in the there-might-be-bugs sense).

var errNoRows = errors.New("no rows returned")

func execTransient(c *sqlite.Conn, query string, row rowFunc, params ...interface{}) (err error) {
	stmt, tail, err := c.PrepareTransient(query)
	if err != nil {
		return err
	}
	defer func() {
		ferr := stmt.Finalize()
		if err == nil {
			err = ferr
		}
	}()
	if tail != 0 {
		return fmt.Errorf("query %q has trailing bytes", query)
	}
	if err := execStmt(stmt, row, params...); err != nil {
		return fmt.Errorf("error executing query %q: %w", query, err)
	}
	return nil
}

func exec(c *sqlite.Conn, query string, row rowFunc, params ...interface{}) (err error) {
	stmt, err := c.Prepare(query)
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
	if err := execStmt(stmt, row, params...); err != nil {
		return fmt.Errorf("error executing query %q: %w", query, err)
	}
	return nil
}

func execStmt(stmt *sqlite.Stmt, row rowFunc, params ...interface{}) error {
	// Bind parameters.
	numBinds := stmt.BindParamCount()
	if numBinds != len(params) {
		return fmt.Errorf("parameter count (%d) does not match bind count (%d) for query",
			len(params), numBinds)
	}
	for param, value := range params {
		if err := bind(stmt, param+1, value); err != nil {
			return err
		}
	}

	// Scan rows.
	for n := 0; ; n++ {
		haveRows, err := stmt.Step()
		if err != nil {
			return err
		} else if !haveRows {
			if n == 0 && row != nil {
				return errNoRows
			}
			return nil
		}
		if row != nil {
			if err = row(stmt); err != nil {
				return err
			}
		}
	}
}

func notFound(err, actual error) error {
	if errors.Is(err, errNoRows) {
		return actual
	}
	return err
}
