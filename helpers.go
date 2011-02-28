// Copyright 2011 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package mongo

import (
	"os"
	"strings"
)

// Query is a helper for specifying complex queries.
type Query struct {
	// The filter. This field is required.
	Query interface{} "$query"

	// Sort order specified by (key, direction) pairs. The direction is 1 for
	// ascending order and -1 for descending order.
	Sort Doc "$orderby"

	// If set to true, then the query returns an explain plan record the query.
	// See http://www.mongodb.org/display/DOCS/Optimization#Optimization-Explain
	Explain bool "$explain"

	// Index hint specified by (key, direction) pairs. 
	// See http://www.mongodb.org/display/DOCS/Optimization#Optimization-Hint
	Hint Doc "$hint"

	// Snapshot mode assures that objects which update during the lifetime of a
	// query are returned once and only once.
	// See http://www.mongodb.org/display/DOCS/How+to+do+Snapshotted+Queries+in+the+Mongo+Database
	Snapshot bool "$snapshot"

	// Min and Max constrain matches to those having index keys between the min
	// and max keys specified.The Min value is included in the range and the
	// Max value is excluded.
	// See http://www.mongodb.org/display/DOCS/min+and+max+Query+Specifiers
	Min interface{} "$min"
	Max interface{} "$max"
}

// SplitNamespace splits a namespace into database name and collection name
// components.
func SplitNamespace(s string) (string, string) {
	if i := strings.Index(s, "."); i > 0 {
		return s[:i], s[i+1:]
	}
	return s, ""
}

// MongoError represents an error for the connection mutation operations.
type MongoError struct {
	Err  string
	N    int
	Code int
}

func (e *MongoError) String() string {
	return e.Err
}

// CommandResponse contains the common fields in command responses from the
// server. 
type CommandResponse struct {
	Ok     bool   "ok"
	Errmsg string "errmsg"
}

// Error returns the error from the response or nil.
func (s CommandResponse) Error() os.Error {
	if s.Ok {
		return nil
	}

	errmsg := s.Errmsg
	if errmsg == "" {
		errmsg = "unspecified error"
	}

	return os.NewError(errmsg)
}

// FindOne returns a single result for a query.
func FindOne(conn Conn, namespace string, query interface{}, options *FindOptions, result interface{}) os.Error {
	o := FindOptions{}
	if options != nil {
		o = *options
	}
	o.Limit = 1
	cursor, err := conn.Find(namespace, query, &o)
	if err != nil {
		return err
	}
	defer cursor.Close()
	return cursor.Next(result)
}

// commandNamespace returns the command namespace give a database name or
// namespace.
func commandNamespace(namespace string) string {
	name, _ := SplitNamespace(namespace)
	return name + ".$cmd"
}

// RunCommand executes the command cmd on the database specified by the
// database component of namespace. The function returns an error if the "ok"
// field in the command response is false.
func RunCommand(conn Conn, namespace string, cmd Doc, result interface{}) os.Error {

	var d BSONData
	if err := FindOne(conn, commandNamespace(namespace), cmd, nil, &d); err != nil {
		return err
	}
	var r CommandResponse
	if err := Decode(d.Data, &r); err != nil {
		return err
	}
	if err := r.Error(); err != nil {
		return err
	}
	if result != nil {
		if err := Decode(d.Data, result); err != nil {
			return err
		}
	}
	return nil
}

// LastError returns the last error for a database. The database is specified
// by the database component of namespace. The command cmd is used to fetch the
// last error. If cmd is nil, then the command {"getLasetError": 1} is used to
// get the error. 
func LastError(conn Conn, namespace string, cmd Doc) os.Error {
	if cmd == nil {
		cmd = Doc{{"getLastError", 1}}
	}
	var r struct {
		CommandResponse
		Err  string "err"
		N    int    "n"
		Code int    "code"
	}
	if err := FindOne(conn, commandNamespace(namespace), cmd, nil, &r); err != nil {
		return err
	}

	if err := r.Error(); err != nil {
		return err
	}

	if r.Err != "" {
		return &MongoError{Err: r.Err, N: r.N, Code: r.Code}
	}
	return nil
}

// SafeInsert returns the last error from the database after calling conn.Insert().
func SafeInsert(conn Conn, namespace string, errorCmd Doc, documents ...interface{}) os.Error {
	if err := conn.Insert(namespace, documents...); err != nil {
		return err
	}
	return LastError(conn, namespace, errorCmd)
}

// SafeUpdate returns the last error from the database after calling conn.Update().
func SafeUpdate(conn Conn, namespace string, errorCmd Doc, selector, update interface{}, options *UpdateOptions) os.Error {
	if err := conn.Update(namespace, selector, update, options); err != nil {
		return err
	}
	return LastError(conn, namespace, errorCmd)
}

// SafeRemove returns the last error from the database after calling conn.Remove().
func SafeRemove(conn Conn, namespace string, errorCmd Doc, selector interface{}, options *RemoveOptions) os.Error {
	if err := conn.Remove(namespace, selector, options); err != nil {
		return err
	}
	return LastError(conn, namespace, errorCmd)
}

// SafeConn wraps a connection with safe mode handling. The wrapper fetches the
// last error from the server after each call to a mutating operation (insert,
// update, remove) and returns the error if any as an os.Error.
type SafeConn struct {
	// The connecion to wrap.
	Conn

	// The command document used to fetch the last error. If cmd is nil, then
	// the command {"getLastError": 1} is used as the command.
	Cmd Doc
}

func (c SafeConn) Update(namespace string, selector, update interface{}, options *UpdateOptions) os.Error {
	return SafeUpdate(c.Conn, namespace, c.Cmd, selector, update, options)
}

func (c SafeConn) Insert(namespace string, documents ...interface{}) os.Error {
	return SafeInsert(c.Conn, namespace, c.Cmd, documents...)
}

func (c SafeConn) Remove(namespace string, selector interface{}, options *RemoveOptions) os.Error {
	return SafeRemove(c.Conn, namespace, c.Cmd, selector, options)
}

// Count returns the number of documents for query.
func Count(conn Conn, namespace string, query interface{}, options *FindOptions) (int64, os.Error) {
	_, name := SplitNamespace(namespace)
	cmd := Doc{{"count", name}}
	if query != nil {
		cmd.Append("query", query)
	}
	if options != nil {
		if options.Limit != 0 {
			cmd.Append("limit", options.Limit)
		}
		if options.Skip != 0 {
			cmd.Append("skip", options.Skip)
			// Copy because we don't want to scribble on caller's object.
			optionsCopy := *options
			options = &optionsCopy
			options.Skip = 0
		}
	}
	var r struct {
		CommandResponse
		N int64 "n"
	}
	if err := FindOne(conn, commandNamespace(namespace), cmd, options, &r); err != nil {
		return 0, err
	}
	return r.N, r.Error()
}

// FindAndModifyOptions specifies options for the FindAndUpdate and FindAndRemove functions.
type FindAndModifyOptions struct {
	// Set to true if you want to return the modified object rather than the original. Ignored for remove.
	New bool

	// Specify subset of fields to return.
	Fields interface{}

	// Create object if it doesn't exist. Ignored for remove.
	Upsert bool

	// If multiple docs match, choose the first one in the specified sort order
	// as the object to update.
	Sort Doc
}

// FindAndUpdate updates and returns a document specified by selector with
// operator update. FindAndUpdate is a wrapper around the MongoDB findAndModify
// command.
func FindAndUpdate(conn Conn, namespace string, selector, update interface{}, options *FindAndModifyOptions, result interface{}) os.Error {
	_, name := SplitNamespace(namespace)
	return findAndModify(
		conn,
		namespace,
		Doc{
			{"findAndModify", name},
			{"query", selector},
			{"update", update}},
		options,
		result)
}

// FindAndRemove removes and returns a document specified by selector.
// FindAndRemove is a wrapper around the MongoDB findAndModify command.
func FindAndRemove(conn Conn, namespace string, selector interface{}, options *FindAndModifyOptions, result interface{}) os.Error {
	_, name := SplitNamespace(namespace)
	return findAndModify(
		conn,
		namespace,
		Doc{
			{"findAndModify", name},
			{"query", selector},
			{"remove", true}},
		options,
		result)
}

func findAndModify(conn Conn, namespace string, cmd Doc, options *FindAndModifyOptions, result interface{}) os.Error {
	if options != nil {
		if options.New {
			cmd.Append("new", true)
		}
		if options.Fields != nil {
			cmd.Append("fields", options.Fields)
		}
		if options.Upsert {
			cmd.Append("upsert", true)
		}
		if options.Sort != nil {
			cmd.Append("sort", options.Sort)
		}
	}
	var r struct {
		CommandResponse
		Value BSONData "value"
	}
	if err := FindOne(conn, commandNamespace(namespace), cmd, nil, &r); err != nil {
		return err
	}
	if err := r.Error(); err != nil {
		return err
	}
	return Decode(r.Value.Data, result)
}
