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
)

// Doc is deprecated. Use D instead.
type Doc []DocItem

func (d *Doc) Append(name string, value interface{}) {
	*d = append(*d, DocItem{name, value})
}

// FindOne is deprecated. Use Collection{conn, namespace}.Find(query).One(result) instead.
func FindOne(conn Conn, namespace string, query interface{}, options *FindOptions, result interface{}) os.Error {
	q := Collection{Conn: conn, Namespace: namespace}.Find(query)
	if options != nil {
		q.Options = *options
	}
	return q.One(result)
}

// RunCommand is deprecated. Use Database{conn, dbname}.Run(cmd, result) instead.
func RunCommand(conn Conn, namespace string, cmd Doc, result interface{}) os.Error {
	dbname, _ := SplitNamespace(namespace)
	return Database{Conn: conn, Name: dbname}.Run(cmd, result)
}

// LastError is deprecated. Use Database{Conn: conn, Name: dbname}.LastError(cmd) instead.
func LastError(conn Conn, namespace string, cmd interface{}) os.Error {
	dbname, _ := SplitNamespace(namespace)
	return Database{Conn: conn, Name: dbname}.LastError(cmd)
}

// commandNamespace returns the command namespace give a database name or
// namespace.
func commandNamespace(namespace string) string {
	name, _ := SplitNamespace(namespace)
	return name + ".$cmd"
}

// SafeInsert is deprecated.
func SafeInsert(conn Conn, namespace string, errorCmd interface{}, documents ...interface{}) os.Error {
	return SafeConn{conn, errorCmd}.Insert(namespace, documents...)
}

// SafeUpdate is deprecated.
func SafeUpdate(conn Conn, namespace string, errorCmd interface{}, selector, update interface{}, options *UpdateOptions) os.Error {
	return SafeConn{conn, errorCmd}.Update(namespace, selector, update, options)
}

// SafeRemove is deprecated.
func SafeRemove(conn Conn, namespace string, errorCmd interface{}, selector interface{}, options *RemoveOptions) os.Error {
	return SafeConn{conn, errorCmd}.Remove(namespace, selector, options)
}

// SafeConn is deprecated.
type SafeConn struct {
	// The connecion to wrap.
	Conn

	// The command document used to fetch the last error. If cmd is nil, then
	// the command {"getLastError": 1} is used as the command.
	Cmd interface{}
}

func (c SafeConn) checkError(namespace string, err os.Error) os.Error {
	if err != nil {
		return err
	}
	dbname, _ := SplitNamespace(namespace)
	return Database{Conn: c.Conn, Name: dbname}.LastError(c.Cmd)
}

func (c SafeConn) Update(namespace string, selector, update interface{}, options *UpdateOptions) os.Error {
	return c.checkError(namespace, c.Conn.Update(namespace, selector, update, options))
}

func (c SafeConn) Insert(namespace string, documents ...interface{}) os.Error {
	return c.checkError(namespace, c.Conn.Insert(namespace, documents...))
}

func (c SafeConn) Remove(namespace string, selector interface{}, options *RemoveOptions) os.Error {
	return c.checkError(namespace, c.Conn.Remove(namespace, selector, options))
}

// Count is deprected. Use Collection{Conn: conn, Namespace:namespace}.Find(query).Count() instead.
func Count(conn Conn, namespace string, query interface{}, options *FindOptions) (int64, os.Error) {
	q := Collection{Conn: conn, Namespace: namespace}.Find(query)
	if options != nil {
		q.Options = *options
	}
	return q.Count()
}

// FindAndUpdate is deprecated. Use the Collection FindAndUpdate method instead.
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

// FindAndRemove is deprecated. Use the Collection FindAndRemove method instead.
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
	return r.Value.Decode(result)
}
