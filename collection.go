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
	"bytes"
	"strconv"
)

var DefaultLastErrorCmd interface{} = map[string]int{"getLastError": 1}

var (
	upsertOptions      = &UpdateOptions{Upsert: true}
	updateAllOptions   = &UpdateOptions{Multi: true}
	removeFirstOptions = &RemoveOptions{Single: true}
)

// Collection represents a MongoDB collection.
type Collection struct {
	// Connection to the database.
	Conn Conn

	// String with the format "<database>.<collection>" where <database> is the
	// name of the database and <collection> is the name of the collection. 
	Namespace string

	// Command used to check for errors after on insert, update or remove
	// operation on the collection. If nil, then errors are not checked.
	LastErrorCmd interface{}
}

// Name returns the collection's name.
func (c Collection) Name() string {
	_, name := SplitNamespace(c.Namespace)
	return name
}

// Db returns the database for this collection.
func (c Collection) Db() Database {
	name, _ := SplitNamespace(c.Namespace)
	return Database{
		Conn:         c.Conn,
		Name:         name,
		LastErrorCmd: c.LastErrorCmd,
	}
}

func (c Collection) checkError(err os.Error) os.Error {
	if err != nil {
		return err
	}
	if c.LastErrorCmd == nil {
		return nil
	}
	return c.Db().LastError(c.LastErrorCmd)
}

// Insert adds document to the collection.
func (c Collection) Insert(documents ...interface{}) os.Error {
	return c.checkError(c.Conn.Insert(c.Namespace, documents...))
}

// Update updates the first document in the collection found by selector with
// update.
func (c Collection) Update(selector, update interface{}) os.Error {
	return c.checkError(c.Conn.Update(c.Namespace, selector, update, nil))
}

// Upsert updates the first document found by selector with update. If no 
// document is found, then the update is inserted instead.
func (c Collection) Upsert(selector interface{}, update interface{}) os.Error {
	return c.checkError(c.Conn.Update(c.Namespace, selector, update, upsertOptions))
}

// UpdateAll updates all documents matching selector with update.
func (c Collection) UpdateAll(selector interface{}, update interface{}) os.Error {
	return c.checkError(c.Conn.Update(c.Namespace, selector, update, updateAllOptions))
}

// RemoveFirst removes the first document found by selector.
func (c Collection) RemoveFirst(selector interface{}) os.Error {
	return c.checkError(c.Conn.Remove(c.Namespace, selector, removeFirstOptions))
}

// Remove removes all documents found by selector.
func (c Collection) Remove(selector interface{}) os.Error {
	return c.checkError(c.Conn.Remove(c.Namespace, selector, nil))
}

// Find returns a query object for the given filter. 
func (c Collection) Find(filter interface{}) *Query {
	if filter == nil {
		filter = emptyDoc
	}
	return &Query{
		Conn:      c.Conn,
		Namespace: c.Namespace,
		Spec:      QuerySpec{Query: filter},
	}
}

// IndexName returns the standard name for an index with keys.
func IndexName(keys D) string {
	var buf bytes.Buffer
	for i, key := range keys {
		if i != 0 {
			buf.WriteByte('_')
		}
		buf.WriteString(key.Key)
		buf.WriteByte('_')
		switch v := key.Value.(type) {
		case int:
			buf.WriteString(strconv.Itoa(v))
		case string:
			buf.WriteString(v)
		default:
			panic("Index direction must be integer or string.")
		}
	}
	return buf.String()
}

// IndexOptions specifies options for the collection CreateIndex method.
type IndexOptions struct {
	// Custom name for this index. If none specified, then a name will be generated.
	Name string "name"

	// Should this index guarantee uniqueness?
	Unique bool "unique/c"

	// Should duplicates be dropped when creating a unique index?
	DropDups bool "dropDups/c"
}

// CreateIndex creates an index on keys.
// 
// More information:
// 
//  http://www.mongodb.org/display/DOCS/Indexes
func (c Collection) CreateIndex(keys D, options *IndexOptions) os.Error {
	index := struct {
		Keys      D      "key"
		Namespace string "ns"
		IndexOptions
	}{
		Keys:      keys,
		Namespace: c.Namespace,
	}

	if options != nil {
		index.IndexOptions = *options
	}

	if index.Name == "" {
		index.Name = IndexName(keys)
	}

	if c.LastErrorCmd == nil {
		c.LastErrorCmd = DefaultLastErrorCmd
	}

	return c.Db().C("system.indexes").Insert(&index)
}

// FindAndModifyOptions specifies options for the FindAndUpdate and FindAndRemove methods.
type FindAndModifyOptions struct {
	// Set to true if you want to return the modified object rather than the
	// original. Ignored for remove.
	New bool "new/c"

	// Specify subset of fields to return.
	Fields interface{} "fields"

	// Create object if it doesn't exist. Ignored for remove.
	Upsert bool "upsert/c"

	// If multiple docs match, choose the first one in the specified sort order
	// as the object to update. 
	Sort interface{} "sort"
}

// FindAndUpdate updates and returns a document specified by selector with
// operator update. FindAndUpdate is a wrapper around the MongoDB findAndModify
// command.
//
// More information:
//
//  http://www.mongodb.org/display/DOCS/findAndModify+Command
func (c Collection) FindAndUpdate(selector, update interface{}, options *FindAndModifyOptions, result interface{}) os.Error {
	_, name := SplitNamespace(c.Namespace)
	cmd := struct {
		Collection string      "findAndModify"
		Selector   interface{} "query"
		Update     interface{} "update"
		FindAndModifyOptions
	}{
		Collection: name,
		Selector:   selector,
		Update:     update,
	}
	if options != nil {
		cmd.FindAndModifyOptions = *options
	}
	return c.findAndModify(&cmd, result)
}

// FindAndRemove removes and returns a document specified by selector.
// FindAndRemove is a wrapper around the MongoDB findAndModify command.
//
// More information:
//
//  http://www.mongodb.org/display/DOCS/findAndModify+Command
func (c Collection) FindAndRemove(selector interface{}, options *FindAndModifyOptions, result interface{}) os.Error {
	_, name := SplitNamespace(c.Namespace)
	cmd := struct {
		Collection string      "findAndModify"
		Selector   interface{} "query"
		Remove     bool        "remove"
		FindAndModifyOptions
	}{
		Collection: name,
		Selector:   selector,
		Remove:     true,
	}
	if options != nil {
		cmd.FindAndModifyOptions = *options
	}
	return c.findAndModify(&cmd, result)
}

func (c Collection) findAndModify(cmd interface{}, result interface{}) os.Error {
	dbname, _ := SplitNamespace(c.Namespace)
	cursor, err := c.Conn.Find(dbname+".$cmd", cmd, runFindOptions)
	if err != nil {
		return err
	}
	defer cursor.Close()
	var r struct {
		CommandResponse
		Value BSONData "value"
	}
	if err := cursor.Next(&r); err != nil {
		return err
	}
	if err := r.Error(); err != nil {
		return err
	}
	return Decode(r.Value.Data, result)
}
