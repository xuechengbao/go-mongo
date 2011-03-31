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
	return &Query{
		Conn:      c.Conn,
		Namespace: c.Namespace,
		Spec:      QuerySpec{Query: filter},
	}
}
