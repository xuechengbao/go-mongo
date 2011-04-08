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

var (
	runFindOptions = &FindOptions{BatchSize: -1, SlaveOk: false}
)

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
	Err  string "err"
	N    int    "n"
	Code int    "code"
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

// Database represents a MongoDb database.
type Database struct {
	Conn         Conn
	Name         string
	LastErrorCmd interface{}
}

// C returns the collection with name. This is a lightweight operation. The
// method does not check to see if the collection exists in the database.
func (db Database) C(name string) Collection {
	return Collection{
		Conn:         db.Conn,
		Namespace:    db.Name + "." + name,
		LastErrorCmd: db.LastErrorCmd,
	}
}

// Run runs the command cmd on the database.
func (db Database) Run(cmd interface{}, result interface{}) os.Error {
	cursor, err := db.Conn.Find(db.Name+".$cmd", cmd, runFindOptions)
	if err != nil {
		return err
	}

	var d BSONData
	if err := cursor.Next(&d); err != nil {
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

// LastError returns the last error for the database using cmd. If cmd is nil,
// then the command {"getLasetError": 1} is used to get the error.
func (db Database) LastError(cmd interface{}) os.Error {
	if cmd == nil {
		cmd = DefaultLastErrorCmd
	}
	cursor, err := db.Conn.Find(db.Name+".$cmd", cmd, runFindOptions)
	if err != nil {
		return err
	}
	defer cursor.Close()
	var r struct {
		CommandResponse
		MongoError
	}
	if err := cursor.Next(&r); err != nil {
		return err
	}
	if err := r.CommandResponse.Error(); err != nil {
		return err
	}
	if r.MongoError.Err != "" {
		return &r.MongoError
	}
	return nil
}

// DBRef is a reference to a document in a database. Use the Database
// Dereference method to get the referenced document. See
// http://www.mongodb.org/display/DOCS/Database+References for more
// information on DBRefs.
type DBRef struct {
	// The target document's id.
	Id ObjectId "$id"

	// The target document's collection.
	Collection string "$ref"

	// The target document's database (optional).
	Database string "$db/c"
}

// Deference fetches the document specified by a database reference.
func (db Database) Dereference(ref DBRef, slaveOk bool, result interface{}) os.Error {
	if ref.Database != "" {
		db.Name = ref.Database
	}
	return db.C(ref.Collection).Find(M{"_id": ref.Id}).SlaveOk(slaveOk).One(result)
}
