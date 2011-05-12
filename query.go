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
	"reflect"
)

// Query represents a query to the database. 
type Query struct {
	Conn      Conn
	Namespace string
	Spec      QuerySpec
	Options   FindOptions
}

// QuerySpec is a helper for specifying complex queries.
type QuerySpec struct {
	// The filter. This field is required.
	Query interface{} "$query"

	// Sort order specified by (key, direction) pairs. The direction is 1 for
	// ascending order and -1 for descending order.
	Sort interface{} "$orderby"

	// If set to true, then the query returns an explain plan record the query.
	// See http://www.mongodb.org/display/DOCS/Optimization#Optimization-Explain
	Explain bool "$explain/c"

	// Index hint specified by (key, direction) pairs. 
	// See http://www.mongodb.org/display/DOCS/Optimization#Optimization-Hint
	Hint interface{} "$hint"

	// Snapshot mode assures that objects which update during the lifetime of a
	// query are returned once and only once.
	// See http://www.mongodb.org/display/DOCS/How+to+do+Snapshotted+Queries+in+the+Mongo+Database
	Snapshot bool "$snapshot/c"

	// Min and Max constrain matches to those having index keys between the min
	// and max keys specified.The Min value is included in the range and the
	// Max value is excluded.
	// See http://www.mongodb.org/display/DOCS/min+and+max+Query+Specifiers
	Min interface{} "$min"
	Max interface{} "$max"
}


// Sort specifies the sort order for the result. The order is specified by
// (key, direction) pairs. Direction is 1 for ascending order and -1 for
// descending order.
func (q *Query) Sort(sort interface{}) *Query {
	q.Spec.Sort = sort
	return q
}

// Hint specifies an index hint. The index is specified by (key, direction)
// pairs. Direction is 1 for ascending order and -1 for descending order.
//
// More information:
//
//  http://www.mongodb.org/display/DOCS/Optimization#Optimization-Hint
func (q *Query) Hint(hint interface{}) *Query {
	q.Spec.Hint = hint
	return q
}

// Limit specifies the number of documents to return from the query.
//
// More information:
//
//  http://www.mongodb.org/display/DOCS/Advanced+Queries#AdvancedQueries-%7B%7Blimit%28%29%7D%7D
func (q *Query) Limit(limit int) *Query {
	q.Options.Limit = limit
	return q
}

// Skip specifies the number of documents the server should skip at the
// beginning of the result set.
//
// More information:
//
//  http://www.mongodb.org/display/DOCS/Advanced+Queries#AdvancedQueries-%7B%7Bskip%28%29%7D%7D
func (q *Query) Skip(skip int) *Query {
	q.Options.Skip = skip
	return q
}

// BatchSize sets the batch sized used for sending documents from the server to
// the client.
func (q *Query) BatchSize(batchSize int) *Query {
	q.Options.BatchSize = batchSize
	return q
}

// Fields limits the fields in the returned documents. Fields contains one or
// more elements, each of which is the name of a field that should be returned,
// and the integer value 1. 
//
// More information:
//
//  http://www.mongodb.org/display/DOCS/Retrieving+a+Subset+of+Fields
func (q *Query) Fields(fields interface{}) *Query {
	q.Options.Fields = fields
	return q
}

// SlaveOk specifies if query can be routed to a slave.
//
// More information:
//
//  http://www.mongodb.org/display/DOCS/Querying#Querying-slaveOk
func (q *Query) SlaveOk(slaveOk bool) *Query {
	q.Options.SlaveOk = slaveOk
	return q
}

// PartialResults specifies if mongos can reply with partial results when a
// shard is missing.
func (q *Query) PartialResults(ok bool) *Query {
	q.Options.PartialResults = ok
	return q
}

// Exhaust specifies if the server should stream data to the client full blast.
// Normally the server waits for a "get more" message before sending a batch of
// data to the client.  With this option set, the server sends batches of data
// without waiting for the "get more" messages. 
func (q *Query) Exhaust(exhaust bool) *Query {
	q.Options.Exhaust = exhaust
	return q
}

// Tailable specifies if the server should not close the cursor when no more
// data is available.
// 
// More informatoin:
//
//  http://www.mongodb.org/display/DOCS/Tailable+Cursors
func (q *Query) Tailable(tailable bool) *Query {
	q.Options.Tailable = tailable
	return q
}

// commandOptions returns copy of options with values set appropriately for
// running a command.
func commandOptions(options *FindOptions) *FindOptions {
	o := *options
	o.BatchSize = -1
	o.Limit = 0
	o.Skip = 0
	return &o
}

// Count returns the number of documents that match the query. Limit and
// skip are considered in the count.
func (q *Query) Count() (int64, os.Error) {
	dbname, cname := SplitNamespace(q.Namespace)
	cmd := D{{"count", cname}}
	if q.Spec.Query != nil {
		cmd.Append("query", &q.Spec.Query)
	}
	if q.Options.Limit != 0 {
		cmd.Append("limit", q.Options.Limit)
	}
	if q.Options.Skip != 0 {
		cmd.Append("skip", q.Options.Skip)
	}
	cursor, err := q.Conn.Find(dbname+".$cmd", cmd, commandOptions(&q.Options))
	if err != nil {
		return 0, err
	}
	defer cursor.Close()
	var r struct {
		CommandResponse
		N int64 "n"
	}
	if err := cursor.Next(&r); err != nil {
		return 0, err
	}
	return r.N, r.Error()
}

// simplifyQuery returns the simplest representation of the query. 
func (q *Query) simplifyQuery() interface{} {
	if q.Spec.Sort == nil &&
		q.Spec.Explain == false &&
		q.Spec.Hint == nil &&
		q.Spec.Snapshot == false &&
		q.Spec.Min == nil &&
		q.Spec.Max == nil {
		return q.Spec.Query
	}
	return &q.Spec
}

// One executes the query and returns the first result. 
func (q *Query) One(output interface{}) os.Error {
	q.Options.Limit = 1
	q.Options.BatchSize = -1
	cursor, err := q.Conn.Find(q.Namespace, q.simplifyQuery(), &q.Options)
	if err != nil {
		return err
	}
	defer cursor.Close()
	return cursor.Next(output)
}

// Cursor executes the query and returns a cursor over the results. Subsequent
// changes to the query object are ignored by the cursor.
func (q *Query) Cursor() (Cursor, os.Error) {
	return q.Conn.Find(q.Namespace, q.simplifyQuery(), &q.Options)
}

// Fill executes the query and copies up to len(slice) documents to output. The
// elements of slice must be valid document types (struct, map with string key)
// or pointers to valid document types. The function returns the number of
// documents in the result set.
func (q *Query) Fill(slice interface{}) (n int, err os.Error) {
	v := reflect.ValueOf(slice)
	if q.Options.Limit == 0 || q.Options.Limit > v.Len() {
		q.Options.Limit = v.Len()
	}
	cursor, err := q.Conn.Find(q.Namespace, q.simplifyQuery(), &q.Options)
	if err != nil {
		return 0, err
	}
	defer cursor.Close()
	i := 0
	for ; cursor.HasNext(); i++ {
		if err := cursor.Next(v.Index(i)); err != nil {
			return i, err
		}
	}
	return i, nil
}

// Explain returns an explanation of how the server will execute the query.
//
// More information:
//
//  http://www.mongodb.org/display/DOCS/Optimization#Optimization-Explain
func (q *Query) Explain(result interface{}) os.Error {
	spec := q.Spec
	spec.Explain = true
	options := q.Options
	if options.Limit != 0 {
		options.BatchSize = options.Limit * -1
	}
	cursor, err := q.Conn.Find(q.Namespace, &spec, &options)
	if err != nil {
		return err
	}
	defer cursor.Close()
	return cursor.Next(result)
}

// Distinct returns the distinct value for key among the docouments in the
// result set for this query.
//
// More information:
//
//  http://www.mongodb.org/display/DOCS/Aggregation#Aggregation-Distinct
func (q *Query) Distinct(key interface{}, result interface{}) os.Error {
	dbname, cname := SplitNamespace(q.Namespace)
	cmd := D{{"distinct", cname}}
	if q.Spec.Query != nil {
		cmd.Append("query", &q.Spec.Query)
	}
	cursor, err := q.Conn.Find(dbname+".$cmd", cmd, commandOptions(&q.Options))
	if err != nil {
		return err
	}
	defer cursor.Close()
	var r struct {
		CommandResponse
		Values BSONData "values"
	}
	if err := cursor.Next(&r); err != nil {
		return err
	}
	if err := r.Error(); err != nil {
		return err
	}
	return r.Values.Decode(result)
}
