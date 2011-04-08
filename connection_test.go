// Copyright 2011 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package mongo

import (
	"testing"
)

func dialAndDrop(t *testing.T, dbname, collectionName string) Collection {
	c, err := Dial("127.0.0.1")
	if err != nil {
		t.Fatal("dial", err)
	}
	db := Database{c, dbname, DefaultLastErrorCmd}
	err = db.Run(D{{"drop", collectionName}}, nil)
	if err != nil && err.String() != "ns not found" {
		db.Conn.Close()
		t.Fatal("drop", err)
	}
	return db.C(collectionName)
}

var findOptionsTests = []struct {
	limit         int
	batchSize     int
	exhaust       bool
	expectedCount int
}{
	{0, 0, false, 200},
	{0, 1, false, 200},
	{0, 2, false, 200},
	{0, 3, false, 200},
	{0, 100, false, 200},
	{0, 500, false, 200},

	{1, 0, false, 1},
	{1, 1, false, 1},
	{1, 2, false, 1},
	{1, 3, false, 1},
	{1, 100, false, 1},
	{1, 500, false, 1},

	{10, 0, false, 10},
	{10, 1, false, 10},
	{10, 2, false, 10},
	{10, 3, false, 10},
	{10, 100, false, 10},
	{10, 500, false, 10},

	{200, 3, false, 200},
	{200, 3, true, 200},
	{0, 3, true, 200},
}

func TestFindOptions(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test", "test")
	defer c.Conn.Close()

	for i := 0; i < 200; i++ {
		err := c.Insert(map[string]int{"x": i})
		if err != nil {
			t.Fatal("insert", err)
		}
	}

	for _, tt := range findOptionsTests {
		r, err := c.Find(nil).
			Limit(tt.limit).
			BatchSize(tt.batchSize).
			Exhaust(tt.exhaust).
			Cursor()
		if err != nil {
			t.Error("find", err)
			continue
		}
		count := 0
		for r.HasNext() {
			var m M
			err = r.Next(&m)
			if err != nil {
				t.Error("findOptionsTest:", tt, "count:", count, "next err:", err)
				break
			}
			count += 1
		}
		if count != tt.expectedCount {
			t.Error("findOptionsTest:", tt, "bad count:", count)
		}
	}
}

func TestTailableCursor(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test", "capped")
	defer c.Conn.Close()

	err := c.Db().Run(
		D{{"create", c.Name()},
			{"capped", true},
			{"size", 1000.0}},
		nil)
	if err != nil {
		t.Fatal("create capped", err)
	}

	var r Cursor
	for n := 1; n < 4; n++ {
		for i := 0; i < n; i++ {
			err = c.Insert(map[string]int{"x": i})
			if err != nil {
				t.Fatal("insert", i, err)
			}
		}

		if r == nil {
			r, err = c.Find(nil).Tailable(true).Cursor()
			if err != nil {
				t.Fatal("find", err)
			}
			defer r.Close()
		}

		i := 0
		for r.HasNext() {
			var m M
			err = r.Next(&m)
			if err != nil {
				t.Fatal("next", n, i, err)
			}
			if m["x"] != i {
				t.Fatal("expect", i, "actual", m["x"])
			}
			i += 1
		}
		if i != n {
			t.Fatal("count: expect", n, "actual", i)
		}
	}
}

func TestStuff(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test", "test")
	defer c.Conn.Close()

	id := NewObjectId()
	err := c.Insert(M{"_id": id, "x": 1})
	if err != nil {
		t.Fatal("insert", err)
	}

	ref := DBRef{Id: id, Collection: c.Name()}
	var m M
	err = c.Db().Dereference(ref, false, &m)
	if err != nil {
		t.Fatal("dereference", err)
	}

	err = c.Update(M{"_id": id}, M{"$inc": map[string]interface{}{"x": 1}})
	if err != nil {
		t.Fatal("update", err)
	}

	m = nil
	err = c.Find(M{"_id": id}).One(&m)
	if err != nil {
		t.Fatal("findone after update", err)
	}

	if m["x"] != 2 {
		t.Fatal("expect x = 2, got", m["x"])
	}

	err = c.Remove(M{"_id": id})
	if err != nil {
		t.Fatal("remove", err)
	}

	m = nil
	err = c.Find(M{"_id": id}).One(&m)
	if err != EOF {
		t.Fatal("findone, expect EOF, got", err)
	}

	// Don't panic of connection closed before cursor.
	r, err := c.Find(nil).Cursor()
	if err != nil {
		t.Fatal("find", err)
	}
	c.Conn.Close()
	r.HasNext()
	r.Close()
}
