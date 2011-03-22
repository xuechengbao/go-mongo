package mongo

import (
	"testing"
)

var countTests = []struct {
	query interface{}
	limit int
	skip  int
	count int64
}{
	{limit: 100, count: 10},
	{limit: 5, count: 5},
	{skip: 5, count: 5},
	{skip: 100, count: 0},
	{query: Doc{{"x", 1}}, count: 1},
}

func TestCount(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test")
	defer c.Close()

	for i := 0; i < 10; i++ {
		err := SafeInsert(c, "go-mongo-test.test", nil, map[string]int{"x": i})
		if err != nil {
			t.Fatal("insert", err)
		}
	}

	for _, tt := range countTests {
		n, err := Count(c, "go-mongo-test.test", tt.query, &FindOptions{Limit: tt.limit, Skip: tt.skip})
		if err != nil {
			t.Fatal("count", err)
		}
		if n != tt.count {
			t.Errorf("test: %+v, actual: %d", tt, n)
		}
	}
}

func TestQuery(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test")
	defer c.Close()

	for i := 0; i < 10; i++ {
		err := SafeInsert(c, "go-mongo-test.test", nil, map[string]int{"x": i})
		if err != nil {
			t.Fatal("insert", err)
		}
	}

	var m map[string]interface{}
	err := FindOne(c, "go-mongo-test.test", &QuerySpec{Query: Doc{}, Sort: Doc{{"x", -1}}}, nil, &m)
	if err != nil {
		t.Fatal("findone", err)
	}

	if m["x"] != 9 {
		t.Fatal("expect max value for descending sort")
	}
}

func TestLastError(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test")
	defer c.Close()

	// Insert duplicate id to create an error.
	id := NewObjectId()
	for i := 0; i < 2; i++ {
		c.Insert("go-mongo-test.test", map[string]interface{}{"_id": id})
	}

	err := LastError(c, "go-mongo-test.test", nil)
	if err, ok := err.(*MongoError); !ok {
		t.Fatalf("expected error, got %+v", err)
	} else if err.Code == 0 {
		t.Fatalf("error code not set, %+v", err)
	}
}

func TestRunCommand(t *testing.T) {
	c, err := Dial("127.0.0.1")
	if err != nil {
		t.Fatal("dial", err)
	}
	defer c.Close()

	var m map[string]interface{}
	err = RunCommand(c, "admin", Doc{{"buildInfo", 1}}, &m)
	if err != nil {
		t.Fatal("runcommand", err)
	}
	if len(m) == 0 {
		t.Fatal("command result not set")
	}
	m = nil
	err = RunCommand(c, "admin", Doc{{"thisIsNotACommand", 1}}, &m)
	if err == nil {
		t.Fatal("error not returned for bad command")
	}
}
