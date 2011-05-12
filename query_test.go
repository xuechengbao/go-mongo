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
	{query: D{{"x", 1}}, count: 1},
}

func TestCount(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test", "test")
	defer c.Conn.Close()

	for i := 0; i < 10; i++ {
		err := c.Insert(map[string]int{"x": i})
		if err != nil {
			t.Fatal("insert", err)
		}
	}

	for _, tt := range countTests {
		n, err := c.Find(tt.query).Limit(tt.limit).Skip(tt.skip).Count()
		if err != nil {
			t.Fatal("count", err)
		}
		if n != tt.count {
			t.Errorf("test: %+v, actual: %d", tt, n)
		}
	}
}

func TestQuery(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test", "test")
	defer c.Conn.Close()

	for i := 0; i < 10; i++ {
		err := c.Insert(map[string]int{"x": i})
		if err != nil {
			t.Fatal("insert", err)
		}
	}

	var m M
	err := c.Find(nil).Sort(D{{"x", -1}}).One(&m)
	if err != nil {
		t.Fatal("findone", err)
	}

	if m["x"] != 9 {
		t.Fatal("expect max value for descending sort")
	}
}

func TestFill(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test", "test")
	defer c.Conn.Close()

	for i := 0; i < 10; i++ {
		err := c.Insert(map[string]int{"x": i})
		if err != nil {
			t.Fatal("insert", err)
		}
	}

	p := make([]M, 11)
	n, err := c.Find(nil).Fill(p)
	if err != nil {
		t.Fatalf("fill() = %v", err)
	}
	if n != 10 {
		t.Fatalf("n=%d, want 10", n)
	}

	for i, m := range p[:n] {
		if m["x"] != i {
			t.Fatalf("p[%d][x]=%v, want %i", i, m["x"], i)
		}
	}
}

func Distinct(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test", "test")
	defer c.Conn.Close()

	for i := 0; i < 10; i++ {
		err := c.Insert(map[string]int{"x": i, "filter": i % 2})
		if err != nil {
			t.Fatal("insert", err)
		}
	}

	var r []int
	err := c.Find(nil).Distinct("x", &r)
	if err != nil {
		t.Fatal("Distinct returned error", err)
	}

	if len(r) != 10 {
		t.Fatalf("Distinct returned %d results, want 10", len(r))
	}

	r = nil
	err = c.Find(M{"filter": 1}).Distinct("x", &r)
	if err != nil {
		t.Fatal("Distinct w/ filter returned error", err)
	}

	if len(r) != 5 {
		t.Fatalf("Distinct  w/ filterreturned %d results, want 5", len(r))
	}
}
