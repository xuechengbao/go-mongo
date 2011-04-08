package mongo

import (
	"testing"
)

func TestLastError(t *testing.T) {
	c := dialAndDrop(t, "go-mongo-test", "test")
	defer c.Conn.Close()
	c.LastErrorCmd = nil

	// Insert duplicate id to create an error.
	id := NewObjectId()
	for i := 0; i < 2; i++ {
		c.Insert(M{"_id": id})
	}

	err := c.Db().LastError(nil)
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

	db := Database{c, "admin", nil}

	var m M
	err = db.Run(D{{"buildInfo", 1}}, &m)
	if err != nil {
		t.Fatal("runcommand", err)
	}
	if len(m) == 0 {
		t.Fatal("command result not set")
	}
	m = nil
	err = db.Run(D{{"thisIsNotACommand", 1}}, &m)
	if err == nil {
		t.Fatal("error not returned for bad command")
	}
}
