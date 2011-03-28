package mongo

import (
	"testing"
)

func TestLastError(t *testing.T) {
	db := dialAndDrop(t, "go-mongo-test")
	defer db.Conn.Close()

	db.LastErrorCmd = nil
	c := db.C("test")

	// Insert duplicate id to create an error.
	id := NewObjectId()
	for i := 0; i < 2; i++ {
		c.Insert(map[string]interface{}{"_id": id})
	}

	err := db.LastError(nil)
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

	var m map[string]interface{}
	err = db.Run(Doc{{"buildInfo", 1}}, &m)
	if err != nil {
		t.Fatal("runcommand", err)
	}
	if len(m) == 0 {
		t.Fatal("command result not set")
	}
	m = nil
	err = db.Run(Doc{{"thisIsNotACommand", 1}}, &m)
	if err == nil {
		t.Fatal("error not returned for bad command")
	}
}
