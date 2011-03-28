package main

import (
    "github.com/garyburd/go-mongo"
    "log"
)

type ExampleDoc struct {
    Id    mongo.ObjectId "_id"
    Title string
    Body  string
}

func main() {

    // Connect to server.

    conn, err := mongo.Dial("localhost")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    c := mongo.Collection{conn, "example-db.example-collection", mongo.DefaultLastErrorCmd}

    // Insert a document.

    id := mongo.NewObjectId()

    err = c.Insert(&ExampleDoc{Id: id, Title: "Hello", Body: "Mongo is fun."})
    if err != nil {
        log.Fatal(err)
    }

    // Find the document.

    var doc ExampleDoc
    err = c.Find(map[string]interface{}{"_id": id}).One(&doc)
    if err != nil {
        log.Fatal(err)
    }

    log.Print(doc.Title, " ", doc.Body)
}
