package main

import (
	"context"
	"fmt"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"log"
	"strconv"
	"time"
)

type testRec struct {
	Workdate time.Time
	Name     string
	Value    float64
}

func (v testRec) String() string {
	return fmt.Sprintf("\tWorkDate=%v\n\tName=%v\n\tValue=%v\n", v.Workdate, v.Name, v.Value)
}

type testRecY struct {
	Workdate time.Time
	Name     string
	Value    int
}

func (v testRecY) String() string {
	return fmt.Sprintf("\tWorkDate=%v\n\tName=%v\n\tValue=%v\n", v.Workdate, v.Name, v.Value)
}

type testRecX struct {
	Workdate time.Time
	Value    float64
	Other    string
}

func (v testRecX) String() string {
	return fmt.Sprintf("\tWorkDate=%v\n\tValue=%v\n\tOther=%v\n", v.Workdate, v.Value, v.Other)
}

func main() {

	// struct to bson
	{
		fmt.Println("\n\nBSON <> Struct")

		data, err := bson.Marshal(testRec{time.Now(), `name1`, float64(time.Now().Minute())})
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(data)
		var reDoc testRec
		_ = bson.Unmarshal(data, &reDoc)
		fmt.Println(reDoc)
	}

	// Connect to MongoDBMS
	client, err := mongo.NewClient("mongodb://localhost:27017")
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Drop collection
	_ = client.Database("testing").Collection("numbers").Drop(ctx)

	// Create collection
	collection := client.Database("testing").Collection("numbers")

	// Insert 3 Docs
	{
		for ix := 0; ix < 2; ix++ {
			doc, _ := bson.Marshal(
				testRec{time.Now(),
					"name" + strconv.Itoa(ix),
					float64(time.Now().Minute()*100 + ix)})
			_, err := collection.InsertOne(ctx, doc)
			if err != nil {
				fmt.Println(err)
			}
		}
	}

	// ReRead and Show via Decode
	{
		fmt.Println("\n\nReRead and Show via Decode")
		ctx, _ = context.WithTimeout(context.Background(), 30*time.Second)
		cur, err := collection.Find(ctx, bson.D{})
		if err != nil {
			log.Fatal(err)
		}
		defer cur.Close(ctx)
		var seq = 0
		var result testRec
		for cur.Next(ctx) {
			err := cur.Decode(&result)
			if err != nil {
				fmt.Println(err)
			} else {
				seq++
				fmt.Println(seq, result)
			}
		}
	}

	// ReRead and Show via another struct
	{
		fmt.Println("\n\nReRead and Show via another struct")
		ctx, _ = context.WithTimeout(context.Background(), 30*time.Second)
		cur, err := collection.Find(ctx, bson.D{})
		if err != nil {
			log.Fatal(err)
		}
		defer cur.Close(ctx)
		var seq = 0
		var result testRecX
		for cur.Next(ctx) {
			err := cur.Decode(&result)
			if err != nil {
				fmt.Println(err)
			} else {
				seq++
				fmt.Println(seq, result)
			}
		}
	}

	// ReRead and Show via another field type
	{
		fmt.Println("\n\nReRead and Show via another field type")
		ctx, _ = context.WithTimeout(context.Background(), 30*time.Second)
		cur, err := collection.Find(ctx, bson.D{})
		if err != nil {
			log.Fatal(err)
		}
		defer cur.Close(ctx)
		var seq = 0
		var result testRecY
		for cur.Next(ctx) {
			err := cur.Decode(&result)
			if err != nil {
				fmt.Println(err)
			} else {
				seq++
				fmt.Println(seq, result)
			}
		}
	}

	// create index
	{
		indexes := collection.Indexes()
		ixOptions := options.IndexOptions{}
		ixOptions.SetUnique(false).SetName("ixname")
		fields := bson.D{{"name", 1}, {"workdate", -1}}
		ixModel := mongo.IndexModel{Keys: fields, Options: &ixOptions}
		retStr, retErr := indexes.CreateOne(ctx, ixModel)
		if retErr != nil {
			log.Fatal(retErr)
		} else {
			fmt.Println(retStr)
		}
	}

	//
	{
		fmt.Println("FindOne")
		var result testRec
		filter := bson.M{"name": "name0"}
		//ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
		err = collection.FindOne(ctx, filter).Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(result)
	}
}
