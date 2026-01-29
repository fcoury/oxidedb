// Driver compatibility test for Go MongoDB driver
// Tests basic operations against OxideDB

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TestResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type Results struct {
	Passed int          `json:"passed"`
	Failed int          `json:"failed"`
	Tests  []TestResult `json:"tests"`
}

func main() {
	oxidedbURL := os.Getenv("OXIDEDB_URL")
	if oxidedbURL == "" {
		oxidedbURL = "mongodb://localhost:27017"
	}
	testDB := "driver_compat_test_go"

	fmt.Println("Go MongoDB Driver Compatibility Test")
	fmt.Println("====================================")
	fmt.Printf("Connecting to: %s\n\n", oxidedbURL)

	results := Results{
		Passed: 0,
		Failed: 0,
		Tests:  []TestResult{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(oxidedbURL))
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "connect", Status: "FAIL", Error: err.Error()})
		printResults(results)
		os.Exit(1)
	}
	defer client.Disconnect(ctx)

	if err := client.Ping(ctx, nil); err != nil {
		fmt.Printf("Failed to ping: %v\n", err)
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "ping", Status: "FAIL", Error: err.Error()})
		printResults(results)
		os.Exit(1)
	}

	fmt.Println("✓ Connected successfully\n")

	db := client.Database(testDB)
	collection := db.Collection("test_collection")

	// Test 1: Ping
	if err := testPing(ctx, client, &results); err != nil {
		fmt.Printf("✗ Test 1: ping - %v\n", err)
	} else {
		fmt.Println("✓ Test 1: ping")
	}

	// Test 2: Create collection
	if err := testCreateCollection(ctx, db, collection, &results); err != nil {
		fmt.Printf("✗ Test 2: createCollection - %v\n", err)
	} else {
		fmt.Println("✓ Test 2: createCollection")
	}

	// Test 3: Insert
	if err := testInsert(ctx, collection, &results); err != nil {
		fmt.Printf("✗ Test 3: insertOne - %v\n", err)
	} else {
		fmt.Println("✓ Test 3: insertOne")
	}

	// Test 4: Find
	if err := testFind(ctx, collection, &results); err != nil {
		fmt.Printf("✗ Test 4: findOne - %v\n", err)
	} else {
		fmt.Println("✓ Test 4: findOne")
	}

	// Test 5: Insert many
	if err := testInsertMany(ctx, collection, &results); err != nil {
		fmt.Printf("✗ Test 5: insertMany - %v\n", err)
	} else {
		fmt.Println("✓ Test 5: insertMany")
	}

	// Test 6: Find with cursor
	if err := testFindWithCursor(ctx, collection, &results); err != nil {
		fmt.Printf("✗ Test 6: find with cursor - %v\n", err)
	} else {
		fmt.Println("✓ Test 6: find with cursor")
	}

	// Test 7: Update
	if err := testUpdate(ctx, collection, &results); err != nil {
		fmt.Printf("✗ Test 7: updateOne - %v\n", err)
	} else {
		fmt.Println("✓ Test 7: updateOne")
	}

	// Test 8: Delete
	if err := testDelete(ctx, collection, &results); err != nil {
		fmt.Printf("✗ Test 8: deleteOne - %v\n", err)
	} else {
		fmt.Println("✓ Test 8: deleteOne")
	}

	// Test 9: List collections
	if err := testListCollections(ctx, db, &results); err != nil {
		fmt.Printf("✗ Test 9: listCollections - %v\n", err)
	} else {
		fmt.Println("✓ Test 9: listCollections")
	}

	// Test 10: Drop collection
	if err := testDrop(ctx, collection, &results); err != nil {
		fmt.Printf("✗ Test 10: drop - %v\n", err)
	} else {
		fmt.Println("✓ Test 10: drop")
	}

	printResults(results)

	if results.Failed > 0 {
		os.Exit(1)
	}
}

func testPing(ctx context.Context, client *mongo.Client, results *Results) error {
	if err := client.Ping(ctx, nil); err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "ping", Status: "FAIL", Error: err.Error()})
		return err
	}
	results.Passed++
	results.Tests = append(results.Tests, TestResult{Name: "ping", Status: "PASS"})
	return nil
}

func testCreateCollection(ctx context.Context, db *mongo.Database, collection *mongo.Collection, results *Results) error {
	// Drop if exists
	collection.Drop(ctx)

	if err := db.CreateCollection(ctx, "test_collection"); err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "createCollection", Status: "FAIL", Error: err.Error()})
		return err
	}
	results.Passed++
	results.Tests = append(results.Tests, TestResult{Name: "createCollection", Status: "PASS"})
	return nil
}

func testInsert(ctx context.Context, collection *mongo.Collection, results *Results) error {
	result, err := collection.InsertOne(ctx, bson.M{"name": "Alice", "age": 30})
	if err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "insertOne", Status: "FAIL", Error: err.Error()})
		return err
	}
	if result.InsertedID == nil {
		err := fmt.Errorf("no InsertedID returned")
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "insertOne", Status: "FAIL", Error: err.Error()})
		return err
	}
	results.Passed++
	results.Tests = append(results.Tests, TestResult{Name: "insertOne", Status: "PASS"})
	return nil
}

func testFind(ctx context.Context, collection *mongo.Collection, results *Results) error {
	var doc bson.M
	if err := collection.FindOne(ctx, bson.M{"name": "Alice"}).Decode(&doc); err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "findOne", Status: "FAIL", Error: err.Error()})
		return err
	}
	if doc["name"] != "Alice" || doc["age"] != int32(30) {
		err := fmt.Errorf("document not found or incorrect")
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "findOne", Status: "FAIL", Error: err.Error()})
		return err
	}
	results.Passed++
	results.Tests = append(results.Tests, TestResult{Name: "findOne", Status: "PASS"})
	return nil
}

func testInsertMany(ctx context.Context, collection *mongo.Collection, results *Results) error {
	docs := []interface{}{
		bson.M{"name": "Bob", "age": 25},
		bson.M{"name": "Charlie", "age": 35},
	}
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "insertMany", Status: "FAIL", Error: err.Error()})
		return err
	}
	if len(result.InsertedIDs) != 2 {
		err := fmt.Errorf("expected 2 inserted documents, got %d", len(result.InsertedIDs))
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "insertMany", Status: "FAIL", Error: err.Error()})
		return err
	}
	results.Passed++
	results.Tests = append(results.Tests, TestResult{Name: "insertMany", Status: "PASS"})
	return nil
}

func testFindWithCursor(ctx context.Context, collection *mongo.Collection, results *Results) error {
	cursor, err := collection.Find(ctx, bson.M{}, options.Find().SetLimit(3))
	if err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "find with cursor", Status: "FAIL", Error: err.Error()})
		return err
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "find with cursor", Status: "FAIL", Error: err.Error()})
		return err
	}
	if len(docs) != 3 {
		err := fmt.Errorf("expected 3 documents, got %d", len(docs))
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "find with cursor", Status: "FAIL", Error: err.Error()})
		return err
	}
	results.Passed++
	results.Tests = append(results.Tests, TestResult{Name: "find with cursor", Status: "PASS"})
	return nil
}

func testUpdate(ctx context.Context, collection *mongo.Collection, results *Results) error {
	result, err := collection.UpdateOne(
		ctx,
		bson.M{"name": "Alice"},
		bson.M{"$set": bson.M{"age": 31}},
	)
	if err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "updateOne", Status: "FAIL", Error: err.Error()})
		return err
	}
	if result.ModifiedCount != 1 {
		err := fmt.Errorf("expected 1 modified document, got %d", result.ModifiedCount)
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "updateOne", Status: "FAIL", Error: err.Error()})
		return err
	}
	results.Passed++
	results.Tests = append(results.Tests, TestResult{Name: "updateOne", Status: "PASS"})
	return nil
}

func testDelete(ctx context.Context, collection *mongo.Collection, results *Results) error {
	result, err := collection.DeleteOne(ctx, bson.M{"name": "Bob"})
	if err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "deleteOne", Status: "FAIL", Error: err.Error()})
		return err
	}
	if result.DeletedCount != 1 {
		err := fmt.Errorf("expected 1 deleted document, got %d", result.DeletedCount)
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "deleteOne", Status: "FAIL", Error: err.Error()})
		return err
	}
	results.Passed++
	results.Tests = append(results.Tests, TestResult{Name: "deleteOne", Status: "PASS"})
	return nil
}

func testListCollections(ctx context.Context, db *mongo.Database, results *Results) error {
	cursor, err := db.ListCollections(ctx, bson.M{})
	if err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "listCollections", Status: "FAIL", Error: err.Error()})
		return err
	}
	defer cursor.Close(ctx)

	var found bool
	for cursor.Next(ctx) {
		var coll bson.M
		if err := cursor.Decode(&coll); err != nil {
			continue
		}
		if coll["name"] == "test_collection" {
			found = true
			break
		}
	}

	if !found {
		err := fmt.Errorf("test_collection not found")
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "listCollections", Status: "FAIL", Error: err.Error()})
		return err
	}
	results.Passed++
	results.Tests = append(results.Tests, TestResult{Name: "listCollections", Status: "PASS"})
	return nil
}

func testDrop(ctx context.Context, collection *mongo.Collection, results *Results) error {
	if err := collection.Drop(ctx); err != nil {
		results.Failed++
		results.Tests = append(results.Tests, TestResult{Name: "drop", Status: "FAIL", Error: err.Error()})
		return err
	}
	results.Passed++
	results.Tests = append(results.Tests, TestResult{Name: "drop", Status: "PASS"})
	return nil
}

func printResults(results Results) {
	fmt.Println("\n====================================")
	fmt.Println("Summary:")
	fmt.Printf("  Passed: %d\n", results.Passed)
	fmt.Printf("  Failed: %d\n", results.Failed)
	fmt.Printf("  Total:  %d\n", results.Passed+results.Failed)
	fmt.Println("====================================")

	// Output JSON for CI
	fmt.Println("\nJSON_OUTPUT:")
	jsonData, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(jsonData))
}
