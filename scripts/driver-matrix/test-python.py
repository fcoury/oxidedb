#!/usr/bin/env python3
"""
Driver compatibility test for Python pymongo driver
Tests basic operations against OxideDB
"""

import os
import sys
import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

OXIDEDB_URL = os.environ.get("OXIDEDB_URL", "mongodb://localhost:27017")
TEST_DB = "driver_compat_test_python"


def run_tests():
    print("Python pymongo Driver Compatibility Test")
    print("=" * 50)
    print(f"Connecting to: {OXIDEDB_URL}")
    print()

    results = {"passed": 0, "failed": 0, "tests": []}

    try:
        client = MongoClient(OXIDEDB_URL, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print("✓ Connected successfully\n")

        db = client[TEST_DB]
        collection = db.test_collection

        # Test 1: Ping
        try:
            db.admin.command("ping")
            results["passed"] += 1
            results["tests"].append({"name": "ping", "status": "PASS"})
            print("✓ Test 1: ping")
        except Exception as e:
            results["failed"] += 1
            results["tests"].append({"name": "ping", "status": "FAIL", "error": str(e)})
            print(f"✗ Test 1: ping - {e}")

        # Test 2: Create collection
        try:
            collection.drop()  # Ignore if doesn't exist
            db.create_collection("test_collection")
            results["passed"] += 1
            results["tests"].append({"name": "createCollection", "status": "PASS"})
            print("✓ Test 2: createCollection")
        except Exception as e:
            results["failed"] += 1
            results["tests"].append(
                {"name": "createCollection", "status": "FAIL", "error": str(e)}
            )
            print(f"✗ Test 2: createCollection - {e}")

        # Test 3: Insert
        try:
            result = collection.insert_one({"name": "Alice", "age": 30})
            if result.inserted_id:
                results["passed"] += 1
                results["tests"].append({"name": "insert_one", "status": "PASS"})
                print("✓ Test 3: insert_one")
            else:
                raise Exception("No inserted_id returned")
        except Exception as e:
            results["failed"] += 1
            results["tests"].append(
                {"name": "insert_one", "status": "FAIL", "error": str(e)}
            )
            print(f"✗ Test 3: insert_one - {e}")

        # Test 4: Find
        try:
            doc = collection.find_one({"name": "Alice"})
            if doc and doc["name"] == "Alice" and doc["age"] == 30:
                results["passed"] += 1
                results["tests"].append({"name": "find_one", "status": "PASS"})
                print("✓ Test 4: find_one")
            else:
                raise Exception("Document not found or incorrect")
        except Exception as e:
            results["failed"] += 1
            results["tests"].append(
                {"name": "find_one", "status": "FAIL", "error": str(e)}
            )
            print(f"✗ Test 4: find_one - {e}")

        # Test 5: Insert many
        try:
            result = collection.insert_many(
                [{"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}]
            )
            if len(result.inserted_ids) == 2:
                results["passed"] += 1
                results["tests"].append({"name": "insert_many", "status": "PASS"})
                print("✓ Test 5: insert_many")
            else:
                raise Exception(
                    f"Expected 2 inserted documents, got {len(result.inserted_ids)}"
                )
        except Exception as e:
            results["failed"] += 1
            results["tests"].append(
                {"name": "insert_many", "status": "FAIL", "error": str(e)}
            )
            print(f"✗ Test 5: insert_many - {e}")

        # Test 6: Find with cursor
        try:
            docs = list(collection.find({}).limit(3))
            if len(docs) == 3:
                results["passed"] += 1
                results["tests"].append({"name": "find with cursor", "status": "PASS"})
                print("✓ Test 6: find with cursor")
            else:
                raise Exception(f"Expected 3 documents, got {len(docs)}")
        except Exception as e:
            results["failed"] += 1
            results["tests"].append(
                {"name": "find with cursor", "status": "FAIL", "error": str(e)}
            )
            print(f"✗ Test 6: find with cursor - {e}")

        # Test 7: Update
        try:
            result = collection.update_one({"name": "Alice"}, {"$set": {"age": 31}})
            if result.modified_count == 1:
                results["passed"] += 1
                results["tests"].append({"name": "update_one", "status": "PASS"})
                print("✓ Test 7: update_one")
            else:
                raise Exception(
                    f"Expected 1 modified document, got {result.modified_count}"
                )
        except Exception as e:
            results["failed"] += 1
            results["tests"].append(
                {"name": "update_one", "status": "FAIL", "error": str(e)}
            )
            print(f"✗ Test 7: update_one - {e}")

        # Test 8: Delete
        try:
            result = collection.delete_one({"name": "Bob"})
            if result.deleted_count == 1:
                results["passed"] += 1
                results["tests"].append({"name": "delete_one", "status": "PASS"})
                print("✓ Test 8: delete_one")
            else:
                raise Exception(
                    f"Expected 1 deleted document, got {result.deleted_count}"
                )
        except Exception as e:
            results["failed"] += 1
            results["tests"].append(
                {"name": "delete_one", "status": "FAIL", "error": str(e)}
            )
            print(f"✗ Test 8: delete_one - {e}")

        # Test 9: List collections
        try:
            collections = [c["name"] for c in db.list_collections()]
            if "test_collection" in collections:
                results["passed"] += 1
                results["tests"].append({"name": "list_collections", "status": "PASS"})
                print("✓ Test 9: list_collections")
            else:
                raise Exception("test_collection not found")
        except Exception as e:
            results["failed"] += 1
            results["tests"].append(
                {"name": "list_collections", "status": "FAIL", "error": str(e)}
            )
            print(f"✗ Test 9: list_collections - {e}")

        # Test 10: Drop collection
        try:
            collection.drop()
            results["passed"] += 1
            results["tests"].append({"name": "drop", "status": "PASS"})
            print("✓ Test 10: drop")
        except Exception as e:
            results["failed"] += 1
            results["tests"].append({"name": "drop", "status": "FAIL", "error": str(e)})
            print(f"✗ Test 10: drop - {e}")

    except ConnectionFailure as e:
        print(f"Failed to connect: {e}")
        results["failed"] += 1
        results["tests"].append({"name": "connect", "status": "FAIL", "error": str(e)})
    finally:
        client.close()

    # Summary
    print("\n" + "=" * 50)
    print("Summary:")
    print(f"  Passed: {results['passed']}")
    print(f"  Failed: {results['failed']}")
    print(f"  Total:  {results['passed'] + results['failed']}")
    print("=" * 50)

    # Output JSON for CI
    print("\nJSON_OUTPUT:")
    print(json.dumps(results, indent=2))

    sys.exit(1 if results["failed"] > 0 else 0)


if __name__ == "__main__":
    run_tests()
