// Driver compatibility test for Node.js MongoDB driver
// Tests basic operations against OxideDB

const { MongoClient } = require('mongodb');

const OXIDEDB_URL = process.env.OXIDEDB_URL || 'mongodb://localhost:27017';
const TEST_DB = 'driver_compat_test_nodejs';

async function runTests() {
    console.log('Node.js MongoDB Driver Compatibility Test');
    console.log('==========================================');
    console.log(`Connecting to: ${OXIDEDB_URL}`);
    console.log('');

    const client = new MongoClient(OXIDEDB_URL, {
        serverSelectionTimeoutMS: 5000,
    });

    const results = {
        passed: 0,
        failed: 0,
        tests: []
    };

    try {
        await client.connect();
        console.log('✓ Connected successfully\n');

        const db = client.db(TEST_DB);
        const collection = db.collection('test_collection');

        // Test 1: Ping
        try {
            await db.admin().ping();
            results.passed++;
            results.tests.push({ name: 'ping', status: 'PASS' });
            console.log('✓ Test 1: ping');
        } catch (e) {
            results.failed++;
            results.tests.push({ name: 'ping', status: 'FAIL', error: e.message });
            console.log('✗ Test 1: ping -', e.message);
        }

        // Test 2: Create collection
        try {
            await collection.drop().catch(() => {}); // Ignore if doesn't exist
            await db.createCollection('test_collection');
            results.passed++;
            results.tests.push({ name: 'createCollection', status: 'PASS' });
            console.log('✓ Test 2: createCollection');
        } catch (e) {
            results.failed++;
            results.tests.push({ name: 'createCollection', status: 'FAIL', error: e.message });
            console.log('✗ Test 2: createCollection -', e.message);
        }

        // Test 3: Insert
        try {
            const insertResult = await collection.insertOne({ name: 'Alice', age: 30 });
            if (insertResult.insertedId) {
                results.passed++;
                results.tests.push({ name: 'insertOne', status: 'PASS' });
                console.log('✓ Test 3: insertOne');
            } else {
                throw new Error('No insertedId returned');
            }
        } catch (e) {
            results.failed++;
            results.tests.push({ name: 'insertOne', status: 'FAIL', error: e.message });
            console.log('✗ Test 3: insertOne -', e.message);
        }

        // Test 4: Find
        try {
            const doc = await collection.findOne({ name: 'Alice' });
            if (doc && doc.name === 'Alice' && doc.age === 30) {
                results.passed++;
                results.tests.push({ name: 'findOne', status: 'PASS' });
                console.log('✓ Test 4: findOne');
            } else {
                throw new Error('Document not found or incorrect');
            }
        } catch (e) {
            results.failed++;
            results.tests.push({ name: 'findOne', status: 'FAIL', error: e.message });
            console.log('✗ Test 4: findOne -', e.message);
        }

        // Test 5: Insert many
        try {
            const insertResult = await collection.insertMany([
                { name: 'Bob', age: 25 },
                { name: 'Charlie', age: 35 }
            ]);
            if (insertResult.insertedCount === 2) {
                results.passed++;
                results.tests.push({ name: 'insertMany', status: 'PASS' });
                console.log('✓ Test 5: insertMany');
            } else {
                throw new Error('Expected 2 inserted documents');
            }
        } catch (e) {
            results.failed++;
            results.tests.push({ name: 'insertMany', status: 'FAIL', error: e.message });
            console.log('✗ Test 5: insertMany -', e.message);
        }

        // Test 6: Find with cursor
        try {
            const cursor = collection.find({}).limit(3);
            const docs = await cursor.toArray();
            if (docs.length === 3) {
                results.passed++;
                results.tests.push({ name: 'find with cursor', status: 'PASS' });
                console.log('✓ Test 6: find with cursor');
            } else {
                throw new Error(`Expected 3 documents, got ${docs.length}`);
            }
        } catch (e) {
            results.failed++;
            results.tests.push({ name: 'find with cursor', status: 'FAIL', error: e.message });
            console.log('✗ Test 6: find with cursor -', e.message);
        }

        // Test 7: Update
        try {
            const updateResult = await collection.updateOne(
                { name: 'Alice' },
                { $set: { age: 31 } }
            );
            if (updateResult.modifiedCount === 1) {
                results.passed++;
                results.tests.push({ name: 'updateOne', status: 'PASS' });
                console.log('✓ Test 7: updateOne');
            } else {
                throw new Error('Expected 1 modified document');
            }
        } catch (e) {
            results.failed++;
            results.tests.push({ name: 'updateOne', status: 'FAIL', error: e.message });
            console.log('✗ Test 7: updateOne -', e.message);
        }

        // Test 8: Delete
        try {
            const deleteResult = await collection.deleteOne({ name: 'Bob' });
            if (deleteResult.deletedCount === 1) {
                results.passed++;
                results.tests.push({ name: 'deleteOne', status: 'PASS' });
                console.log('✓ Test 8: deleteOne');
            } else {
                throw new Error('Expected 1 deleted document');
            }
        } catch (e) {
            results.failed++;
            results.tests.push({ name: 'deleteOne', status: 'FAIL', error: e.message });
            console.log('✗ Test 8: deleteOne -', e.message);
        }

        // Test 9: List collections
        try {
            const collections = await db.listCollections().toArray();
            const hasTestCollection = collections.some(c => c.name === 'test_collection');
            if (hasTestCollection) {
                results.passed++;
                results.tests.push({ name: 'listCollections', status: 'PASS' });
                console.log('✓ Test 9: listCollections');
            } else {
                throw new Error('test_collection not found');
            }
        } catch (e) {
            results.failed++;
            results.tests.push({ name: 'listCollections', status: 'FAIL', error: e.message });
            console.log('✗ Test 9: listCollections -', e.message);
        }

        // Test 10: Drop collection
        try {
            await collection.drop();
            results.passed++;
            results.tests.push({ name: 'drop', status: 'PASS' });
            console.log('✓ Test 10: drop');
        } catch (e) {
            results.failed++;
            results.tests.push({ name: 'drop', status: 'FAIL', error: e.message });
            console.log('✗ Test 10: drop -', e.message);
        }

    } catch (e) {
        console.error('Failed to connect:', e.message);
        results.failed++;
        results.tests.push({ name: 'connect', status: 'FAIL', error: e.message });
    } finally {
        await client.close();
    }

    // Summary
    console.log('\n==========================================');
    console.log('Summary:');
    console.log(`  Passed: ${results.passed}`);
    console.log(`  Failed: ${results.failed}`);
    console.log(`  Total:  ${results.passed + results.failed}`);
    console.log('==========================================');

    // Output JSON for CI
    console.log('\nJSON_OUTPUT:');
    console.log(JSON.stringify(results, null, 2));

    process.exit(results.failed > 0 ? 1 : 0);
}

runTests();
