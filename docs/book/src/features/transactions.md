# Transaction Support

OxideDB implements MongoDB's multi-document ACID transaction protocol, enabling atomic operations across multiple documents and collections.

## Overview

Transactions in OxideDB provide:

- **Atomicity**: All operations succeed or all fail
- **Consistency**: Data remains valid before and after transactions
- **Isolation**: Transactions don't interfere with each other
- **Durability**: Committed transactions survive crashes

## Session Management

### Logical Sessions

MongoDB uses logical sessions to track operations and enable transactions:

```javascript
// Start a session
const session = client.startSession();

try {
    // Use session for operations
    await collection.findOne({ _id: 1 }, { session });
} finally {
    // End session
    await session.endSession();
}
```

**Session Properties:**
- **lsid**: Unique session identifier (UUID)
- **txnNumber**: Monotonically increasing transaction counter
- **autocommit**: Whether to auto-commit (default: true)
- **in_transaction**: Current transaction state

### Session Lifecycle

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Start     │───▶│  Operations │───▶│    End      │
│   Session   │    │  (Optional  │    │   Session   │
│             │    │Transaction) │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
       │                  │                  │
       ▼                  ▼                  ▼
  Session created    Transaction may    Session cleaned
  in memory          be started         up, resources
                                          released
```

## Transaction Commands

### startTransaction

Begins a multi-document transaction.

```javascript
const session = client.startSession();

// Start transaction
session.startTransaction({
    readConcern: { level: 'snapshot' },
    writeConcern: { w: 'majority' }
});

try {
    // Perform operations within transaction
    await db.collection('accounts').updateOne(
        { _id: 'A' },
        { $inc: { balance: -100 } },
        { session }
    );
    
    await db.collection('accounts').updateOne(
        { _id: 'B' },
        { $inc: { balance: 100 } },
        { session }
        );
    
    // Commit transaction
    await session.commitTransaction();
} catch (error) {
    // Abort transaction on error
    await session.abortTransaction();
    throw error;
} finally {
    await session.endSession();
}
```

**Options:**
- `readConcern`: Read isolation level (`local`, `majority`, `snapshot`)
- `writeConcern`: Write durability (`w: 1`, `w: 'majority'`)

### commitTransaction

Commits all operations in the transaction.

```javascript
// Using MongoDB driver
await session.commitTransaction();

// Or using runCommand
db.adminCommand({
    commitTransaction: 1,
    lsid: { id: UUID("...") },
    txnNumber: 1,
    autocommit: false
});
```

### abortTransaction

Rolls back all operations in the transaction.

```javascript
// Using MongoDB driver
await session.abortTransaction();

// Or using runCommand
db.adminCommand({
    abortTransaction: 1,
    lsid: { id: UUID("...") },
    txnNumber: 1,
    autocommit: false
});
```

## Transaction Examples

### Bank Transfer

```javascript
async function transferMoney(fromAccount, toAccount, amount) {
    const session = client.startSession();
    
    try {
        session.startTransaction({
            readConcern: { level: 'snapshot' },
            writeConcern: { w: 'majority' },
            readPreference: 'primary'
        });
        
        const accounts = db.collection('accounts');
        
        // Deduct from source account
        const fromResult = await accounts.updateOne(
            { _id: fromAccount, balance: { $gte: amount } },
            { $inc: { balance: -amount } },
            { session }
        );
        
        if (fromResult.matchedCount === 0) {
            throw new Error('Insufficient funds');
        }
        
        // Add to destination account
        await accounts.updateOne(
            { _id: toAccount },
            { $inc: { balance: amount } },
            { session }
        );
        
        // Record transaction
        await db.collection('transactions').insertOne({
            from: fromAccount,
            to: toAccount,
            amount: amount,
            timestamp: new Date()
        }, { session });
        
        await session.commitTransaction();
        console.log('Transfer completed successfully');
        
    } catch (error) {
        await session.abortTransaction();
        console.error('Transfer failed:', error);
        throw error;
    } finally {
        await session.endSession();
    }
}

// Usage
await transferMoney('account-001', 'account-002', 100.00);
```

### Order Processing

```javascript
async function createOrder(userId, items) {
    const session = client.startSession();
    
    try {
        session.startTransaction();
        
        const orders = db.collection('orders');
        const inventory = db.collection('inventory');
        const users = db.collection('users');
        
        // Calculate total
        let total = 0;
        for (const item of items) {
            const product = await inventory.findOne(
                { _id: item.productId },
                { session }
            );
            
            if (!product || product.stock < item.quantity) {
                throw new Error(`Insufficient stock for ${item.productId}`);
            }
            
            total += product.price * item.quantity;
        }
        
        // Check user balance
        const user = await users.findOne(
            { _id: userId },
            { session }
        );
        
        if (user.balance < total) {
            throw new Error('Insufficient balance');
        }
        
        // Deduct balance
        await users.updateOne(
            { _id: userId },
            { $inc: { balance: -total } },
            { session }
        );
        
        // Update inventory
        for (const item of items) {
            await inventory.updateOne(
                { _id: item.productId },
                { $inc: { stock: -item.quantity } },
                { session }
            );
        }
        
        // Create order
        const order = await orders.insertOne({
            userId: userId,
            items: items,
            total: total,
            status: 'confirmed',
            createdAt: new Date()
        }, { session });
        
        await session.commitTransaction();
        return order.insertedId;
        
    } catch (error) {
        await session.abortTransaction();
        throw error;
    } finally {
        await session.endSession();
    }
}
```

### Multi-Collection Update

```javascript
async function updateUserProfile(userId, updates) {
    const session = client.startSession();
    
    try {
        session.startTransaction();
        
        // Update user document
        await db.collection('users').updateOne(
            { _id: userId },
            { $set: updates },
            { session }
        );
        
        // Update search index
        await db.collection('user_search_index').updateOne(
            { user_id: userId },
            { 
                $set: {
                    name: updates.name,
                    email: updates.email,
                    updated_at: new Date()
                }
            },
            { session, upsert: true }
        );
        
        // Log activity
        await db.collection('activity_log').insertOne({
            user_id: userId,
            action: 'profile_update',
            timestamp: new Date(),
            changes: Object.keys(updates)
        }, { session });
        
        await session.commitTransaction();
        
    } catch (error) {
        await session.abortTransaction();
        throw error;
    } finally {
        await session.endSession();
    }
}
```

## Retryable Writes

### Overview

Retryable writes ensure that operations are executed exactly once, even if network errors occur.

```javascript
// Enable retryable writes in connection string
const client = new MongoClient('mongodb://localhost:27017/?retryWrites=true');
```

### How It Works

1. Client sends write operation with `txnNumber`
2. Server executes operation and stores result
3. If network error occurs, client retries with same `txnNumber`
4. Server returns cached result instead of re-executing

### Example

```javascript
// This operation is automatically retryable
await db.collection('users').updateOne(
    { _id: userId },
    { $set: { last_login: new Date() } }
);

// With explicit session
const session = client.startSession();
try {
    await db.collection('users').updateOne(
        { _id: userId },
        { $set: { last_login: new Date() } },
        { session }
    );
} finally {
    await session.endSession();
}
```

### Retryable Operations

- `insertOne`
- `updateOne`
- `updateMany`
- `deleteOne`
- `deleteMany`
- `replaceOne`
- `findAndModify`

## Transaction Best Practices

### Keep Transactions Short

```javascript
// Good: Short transaction
session.startTransaction();
await collection.updateOne({ _id: 1 }, { $inc: { count: 1 } }, { session });
await collection.updateOne({ _id: 2 }, { $inc: { count: -1 } }, { session });
await session.commitTransaction();

// Bad: Long transaction with external calls
session.startTransaction();
const doc = await collection.findOne({ _id: 1 }, { session });
const result = await externalAPI.call(doc);  // Don't do this!
await collection.updateOne({ _id: 1 }, { $set: { result } }, { session });
await session.commitTransaction();
```

### Handle Errors Properly

```javascript
async function safeTransaction(operations) {
    const session = client.startSession();
    
    try {
        session.startTransaction();
        
        for (const operation of operations) {
            await operation(session);
        }
        
        await session.commitTransaction();
        return { success: true };
        
    } catch (error) {
        // Always abort on error
        await session.abortTransaction();
        
        // Handle specific error types
        if (error.codeName === 'WriteConflict') {
            return { success: false, error: 'Write conflict, retry' };
        }
        if (error.codeName === 'NoSuchTransaction') {
            return { success: false, error: 'Transaction expired' };
        }
        
        throw error;
    } finally {
        await session.endSession();
    }
}
```

### Use Appropriate Write Concern

```javascript
// For critical operations
session.startTransaction({
    writeConcern: { w: 'majority', j: true, wtimeout: 5000 }
});

// For less critical operations
session.startTransaction({
    writeConcern: { w: 1 }
});
```

### Transaction Size Limits

```javascript
// Don't modify too many documents in one transaction
// Recommended: < 1000 documents

async function batchUpdateInTransactions(docs) {
    const batchSize = 100;
    
    for (let i = 0; i < docs.length; i += batchSize) {
        const batch = docs.slice(i, i + batchSize);
        const session = client.startSession();
        
        try {
            session.startTransaction();
            
            for (const doc of batch) {
                await collection.updateOne(
                    { _id: doc._id },
                    { $set: doc.updates },
                    { session }
                );
            }
            
            await session.commitTransaction();
        } finally {
            await session.endSession();
        }
    }
}
```

## Error Handling

### Common Error Codes

| Code | Name | Description |
|------|------|-------------|
| 251 | NoSuchTransaction | Transaction not found or expired |
| 211 | TransactionExpired | Transaction exceeded time limit |
| 20 | IllegalOperation | Invalid transaction operation |
| 112 | WriteConflict | Write conflict with another transaction |
| 13 | Unauthorized | Insufficient permissions |

### Error Handling Example

```javascript
async function robustTransaction(operations, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        const session = client.startSession();
        
        try {
            session.startTransaction();
            
            for (const operation of operations) {
                await operation(session);
            }
            
            await session.commitTransaction();
            return { success: true, attempts: attempt };
            
        } catch (error) {
            await session.abortTransaction();
            
            // Retry on transient errors
            if (error.hasErrorLabel('TransientTransactionError') && attempt < maxRetries) {
                console.log(`Retrying transaction (attempt ${attempt + 1})...`);
                await sleep(100 * attempt);  // Exponential backoff
                continue;
            }
            
            // Don't retry on permanent errors
            if (error.hasErrorLabel('UnknownTransactionCommitResult')) {
                // Commit result unknown, may need investigation
                console.error('Unknown commit result:', error);
            }
            
            throw error;
        } finally {
            await session.endSession();
        }
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
```

## Session Management

### endSessions Command

Explicitly end sessions to free resources:

```javascript
// End specific sessions
db.adminCommand({
    endSessions: [
        { id: UUID("550c3c62-ff0b-4c59-8982-0e016b5c024e") },
        { id: UUID("4d6f8f5e-2e2d-4d5a-9f2a-0e016b5c024f") }
    ]
});
```

### Session Timeout

Sessions automatically expire after 30 minutes of inactivity. Transactions expire after 1 minute.

```javascript
// Check session status (OxideDB extension)
db.adminCommand({
    oxidedbSessionInfo: 1,
    lsid: { id: UUID("...") }
});
```

## PostgreSQL Implementation

### Transaction Mapping

OxideDB maps MongoDB transactions to PostgreSQL transactions:

```
MongoDB                    OxideDB                    PostgreSQL
   │                          │                          │
   │─ startTransaction ──────▶│                          │
   │                          │─ BEGIN ─────────────────▶│
   │                          │◀─ OK ────────────────────│
   │◀─ ok: 1 ─────────────────│                          │
   │                          │                          │
   │─ insert/update/delete ──▶│                          │
   │                          │─ INSERT/UPDATE/DELETE ──▶│
   │◀─ ok: 1 ─────────────────│                          │
   │                          │                          │
   │─ commitTransaction ─────▶│                          │
   │                          │─ COMMIT ────────────────▶│
   │                          │◀─ OK ────────────────────│
   │◀─ ok: 1 ─────────────────│                          │
   │                          │                          │
   │─ abortTransaction ──────▶│                          │
   │                          │─ ROLLBACK ──────────────▶│
   │                          │◀─ OK ────────────────────│
   │◀─ ok: 1 ─────────────────│                          │
```

### Isolation Level

OxideDB uses PostgreSQL's **Read Committed** isolation level by default, providing:
- No dirty reads
- Non-repeatable reads possible
- No phantom reads in simple queries

For stricter isolation, use `readConcern: { level: 'snapshot' }` which maps to **Repeatable Read**.

## Limitations

- **Transaction Duration**: Maximum 60 seconds (configurable)
- **Document Limit**: No hard limit, but keep transactions small (< 1000 docs)
- **Collection Limit**: Can read/write multiple collections
- **Capped Collections**: Not supported in transactions
- **System Collections**: Cannot modify system collections

## Next Steps

- Learn about [Query Operators](./queries.md)
- Explore [Aggregation Pipeline](./aggregation.md)
- Review [MongoDB Compatibility Matrix](../reference/compatibility.md)
