# Query Operators

OxideDB supports a comprehensive set of MongoDB query operators for filtering documents. These operators are translated to PostgreSQL SQL using JSONB functions and operators.

## Comparison Operators

### $eq (Equals)

Matches values that are equal to a specified value.

```javascript
// Find documents where status equals "active"
db.users.find({ status: { $eq: "active" } })
// Or simply:
db.users.find({ status: "active" })

// Find documents where age equals 25
db.users.find({ age: { $eq: 25 } })

// Find documents where tags array contains "premium"
db.users.find({ tags: { $eq: "premium" } })
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE jsonb_path_exists(doc, '$."status" ? (@ == "active")')
```

### $ne (Not Equals)

Matches all values that are not equal to a specified value.

```javascript
// Find documents where status is not "inactive"
db.users.find({ status: { $ne: "inactive" } })

// Find users who are not 18 years old
db.users.find({ age: { $ne: 18 } })
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (jsonb_path_exists(doc, '$."status" ? (@ != "inactive")')
   OR jsonb_path_exists(doc, '$."status"[*] ? (@ != "inactive")'))
```

### $gt (Greater Than)

Matches values that are greater than a specified value.

```javascript
// Find users older than 21
db.users.find({ age: { $gt: 21 } })

// Find products with price greater than 100
db.products.find({ price: { $gt: 100 } })

// Find orders after a specific date
db.orders.find({ created_at: { $gt: new Date("2024-01-01") } })
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (jsonb_path_exists(doc, '$."age" ? (@ > 21)')
   OR jsonb_path_exists(doc, '$."age"[*] ? (@ > 21)'))
```

### $gte (Greater Than or Equal)

Matches values that are greater than or equal to a specified value.

```javascript
// Find users aged 18 or older
db.users.find({ age: { $gte: 18 } })

// Find products with rating 4 or higher
db.products.find({ rating: { $gte: 4.0 } })
```

### $lt (Less Than)

Matches values that are less than a specified value.

```javascript
// Find users younger than 30
db.users.find({ age: { $lt: 30 } })

// Find items with quantity less than 10
db.inventory.find({ quantity: { $lt: 10 } })
```

### $lte (Less Than or Equal)

Matches values that are less than or equal to a specified value.

```javascript
// Find users aged 65 or younger
db.users.find({ age: { $lte: 65 } })

// Find products priced at $50 or less
db.products.find({ price: { $lte: 50 } })
```

### $in (In Array)

Matches any of the values specified in an array.

```javascript
// Find users in specific states
db.users.find({ state: { $in: ["CA", "NY", "TX"] } })

// Find products in specific categories
db.products.find({ category: { $in: ["electronics", "computers"] } })

// Find orders with specific statuses
db.orders.find({ status: { $in: ["pending", "processing", "shipped"] } })
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (jsonb_path_exists(doc, '$."state" ? (@ == "CA" || @ == "NY" || @ == "TX")')
   OR jsonb_path_exists(doc, '$."state"[*] ? (@ == "CA" || @ == "NY" || @ == "TX")'))
```

### $nin (Not In Array)

Matches none of the values specified in an array.

```javascript
// Find users not in specific states
db.users.find({ state: { $nin: ["AK", "HI"] } })

// Find products not in excluded categories
db.products.find({ category: { $nin: ["discontinued", "restricted"] } })
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (jsonb_path_exists(doc, '$."state" ? (@ != "AK" && @ != "HI")')
   AND jsonb_path_exists(doc, '$."state"[*] ? (@ != "AK" && @ != "HI")'))
```

## Logical Operators

### $or (Or)

Joins query clauses with a logical OR.

```javascript
// Find users who are either VIP or have high spending
db.users.find({
    $or: [
        { tier: "vip" },
        { total_spent: { $gte: 10000 } }
    ]
})

// Find products that are either on sale or new arrivals
db.products.find({
    $or: [
        { on_sale: true },
        { tags: "new-arrival" }
    ]
})
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (jsonb_path_exists(doc, '$."tier" ? (@ == "vip")')
   OR jsonb_path_exists(doc, '$."total_spent" ? (@ >= 10000)'))
```

### $and (And)

Joins query clauses with a logical AND.

```javascript
// Find active users with verified email
db.users.find({
    $and: [
        { status: "active" },
        { email_verified: true }
    ]
})

// Find products in stock and on sale
db.products.find({
    $and: [
        { in_stock: { $gt: 0 } },
        { discount: { $gt: 0 } }
    ]
})

// Note: Implicit AND is used when specifying multiple fields
db.users.find({
    status: "active",
    email_verified: true
})
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (jsonb_path_exists(doc, '$."status" ? (@ == "active")')
   AND jsonb_path_exists(doc, '$."email_verified" ? (@ == true)'))
```

### $not (Not)

Inverts the effect of a query expression.

```javascript
// Find users who are not inactive
db.users.find({
    $not: { status: "inactive" }
})

// Find products that don't have the discontinued tag
db.products.find({
    $not: { tags: "discontinued" }
})
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE NOT (jsonb_path_exists(doc, '$."status" ? (@ == "inactive")'))
```

### $nor (Nor)

Joins query clauses with a logical NOR.

```javascript
// Find users who are neither inactive nor suspended
db.users.find({
    $nor: [
        { status: "inactive" },
        { status: "suspended" }
    ]
})

// Find products that are neither out of stock nor discontinued
db.products.find({
    $nor: [
        { stock: 0 },
        { status: "discontinued" }
    ]
})
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE NOT (jsonb_path_exists(doc, '$."status" ? (@ == "inactive")')
        OR jsonb_path_exists(doc, '$."status" ? (@ == "suspended")'))
```

## Array Operators

### $all (All)

Matches arrays that contain all elements specified in the query.

```javascript
// Find users with all specified tags
db.users.find({
    tags: { $all: ["premium", "verified", "active"] }
})

// Find products that have all the features
db.products.find({
    features: { $all: ["wifi", "bluetooth", "gps"] }
})
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (jsonb_path_exists(doc, '$."tags"[*] ? (@ == "premium")')
   AND jsonb_path_exists(doc, '$."tags"[*] ? (@ == "verified")')
   AND jsonb_path_exists(doc, '$."tags"[*] ? (@ == "active")'))
```

### $size (Size)

Selects documents if the array field is a specified size.

```javascript
// Find users with exactly 3 addresses
db.users.find({
    addresses: { $size: 3 }
})

// Find orders with exactly 5 items
db.orders.find({
    items: { $size: 5 }
})
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE jsonb_array_length(doc->'addresses') = 3
```

### $elemMatch (Element Match)

Selects documents if at least one array element matches all specified query criteria.

```javascript
// Find users with at least one address in California
db.users.find({
    addresses: {
        $elemMatch: {
            state: "CA",
            zip: { $exists: true }
        }
    }
})

// Find products with reviews having high ratings
db.products.find({
    reviews: {
        $elemMatch: {
            rating: { $gte: 4 },
            verified: true
        }
    }
})

// Find orders with items matching criteria
db.orders.find({
    items: {
        $elemMatch: {
            price: { $gt: 100 },
            quantity: { $gte: 2 }
        }
    }
})
```

**SQL Translation:**
```sql
SELECT doc_bson, doc FROM mdb_test.users 
WHERE jsonb_path_exists(doc, '$."addresses"[*] ? (@."state" == "CA" && @."zip" != null)')
```

## Element Operators

### $exists (Exists)

Selects documents based on field existence.

```javascript
// Find users with an email field
db.users.find({
    email: { $exists: true }
})

// Find users without a phone field
db.users.find({
    phone: { $exists: false }
})

// Find documents with nested field
db.users.find({
    "profile.bio": { $exists: true }
})
```

**SQL Translation:**
```sql
-- Field exists
SELECT doc_bson, doc FROM mdb_test.users 
WHERE jsonb_path_exists(doc, '$."email"')

-- Field does not exist
SELECT doc_bson, doc FROM mdb_test.users 
WHERE NOT jsonb_path_exists(doc, '$."phone"')
```

### $type (Type)

Selects documents where the value of a field is of a specific BSON type.

```javascript
// Find documents where age is stored as a number
db.users.find({
    age: { $type: "number" }
})

// Find documents where tags is an array
db.users.find({
    tags: { $type: "array" }
})

// Find documents where name is a string
db.users.find({
    name: { $type: "string" }
})
```

**Supported Types:**
- `"double"` - 64-bit floating point
- `"string"` - UTF-8 string
- `"object"` - Embedded document
- `"array"` - Array
- `"binData"` - Binary data
- `"objectId"` - ObjectId
- `"bool"` - Boolean
- `"date"` - UTC datetime
- `"null"` - Null
- `"regex"` - Regular expression
- `"int"` - 32-bit integer
- `"timestamp"` - Timestamp
- `"long"` - 64-bit integer
- `"decimal"` - 128-bit decimal

## Evaluation Operators

### $regex (Regular Expression)

Selects documents where values match a specified regular expression.

```javascript
// Find users with email from gmail.com
db.users.find({
    email: { $regex: /@gmail\.com$/ }
})

// Find users with names starting with "A" (case-insensitive)
db.users.find({
    name: { $regex: "^A", $options: "i" }
})

// Find products with SKU pattern
db.products.find({
    sku: { $regex: "^PROD-[0-9]{4}$" }
})

// Multiline matching
db.documents.find({
    content: { $regex: "^Chapter", $options: "m" }
})
```

**Options:**
- `i` - Case insensitive
- `m` - Multiline (makes ^ and $ match start/end of lines)
- `s` - Dotall (makes . match newlines)
- `x` - Extended (ignores whitespace in pattern)

**SQL Translation:**
```sql
-- Simple regex
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (doc->>'email') ~ '@gmail\.com$'

-- Case-insensitive
SELECT doc_bson, doc FROM mdb_test.users 
WHERE (doc->>'name') ~ '(?i)^A'
```

### $mod (Modulo)

Performs a modulo operation on the value of a field.

```javascript
// Find even numbers
db.numbers.find({
    value: { $mod: [2, 0] }
})

// Find numbers divisible by 5 with remainder 2
db.numbers.find({
    value: { $mod: [5, 2] }
})
```

### $text (Text Search)

**Note:** Full-text search via `$text` is currently not supported. Use `$regex` for pattern matching or create PostgreSQL full-text search indexes for advanced text search.

**Alternative using regex:**
```javascript
// Instead of $text search
db.articles.find({
    $or: [
        { title: { $regex: "mongodb", $options: "i" } },
        { content: { $regex: "mongodb", $options: "i" } }
    ]
})
```

## Combining Operators

### Complex Query Examples

```javascript
// Find active VIP users with verified email and at least one order
db.users.find({
    $and: [
        { status: "active" },
        { tier: "vip" },
        { email_verified: true },
        { order_count: { $gt: 0 } }
    ]
})

// Find products that are either electronics on sale OR high-rated books
db.products.find({
    $or: [
        {
            $and: [
                { category: "electronics" },
                { on_sale: true }
            ]
        },
        {
            $and: [
                { category: "books" },
                { rating: { $gte: 4.5 } }
            ]
        }
    ]
})

// Find users with specific address criteria
db.users.find({
    addresses: {
        $elemMatch: {
            country: "USA",
            $or: [
                { state: "CA" },
                { state: "NY" }
            ]
        }
    }
})

// Find orders with complex criteria
db.orders.find({
    status: { $in: ["pending", "processing"] },
    total: { $gte: 100, $lte: 500 },
    items: {
        $elemMatch: {
            price: { $gt: 50 },
            quantity: { $gte: 1 }
        }
    },
    created_at: { $gte: new Date("2024-01-01") }
})
```

## Query Performance Tips

### Use Indexes

Create indexes on frequently queried fields:

```javascript
// Single field index
db.users.createIndex({ email: 1 })

// Compound index
db.orders.createIndex({ user_id: 1, created_at: -1 })

// Multi-key index for arrays
db.products.createIndex({ tags: 1 })
```

### Query Selectivity

Place the most selective conditions first:

```javascript
// Good: status is likely more selective than age
db.users.find({
    status: "active",  // Filter first
    age: { $gte: 18 }
})
```

### Avoid Negations When Possible

`$ne`, `$nin`, and `$not` can be slower:

```javascript
// Instead of:
db.users.find({ status: { $ne: "inactive" } })

// Consider:
db.users.find({ status: { $in: ["active", "pending", "suspended"] } })
```

### Use $in for Multiple OR Conditions

```javascript
// Instead of:
db.users.find({
    $or: [
        { state: "CA" },
        { state: "NY" },
        { state: "TX" }
    ]
})

// Use:
db.users.find({ state: { $in: ["CA", "NY", "TX"] } })
```

## Limitations

- **$type** operator has limited support for some BSON types
- **$text** full-text search is not implemented (use `$regex` as alternative)
- **$where** JavaScript expression evaluation is not supported
- **$geoWithin**, **$geoIntersects**, **$near** geospatial operators are not supported

## Next Steps

- Learn about [Aggregation Pipeline](./aggregation.md)
- Explore [Transaction Support](./transactions.md)
- Review [MongoDB Compatibility Matrix](../reference/compatibility.md)
