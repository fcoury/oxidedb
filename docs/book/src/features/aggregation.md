# Aggregation Pipeline

The aggregation pipeline is a powerful framework for data aggregation and transformation. OxideDB translates aggregation stages into PostgreSQL SQL queries when possible, falling back to in-memory processing for complex operations.

## Pipeline Overview

```javascript
db.collection.aggregate([
    { $stage1: { ... } },  // Stage 1: Filter/transform
    { $stage2: { ... } },  // Stage 2: Group/sort
    { $stage3: { ... } },  // Stage 3: Project/limit
    // ... more stages
])
```

Each stage processes documents and passes the results to the next stage.

## Stage Reference

### $match (Filter)

Filters documents using query operators. Uses the same syntax as `find()`.

```javascript
// Filter active users
db.users.aggregate([
    { $match: { status: "active" } }
])

// Complex filter
db.orders.aggregate([
    { 
        $match: { 
            status: "completed",
            total: { $gte: 100 },
            created_at: { $gte: new Date("2024-01-01") }
        } 
    }
])
```

**Execution:** SQL pushdown to PostgreSQL WHERE clause

### $project (Projection)

Reshapes documents by including, excluding, or computing fields.

```javascript
// Include only specific fields
db.users.aggregate([
    { $project: { name: 1, email: 1, _id: 0 } }
])

// Exclude fields
db.users.aggregate([
    { $project: { password: 0, internal_notes: 0 } }
])

// Compute new fields
db.users.aggregate([
    { 
        $project: { 
            name: 1,
            email: 1,
            display_name: { $concat: ["$first_name", " ", "$last_name"] }
        } 
    }
])

// Nested projection
db.orders.aggregate([
    {
        $project: {
            order_id: "$_id",
            customer: "$user_id",
            "shipping.city": 1,
            item_count: { $size: "$items" }
        }
    }
])
```

**Execution:** SQL pushdown when using simple field inclusion/exclusion

### $sort (Sorting)

Sorts documents by specified fields.

```javascript
// Simple sort
db.users.aggregate([
    { $sort: { age: 1 } }  // 1 = ascending, -1 = descending
])

// Multi-field sort
db.orders.aggregate([
    { $sort: { status: 1, created_at: -1 } }
])

// Sort after match
db.products.aggregate([
    { $match: { category: "electronics" } },
    { $sort: { price: 1, name: 1 } }
])
```

**Execution:** SQL pushdown to ORDER BY clause

### $limit (Limit)

Limits the number of documents passed to the next stage.

```javascript
// Get top 10
db.products.aggregate([
    { $sort: { sales: -1 } },
    { $limit: 10 }
])

// Pagination with skip
db.users.aggregate([
    { $sort: { created_at: -1 } },
    { $skip: 20 },
    { $limit: 10 }
])
```

**Execution:** SQL pushdown to LIMIT clause

### $skip (Skip)

Skips a specified number of documents.

```javascript
// Pagination
db.users.aggregate([
    { $sort: { created_at: -1 } },
    { $skip: 50 },
    { $limit: 25 }
])
```

**Execution:** SQL pushdown to OFFSET clause

### $group (Grouping)

Groups documents by a specified expression and computes aggregations.

```javascript
// Group by single field
db.orders.aggregate([
    {
        $group: {
            _id: "$status",
            count: { $sum: 1 },
            total: { $sum: "$amount" }
        }
    }
])

// Group by multiple fields
db.orders.aggregate([
    {
        $group: {
            _id: { year: "$year", month: "$month" },
            count: { $sum: 1 },
            revenue: { $sum: "$total" }
        }
    }
])

// Group with multiple accumulators
db.products.aggregate([
    {
        $group: {
            _id: "$category",
            count: { $sum: 1 },
            avg_price: { $avg: "$price" },
            min_price: { $min: "$price" },
            max_price: { $max: "$price" },
            products: { $push: "$name" }
        }
    }
])

// Group all documents (global aggregation)
db.users.aggregate([
    {
        $group: {
            _id: null,
            total_users: { $sum: 1 },
            avg_age: { $avg: "$age" }
        }
    }
])
```

**Execution:** SQL pushdown with GROUP BY and aggregate functions

### $unwind (Unwind Arrays)

Deconstructs an array field to output a document for each element.

```javascript
// Simple unwind
db.orders.aggregate([
    { $unwind: "$items" }
])

// Unwind with options
db.orders.aggregate([
    { 
        $unwind: { 
            path: "$items",
            preserveNullAndEmptyArrays: true,
            includeArrayIndex: "item_index"
        } 
    }
])

// Unwind after filtering
db.users.aggregate([
    { $match: { status: "active" } },
    { $unwind: "$addresses" },
    { $match: { "addresses.country": "USA" } }
])
```

**Execution:** SQL pushdown using LATERAL JOIN with jsonb_array_elements

### $lookup (Join)

Performs a left outer join to another collection.

```javascript
// Basic lookup
db.orders.aggregate([
    {
        $lookup: {
            from: "users",
            localField: "user_id",
            foreignField: "_id",
            as: "user"
        }
    }
])

// Lookup with pipeline
db.orders.aggregate([
    {
        $lookup: {
            from: "products",
            let: { product_ids: "$product_ids" },
            pipeline: [
                { 
                    $match: { 
                        $expr: { $in: ["$_id", "$$product_ids"] }
                    } 
                }
            ],
            as: "products"
        }
    }
])
```

**Execution:** Engine execution (SQL join optimization planned)

### $addFields (Add Fields)

Adds new fields to documents while preserving existing fields.

```javascript
// Add computed fields
db.orders.aggregate([
    {
        $addFields: {
            total_with_tax: { $multiply: ["$subtotal", 1.08] },
            item_count: { $size: "$items" },
            has_discount: { $gt: ["$discount", 0] }
        }
    }
])

// Add nested fields
db.users.aggregate([
    {
        $addFields: {
            "profile.full_name": { 
                $concat: ["$profile.first_name", " ", "$profile.last_name"] 
            }
        }
    }
])
```

**Execution:** SQL pushdown when using supported expressions

### $replaceRoot (Replace Root)

Replaces the root document with a specified document.

```javascript
// Replace with nested document
db.users.aggregate([
    { $replaceRoot: { newRoot: "$profile" } }
])

// Replace with computed document
db.orders.aggregate([
    {
        $replaceRoot: {
            newRoot: {
                order_id: "$_id",
                customer: "$user_id",
                total: "$amount",
                items: "$items"
            }
        }
    }
])
```

**Execution:** SQL pushdown with jsonb_build_object

### $count (Count)

Returns a count of documents.

```javascript
// Simple count
db.users.aggregate([
    { $match: { status: "active" } },
    { $count: "active_users" }
])

// Count with grouping
db.orders.aggregate([
    { $match: { status: "completed" } },
    { $group: { _id: "$user_id" } },
    { $count: "unique_customers" }
])
```

**Execution:** SQL pushdown with COUNT(*)

### $sample (Random Sample)

Randomly selects a specified number of documents.

```javascript
// Sample 100 random users
db.users.aggregate([
    { $sample: { size: 100 } }
])

// Sample after filtering
db.products.aggregate([
    { $match: { category: "electronics" } },
    { $sample: { size: 10 } }
])
```

**Execution:** SQL pushdown with ORDER BY random() LIMIT

### $facet (Multi-Faceted Aggregation)

Processes multiple aggregation pipelines within a single stage.

```javascript
db.products.aggregate([
    {
        $facet: {
            by_category: [
                { $group: { _id: "$category", count: { $sum: 1 } } }
            ],
            by_price_range: [
                {
                    $bucket: {
                        groupBy: "$price",
                        boundaries: [0, 50, 100, 500, 1000],
                        default: "Other",
                        output: { count: { $sum: 1 } }
                    }
                }
            ],
            top_rated: [
                { $sort: { rating: -1 } },
                { $limit: 5 }
            ]
        }
    }
])
```

**Execution:** Engine execution (multiple parallel queries)

### $bucket (Bucket)

Categorizes documents into groups (buckets) based on specified boundaries.

```javascript
db.users.aggregate([
    {
        $bucket: {
            groupBy: "$age",
            boundaries: [0, 18, 30, 50, 65, 100],
            default: "Other",
            output: {
                count: { $sum: 1 },
                names: { $push: "$name" }
            }
        }
    }
])
```

**Execution:** Engine execution with SQL CASE statements

### $unionWith (Union)

Combines results from multiple collections.

```javascript
db.users.aggregate([
    { $match: { status: "active" } },
    {
        $unionWith: {
            coll: "archived_users",
            pipeline: [
                { $match: { restored: true } }
            ]
        }
    }
])
```

**Execution:** Engine execution (UNION ALL)

### $out (Output to Collection)

Writes aggregation results to a new collection.

```javascript
db.orders.aggregate([
    { $match: { status: "completed" } },
    {
        $group: {
            _id: "$user_id",
            total_spent: { $sum: "$amount" }
        }
    },
    { $out: "customer_totals" }
])
```

**Execution:** Special handling - creates new collection

### $merge (Merge into Collection)

Writes aggregation results to an existing collection with merge semantics.

```javascript
db.daily_orders.aggregate([
    {
        $group: {
            _id: "$product_id",
            daily_sales: { $sum: "$quantity" }
        }
    },
    {
        $merge: {
            into: "product_stats",
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
])
```

**Execution:** Special handling - upsert operations

## Expression Operators

### String Operators

#### $concat (Concatenate)

Concatenates strings.

```javascript
db.users.aggregate([
    {
        $project: {
            full_name: { $concat: ["$first_name", " ", "$last_name"] },
            email_domain: { $concat: ["Email: ", "$email"] }
        }
    }
])
```

#### $substr / $substrCP (Substring)

Extracts a substring.

```javascript
db.users.aggregate([
    {
        $project: {
            initials: { $substr: ["$first_name", 0, 1] },
            year: { $substr: ["$date_string", 0, 4] }
        }
    }
])
```

#### $toString (Convert to String)

Converts a value to string.

```javascript
db.orders.aggregate([
    {
        $project: {
            order_id_str: { $toString: "$_id" },
            amount_str: { $toString: "$amount" }
        }
    }
])
```

### Type Conversion Operators

```javascript
db.products.aggregate([
    {
        $project: {
            // Convert to integer
            quantity_int: { $toInt: "$quantity" },
            
            // Convert to double
            price_double: { $toDouble: "$price" },
            
            // Convert to boolean
            in_stock_bool: { $toBool: "$in_stock" },
            
            // Convert to string
            id_string: { $toString: "$_id" }
        }
    }
])
```

### Array Operators

#### $concatArrays (Concatenate Arrays)

Concatenates multiple arrays.

```javascript
db.users.aggregate([
    {
        $project: {
            all_tags: { $concatArrays: ["$tags", "$interests", ["user"]] }
        }
    }
])
```

#### $size (Array Size)

Returns the size of an array.

```javascript
db.orders.aggregate([
    {
        $project: {
            item_count: { $size: "$items" }
        }
    }
])
```

### Conditional Operators

#### $cond (Conditional)

Evaluates a boolean expression and returns different values.

```javascript
db.orders.aggregate([
    {
        $project: {
            status_label: {
                $cond: {
                    if: { $gte: ["$total", 100] },
                    then: "High Value",
                    else: "Standard"
                }
            }
        }
    }
])

// Array syntax
{
    $cond: [
        { $gte: ["$total", 100] },
        "High Value",
        "Standard"
    ]
}
```

#### $ifNull (If Null)

Returns a value if the expression is null.

```javascript
db.users.aggregate([
    {
        $project: {
            display_name: { $ifNull: ["$nickname", "$name"] },
            phone: { $ifNull: ["$phone", "N/A"] }
        }
    }
])
```

### Comparison Operators

```javascript
db.products.aggregate([
    {
        $project: {
            is_expensive: { $gt: ["$price", 100] },
            has_discount: { $lt: ["$sale_price", "$price"] },
            in_stock: { $gte: ["$quantity", 1] }
        }
    }
])
```

### Arithmetic Operators

```javascript
db.orders.aggregate([
    {
        $project: {
            total_with_tax: { $multiply: ["$subtotal", 1.08] },
            discount_amount: { $multiply: ["$total", { $divide: ["$discount_pct", 100] }] },
            per_item_price: { $divide: ["$total", "$quantity"] },
            rounded_total: { $round: ["$total", 2] }
        }
    }
])
```

## Accumulators

Accumulators are used in `$group` stages to compute aggregate values.

### $sum (Sum)

```javascript
db.orders.aggregate([
    {
        $group: {
            _id: "$user_id",
            total_spent: { $sum: "$amount" },
            order_count: { $sum: 1 }
        }
    }
])
```

### $avg (Average)

```javascript
db.products.aggregate([
    {
        $group: {
            _id: "$category",
            avg_price: { $avg: "$price" },
            avg_rating: { $avg: "$rating" }
        }
    }
])
```

### $min (Minimum)

```javascript
db.orders.aggregate([
    {
        $group: {
            _id: "$user_id",
            first_order_date: { $min: "$created_at" },
            min_order_value: { $min: "$total" }
        }
    }
])
```

### $max (Maximum)

```javascript
db.orders.aggregate([
    {
        $group: {
            _id: "$user_id",
            last_order_date: { $max: "$created_at" },
            max_order_value: { $max: "$total" }
        }
    }
])
```

### $count (Count)

```javascript
db.orders.aggregate([
    {
        $group: {
            _id: "$status",
            count: { $count: {} }
        }
    }
])
```

### $push (Push to Array)

```javascript
db.orders.aggregate([
    {
        $group: {
            _id: "$user_id",
            order_ids: { $push: "$_id" },
            amounts: { $push: "$total" }
        }
    }
])
```

### $addToSet (Add to Set)

Adds unique values to an array.

```javascript
db.orders.aggregate([
    {
        $group: {
            _id: "$user_id",
            unique_products: { $addToSet: "$product_id" },
            categories: { $addToSet: "$category" }
        }
    }
])
```

### $first / $last

Returns the first or last value.

```javascript
db.orders.aggregate([
    { $sort: { created_at: 1 } },
    {
        $group: {
            _id: "$user_id",
            first_order: { $first: "$created_at" },
            last_order: { $last: "$created_at" }
        }
    }
])
```

## SQL Pushdown vs Engine Execution

### SQL Pushdown Stages

These stages are translated directly to PostgreSQL SQL:

| Stage | SQL Equivalent | Performance |
|-------|---------------|-------------|
| $match | WHERE clause | Fast - uses indexes |
| $project | SELECT with jsonb_build_object | Fast |
| $sort | ORDER BY | Fast - uses indexes |
| $limit | LIMIT | Fast |
| $skip | OFFSET | Fast |
| $group | GROUP BY with aggregates | Fast |
| $unwind | LATERAL JOIN | Medium |
| $sample | ORDER BY random() LIMIT | Medium |

### Engine Execution Stages

These stages require in-memory processing:

| Stage | Reason | Performance |
|-------|--------|-------------|
| $facet | Multiple parallel pipelines | Slower |
| $unionWith | UNION ALL between collections | Medium |
| $lookup | Cross-collection joins | Medium |
| $out | Write to new collection | Slow |
| $merge | Upsert operations | Slow |
| $bucket | Complex bucketing logic | Medium |

### Optimization Tips

1. **Order matters**: Place `$match` early to filter data
2. **Use indexes**: Create indexes on fields used in `$match` and `$sort`
3. **Minimize data**: Use `$project` early to reduce document size
4. **Avoid engine stages**: Use SQL pushdown stages when possible

```javascript
// Good: Filter first, then process
db.orders.aggregate([
    { $match: { status: "completed" } },  // Filter early
    { $sort: { created_at: -1 } },        // Use index
    { $limit: 100 },                      // Limit data
    { $group: {                           // Group smaller dataset
        _id: "$user_id",
        total: { $sum: "$amount" }
    }}
])

// Less efficient: Process all data
db.orders.aggregate([
    { $group: { _id: "$user_id", total: { $sum: "$amount" } } },
    { $match: { "_id.status": "completed" } }  // Too late!
])
```

## Complex Pipeline Examples

### E-commerce Analytics

```javascript
db.orders.aggregate([
    // Match completed orders from last month
    { 
        $match: { 
            status: "completed",
            created_at: { 
                $gte: new Date("2024-01-01"),
                $lt: new Date("2024-02-01")
            }
        } 
    },
    
    // Unwind order items
    { $unwind: "$items" },
    
    // Lookup product details
    {
        $lookup: {
            from: "products",
            localField: "items.product_id",
            foreignField: "_id",
            as: "product"
        }
    },
    
    // Add computed fields
    {
        $addFields: {
            item_revenue: { 
                $multiply: ["$items.quantity", "$items.price"] 
            },
            product_category: { $arrayElemAt: ["$product.category", 0] }
        }
    },
    
    // Group by category
    {
        $group: {
            _id: "$product_category",
            total_revenue: { $sum: "$item_revenue" },
            total_items: { $sum: "$items.quantity" },
            order_count: { $addToSet: "$_id" }
        }
    },
    
    // Add count of unique orders
    {
        $addFields: {
            unique_orders: { $size: "$order_count" }
        }
    },
    
    // Project final results
    {
        $project: {
            category: "$_id",
            total_revenue: 1,
            total_items: 1,
            unique_orders: 1,
            avg_order_value: { $divide: ["$total_revenue", "$unique_orders"] }
        }
    },
    
    // Sort by revenue
    { $sort: { total_revenue: -1 } }
])
```

### User Activity Report

```javascript
db.user_events.aggregate([
    // Match events from last 7 days
    {
        $match: {
            timestamp: { $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) }
        }
    },
    
    // Group by user and day
    {
        $group: {
            _id: {
                user_id: "$user_id",
                date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" } }
            },
            event_count: { $sum: 1 },
            events: { $push: "$event_type" }
        }
    },
    
    // Group by user
    {
        $group: {
            _id: "$_id.user_id",
            daily_stats: {
                $push: {
                    date: "$_id.date",
                    count: "$event_count",
                    events: "$events"
                }
            },
            total_events: { $sum: "$event_count" }
        }
    },
    
    // Sort by activity
    { $sort: { total_events: -1 } },
    { $limit: 100 }
])
```

## Limitations

- **$geoNear**: Geospatial aggregation not supported
- **$redact**: Document redaction not implemented
- **$graphLookup**: Graph traversal not supported
- **$sortByCount**: Use `$group` + `$sort` instead
- Some complex expressions may require engine execution

## Next Steps

- Learn about [Transaction Support](./transactions.md)
- Explore [Query Operators](./queries.md)
- Review [MongoDB Compatibility Matrix](../reference/compatibility.md)
