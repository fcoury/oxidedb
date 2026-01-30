# MongoDB Compatibility Matrix

This document details the compatibility between OxideDB and MongoDB features.

## Legend

- **Full** - Fully supported with identical behavior
- **Partial** - Supported with some limitations
- **Planned** - On the roadmap for future implementation
- **Not Supported** - Not currently supported

## Commands Support

### Database Commands

| Command | Status | Notes |
|---------|--------|-------|
| `hello` / `ismaster` | Full | Wire protocol compatibility |
| `ping` | Full | Health check |
| `buildInfo` | Full | Server information |
| `listDatabases` | Full | Lists all databases |
| `dropDatabase` | Full | Drops entire database |
| `serverStatus` | Partial | Basic uptime and version info |
| `endSessions` | Full | Session cleanup |

### Collection Commands

| Command | Status | Notes |
|---------|--------|-------|
| `create` | Full | Creates collections |
| `drop` | Full | Drops collections |
| `listCollections` | Full | Lists collections in database |
| `createIndexes` | Full | Single and compound indexes |
| `dropIndexes` | Full | Removes indexes |
| `collStats` | Not Supported | Collection statistics |
| `validate` | Not Supported | Collection validation |
| `compact` | Not Supported | Compact collection |

### CRUD Commands

| Command | Status | Notes |
|---------|--------|-------|
| `insert` | Full | Single and bulk insert |
| `find` | Full | Query with filters, sort, projection |
| `getMore` | Full | Cursor iteration |
| `killCursors` | Full | Cursor cleanup |
| `update` | Full | $set, $unset, $inc, $rename, $push, $pull |
| `delete` | Full | Single and multi-document delete |
| `findAndModify` | Partial | Basic findAndModify supported |
| `aggregate` | Partial | See Aggregation Stages section |

### Transaction Commands

| Command | Status | Notes |
|---------|--------|-------|
| `startTransaction` | Full | Begin transaction |
| `commitTransaction` | Full | Commit transaction |
| `abortTransaction` | Full | Rollback transaction |

### Authentication Commands

| Command | Status | Notes |
|---------|--------|-------|
| `saslStart` | Full | SCRAM-SHA-256 authentication |
| `saslContinue` | Full | Authentication continuation |
| `logout` | Full | End session |
| `createUser` | Not Supported | User management |
| `dropUser` | Not Supported | User removal |
| `grantRolesToUser` | Not Supported | Role assignment |

## Query Operators

### Comparison Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$eq` | Full | Equality match |
| `$ne` | Full | Not equal |
| `$gt` | Full | Greater than |
| `$gte` | Full | Greater than or equal |
| `$lt` | Full | Less than |
| `$lte` | Full | Less than or equal |
| `$in` | Full | Match any value in array |
| `$nin` | Full | Match none in array |

### Logical Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$or` | Full | Logical OR |
| `$and` | Full | Logical AND |
| `$not` | Full | Logical NOT |
| `$nor` | Full | Logical NOR |

### Array Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$all` | Full | Match all elements |
| `$size` | Full | Array size match |
| `$elemMatch` | Full | Match array element |

### Element Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$exists` | Full | Field existence check |
| `$type` | Partial | Basic type checking |

### Evaluation Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$regex` | Full | Regular expression matching |
| `$mod` | Full | Modulo operation |
| `$text` | Not Supported | Full-text search |
| `$where` | Not Supported | JavaScript expression |

### Geospatial Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$geoWithin` | Not Supported | Geospatial containment |
| `$geoIntersects` | Not Supported | Geospatial intersection |
| `$near` | Not Supported | Proximity search |
| `$nearSphere` | Not Supported | Spherical proximity |

## Update Operators

### Field Update Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$set` | Full | Set field value |
| `$unset` | Full | Remove field |
| `$setOnInsert` | Not Supported | Set on upsert insert |
| `$rename` | Full | Rename field |
| `$inc` | Full | Increment value |
| `$mul` | Not Supported | Multiply value |
| `$min` | Not Supported | Update if less than |
| `$max` | Not Supported | Update if greater than |

### Array Update Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$push` | Full | Add to array |
| `$pop` | Not Supported | Remove first/last element |
| `$pull` | Full | Remove matching elements |
| `$pullAll` | Not Supported | Remove all matching |
| `$addToSet` | Not Supported | Add unique to array |
| `$each` | Not Supported | Multiple array ops |
| `$position` | Not Supported | Insert at position |
| `$slice` | Not Supported | Limit array size |
| `$sort` | Not Supported | Sort array elements |

## Aggregation Stages

### SQL Pushdown Stages

These stages are translated to PostgreSQL SQL for optimal performance:

| Stage | Status | Notes |
|-------|--------|-------|
| `$match` | Full | Filter documents |
| `$project` | Full | Reshape documents |
| `$sort` | Full | Sort documents |
| `$limit` | Full | Limit results |
| `$skip` | Full | Skip documents |
| `$group` | Full | Group and aggregate |
| `$unwind` | Full | Deconstruct arrays |
| `$sample` | Full | Random sampling |
| `$count` | Full | Count documents |

### Engine Execution Stages

These stages require in-memory processing:

| Stage | Status | Notes |
|-------|--------|-------|
| `$lookup` | Partial | Left outer join |
| `$addFields` | Partial | Add computed fields |
| `$replaceRoot` | Partial | Replace document root |
| `$facet` | Partial | Multi-faceted aggregation |
| `$bucket` | Partial | Categorize into buckets |
| `$unionWith` | Partial | Union collections |
| `$out` | Partial | Output to collection |
| `$merge` | Partial | Merge into collection |

### Not Supported Stages

| Stage | Status | Notes |
|-------|--------|-------|
| `$geoNear` | Not Supported | Geospatial aggregation |
| `$graphLookup` | Not Supported | Graph traversal |
| `$redact` | Not Supported | Document redaction |
| `$collStats` | Not Supported | Collection stats |
| `$indexStats` | Not Supported | Index statistics |
| `$planCacheStats` | Not Supported | Plan cache info |
| `$listLocalSessions` | Not Supported | List sessions |
| `$listSessions` | Not Supported | List all sessions |

## Aggregation Expressions

### String Expressions

| Expression | Status | Notes |
|------------|--------|-------|
| `$concat` | Full | Concatenate strings |
| `$substr` | Full | Substring extraction |
| `$substrCP` | Full | Substring with codepoints |
| `$toString` | Full | Convert to string |
| `$toLower` | Not Supported | Lowercase conversion |
| `$toUpper` | Not Supported | Uppercase conversion |
| `$trim` | Not Supported | Remove whitespace |
| `$ltrim` | Not Supported | Remove left whitespace |
| `$rtrim` | Not Supported | Remove right whitespace |
| `$split` | Not Supported | Split string |
| `$indexOfCP` | Not Supported | Find substring index |

### Type Conversion

| Expression | Status | Notes |
|------------|--------|-------|
| `$toInt` | Full | Convert to integer |
| `$toDouble` | Full | Convert to double |
| `$toBool` | Full | Convert to boolean |
| `$toDate` | Not Supported | Convert to date |
| `$toObjectId` | Not Supported | Convert to ObjectId |
| `$convert` | Not Supported | Generic conversion |

### Array Expressions

| Expression | Status | Notes |
|------------|--------|-------|
| `$concatArrays` | Full | Concatenate arrays |
| `$size` | Full | Array size |
| `$arrayElemAt` | Not Supported | Element at index |
| `$slice` | Not Supported | Array slice |
| `$filter` | Not Supported | Filter array |
| `$map` | Not Supported | Map array |
| `$reduce` | Not Supported | Reduce array |
| `$range` | Not Supported | Generate range |
| `$reverseArray` | Not Supported | Reverse array |
| `$in` | Not Supported | Check membership |

### Conditional Expressions

| Expression | Status | Notes |
|------------|--------|-------|
| `$cond` | Full | If-then-else |
| `$ifNull` | Full | Null coalescing |
| `$switch` | Not Supported | Multi-case switch |

### Comparison Expressions

| Expression | Status | Notes |
|------------|--------|-------|
| `$eq` | Partial | Equality (in expressions) |
| `$ne` | Not Supported | Not equal |
| `$gt` | Partial | Greater than |
| `$gte` | Partial | Greater than or equal |
| `$lt` | Partial | Less than |
| `$lte` | Partial | Less than or equal |
| `$cmp` | Not Supported | Compare |

### Arithmetic Expressions

| Expression | Status | Notes |
|------------|--------|-------|
| `$add` | Partial | Addition |
| `$subtract` | Partial | Subtraction |
| `$multiply` | Partial | Multiplication |
| `$divide` | Partial | Division |
| `$mod` | Not Supported | Modulo |
| `$abs` | Not Supported | Absolute value |
| `$ceil` | Not Supported | Ceiling |
| `$floor` | Not Supported | Floor |
| `$round` | Partial | Round to precision |
| `$sqrt` | Not Supported | Square root |
| `$pow` | Not Supported | Power |
| `$log` | Not Supported | Logarithm |
| `$ln` | Not Supported | Natural log |
| `$exp` | Not Supported | Exponential |

### Date Expressions

| Expression | Status | Notes |
|------------|--------|-------|
| `$dateToString` | Not Supported | Format date |
| `$dateFromString` | Not Supported | Parse date |
| `$dayOfMonth` | Not Supported | Day of month |
| `$dayOfWeek` | Not Supported | Day of week |
| `$dayOfYear` | Not Supported | Day of year |
| `$year` | Not Supported | Year |
| `$month` | Not Supported | Month |
| `$week` | Not Supported | Week |
| `$hour` | Not Supported | Hour |
| `$minute` | Not Supported | Minute |
| `$second` | Not Supported | Second |
| `$millisecond` | Not Supported | Millisecond |
| `$now` | Not Supported | Current time |

## Accumulators

### Group Accumulators

| Accumulator | Status | Notes |
|-------------|--------|-------|
| `$sum` | Full | Sum values |
| `$avg` | Full | Average |
| `$min` | Full | Minimum |
| `$max` | Full | Maximum |
| `$count` | Full | Count documents |
| `$push` | Full | Push to array |
| `$addToSet` | Full | Add unique to set |
| `$first` | Full | First value |
| `$last` | Full | Last value |
| `$stdDevPop` | Not Supported | Population std dev |
| `$stdDevSamp` | Not Supported | Sample std dev |

### Window Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$sum` (window) | Not Supported | Windowed sum |
| `$avg` (window) | Not Supported | Windowed average |
| `$min` (window) | Not Supported | Windowed minimum |
| `$max` (window) | Not Supported | Windowed maximum |
| `$rank` | Not Supported | Rank |
| `$denseRank` | Not Supported | Dense rank |
| `$documentNumber` | Not Supported | Row number |
| `$covariancePop` | Not Supported | Population covariance |
| `$covarianceSamp` | Not Supported | Sample covariance |

## Index Types

| Index Type | Status | Notes |
|------------|--------|-------|
| Single Field | Full | Single field index |
| Compound | Full | Multi-field index |
| Multikey | Partial | Array field index (via expression) |
| Text | Not Supported | Full-text search index |
| Hashed | Not Supported | Hashed index |
| Geospatial 2d | Not Supported | 2D geospatial |
| Geospatial 2dsphere | Not Supported | Spherical geospatial |
| Unique | Not Supported | Unique constraint |
| Partial | Not Supported | Partial filter |
| Sparse | Not Supported | Sparse index |
| TTL | Not Supported | Time-to-live |
| Hidden | Not Supported | Hidden index |
| Wildcard | Not Supported | Wildcard field |

## Wire Protocol

| Feature | Status | Notes |
|---------|--------|-------|
| OP_MSG | Full | Modern message protocol |
| OP_QUERY | Full | Legacy query protocol |
| OP_COMPRESSED | Partial | Compression (Snappy, zlib, zstd) |
| OP_INSERT | Not Supported | Legacy insert |
| OP_UPDATE | Not Supported | Legacy update |
| OP_DELETE | Not Supported | Legacy delete |
| OP_GET_MORE | Not Supported | Legacy getMore |
| OP_KILL_CURSORS | Not Supported | Legacy killCursors |

## Security Features

| Feature | Status | Notes |
|---------|--------|-------|
| SCRAM-SHA-256 | Full | Authentication mechanism |
| SCRAM-SHA-1 | Not Supported | Legacy authentication |
| TLS/SSL | Full | Encryption in transit |
| x.509 | Not Supported | Certificate auth |
| LDAP | Not Supported | LDAP authentication |
| Kerberos | Not Supported | Kerberos auth |
| Role-Based Access | Not Supported | RBAC |
| Field-Level Encryption | Not Supported | Client-side encryption |
| Client-Side FLE | Not Supported | Automatic encryption |

## Change Streams

| Feature | Status | Notes |
|---------|--------|-------|
| Change Streams | Not Supported | Real-time change notifications |
| Resume Token | Not Supported | Resume after disconnect |
| Full Document Lookup | Not Supported | Include full document |
| Pipeline Filtering | Not Supported | Filter change events |

## Known Limitations

### Query Limitations

1. **Text Search**: `$text` operator not supported. Use `$regex` as alternative.
2. **Geospatial**: No geospatial query support. Store coordinates as numbers.
3. **JavaScript**: `$where` and `$expr` with JavaScript not supported.
4. **Bitwise**: Bitwise operators (`$bitsAllSet`, etc.) not supported.

### Aggregation Limitations

1. **Complex Joins**: `$lookup` with complex pipelines limited
2. **Window Functions**: No window function support
3. **Graph Operations**: `$graphLookup` not supported
4. **Date Operations**: Limited date expression support

### Transaction Limitations

1. **Duration**: Transactions timeout after 60 seconds (configurable)
2 **Size**: Large transactions may impact performance
3. **Capped Collections**: Cannot use in transactions
4. **System Collections**: Cannot modify system collections in transactions

### Storage Limitations

1. **Document Size**: Limited by PostgreSQL JSONB (1GB theoretical)
2. **Index Size**: Subject to PostgreSQL btree limits
3. **Collection Size**: No practical limit (PostgreSQL table limit)
4. **Database Size**: No practical limit (PostgreSQL database limit)

### Performance Considerations

1. **Aggregation**: Complex pipelines may require engine execution
2. **Sorting**: Large sorts without indexes may be slow
3. **Array Operations**: Nested array queries can be expensive
4. **Cross-Collection**: `$lookup` and `$unionWith` require multiple queries

## Version Compatibility

### MongoDB Wire Protocol Version

OxideDB advertises compatibility with **Wire Protocol Version 8** (MongoDB 4.2+).

### Driver Compatibility

| Driver | Version | Status |
|--------|---------|--------|
| Node.js | 4.x, 5.x, 6.x | Full |
| Python (PyMongo) | 4.x | Full |
| Java | 4.x | Full |
| Go | 1.x | Full |
| C# | 2.x | Full |
| Ruby | 2.x | Full |
| Rust | 2.x, 3.x | Full |
| PHP | 1.x | Partial |

## Testing Compatibility

### Shadow Mode

Use shadow mode to test compatibility with your specific workload:

```toml
[shadow]
enabled = true
addr = "127.0.0.1:27018"
sample_rate = 1.0
mode = "CompareOnly"
```

### Compatibility Test Suite

Run the driver compatibility matrix:

```bash
cd scripts/driver-matrix
cargo run
```

## Reporting Issues

If you encounter compatibility issues:

1. Enable shadow mode to compare with MongoDB
2. Check the [GitHub Issues](https://github.com/fcoury/oxidedb/issues)
3. Provide:
   - MongoDB driver and version
   - Operation that failed
   - Expected vs actual behavior
   - Shadow mode comparison results (if applicable)

## Next Steps

- Review [Configuration Reference](./config.md)
- Learn about [Query Operators](../features/queries.md)
- Explore [Aggregation Pipeline](../features/aggregation.md)
