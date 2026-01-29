// Comprehensive namespace rewrite utilities for MongoDB wire protocol commands
// Supports namespace rewriting for shadow mode with db_prefix

use bson::{Bson, Document};

/// Rewrite command document namespaces with the given prefix.
/// Returns a new document with rewritten fields.
pub fn rewrite_command_doc(doc: &Document, prefix: &str) -> Document {
    let mut new_doc = doc.clone();

    // Rewrite $db field
    if let Some(db) = doc.get_str("$db").ok() {
        new_doc.insert("$db", format!("{}_{}", prefix, db));
    }

    // Determine command type and rewrite collection/namespace fields
    if let Some(cmd_name) = doc.keys().next() {
        match cmd_name.as_str() {
            "find" | "insert" | "update" | "delete" | "aggregate" | "create" | "drop"
            | "createIndexes" | "dropIndexes" => {
                // Rewrite collection name string
                if let Ok(coll) = doc.get_str(cmd_name) {
                    new_doc.insert(cmd_name, coll.to_string());
                }
            }
            "getMore" => {
                // Rewrite collection field
                if let Ok(coll) = doc.get_str("collection") {
                    new_doc.insert("collection", coll.to_string());
                }
            }
            "killCursors" => {
                // Rewrite collection field
                if let Ok(coll) = doc.get_str("collection") {
                    new_doc.insert("collection", coll.to_string());
                }
            }
            _ => {}
        }
    }

    // Handle nested namespace fields in indexes
    if doc.keys().next().map(|s| s.as_str()) == Some("createIndexes") {
        if let Some(Bson::Array(indexes)) = doc.get("indexes") {
            let new_indexes: Vec<Bson> = indexes
                .iter()
                .map(|idx| {
                    if let Bson::Document(idx_doc) = idx {
                        let mut new_idx = idx_doc.clone();
                        // Some drivers include ns field: "db.collection"
                        if let Ok(ns) = idx_doc.get_str("ns") {
                            if let Some((db, coll)) = ns.split_once('.') {
                                new_idx.insert("ns", format!("{}_{}.{}", prefix, db, coll));
                            }
                        }
                        Bson::Document(new_idx)
                    } else {
                        idx.clone()
                    }
                })
                .collect();
            new_doc.insert("indexes", Bson::Array(new_indexes));
        }
    }

    new_doc
}

/// Rewrite a namespace string like "db.collection" to "prefix_db.collection"
pub fn rewrite_namespace_string(ns: &str, prefix: &str) -> Option<String> {
    ns.split_once('.')
        .map(|(db, coll)| format!("{}_{}.{}", prefix, db, coll))
}

/// Rewrite full collection name for OP_QUERY
pub fn rewrite_full_collection_name(fqn: &str, prefix: &str) -> Option<String> {
    rewrite_namespace_string(fqn, prefix)
}

/// Check if a command has explicit namespace fields that need rewriting
pub fn has_namespace_fields(doc: &Document) -> bool {
    if doc.contains_key("$db") {
        return true;
    }

    if let Some(cmd_name) = doc.keys().next() {
        match cmd_name.as_str() {
            "find" | "insert" | "update" | "delete" | "aggregate" | "create" | "drop"
            | "createIndexes" | "dropIndexes" => {
                return doc.get_str(cmd_name).is_ok();
            }
            "getMore" | "killCursors" => {
                return doc.get_str("collection").is_ok();
            }
            _ => {}
        }
    }

    // Check for nested ns fields in createIndexes
    if let Some(Bson::Array(indexes)) = doc.get("indexes") {
        for idx in indexes {
            if let Bson::Document(idx_doc) = idx {
                if idx_doc.get_str("ns").is_ok() {
                    return true;
                }
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_rewrite_db_field() {
        let doc = doc! {"find": "users", "$db": "myapp"};
        let rewritten = rewrite_command_doc(&doc, "shadow");
        assert_eq!(rewritten.get_str("$db").unwrap(), "shadow_myapp");
        assert_eq!(rewritten.get_str("find").unwrap(), "users");
    }

    #[test]
    fn test_rewrite_collection_commands() {
        let commands = vec![
            doc! {"find": "users", "$db": "myapp"},
            doc! {"insert": "users", "$db": "myapp"},
            doc! {"update": "users", "$db": "myapp"},
            doc! {"delete": "users", "$db": "myapp"},
            doc! {"aggregate": "users", "$db": "myapp"},
            doc! {"create": "users", "$db": "myapp"},
            doc! {"drop": "users", "$db": "myapp"},
        ];

        for cmd in commands {
            let rewritten = rewrite_command_doc(&cmd, "shadow");
            assert_eq!(rewritten.get_str("$db").unwrap(), "shadow_myapp");
        }
    }

    #[test]
    fn test_rewrite_getmore() {
        let doc = doc! {"getMore": 12345i64, "collection": "users", "$db": "myapp"};
        let rewritten = rewrite_command_doc(&doc, "shadow");
        assert_eq!(rewritten.get_str("$db").unwrap(), "shadow_myapp");
        assert_eq!(rewritten.get_str("collection").unwrap(), "users");
    }

    #[test]
    fn test_rewrite_killcursors() {
        let doc = doc! {"killCursors": "users", "cursors": [12345i64], "$db": "myapp"};
        let rewritten = rewrite_command_doc(&doc, "shadow");
        assert_eq!(rewritten.get_str("$db").unwrap(), "shadow_myapp");
    }

    #[test]
    fn test_rewrite_create_indexes_with_ns() {
        let doc = doc! {
            "createIndexes": "users",
            "$db": "myapp",
            "indexes": [
                {"key": {"name": 1}, "name": "name_idx", "ns": "myapp.users"},
                {"key": {"email": 1}, "name": "email_idx"}
            ]
        };
        let rewritten = rewrite_command_doc(&doc, "shadow");
        assert_eq!(rewritten.get_str("$db").unwrap(), "shadow_myapp");

        let indexes = rewritten.get_array("indexes").unwrap();
        assert_eq!(indexes.len(), 2);

        let first_idx = indexes[0].as_document().unwrap();
        assert_eq!(first_idx.get_str("ns").unwrap(), "shadow_myapp.users");
    }

    #[test]
    fn test_rewrite_namespace_string() {
        assert_eq!(
            rewrite_namespace_string("myapp.users", "shadow"),
            Some("shadow_myapp.users".to_string())
        );
        assert_eq!(rewrite_namespace_string("myapp", "shadow"), None);
    }

    #[test]
    fn test_has_namespace_fields() {
        assert!(has_namespace_fields(
            &doc! {"find": "users", "$db": "myapp"}
        ));
        assert!(has_namespace_fields(
            &doc! {"getMore": 123i64, "collection": "users"}
        ));
        assert!(!has_namespace_fields(&doc! {"ping": 1}));
    }
}
