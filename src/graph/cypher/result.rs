// src/graph/cypher/result.rs
// Result types for the Cypher query pipeline

use crate::datatypes::values::Value;
use petgraph::graph::{EdgeIndex, NodeIndex};
use std::collections::HashMap;

// ============================================================================
// Bindings — compact ordered map for small variable counts
// ============================================================================

/// Compact ordered map using `Vec<(String, V)>` with linear search.
/// Faster than HashMap for typical Cypher variable counts (1-8 entries):
/// no hashing overhead, cache-friendly sequential access, cheaper clone
/// (one Vec allocation vs HashMap bucket array + entries).
#[derive(Debug, Clone, Default)]
pub struct Bindings<V> {
    entries: Vec<(String, V)>,
}

impl<V> Bindings<V> {
    pub fn new() -> Self {
        Bindings {
            entries: Vec::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Bindings {
            entries: Vec::with_capacity(cap),
        }
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    #[allow(dead_code)]
    pub fn get_mut(&mut self, key: &str) -> Option<&mut V> {
        self.entries
            .iter_mut()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    /// Upsert: update if key exists, push if not.
    pub fn insert(&mut self, key: String, val: V) {
        if let Some(entry) = self.entries.iter_mut().find(|(k, _)| *k == key) {
            entry.1 = val;
        } else {
            self.entries.push((key, val));
        }
    }

    #[allow(dead_code)]
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.iter().any(|(k, _)| k == key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.entries.iter().map(|(k, _)| k)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &V)> {
        self.entries.iter().map(|(k, v)| (k, v))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove a key and return its value (move, no clone).
    pub fn remove(&mut self, key: &str) -> Option<V> {
        if let Some(pos) = self.entries.iter().position(|(k, _)| k == key) {
            Some(self.entries.swap_remove(pos).1)
        } else {
            None
        }
    }

    /// Convert to HashMap for interop with pattern_matching pre_bindings.
    pub fn to_hashmap(&self) -> HashMap<String, V>
    where
        V: Clone,
    {
        self.entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl<V> IntoIterator for Bindings<V> {
    type Item = (String, V);
    type IntoIter = std::vec::IntoIter<(String, V)>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<'a, V> IntoIterator for &'a Bindings<V> {
    type Item = &'a (String, V);
    type IntoIter = std::slice::Iter<'a, (String, V)>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

// ============================================================================
// Pipeline Result Types
// ============================================================================

/// A single row in the pipeline result set.
/// During execution, rows carry lightweight NodeIndex/EdgeIndex references.
/// Properties are resolved on-demand from the graph.
#[derive(Debug, Clone)]
pub struct ResultRow {
    /// Node variable bindings: variable_name -> NodeIndex
    pub node_bindings: Bindings<NodeIndex>,
    /// Edge variable bindings: variable_name -> EdgeBinding (source, target, edge_index)
    pub edge_bindings: Bindings<EdgeBinding>,
    /// Variable-length path bindings
    pub path_bindings: Bindings<PathBinding>,
    /// Projected values from WITH/RETURN
    pub projected: Bindings<Value>,
}

/// Lightweight edge binding — stores only indices, no cloned data.
/// Edge properties are resolved on-demand from the graph via edge_index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeBinding {
    pub source: NodeIndex,
    pub target: NodeIndex,
    pub edge_index: EdgeIndex,
}

/// Variable-length path binding
#[derive(Debug, Clone)]
pub struct PathBinding {
    #[allow(dead_code)]
    pub source: NodeIndex,
    #[allow(dead_code)]
    pub target: NodeIndex,
    pub hops: usize,
    #[allow(dead_code)]
    pub path: Vec<(NodeIndex, EdgeIndex, String)>,
}

impl Default for ResultRow {
    fn default() -> Self {
        Self::new()
    }
}

impl ResultRow {
    pub fn new() -> Self {
        ResultRow {
            node_bindings: Bindings::new(),
            edge_bindings: Bindings::new(),
            path_bindings: Bindings::new(),
            projected: Bindings::new(),
        }
    }

    /// Pre-sized constructor to avoid reallocation.
    pub fn with_capacity(nodes: usize, edges: usize, projected: usize) -> Self {
        ResultRow {
            node_bindings: Bindings::with_capacity(nodes),
            edge_bindings: Bindings::with_capacity(edges),
            path_bindings: Bindings::new(),
            projected: Bindings::with_capacity(projected),
        }
    }

    /// Create a row with only projected values (for aggregation results)
    pub fn from_projected(projected: Bindings<Value>) -> Self {
        ResultRow {
            node_bindings: Bindings::new(),
            edge_bindings: Bindings::new(),
            path_bindings: Bindings::new(),
            projected,
        }
    }
}

/// The result set flowing through the pipeline
#[derive(Debug)]
pub struct ResultSet {
    pub rows: Vec<ResultRow>,
    /// Column names in output order (populated by RETURN)
    pub columns: Vec<String>,
}

impl Default for ResultSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ResultSet {
    pub fn new() -> Self {
        ResultSet {
            rows: Vec::new(),
            columns: Vec::new(),
        }
    }
}

// ============================================================================
// Final Output
// ============================================================================

/// Per-clause execution statistics collected during PROFILE mode.
#[derive(Debug, Clone)]
pub struct ClauseStats {
    pub clause_name: String,
    pub rows_in: usize,
    pub rows_out: usize,
    pub elapsed_us: u64,
}

/// Mutation statistics returned from CREATE/SET/DELETE queries
#[derive(Debug, Clone, Default)]
pub struct MutationStats {
    pub nodes_created: usize,
    pub relationships_created: usize,
    pub properties_set: usize,
    pub nodes_deleted: usize,
    pub relationships_deleted: usize,
    pub properties_removed: usize,
}

/// Final query result returned to Python
#[derive(Debug)]
pub struct CypherResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub stats: Option<MutationStats>,
    pub profile: Option<Vec<ClauseStats>>,
}

impl CypherResult {
    pub fn empty() -> Self {
        CypherResult {
            columns: Vec::new(),
            rows: Vec::new(),
            stats: None,
            profile: None,
        }
    }

    /// Serialize the result as a CSV string.
    pub fn to_csv(&self) -> String {
        let mut buf = String::new();
        // Header
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                buf.push(',');
            }
            csv_field(&mut buf, col);
        }
        buf.push('\n');
        // Rows
        for row in &self.rows {
            for (i, val) in row.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                csv_value(&mut buf, val);
            }
            buf.push('\n');
        }
        buf
    }
}

/// Write a CSV field, quoting if it contains comma, quote, or newline.
fn csv_field(buf: &mut String, s: &str) {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        buf.push('"');
        for c in s.chars() {
            if c == '"' {
                buf.push('"');
            }
            buf.push(c);
        }
        buf.push('"');
    } else {
        buf.push_str(s);
    }
}

/// Write a Value as a CSV field.
fn csv_value(buf: &mut String, val: &Value) {
    match val {
        Value::Null => {} // empty cell
        Value::String(s) => csv_field(buf, s),
        Value::Int64(n) => {
            use std::fmt::Write;
            let _ = write!(buf, "{}", n);
        }
        Value::Float64(f) => {
            use std::fmt::Write;
            let _ = write!(buf, "{}", f);
        }
        Value::Boolean(b) => buf.push_str(if *b { "true" } else { "false" }),
        Value::UniqueId(u) => {
            use std::fmt::Write;
            let _ = write!(buf, "{}", u);
        }
        Value::DateTime(d) => buf.push_str(&d.format("%Y-%m-%d").to_string()),
        Value::Point { lat, lon } => {
            use std::fmt::Write;
            let _ = write!(buf, "POINT({} {})", lon, lat);
        }
        Value::NodeRef(idx) => {
            use std::fmt::Write;
            let _ = write!(buf, "{}", idx);
        }
        Value::EdgeRef { edge_idx, .. } => {
            use std::fmt::Write;
            let _ = write!(buf, "{}", edge_idx);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bindings_new() {
        let bindings: Bindings<i32> = Bindings::new();
        assert!(bindings.is_empty());
        assert_eq!(bindings.len(), 0);
    }

    #[test]
    fn test_bindings_with_capacity() {
        let bindings: Bindings<i32> = Bindings::with_capacity(10);
        assert!(bindings.is_empty());
        assert_eq!(bindings.len(), 0);
    }

    #[test]
    fn test_bindings_insert_and_get() {
        let mut bindings: Bindings<i32> = Bindings::new();
        bindings.insert("key1".to_string(), 42);
        assert_eq!(bindings.get("key1"), Some(&42));
        assert_eq!(bindings.get("key2"), None);
    }

    #[test]
    fn test_bindings_insert_update() {
        let mut bindings: Bindings<i32> = Bindings::new();
        bindings.insert("key".to_string(), 10);
        assert_eq!(bindings.get("key"), Some(&10));
        bindings.insert("key".to_string(), 20);
        assert_eq!(bindings.get("key"), Some(&20));
        assert_eq!(bindings.len(), 1);
    }

    #[test]
    fn test_bindings_get_mut() {
        let mut bindings: Bindings<i32> = Bindings::new();
        bindings.insert("key".to_string(), 10);
        if let Some(val) = bindings.get_mut("key") {
            *val = 20;
        }
        assert_eq!(bindings.get("key"), Some(&20));
    }

    #[test]
    fn test_bindings_contains_key() {
        let mut bindings: Bindings<i32> = Bindings::new();
        bindings.insert("key".to_string(), 42);
        assert!(bindings.contains_key("key"));
        assert!(!bindings.contains_key("missing"));
    }

    #[test]
    fn test_bindings_keys() {
        let mut bindings: Bindings<i32> = Bindings::new();
        bindings.insert("a".to_string(), 1);
        bindings.insert("b".to_string(), 2);
        let keys: Vec<_> = bindings.keys().cloned().collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"a".to_string()));
        assert!(keys.contains(&"b".to_string()));
    }

    #[test]
    fn test_bindings_iter() {
        let mut bindings: Bindings<i32> = Bindings::new();
        bindings.insert("a".to_string(), 1);
        bindings.insert("b".to_string(), 2);
        let items: Vec<_> = bindings.iter().collect();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_bindings_remove() {
        let mut bindings: Bindings<i32> = Bindings::new();
        bindings.insert("key".to_string(), 42);
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings.remove("key"), Some(42));
        assert_eq!(bindings.len(), 0);
        assert_eq!(bindings.remove("key"), None);
    }

    #[test]
    fn test_bindings_into_iter() {
        let mut bindings: Bindings<i32> = Bindings::new();
        bindings.insert("a".to_string(), 1);
        bindings.insert("b".to_string(), 2);
        let items: Vec<_> = bindings.into_iter().collect();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_bindings_ref_iter() {
        let mut bindings: Bindings<i32> = Bindings::new();
        bindings.insert("a".to_string(), 1);
        bindings.insert("b".to_string(), 2);
        let items: Vec<_> = (&bindings).into_iter().collect();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_bindings_to_hashmap() {
        let mut bindings: Bindings<i32> = Bindings::new();
        bindings.insert("a".to_string(), 1);
        bindings.insert("b".to_string(), 2);
        let map = bindings.to_hashmap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("a"), Some(&1));
        assert_eq!(map.get("b"), Some(&2));
    }

    #[test]
    fn test_result_row_new() {
        let row = ResultRow::new();
        assert!(row.node_bindings.is_empty());
        assert!(row.edge_bindings.is_empty());
        assert!(row.path_bindings.is_empty());
        assert!(row.projected.is_empty());
    }

    #[test]
    fn test_result_row_with_capacity() {
        let row = ResultRow::with_capacity(5, 3, 2);
        assert!(row.node_bindings.is_empty());
        assert!(row.edge_bindings.is_empty());
        assert!(row.path_bindings.is_empty());
        assert!(row.projected.is_empty());
    }

    #[test]
    fn test_result_row_from_projected() {
        let mut projected = Bindings::new();
        projected.insert("result".to_string(), Value::Int64(42));
        let row = ResultRow::from_projected(projected);
        assert!(row.node_bindings.is_empty());
        assert!(row.edge_bindings.is_empty());
        assert!(row.path_bindings.is_empty());
        assert_eq!(row.projected.get("result"), Some(&Value::Int64(42)));
    }

    #[test]
    fn test_result_set_new() {
        let rs = ResultSet::new();
        assert!(rs.rows.is_empty());
        assert!(rs.columns.is_empty());
    }

    #[test]
    fn test_cypher_result_empty() {
        let result = CypherResult::empty();
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
        assert!(result.stats.is_none());
        assert!(result.profile.is_none());
    }

    #[test]
    fn test_csv_field_no_special_chars() {
        let mut buf = String::new();
        csv_field(&mut buf, "simple");
        assert_eq!(buf, "simple");
    }

    #[test]
    fn test_csv_field_with_comma() {
        let mut buf = String::new();
        csv_field(&mut buf, "hello,world");
        assert_eq!(buf, "\"hello,world\"");
    }

    #[test]
    fn test_csv_field_with_quote() {
        let mut buf = String::new();
        csv_field(&mut buf, "hello\"world");
        assert_eq!(buf, "\"hello\"\"world\"");
    }

    #[test]
    fn test_csv_field_with_newline() {
        let mut buf = String::new();
        csv_field(&mut buf, "hello\nworld");
        assert_eq!(buf, "\"hello\nworld\"");
    }

    #[test]
    fn test_csv_value_null() {
        let mut buf = String::new();
        csv_value(&mut buf, &Value::Null);
        assert_eq!(buf, "");
    }

    #[test]
    fn test_csv_value_string() {
        let mut buf = String::new();
        csv_value(&mut buf, &Value::String("test".to_string()));
        assert_eq!(buf, "test");
    }

    #[test]
    fn test_csv_value_int64() {
        let mut buf = String::new();
        csv_value(&mut buf, &Value::Int64(42));
        assert_eq!(buf, "42");
    }

    #[test]
    fn test_csv_value_float64() {
        let mut buf = String::new();
        csv_value(&mut buf, &Value::Float64(3.14));
        assert!(buf.contains("3.14"));
    }

    #[test]
    fn test_csv_value_boolean_true() {
        let mut buf = String::new();
        csv_value(&mut buf, &Value::Boolean(true));
        assert_eq!(buf, "true");
    }

    #[test]
    fn test_csv_value_boolean_false() {
        let mut buf = String::new();
        csv_value(&mut buf, &Value::Boolean(false));
        assert_eq!(buf, "false");
    }

    #[test]
    fn test_to_csv_empty() {
        let result = CypherResult::empty();
        let csv = result.to_csv();
        assert_eq!(csv, "\n");
    }

    #[test]
    fn test_to_csv_with_columns() {
        let result = CypherResult {
            columns: vec!["name".to_string(), "age".to_string()],
            rows: vec![vec![Value::String("Alice".to_string()), Value::Int64(30)]],
            stats: None,
            profile: None,
        };
        let csv = result.to_csv();
        assert!(csv.contains("name"));
        assert!(csv.contains("age"));
        assert!(csv.contains("Alice"));
        assert!(csv.contains("30"));
    }

    #[test]
    fn test_to_csv_with_special_chars() {
        let result = CypherResult {
            columns: vec!["text".to_string()],
            rows: vec![vec![Value::String("hello,world".to_string())]],
            stats: None,
            profile: None,
        };
        let csv = result.to_csv();
        assert!(csv.contains("\"hello,world\""));
    }

    #[test]
    fn test_mutation_stats_default() {
        let stats = MutationStats::default();
        assert_eq!(stats.nodes_created, 0);
        assert_eq!(stats.relationships_created, 0);
        assert_eq!(stats.properties_set, 0);
        assert_eq!(stats.nodes_deleted, 0);
        assert_eq!(stats.relationships_deleted, 0);
        assert_eq!(stats.properties_removed, 0);
    }

    #[test]
    fn test_clause_stats_creation() {
        let stats = ClauseStats {
            clause_name: "MATCH".to_string(),
            rows_in: 100,
            rows_out: 50,
            elapsed_us: 1000,
        };
        assert_eq!(stats.clause_name, "MATCH");
        assert_eq!(stats.rows_in, 100);
        assert_eq!(stats.rows_out, 50);
        assert_eq!(stats.elapsed_us, 1000);
    }
}
