// src/graph/introspection.rs
//
// Schema introspection functions for exploring graph structure.
// All functions take &DirGraph and return Rust structs — PyO3 conversion in mod.rs.

use crate::datatypes::values::Value;
use crate::graph::schema::{DirGraph, NodeData};
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use petgraph::Direction;
use std::collections::{HashMap, HashSet};

// ── Return types ────────────────────────────────────────────────────────────

/// Statistics about a connection type: count, source/target node types, property names.
pub struct ConnectionTypeStats {
    pub connection_type: String,
    pub count: usize,
    pub source_types: Vec<String>,
    pub target_types: Vec<String>,
    pub property_names: Vec<String>,
}

/// Summary of a node type: count and property schemas with types.
pub struct NodeTypeOverview {
    pub count: usize,
    pub properties: HashMap<String, String>,
}

/// Complete schema summary: all node types, connection types, indexes, and totals.
pub struct SchemaOverview {
    pub node_types: Vec<(String, NodeTypeOverview)>,
    pub connection_types: Vec<ConnectionTypeStats>,
    pub indexes: Vec<String>,
    pub node_count: usize,
    pub edge_count: usize,
}

/// Per-property statistics: data type, non-null count, unique count, and optional value list.
#[derive(Debug)]
pub struct PropertyStatInfo {
    pub property_name: String,
    pub type_string: String,
    pub non_null: usize,
    pub unique: usize,
    pub values: Option<Vec<Value>>,
}

/// A single neighbor connection: edge type, connected node type, and count.
pub struct NeighborConnection {
    pub connection_type: String,
    pub other_type: String,
    pub count: usize,
}

/// Grouped neighbor connections for a node type: incoming and outgoing edges.
pub struct NeighborsSchema {
    pub outgoing: Vec<NeighborConnection>,
    pub incoming: Vec<NeighborConnection>,
}

/// Level of Cypher documentation requested via `describe(cypher=...)`.
pub enum CypherDetail {
    /// No Cypher docs (default).
    Off,
    /// Tier 2: compact reference listing — all clauses, operators, functions, procedures.
    Overview,
    /// Tier 3: detailed docs with params and examples for specific topics.
    Topics(Vec<String>),
}

/// Level of fluent API documentation requested via `describe(fluent=...)`.
pub enum FluentDetail {
    /// No fluent docs (default).
    Off,
    /// Compact reference: all methods grouped by area with 1-line descriptions.
    Overview,
    /// Detailed docs with params and examples for specific topics.
    Topics(Vec<String>),
}

/// Level of connection documentation requested via `describe(connections=...)`.
pub enum ConnectionDetail {
    /// No standalone connection docs (default — connections shown in inventory).
    Off,
    /// Overview: all connection types with count, endpoints, property names.
    Overview,
    /// Deep-dive: specific connection types with per-pair counts, property stats, samples.
    Topics(Vec<String>),
}

// ── Describe helpers ────────────────────────────────────────────────────────

/// Capability flags for a node type (used by `describe()`).
struct TypeCapabilities {
    has_timeseries: bool,
    has_location: bool,
    has_geometry: bool,
    has_embeddings: bool,
}

impl TypeCapabilities {
    /// Format inline capability flags: "ts", "geo", "loc", "vec".
    fn flags_csv(&self) -> String {
        let mut flags = Vec::new();
        if self.has_timeseries {
            flags.push("ts");
        }
        if self.has_geometry {
            flags.push("geo");
        }
        if self.has_location && !self.has_geometry {
            flags.push("loc");
        }
        if self.has_embeddings {
            flags.push("vec");
        }
        flags.join(",")
    }

    /// Merge another type's capabilities into this one (for bubbling up).
    fn merge(&mut self, other: &TypeCapabilities) {
        self.has_timeseries |= other.has_timeseries;
        self.has_location |= other.has_location;
        self.has_geometry |= other.has_geometry;
        self.has_embeddings |= other.has_embeddings;
    }
}

/// Property complexity marker based on property count.
fn property_complexity(count: usize) -> &'static str {
    match count {
        0..=3 => "vl",
        4..=8 => "l",
        9..=15 => "m",
        16..=30 => "h",
        _ => "vh",
    }
}

/// Size tier for node count: vs (<10), s (10-99), m (100-999), l (1K-9999), vl (10K+).
fn size_tier(count: usize) -> &'static str {
    match count {
        0..=9 => "vs",
        10..=99 => "s",
        100..=999 => "m",
        1000..=9999 => "l",
        _ => "vl",
    }
}

/// Format a compact type descriptor: `Name[size,complexity,flags]` or `Name[size,complexity]`.
fn format_type_descriptor(
    name: &str,
    count: usize,
    prop_count: usize,
    caps: &TypeCapabilities,
) -> String {
    let size = size_tier(count);
    let complexity = property_complexity(prop_count);
    let flags = caps.flags_csv();
    if flags.is_empty() {
        format!("{}[{},{}]", xml_escape(name), size, complexity)
    } else {
        format!("{}[{},{},{}]", xml_escape(name), size, complexity, flags)
    }
}

/// Bubble capabilities from supporting types up to their parent core types.
fn bubble_capabilities(
    caps: &mut HashMap<String, TypeCapabilities>,
    parent_types: &HashMap<String, String>,
) {
    // Collect child caps first to avoid borrow issues
    let child_caps: Vec<(String, TypeCapabilities)> = parent_types
        .iter()
        .filter_map(|(child, parent)| {
            caps.get(child).map(|c| {
                (
                    parent.clone(),
                    TypeCapabilities {
                        has_timeseries: c.has_timeseries,
                        has_location: c.has_location,
                        has_geometry: c.has_geometry,
                        has_embeddings: c.has_embeddings,
                    },
                )
            })
        })
        .collect();
    for (parent, child_cap) in &child_caps {
        if let Some(parent_cap) = caps.get_mut(parent) {
            parent_cap.merge(child_cap);
        }
    }
}

/// Count supporting children per parent type.
fn children_counts(parent_types: &HashMap<String, String>) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for parent in parent_types.values() {
        *counts.entry(parent.clone()).or_insert(0) += 1;
    }
    counts
}

/// Detect capabilities for all node types in the graph.
fn compute_type_capabilities(graph: &DirGraph) -> HashMap<String, TypeCapabilities> {
    let mut caps: HashMap<String, TypeCapabilities> = HashMap::new();

    for node_type in graph.type_indices.keys() {
        let mut tc = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };

        // Timeseries
        tc.has_timeseries = graph.timeseries_configs.contains_key(node_type);

        // Spatial
        if let Some(sc) = graph.spatial_configs.get(node_type) {
            tc.has_location = sc.location.is_some() || !sc.points.is_empty();
            tc.has_geometry = sc.geometry.is_some() || !sc.shapes.is_empty();
        }

        // Also check metadata for point-type fields (no SpatialConfig set)
        if !tc.has_location {
            if let Some(meta) = graph.node_type_metadata.get(node_type) {
                tc.has_location = meta.values().any(|t| t.eq_ignore_ascii_case("point"));
            }
        }

        // Embeddings
        tc.has_embeddings = graph.embeddings.keys().any(|(nt, _)| nt == node_type);

        caps.insert(node_type.clone(), tc);
    }
    caps
}

// ── Core functions ──────────────────────────────────────────────────────────

/// Compute per-connection-type stats.
///
/// Fast path: uses connection_type_metadata + cached edge counts (O(types)).
/// Fallback: scans all edges (O(edges)) for pre-metadata graphs.
pub fn compute_connection_type_stats(graph: &DirGraph) -> Vec<ConnectionTypeStats> {
    // Fast path: use metadata (already has source/target types) + cached counts
    if !graph.connection_type_metadata.is_empty() {
        let counts = graph.get_edge_type_counts();
        let mut result: Vec<ConnectionTypeStats> = graph
            .connection_type_metadata
            .iter()
            .map(|(conn_type, info)| {
                let mut source_types: Vec<String> = info.source_types.iter().cloned().collect();
                source_types.sort();
                let mut target_types: Vec<String> = info.target_types.iter().cloned().collect();
                target_types.sort();
                let mut property_names: Vec<String> = info.property_types.keys().cloned().collect();
                property_names.sort();
                ConnectionTypeStats {
                    connection_type: conn_type.clone(),
                    count: counts.get(conn_type).copied().unwrap_or(0),
                    source_types,
                    target_types,
                    property_names,
                }
            })
            .collect();
        result.sort_by(|a, b| a.connection_type.cmp(&b.connection_type));
        return result;
    }

    // Fallback: scan all edges (pre-metadata graphs)
    struct Accum {
        count: usize,
        sources: HashSet<String>,
        targets: HashSet<String>,
        props: HashSet<String>,
    }
    let mut stats: HashMap<String, Accum> = HashMap::new();

    for edge_ref in graph.graph.edge_references() {
        let edge_data = edge_ref.weight();
        let entry = stats
            .entry(edge_data.connection_type_str(&graph.interner).to_string())
            .or_insert_with(|| Accum {
                count: 0,
                sources: HashSet::new(),
                targets: HashSet::new(),
                props: HashSet::new(),
            });
        entry.count += 1;

        if let Some(source_node) = graph.get_node(edge_ref.source()) {
            entry.sources.insert(source_node.node_type.clone());
        }
        if let Some(target_node) = graph.get_node(edge_ref.target()) {
            entry.targets.insert(target_node.node_type.clone());
        }
        for key in edge_data.property_keys(&graph.interner) {
            entry.props.insert(key.to_string());
        }
    }

    let mut result: Vec<ConnectionTypeStats> = stats
        .into_iter()
        .map(|(conn_type, acc)| {
            let mut source_types: Vec<String> = acc.sources.into_iter().collect();
            source_types.sort();
            let mut target_types: Vec<String> = acc.targets.into_iter().collect();
            target_types.sort();
            let mut property_names: Vec<String> = acc.props.into_iter().collect();
            property_names.sort();
            ConnectionTypeStats {
                connection_type: conn_type,
                count: acc.count,
                source_types,
                target_types,
                property_names,
            }
        })
        .collect();
    result.sort_by(|a, b| a.connection_type.cmp(&b.connection_type));
    result
}

/// Set of node types that participate in at least one edge (as source or target).
fn compute_connected_types(conn_stats: &[ConnectionTypeStats]) -> HashSet<String> {
    let mut connected = HashSet::new();
    for ct in conn_stats {
        for s in &ct.source_types {
            connected.insert(s.clone());
        }
        for t in &ct.target_types {
            connected.insert(t.clone());
        }
    }
    connected
}

/// Set of unordered (TypeA, TypeB) pairs directly connected by at least one edge type.
fn compute_connected_type_pairs(conn_stats: &[ConnectionTypeStats]) -> HashSet<(String, String)> {
    let mut pairs = HashSet::new();
    for ct in conn_stats {
        for s in &ct.source_types {
            for t in &ct.target_types {
                // Store both orderings so lookup is direction-independent
                pairs.insert((s.clone(), t.clone()));
                pairs.insert((t.clone(), s.clone()));
            }
        }
    }
    pairs
}

/// A candidate join between two disconnected types based on property value overlap.
struct JoinCandidate {
    left_type: String,
    left_prop: String,
    left_unique: usize,
    right_type: String,
    right_prop: String,
    right_unique: usize,
    overlap: usize,
}

/// Check whether two property type strings are compatible for join candidate comparison.
/// Metadata types use Rust names: "String", "Int64", "Float64", "UniqueId", etc.
fn types_compatible(left: &str, right: &str) -> bool {
    let is_str = |t: &str| {
        t.eq_ignore_ascii_case("string")
            || t.eq_ignore_ascii_case("uniqueid")
            || t.eq_ignore_ascii_case("str")
    };
    let is_num = |t: &str| {
        t.eq_ignore_ascii_case("int64")
            || t.eq_ignore_ascii_case("float64")
            || t.eq_ignore_ascii_case("int")
            || t.eq_ignore_ascii_case("float")
    };
    (is_str(left) && is_str(right)) || (is_num(left) && is_num(right))
}

/// Sample up to `max` unique non-null values from a type's property.
fn sample_unique_values(
    graph: &DirGraph,
    node_type: &str,
    property: &str,
    max: usize,
) -> HashSet<String> {
    let mut unique = HashSet::new();
    if let Some(indices) = graph.type_indices.get(node_type) {
        for &idx in indices {
            if unique.len() >= max {
                break;
            }
            if let Some(node) = graph.get_node(idx) {
                if let Some(val) = node.get_property(property) {
                    if !is_null_value(&val) {
                        let s = match &*val {
                            Value::String(s) => s.clone(),
                            Value::Int64(n) => n.to_string(),
                            Value::Float64(f) => f.to_string(),
                            Value::UniqueId(id) => id.to_string(),
                            _ => format!("{:?}", val),
                        };
                        unique.insert(s);
                    }
                }
            }
        }
    }
    unique
}

/// Find join candidates between disconnected core type pairs.
fn compute_join_candidates(
    graph: &DirGraph,
    connected_pairs: &HashSet<(String, String)>,
    max_candidates: usize,
    max_sample: usize,
) -> Vec<JoinCandidate> {
    // Collect core types (exclude supporting types)
    let mut core_types: Vec<&String> = graph
        .type_indices
        .keys()
        .filter(|nt| !graph.parent_types.contains_key(*nt))
        .collect();
    core_types.sort();

    let mut candidates: Vec<JoinCandidate> = Vec::new();

    // Check all unordered pairs of disconnected core types
    for i in 0..core_types.len() {
        if candidates.len() >= max_candidates * 3 {
            break; // Early exit: we have enough raw candidates
        }
        for j in (i + 1)..core_types.len() {
            let left = core_types[i];
            let right = core_types[j];

            // Skip already-connected pairs
            if connected_pairs.contains(&(left.clone(), right.clone())) {
                continue;
            }

            let left_meta = match graph.node_type_metadata.get(left) {
                Some(m) => m,
                None => continue,
            };
            let right_meta = match graph.node_type_metadata.get(right) {
                Some(m) => m,
                None => continue,
            };

            // Find shared property names with compatible types
            for (prop, left_type) in left_meta {
                if let Some(right_type) = right_meta.get(prop) {
                    if types_compatible(left_type, right_type) {
                        let left_vals = sample_unique_values(graph, left, prop, max_sample);
                        if left_vals.is_empty() {
                            continue;
                        }
                        let right_vals = sample_unique_values(graph, right, prop, max_sample);
                        if right_vals.is_empty() {
                            continue;
                        }
                        let overlap = left_vals.intersection(&right_vals).count();
                        if overlap > 0 {
                            candidates.push(JoinCandidate {
                                left_type: left.clone(),
                                left_prop: prop.clone(),
                                left_unique: left_vals.len(),
                                right_type: right.clone(),
                                right_prop: prop.clone(),
                                right_unique: right_vals.len(),
                                overlap,
                            });
                        }
                    }
                }
            }
        }
    }

    // Sort by overlap descending, truncate
    candidates.sort_by(|a, b| b.overlap.cmp(&a.overlap));
    candidates.truncate(max_candidates);
    candidates
}

/// Full schema overview: node types, connection types, indexes, totals.
pub fn compute_schema(graph: &DirGraph) -> SchemaOverview {
    // Node types from type_indices
    let mut node_types: Vec<(String, NodeTypeOverview)> = graph
        .type_indices
        .iter()
        .map(|(nt, indices)| {
            let properties = graph
                .node_type_metadata
                .get(nt)
                .cloned()
                .unwrap_or_default();
            (
                nt.clone(),
                NodeTypeOverview {
                    count: indices.len(),
                    properties,
                },
            )
        })
        .collect();
    node_types.sort_by(|a, b| a.0.cmp(&b.0));

    // Connection types via edge scan
    let connection_types = compute_connection_type_stats(graph);

    // Indexes
    let mut indexes: Vec<String> = Vec::new();
    for (node_type, property) in graph.property_indices.keys() {
        indexes.push(format!("{}.{}", node_type, property));
    }
    for (node_type, properties) in graph.composite_indices.keys() {
        indexes.push(format!("{}.({})", node_type, properties.join(", ")));
    }
    for (node_type, property) in graph.range_indices.keys() {
        indexes.push(format!("{}.{} [range]", node_type, property));
    }
    indexes.sort();

    SchemaOverview {
        node_types,
        connection_types,
        indexes,
        node_count: graph.graph.node_count(),
        edge_count: graph.graph.edge_count(),
    }
}

fn is_null_value(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Float64(f) => f.is_nan(),
        _ => false,
    }
}

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::String(_) => "str",
        Value::Int64(_) => "int",
        Value::Float64(_) => "float",
        Value::Boolean(_) => "bool",
        Value::DateTime(_) => "datetime",
        Value::UniqueId(_) => "uniqueid",
        Value::Point { .. } => "point",
        Value::Null => "unknown",
        Value::NodeRef(_) => "noderef",
        Value::EdgeRef { .. } => "edgeref",
    }
}

/// Compact display string for a Value (used in agent description `vals` attributes).
/// Truncates long strings to keep output concise.
fn value_display_compact(v: &Value) -> String {
    match v {
        Value::String(s) => {
            if s.chars().count() > 40 {
                let truncated: String = s.chars().take(37).collect();
                format!("{}...", truncated)
            } else {
                s.clone()
            }
        }
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => format!("{}", f),
        Value::Boolean(b) => {
            if *b {
                "true"
            } else {
                "false"
            }
        }
        .to_string(),
        Value::DateTime(d) => d.to_string(),
        Value::UniqueId(u) => u.to_string(),
        Value::Point { lat, lon } => format!("({},{})", lat, lon),
        Value::NodeRef(idx) => format!("node#{}", idx),
        Value::EdgeRef { edge_idx, .. } => format!("edge#{}", edge_idx),
        Value::Null => String::new(),
    }
}

/// Property stats for one node type.
/// `max_values`: include `values` list when unique count ≤ this threshold (0 = never).
/// `sample_size`: when Some(n), sample n evenly-spaced nodes instead of scanning all.
///   Sampled non_null counts are scaled to the full population.
pub fn compute_property_stats(
    graph: &DirGraph,
    node_type: &str,
    max_values: usize,
    sample_size: Option<usize>,
) -> Result<Vec<PropertyStatInfo>, String> {
    let node_indices = graph
        .type_indices
        .get(node_type)
        .ok_or_else(|| format!("Node type '{}' not found", node_type))?;

    let total_nodes = node_indices.len();

    // Per-property accumulator
    // Cap value_set at max_values+1 to avoid cloning every value when there are
    // thousands of unique values. We only need the set for small-cardinality props.
    // Cap at max_values+1: we need one extra to detect "too many unique values".
    // When capped, unique count is a lower bound (max_values+1) and values = None.
    let value_cap = if max_values > 0 {
        max_values + 1
    } else {
        usize::MAX // still need unique counts even when not reporting values
    };

    struct PropAccum {
        non_null: usize,
        value_set: HashSet<Value>,
        value_cap: usize,
        first_type: Option<&'static str>,
    }
    impl PropAccum {
        fn new(cap: usize) -> Self {
            Self {
                non_null: 0,
                value_set: HashSet::new(),
                value_cap: cap,
                first_type: None,
            }
        }
        fn add(&mut self, v: &Value) {
            if !is_null_value(v) {
                self.non_null += 1;
                if self.value_set.len() < self.value_cap {
                    self.value_set.insert(v.clone());
                }
                if self.first_type.is_none() {
                    self.first_type = Some(value_type_name(v));
                }
            }
        }
    }

    // Determine which nodes to scan (all or sampled)
    let (scan_indices, sample_count): (Vec<petgraph::graph::NodeIndex>, usize) = match sample_size {
        Some(n) if n > 0 && n < total_nodes => {
            let step = total_nodes / n;
            let sampled: Vec<_> = (0..n).map(|i| node_indices[i * step]).collect();
            let count = sampled.len();
            (sampled, count)
        }
        _ => {
            // No sampling — scan all nodes
            (node_indices.to_vec(), total_nodes)
        }
    };

    // Single pass: accumulate stats for all properties simultaneously
    let mut accum: HashMap<String, PropAccum> = HashMap::new();
    // Pre-insert built-in fields so they appear even when all null
    accum.insert("title".to_string(), PropAccum::new(value_cap));
    accum.insert("id".to_string(), PropAccum::new(value_cap));

    // When sampling, pre-populate property keys from TypeSchema (knows ALL keys)
    if sample_size.is_some() {
        if let Some(schema) = graph.type_schemas.get(node_type) {
            for slot_key in schema.iter() {
                if let Some(key_str) = graph.interner.try_resolve(slot_key.1) {
                    accum
                        .entry(key_str.to_string())
                        .or_insert_with(|| PropAccum::new(value_cap));
                }
            }
        }
    }

    for &idx in &scan_indices {
        if let Some(node) = graph.get_node(idx) {
            accum
                .entry("id".to_string())
                .or_insert_with(|| PropAccum::new(value_cap))
                .add(&node.id);
            accum
                .entry("title".to_string())
                .or_insert_with(|| PropAccum::new(value_cap))
                .add(&node.title);
            for (key, value) in node.property_iter(&graph.interner) {
                accum
                    .entry(key.to_string())
                    .or_insert_with(|| PropAccum::new(value_cap))
                    .add(value);
            }
        }
    }

    // When sampling, scale non_null counts to the full population
    let scale_factor = if sample_count < total_nodes && sample_count > 0 {
        total_nodes as f64 / sample_count as f64
    } else {
        1.0
    };

    // Build ordered property list: type, title, id, then remaining sorted
    let mut results = Vec::new();

    // "type" is always synthetic
    results.push(PropertyStatInfo {
        property_name: "type".to_string(),
        type_string: "str".to_string(),
        non_null: total_nodes,
        unique: 1,
        values: Some(vec![Value::String(node_type.to_string())]),
    });

    // Canonical order for remaining: title, id first, then sorted discovered
    let builtins = ["title", "id"];
    let mut discovered: Vec<String> = accum
        .keys()
        .filter(|k| !builtins.contains(&k.as_str()))
        .cloned()
        .collect();
    discovered.sort();

    let ordered: Vec<String> = builtins
        .iter()
        .map(|s| s.to_string())
        .chain(discovered)
        .collect();

    let metadata = graph.node_type_metadata.get(node_type);

    for prop_name in &ordered {
        if let Some(pa) = accum.remove(prop_name) {
            let type_string = metadata
                .and_then(|meta| meta.get(prop_name))
                .cloned()
                .unwrap_or_else(|| pa.first_type.unwrap_or("unknown").to_string());

            let unique = pa.value_set.len();
            let non_null = (pa.non_null as f64 * scale_factor).round() as usize;
            let values = if max_values > 0 && unique <= max_values && unique > 0 {
                let mut vals: Vec<Value> = pa.value_set.into_iter().collect();
                vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                Some(vals)
            } else {
                None
            };

            results.push(PropertyStatInfo {
                property_name: prop_name.clone(),
                type_string,
                non_null,
                unique,
                values,
            });
        }
    }

    Ok(results)
}

/// Connection topology for one node type: outgoing and incoming grouped by (conn_type, other_type).
pub fn compute_neighbors_schema(
    graph: &DirGraph,
    node_type: &str,
) -> Result<NeighborsSchema, String> {
    let node_indices = graph
        .type_indices
        .get(node_type)
        .ok_or_else(|| format!("Node type '{}' not found", node_type))?;

    let mut outgoing: HashMap<(String, String), usize> = HashMap::new();
    let mut incoming: HashMap<(String, String), usize> = HashMap::new();

    for &node_idx in node_indices {
        for edge_ref in graph.graph.edges_directed(node_idx, Direction::Outgoing) {
            if let Some(target_node) = graph.get_node(edge_ref.target()) {
                let key = (
                    edge_ref
                        .weight()
                        .connection_type_str(&graph.interner)
                        .to_string(),
                    target_node.node_type.clone(),
                );
                *outgoing.entry(key).or_insert(0) += 1;
            }
        }
        for edge_ref in graph.graph.edges_directed(node_idx, Direction::Incoming) {
            if let Some(source_node) = graph.get_node(edge_ref.source()) {
                let key = (
                    edge_ref
                        .weight()
                        .connection_type_str(&graph.interner)
                        .to_string(),
                    source_node.node_type.clone(),
                );
                *incoming.entry(key).or_insert(0) += 1;
            }
        }
    }

    let mut outgoing_list: Vec<NeighborConnection> = outgoing
        .into_iter()
        .map(|((ct, ot), count)| NeighborConnection {
            connection_type: ct,
            other_type: ot,
            count,
        })
        .collect();
    outgoing_list.sort_by(|a, b| {
        (&a.connection_type, &a.other_type).cmp(&(&b.connection_type, &b.other_type))
    });

    let mut incoming_list: Vec<NeighborConnection> = incoming
        .into_iter()
        .map(|((ct, ot), count)| NeighborConnection {
            connection_type: ct,
            other_type: ot,
            count,
        })
        .collect();
    incoming_list.sort_by(|a, b| {
        (&a.connection_type, &a.other_type).cmp(&(&b.connection_type, &b.other_type))
    });

    Ok(NeighborsSchema {
        outgoing: outgoing_list,
        incoming: incoming_list,
    })
}

/// Pre-compute neighbor schemas for ALL types in a single pass over edges.
/// Much faster than calling `compute_neighbors_schema` per type in `describe()`.
pub fn compute_all_neighbors_schemas(graph: &DirGraph) -> HashMap<String, NeighborsSchema> {
    // Key: (source_type, conn_type, target_type) → count
    let mut edge_counts: HashMap<(String, String, String), usize> = HashMap::new();

    for edge_ref in graph.graph.edge_references() {
        if let (Some(source), Some(target)) = (
            graph.get_node(edge_ref.source()),
            graph.get_node(edge_ref.target()),
        ) {
            let conn_type = edge_ref
                .weight()
                .connection_type_str(&graph.interner)
                .to_string();
            let key = (
                source.node_type.clone(),
                conn_type,
                target.node_type.clone(),
            );
            *edge_counts.entry(key).or_insert(0) += 1;
        }
    }

    let mut result: HashMap<String, NeighborsSchema> = HashMap::new();
    for ((src_type, conn_type, tgt_type), count) in &edge_counts {
        // Outgoing for src_type
        let schema = result
            .entry(src_type.clone())
            .or_insert_with(|| NeighborsSchema {
                outgoing: Vec::new(),
                incoming: Vec::new(),
            });
        schema.outgoing.push(NeighborConnection {
            connection_type: conn_type.clone(),
            other_type: tgt_type.clone(),
            count: *count,
        });

        // Incoming for tgt_type
        let schema = result
            .entry(tgt_type.clone())
            .or_insert_with(|| NeighborsSchema {
                outgoing: Vec::new(),
                incoming: Vec::new(),
            });
        schema.incoming.push(NeighborConnection {
            connection_type: conn_type.clone(),
            other_type: src_type.clone(),
            count: *count,
        });
    }

    // Sort each type's lists for deterministic output
    for schema in result.values_mut() {
        schema.outgoing.sort_by(|a, b| {
            (&a.connection_type, &a.other_type).cmp(&(&b.connection_type, &b.other_type))
        });
        schema.incoming.sort_by(|a, b| {
            (&a.connection_type, &a.other_type).cmp(&(&b.connection_type, &b.other_type))
        });
    }

    result
}

/// Return first N nodes of a type for quick inspection.
pub fn compute_sample<'a>(
    graph: &'a DirGraph,
    node_type: &str,
    n: usize,
) -> Result<Vec<&'a NodeData>, String> {
    let node_indices = graph
        .type_indices
        .get(node_type)
        .ok_or_else(|| format!("Node type '{}' not found", node_type))?;

    let mut result = Vec::with_capacity(n.min(node_indices.len()));
    for &idx in node_indices.iter().take(n) {
        if let Some(node) = graph.get_node(idx) {
            result.push(node);
        }
    }
    Ok(result)
}

// ── Describe: shared XML writers ────────────────────────────────────────────

/// Write the `<conventions>` element.
fn write_conventions(xml: &mut String, caps: &HashMap<String, TypeCapabilities>) {
    let mut specials: Vec<&str> = Vec::new();
    if caps.values().any(|c| c.has_location) {
        specials.push("location");
    }
    if caps.values().any(|c| c.has_geometry) {
        specials.push("geometry");
    }
    if caps.values().any(|c| c.has_timeseries) {
        specials.push("timeseries");
    }
    if caps.values().any(|c| c.has_embeddings) {
        specials.push("embeddings");
    }
    if specials.is_empty() {
        xml.push_str("  <conventions>All nodes have .id and .title</conventions>\n");
    } else {
        xml.push_str(&format!(
            "  <conventions>All nodes have .id and .title. Some have: {}</conventions>\n",
            specials.join(", ")
        ));
    }
}

/// Write a `<read-only>` element when the graph is in read-only mode.
fn write_read_only_notice(xml: &mut String, graph: &DirGraph) {
    if graph.read_only {
        xml.push_str(
            "  <read-only>Cypher mutations disabled: CREATE, SET, DELETE, REMOVE, MERGE</read-only>\n",
        );
    }
}

/// Write the `<connections>` element from global edge stats.
/// When `parent_types` is non-empty, filter out connections where ALL source types
/// are supporting children of the target type (the implicit OF_* pattern).
fn write_connection_map(xml: &mut String, graph: &DirGraph, conn_stats: &[ConnectionTypeStats]) {
    let has_tiers = !graph.parent_types.is_empty();

    let filtered: Vec<&ConnectionTypeStats> = conn_stats
        .iter()
        .filter(|ct| {
            if !has_tiers {
                return true;
            }
            // Filter out connections where ALL sources are children of the single target
            if ct.target_types.len() == 1 {
                let target = &ct.target_types[0];
                let all_sources_are_children = ct.source_types.iter().all(|src| {
                    graph
                        .parent_types
                        .get(src)
                        .is_some_and(|parent| parent == target)
                });
                if all_sources_are_children {
                    return false;
                }
            }
            true
        })
        .collect();

    if filtered.is_empty() {
        xml.push_str("  <connections/>\n");
    } else {
        xml.push_str("  <connections>\n");
        for ct in &filtered {
            // When tiers are active, filter supporting types from source/target lists
            let sources: Vec<&str> = if has_tiers {
                ct.source_types
                    .iter()
                    .filter(|s| !graph.parent_types.contains_key(*s))
                    .map(|s| s.as_str())
                    .collect()
            } else {
                ct.source_types.iter().map(|s| s.as_str()).collect()
            };
            let targets: Vec<&str> = if has_tiers {
                ct.target_types
                    .iter()
                    .filter(|s| !graph.parent_types.contains_key(*s))
                    .map(|s| s.as_str())
                    .collect()
            } else {
                ct.target_types.iter().map(|s| s.as_str()).collect()
            };
            if sources.is_empty() || targets.is_empty() {
                continue;
            }
            let temporal_attr =
                if let Some(configs) = graph.temporal_edge_configs.get(&ct.connection_type) {
                    configs
                        .iter()
                        .map(|tc| {
                            format!(
                                " temporal_from=\"{}\" temporal_to=\"{}\"",
                                xml_escape(&tc.valid_from),
                                xml_escape(&tc.valid_to)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("")
                } else {
                    String::new()
                };
            let props_attr = if ct.property_names.is_empty() {
                String::new()
            } else {
                format!(
                    " properties=\"{}\"",
                    xml_escape(&ct.property_names.join(","))
                )
            };
            xml.push_str(&format!(
                "    <conn type=\"{}\" count=\"{}\" from=\"{}\" to=\"{}\"{}{}/>\n",
                xml_escape(&ct.connection_type),
                ct.count,
                sources.join(","),
                targets.join(","),
                props_attr,
                temporal_attr,
            ));
        }
        xml.push_str("  </connections>\n");
    }
}

/// Compute property stats for edges of a given connection type.
fn compute_edge_property_stats(
    graph: &DirGraph,
    connection_type: &str,
    max_values: usize,
) -> Vec<PropertyStatInfo> {
    let mut all_props: HashSet<String> = HashSet::new();
    let mut total_edges: usize = 0;

    // First pass: discover property names
    for edge_ref in graph.graph.edge_references() {
        let ed = edge_ref.weight();
        if ed.connection_type_str(&graph.interner) == connection_type {
            total_edges += 1;
            for key in ed.property_keys(&graph.interner) {
                all_props.insert(key.to_string());
            }
        }
    }

    if all_props.is_empty() {
        return Vec::new();
    }

    let mut prop_names: Vec<String> = all_props.into_iter().collect();
    prop_names.sort();

    let mut results = Vec::new();
    for prop_name in &prop_names {
        let mut non_null: usize = 0;
        let mut value_set: HashSet<Value> = HashSet::new();
        let mut first_type: Option<&'static str> = None;

        for edge_ref in graph.graph.edge_references() {
            let ed = edge_ref.weight();
            if ed.connection_type_str(&graph.interner) != connection_type {
                continue;
            }
            if let Some(v) = ed.get_property(prop_name) {
                if !is_null_value(v) {
                    non_null += 1;
                    if first_type.is_none() {
                        first_type = Some(value_type_name(v));
                    }
                    value_set.insert(v.clone());
                }
            }
        }

        let unique = value_set.len();
        let values = if max_values > 0 && unique <= max_values && unique > 0 {
            let mut vals: Vec<Value> = value_set.into_iter().collect();
            vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            Some(vals)
        } else {
            None
        };

        results.push(PropertyStatInfo {
            property_name: prop_name.clone(),
            type_string: first_type.unwrap_or("unknown").to_string(),
            non_null,
            unique,
            values,
        });
    }
    let _ = total_edges; // used implicitly by context
    results
}

/// Connections overview: all connection types with count, endpoints, property names.
fn write_connections_overview(xml: &mut String, graph: &DirGraph) {
    let conn_stats = compute_connection_type_stats(graph);
    if conn_stats.is_empty() {
        xml.push_str("<connections/>\n");
        return;
    }
    xml.push_str("<connections>\n");
    for ct in &conn_stats {
        let props_attr = if ct.property_names.is_empty() {
            String::new()
        } else {
            format!(
                " properties=\"{}\"",
                xml_escape(&ct.property_names.join(","))
            )
        };

        xml.push_str(&format!(
            "  <conn type=\"{}\" count=\"{}\" from=\"{}\" to=\"{}\"{}/>\n",
            xml_escape(&ct.connection_type),
            ct.count,
            ct.source_types.join(","),
            ct.target_types.join(","),
            props_attr,
        ));
    }
    xml.push_str("</connections>\n");
}

/// Connections deep-dive: per-pair counts, property stats, sample edges.
fn write_connections_detail(
    xml: &mut String,
    graph: &DirGraph,
    topics: &[String],
) -> Result<(), String> {
    // Validate all connection types exist
    let conn_stats = compute_connection_type_stats(graph);
    let valid_types: HashSet<&str> = conn_stats
        .iter()
        .map(|c| c.connection_type.as_str())
        .collect();
    for topic in topics {
        if !valid_types.contains(topic.as_str()) {
            let mut available: Vec<&str> = valid_types.iter().copied().collect();
            available.sort();
            return Err(format!(
                "Connection type '{}' not found. Available: {}",
                topic,
                available.join(", ")
            ));
        }
    }

    xml.push_str("<connections>\n");
    for topic in topics {
        let ct = conn_stats
            .iter()
            .find(|c| c.connection_type == *topic)
            .unwrap();

        xml.push_str(&format!(
            "  <{} count=\"{}\">\n",
            xml_escape(&ct.connection_type),
            ct.count
        ));

        // Per source→target pair counts
        let mut pair_counts: HashMap<(String, String), usize> = HashMap::new();
        for edge_ref in graph.graph.edge_references() {
            let ed = edge_ref.weight();
            if ed.connection_type_str(&graph.interner) != *topic {
                continue;
            }
            let src_type = graph
                .get_node(edge_ref.source())
                .map(|n| n.node_type.clone())
                .unwrap_or_default();
            let tgt_type = graph
                .get_node(edge_ref.target())
                .map(|n| n.node_type.clone())
                .unwrap_or_default();
            *pair_counts.entry((src_type, tgt_type)).or_insert(0) += 1;
        }
        let mut pairs: Vec<((String, String), usize)> = pair_counts.into_iter().collect();
        pairs.sort_by(|a, b| b.1.cmp(&a.1));

        xml.push_str("    <endpoints>\n");
        for ((src, tgt), count) in &pairs {
            xml.push_str(&format!(
                "      <pair from=\"{}\" to=\"{}\" count=\"{}\"/>\n",
                xml_escape(src),
                xml_escape(tgt),
                count
            ));
        }
        xml.push_str("    </endpoints>\n");

        // Edge property stats
        let prop_stats = compute_edge_property_stats(graph, topic, 15);
        if !prop_stats.is_empty() {
            xml.push_str("    <properties>\n");
            for ps in &prop_stats {
                if ps.non_null == 0 {
                    continue;
                }
                let vals_attr = match &ps.values {
                    Some(vals) if !vals.is_empty() => {
                        let vals_str: Vec<String> =
                            vals.iter().map(value_display_compact).collect();
                        format!(" vals=\"{}\"", xml_escape(&vals_str.join("|")))
                    }
                    _ => String::new(),
                };
                xml.push_str(&format!(
                    "      <prop name=\"{}\" type=\"{}\" non_null=\"{}\" unique=\"{}\"{}/>\n",
                    xml_escape(&ps.property_name),
                    xml_escape(&ps.type_string),
                    ps.non_null,
                    ps.unique,
                    vals_attr,
                ));
            }
            xml.push_str("    </properties>\n");
        }

        // Sample edges (first 2)
        xml.push_str("    <samples>\n");
        let mut sample_count = 0;
        for edge_ref in graph.graph.edge_references() {
            let ed = edge_ref.weight();
            if ed.connection_type_str(&graph.interner) != *topic {
                continue;
            }
            if sample_count >= 2 {
                break;
            }
            let src_label = graph
                .get_node(edge_ref.source())
                .map(|n| format!("{}:{}", n.node_type, value_display_compact(&n.title)))
                .unwrap_or_default();
            let tgt_label = graph
                .get_node(edge_ref.target())
                .map(|n| format!("{}:{}", n.node_type, value_display_compact(&n.title)))
                .unwrap_or_default();

            let mut attrs = format!(
                "from=\"{}\" to=\"{}\"",
                xml_escape(&src_label),
                xml_escape(&tgt_label),
            );
            // Add up to 4 edge properties
            let mut prop_count = 0;
            let mut keys: Vec<&str> = ed.property_keys(&graph.interner).collect();
            keys.sort();
            for key in keys {
                if prop_count >= 4 {
                    break;
                }
                if let Some(v) = ed.get_property(key) {
                    if !is_null_value(v) {
                        attrs.push_str(&format!(
                            " {}=\"{}\"",
                            xml_escape(key),
                            xml_escape(&value_display_compact(v))
                        ));
                        prop_count += 1;
                    }
                }
            }
            xml.push_str(&format!("      <edge {}/>\n", attrs));
            sample_count += 1;
        }
        xml.push_str("    </samples>\n");

        xml.push_str(&format!("  </{}>\n", xml_escape(&ct.connection_type)));
    }
    xml.push_str("</connections>\n");
    Ok(())
}

/// Write the `<extensions>` element — only sections the graph actually uses.
fn write_extensions(xml: &mut String, graph: &DirGraph) {
    let has_timeseries = !graph.timeseries_configs.is_empty();
    let has_spatial = !graph.spatial_configs.is_empty()
        || graph
            .node_type_metadata
            .values()
            .any(|props| props.values().any(|t| t.eq_ignore_ascii_case("point")));
    let has_embeddings = !graph.embeddings.is_empty();

    xml.push_str("  <extensions>\n");

    if has_timeseries {
        xml.push_str("    <timeseries hint=\"ts_avg(n.ch, start?, end?), ts_sum, ts_min, ts_max, ts_count, ts_first, ts_last, ts_delta, ts_at, ts_series — date args: 'YYYY', 'YYYY-M', 'YYYY-M-D' or DateTime properties. NaN skipped.\"/>\n");
    }
    if has_spatial {
        xml.push_str("    <spatial hint=\"distance(a,b)→m, contains(a,b), intersects(a,b), centroid(n), area(n)→m², perimeter(n)→m\"/>\n");
    }
    if has_embeddings {
        xml.push_str(
            "    <semantic hint=\"text_score(n, 'col', 'query', metric) — similarity (metric: 'cosine'|'poincare'|'dot_product'|'euclidean'); embedding_norm(n, 'col') — L2 norm (hierarchy depth in Poincaré space)\"/>\n",
        );
    }
    xml.push_str("    <algorithms hint=\"CALL proc() YIELD node, col — score (pagerank/betweenness/degree/closeness), community (louvain/label_propagation), component (connected_components), cluster (cluster)\"/>\n");
    xml.push_str("    <cypher hint=\"Full Cypher with extensions: ||, =~, coalesce(), CALL cluster/pagerank/louvain/..., distance(), contains(). describe(cypher=True) for reference, describe(cypher=['topic']) for detailed docs.\"/>\n");
    xml.push_str("    <fluent_api hint=\"Method-chaining API: select/where/traverse/collect. describe(fluent=True) for reference, describe(fluent=['topic']) for detailed docs.\"/>\n");
    if graph.graph.edge_count() > 0 {
        xml.push_str("    <connections hint=\"describe(connections=True) for all connection types, describe(connections=['TYPE']) for deep-dive with properties and samples.\"/>\n");
    }
    xml.push_str("    <temporal hint=\"valid_at(entity, date, 'from', 'to'), valid_during(entity, start, end, 'from', 'to') — temporal filtering on nodes/edges. NULL = open-ended.\"/>\n");
    xml.push_str("    <bug_report hint=\"bug_report(query, result, expected, description) — file a Cypher bug report to reported_bugs.md.\"/>\n");
    xml.push_str("  </extensions>\n");
}

/// Write `<exploration_hints>` — disconnected types and join candidates.
/// Skipped for graphs with < 2 types or 0 edges (all disconnected = not useful).
fn write_exploration_hints(xml: &mut String, graph: &DirGraph, conn_stats: &[ConnectionTypeStats]) {
    let type_count = graph.type_indices.len();
    let edge_count = graph.graph.edge_count();

    // Guard: not useful for trivial graphs or when there are no edges at all
    if type_count < 2 || edge_count == 0 {
        return;
    }

    let connected_types = compute_connected_types(conn_stats);
    let connected_pairs = compute_connected_type_pairs(conn_stats);

    // Find disconnected types (core types with zero connections)
    let mut disconnected: Vec<(&String, usize)> = graph
        .type_indices
        .iter()
        .filter(|(nt, _)| !graph.parent_types.contains_key(*nt) && !connected_types.contains(*nt))
        .map(|(nt, indices)| (nt, indices.len()))
        .collect();
    disconnected.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    disconnected.truncate(10);

    // Compute join candidates
    let join_candidates = compute_join_candidates(graph, &connected_pairs, 5, 100);

    // Nothing to report
    if disconnected.is_empty() && join_candidates.is_empty() {
        return;
    }

    xml.push_str("  <exploration_hints>\n");

    if !disconnected.is_empty() {
        xml.push_str("    <disconnected>\n");
        for (nt, count) in &disconnected {
            xml.push_str(&format!(
                "      <type name=\"{}\" nodes=\"{}\" hint=\"No connections to other types\"/>\n",
                xml_escape(nt),
                count
            ));
        }
        xml.push_str("    </disconnected>\n");
    }

    if !join_candidates.is_empty() {
        xml.push_str("    <join_candidates>\n");
        for c in &join_candidates {
            xml.push_str(&format!(
                "      <candidate left=\"{}.{}\" left_unique=\"{}\" right=\"{}.{}\" right_unique=\"{}\" overlap=\"{}\" hint=\"Possible name-based link\"/>\n",
                xml_escape(&c.left_type),
                xml_escape(&c.left_prop),
                c.left_unique,
                xml_escape(&c.right_type),
                xml_escape(&c.right_prop),
                c.right_unique,
                c.overlap
            ));
        }
        xml.push_str("    </join_candidates>\n");
    }

    xml.push_str("  </exploration_hints>\n");
}

/// Tier 2: compact Cypher reference — all clauses, operators, functions, procedures.
/// No examples. Ends with hint to use tier 3.
fn write_cypher_overview(xml: &mut String) {
    xml.push_str("<cypher>\n");

    // Clauses
    xml.push_str("  <clauses>\n");
    xml.push_str("    <clause name=\"MATCH\">Pattern-match nodes and relationships. OPTIONAL MATCH for left-join semantics.</clause>\n");
    xml.push_str("    <clause name=\"WHERE\">Filter by predicate (comparison, null check, regex, string predicates).</clause>\n");
    xml.push_str("    <clause name=\"RETURN\">Project columns. Supports DISTINCT, aliases (AS), aggregations.</clause>\n");
    xml.push_str("    <clause name=\"WITH\">Intermediate projection, aggregation, and variable scoping.</clause>\n");
    xml.push_str("    <clause name=\"ORDER BY\">Sort results. Append DESC for descending. Combine with SKIP n, LIMIT n.</clause>\n");
    xml.push_str("    <clause name=\"UNWIND\">Expand a list into individual rows: UNWIND expr AS var.</clause>\n");
    xml.push_str(
        "    <clause name=\"UNION\">Combine result sets. UNION ALL keeps duplicates.</clause>\n",
    );
    xml.push_str("    <clause name=\"CASE\">Conditional expression: CASE WHEN cond THEN val ... ELSE val END.</clause>\n");
    xml.push_str(
        "    <clause name=\"CREATE\">Create nodes and relationships with properties.</clause>\n",
    );
    xml.push_str("    <clause name=\"SET\">Set or update node/relationship properties.</clause>\n");
    xml.push_str("    <clause name=\"DELETE\">Delete nodes/relationships. REMOVE to drop individual properties.</clause>\n");
    xml.push_str(
        "    <clause name=\"MERGE\">Match existing or create new (upsert pattern).</clause>\n",
    );
    xml.push_str("    <clause name=\"HAVING\">Post-aggregation filter on RETURN/WITH. Example: RETURN n.type, count(*) AS cnt HAVING cnt > 5</clause>\n");
    xml.push_str("    <clause name=\"EXPLAIN\">Prefix to show query plan as ResultView [step, operation, estimated_rows] without executing.</clause>\n");
    xml.push_str("    <clause name=\"PROFILE\">Prefix to execute and collect per-clause stats. Result has .profile with [clause, rows_in, rows_out, elapsed_us].</clause>\n");
    xml.push_str("  </clauses>\n");

    // Operators
    xml.push_str("  <operators>\n");
    xml.push_str("    <group name=\"math\">+ - * /</group>\n");
    xml.push_str("    <group name=\"string\">|| (concatenation)</group>\n");
    xml.push_str("    <group name=\"comparison\">= &lt;&gt; &lt; &gt; &lt;= &gt;= IN</group>\n");
    xml.push_str("    <group name=\"logical\">AND OR NOT XOR</group>\n");
    xml.push_str("    <group name=\"null\">IS NULL, IS NOT NULL</group>\n");
    xml.push_str("    <group name=\"regex\">=~ 'pattern'</group>\n");
    xml.push_str("    <group name=\"predicates\">CONTAINS, STARTS WITH, ENDS WITH</group>\n");
    xml.push_str("  </operators>\n");

    // Functions
    xml.push_str("  <functions>\n");
    xml.push_str("    <group name=\"math\">abs, ceil, floor, round(x [,decimals]), sqrt, sign, log, log10, exp, pow(x,y), pi, rand, toInteger, toFloat</group>\n");
    xml.push_str("    <group name=\"string\">toString, toUpper, toLower, trim, lTrim, rTrim, replace, substring, left, right, split, reverse</group>\n");
    xml.push_str(
        "    <group name=\"aggregate\">count, sum, avg, min, max, collect, stDev</group>\n",
    );
    xml.push_str(
        "    <group name=\"graph\">size, length, id, labels, type, coalesce, range, keys</group>\n",
    );
    xml.push_str("    <group name=\"spatial\">distance(a,b)→m, contains(a,b), intersects(a,b), centroid(n), area(n)→m², perimeter(n)→m</group>\n");
    xml.push_str("    <group name=\"temporal\">date(str)/datetime(str), date_diff(d1,d2), date ± N (days), date - date → int, d.year/d.month/d.day, valid_at(...), valid_during(...)</group>\n");
    xml.push_str("    <group name=\"window\">row_number() OVER (...), rank() OVER (...), dense_rank() OVER (...). OVER (PARTITION BY expr ORDER BY expr [DESC])</group>\n");
    xml.push_str("  </functions>\n");

    // Procedures
    xml.push_str("  <procedures>\n");
    xml.push_str("    <proc name=\"pagerank\" yields=\"node, score\">PageRank centrality for all nodes.</proc>\n");
    xml.push_str("    <proc name=\"betweenness\" yields=\"node, score\">Betweenness centrality for all nodes.</proc>\n");
    xml.push_str("    <proc name=\"degree\" yields=\"node, score\">Degree centrality for all nodes.</proc>\n");
    xml.push_str("    <proc name=\"closeness\" yields=\"node, score\">Closeness centrality for all nodes.</proc>\n");
    xml.push_str("    <proc name=\"louvain\" yields=\"node, community\">Community detection (Louvain algorithm).</proc>\n");
    xml.push_str("    <proc name=\"label_propagation\" yields=\"node, community\">Community detection (label propagation).</proc>\n");
    xml.push_str("    <proc name=\"connected_components\" yields=\"node, component\">Weakly connected components.</proc>\n");
    xml.push_str("    <proc name=\"cluster\" yields=\"node, cluster\">DBSCAN/K-means clustering on spatial or property data.</proc>\n");
    xml.push_str("  </procedures>\n");

    // Patterns
    xml.push_str("  <patterns>(n:Label), (n {prop: val}), (a)-[:TYPE]-&gt;(b), (a)-[:T*1..3]-&gt;(b), [x IN list WHERE pred | expr], n {.p1, .p2}</patterns>\n");

    xml.push_str("  <limitations>\n");
    xml.push_str("    <item feature=\"FOREACH\" workaround=\"UNWIND list AS x CREATE/SET ... (equivalent result)\"/>\n");
    xml.push_str("    <item feature=\"CALL {} subqueries\" workaround=\"Use WITH chaining or multiple cypher() calls\"/>\n");
    xml.push_str("    <item feature=\"LOAD CSV\" workaround=\"Use Python pandas/csv, then CREATE nodes from dicts\"/>\n");
    xml.push_str("    <item feature=\"CREATE INDEX\" note=\"Type indices are automatic; no manual index management needed\"/>\n");
    xml.push_str("    <item feature=\"Multi-label nodes\" note=\"Supported: CREATE (n:Primary:Extra), SET n:Label, REMOVE n:Label. Primary label is immutable; use SET n.type = 'NewType' to change it. labels(n) returns JSON array of all labels.\"/>\n");
    xml.push_str("    <item feature=\"Variable-length weighted paths\" note=\"Unweighted variable-length paths (*1..3) are supported\"/>\n");
    xml.push_str("  </limitations>\n");
    xml.push_str("  <hint>Use describe(cypher=['MATCH','cluster','spatial',...]) for detailed docs with examples.</hint>\n");
    xml.push_str("</cypher>\n");
}

// ── Cypher tier 3: topic detail functions ──────────────────────────────────

const CYPHER_TOPIC_LIST: &str = "MATCH, WHERE, RETURN, WITH, HAVING, ORDER BY, UNWIND, UNION, \
    CASE, CREATE, SET, DELETE, MERGE, EXPLAIN, PROFILE, operators, functions, patterns, spatial, \
    temporal, pagerank, betweenness, degree, closeness, louvain, \
    label_propagation, connected_components, cluster";

/// Tier 3: detailed Cypher docs for specific topics with params and examples.
fn write_cypher_topics(xml: &mut String, topics: &[String]) -> Result<(), String> {
    // Empty list → tier 2 overview
    if topics.is_empty() {
        write_cypher_overview(xml);
        return Ok(());
    }

    xml.push_str("<cypher>\n");
    for topic in topics {
        let key = topic.to_uppercase();
        match key.as_str() {
            "MATCH" => write_topic_match(xml),
            "WHERE" => write_topic_where(xml),
            "RETURN" => write_topic_return(xml),
            "WITH" => write_topic_with(xml),
            "HAVING" => write_topic_having(xml),
            "ORDER BY" | "ORDERBY" | "ORDER_BY" => write_topic_order_by(xml),
            "UNWIND" => write_topic_unwind(xml),
            "UNION" => write_topic_union(xml),
            "CASE" => write_topic_case(xml),
            "CREATE" => write_topic_create(xml),
            "SET" => write_topic_set(xml),
            "DELETE" | "REMOVE" => write_topic_delete(xml),
            "MERGE" => write_topic_merge(xml),
            "OPERATORS" => write_topic_operators(xml),
            "FUNCTIONS" => write_topic_functions(xml),
            "PATTERNS" => write_topic_patterns(xml),
            "PAGERANK" => write_topic_pagerank(xml),
            "BETWEENNESS" => write_topic_betweenness(xml),
            "DEGREE" => write_topic_degree(xml),
            "CLOSENESS" => write_topic_closeness(xml),
            "LOUVAIN" => write_topic_louvain(xml),
            "LABEL_PROPAGATION" | "LABELPROPAGATION" => write_topic_label_propagation(xml),
            "CONNECTED_COMPONENTS" | "CONNECTEDCOMPONENTS" => {
                write_topic_connected_components(xml);
            }
            "CLUSTER" => write_topic_cluster(xml),
            "SPATIAL" => write_topic_spatial(xml),
            "TEMPORAL" => write_topic_temporal(xml),
            "EXPLAIN" => write_topic_explain(xml),
            "PROFILE" => write_topic_profile(xml),
            _ => {
                return Err(format!(
                    "Unknown Cypher topic '{}'. Available: {}",
                    topic, CYPHER_TOPIC_LIST
                ));
            }
        }
    }
    xml.push_str("</cypher>\n");
    Ok(())
}

fn write_topic_match(xml: &mut String) {
    xml.push_str("  <MATCH>\n");
    xml.push_str("    <desc>Pattern-match nodes and relationships. OPTIONAL MATCH returns nulls for non-matching patterns (left join).</desc>\n");
    xml.push_str("    <syntax>MATCH (n:Label {prop: val})-[r:TYPE]-&gt;(m)</syntax>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"all nodes of type\">MATCH (n:Field) RETURN n.name</ex>\n");
    xml.push_str("      <ex desc=\"with relationship\">MATCH (a:Person)-[:KNOWS]-&gt;(b) RETURN a.name, b.name</ex>\n");
    xml.push_str("      <ex desc=\"variable-length path\">MATCH (a)-[:KNOWS*1..3]-&gt;(b) RETURN a, b</ex>\n");
    xml.push_str("      <ex desc=\"inline property filter\">MATCH (n:Field {status: 'active'}) RETURN n</ex>\n");
    xml.push_str("      <ex desc=\"optional match\">MATCH (a:Field) OPTIONAL MATCH (a)-[:HAS]-&gt;(b:Well) RETURN a.name, b.name</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("    <pitfall name=\"cartesian product from multiple OPTIONAL MATCH\">\n");
    xml.push_str(
        "      Multiple OPTIONAL MATCH clauses create a cross-product of all matched paths.\n",
    );
    xml.push_str(
        "      If a node connects to 10 prospects × 5 plays × 3 licences = 150 rows per node.\n",
    );
    xml.push_str("      Fix: break with WITH to collapse dimensions before expanding the next.\n");
    xml.push_str("      <bad>MATCH (w:Well) OPTIONAL MATCH (w)-[:A]-&gt;(a) OPTIONAL MATCH (w)-[:B]-&gt;(b) OPTIONAL MATCH (w)-[:C]-&gt;(c) RETURN w, collect(a), collect(b), collect(c)</bad>\n");
    xml.push_str("      <good>MATCH (w:Well) OPTIONAL MATCH (w)-[:A]-&gt;(a) WITH w, collect(DISTINCT a.title) AS as_ OPTIONAL MATCH (w)-[:B]-&gt;(b) WITH w, as_, collect(DISTINCT b.title) AS bs OPTIONAL MATCH (w)-[:C]-&gt;(c) RETURN w.title, as_, bs, collect(DISTINCT c.title) AS cs</good>\n");
    xml.push_str("    </pitfall>\n");
    xml.push_str("  </MATCH>\n");
}

fn write_topic_where(xml: &mut String) {
    xml.push_str("  <WHERE>\n");
    xml.push_str("    <desc>Filter results by predicate. Supports comparison, null checks, regex, string predicates, boolean logic.</desc>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"comparison\">WHERE n.depth &gt; 3000</ex>\n");
    xml.push_str("      <ex desc=\"string contains\">WHERE n.name CONTAINS 'oil'</ex>\n");
    xml.push_str("      <ex desc=\"starts/ends with\">WHERE n.name STARTS WITH '35/'</ex>\n");
    xml.push_str("      <ex desc=\"regex\">WHERE n.name =~ '35/9-.*'</ex>\n");
    xml.push_str("      <ex desc=\"null check\">WHERE n.depth IS NOT NULL</ex>\n");
    xml.push_str("      <ex desc=\"IN list\">WHERE n.status IN ['active', 'planned']</ex>\n");
    xml.push_str("      <ex desc=\"boolean\">WHERE n.depth &gt; 1000 AND n.temp &lt; 100</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </WHERE>\n");
}

fn write_topic_return(xml: &mut String) {
    xml.push_str("  <RETURN>\n");
    xml.push_str("    <desc>Project columns to output. Supports DISTINCT, aliases (AS), expressions, aggregations.</desc>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic\">RETURN n.name, n.depth</ex>\n");
    xml.push_str("      <ex desc=\"alias\">RETURN n.name AS field_name</ex>\n");
    xml.push_str("      <ex desc=\"distinct\">RETURN DISTINCT n.status</ex>\n");
    xml.push_str(
        "      <ex desc=\"expression\">RETURN n.name || ' (' || n.status || ')' AS label</ex>\n",
    );
    xml.push_str("      <ex desc=\"aggregation\">RETURN n.status, count(*) AS n, collect(n.name) AS names</ex>\n");
    xml.push_str("      <ex desc=\"having\">RETURN n.type, count(*) AS cnt HAVING cnt > 5</ex>\n");
    xml.push_str("      <ex desc=\"window\">RETURN n.name, row_number() OVER (ORDER BY n.score DESC) AS rn</ex>\n");
    xml.push_str("      <ex desc=\"window-partition\">RETURN n.name, rank() OVER (PARTITION BY n.dept ORDER BY n.score DESC) AS r</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </RETURN>\n");
}

fn write_topic_with(xml: &mut String) {
    xml.push_str("  <WITH>\n");
    xml.push_str("    <desc>Intermediate projection and aggregation. Creates a new scope — only variables listed in WITH are available in subsequent clauses.</desc>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"filter after aggregation\">MATCH (n:Field) WITH n.area AS area, count(*) AS c WHERE c &gt; 5 RETURN area, c</ex>\n");
    xml.push_str("      <ex desc=\"pipe between matches\">MATCH (a:Field) WITH a MATCH (a)-[:HAS]-&gt;(b) RETURN a.name, b.name</ex>\n");
    xml.push_str("      <ex desc=\"limit intermediate\">MATCH (n:Field) WITH n ORDER BY n.name LIMIT 10 RETURN n.name</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </WITH>\n");
}

fn write_topic_having(xml: &mut String) {
    xml.push_str("  <HAVING>\n");
    xml.push_str("    <desc>Post-aggregation filter. Applies after grouping/aggregation in RETURN or WITH. Equivalent to WHERE but for aggregated results.</desc>\n");
    xml.push_str("    <syntax>RETURN group_expr, agg_func() AS alias HAVING predicate</syntax>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"filter by count\">MATCH (n:Person) RETURN n.city, count(*) AS pop HAVING pop > 1000</ex>\n");
    xml.push_str("      <ex desc=\"with WITH\">MATCH (n) WITH n.type AS t, count(*) AS c HAVING c >= 5 RETURN t, c</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </HAVING>\n");
}

fn write_topic_order_by(xml: &mut String) {
    xml.push_str("  <ORDER_BY>\n");
    xml.push_str("    <desc>Sort results. Default ascending; append DESC for descending. Combine with SKIP and LIMIT for pagination.</desc>\n");
    xml.push_str("    <syntax>ORDER BY expr [DESC] [SKIP n] [LIMIT n]</syntax>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"ascending\">ORDER BY n.name</ex>\n");
    xml.push_str("      <ex desc=\"descending\">ORDER BY n.depth DESC</ex>\n");
    xml.push_str("      <ex desc=\"pagination\">ORDER BY n.name SKIP 20 LIMIT 10</ex>\n");
    xml.push_str("      <ex desc=\"multi-key\">ORDER BY n.status, n.name DESC</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </ORDER_BY>\n");
}

fn write_topic_unwind(xml: &mut String) {
    xml.push_str("  <UNWIND>\n");
    xml.push_str("    <desc>Expand a list expression into individual rows. Each element becomes a new row bound to the alias.</desc>\n");
    xml.push_str("    <syntax>UNWIND expression AS variable</syntax>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"literal list\">UNWIND ['A','B','C'] AS x MATCH (n {code: x}) RETURN n</ex>\n");
    xml.push_str("      <ex desc=\"collected list\">MATCH (n:Field) WITH collect(n.name) AS names UNWIND names AS name RETURN name</ex>\n");
    xml.push_str("      <ex desc=\"range\">UNWIND range(1, 10) AS i RETURN i</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </UNWIND>\n");
}

fn write_topic_union(xml: &mut String) {
    xml.push_str("  <UNION>\n");
    xml.push_str("    <desc>Combine result sets from two queries. UNION removes duplicates; UNION ALL keeps all rows. Column names must match.</desc>\n");
    xml.push_str("    <syntax>query1 UNION [ALL] query2</syntax>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic union\">MATCH (a:Field) RETURN a.name AS name UNION MATCH (b:Discovery) RETURN b.name AS name</ex>\n");
    xml.push_str("      <ex desc=\"union all\">MATCH (a:Field) RETURN a.name AS name UNION ALL MATCH (b:Field) RETURN b.name AS name</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </UNION>\n");
}

fn write_topic_case(xml: &mut String) {
    xml.push_str("  <CASE>\n");
    xml.push_str("    <desc>Conditional expression. Two forms: simple (CASE expr WHEN val THEN ...) and generic (CASE WHEN cond THEN ...).</desc>\n");
    xml.push_str("    <syntax>CASE WHEN condition THEN value [WHEN ... THEN ...] [ELSE default] END</syntax>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"generic\">RETURN CASE WHEN n.depth &gt; 3000 THEN 'deep' WHEN n.depth &gt; 1000 THEN 'medium' ELSE 'shallow' END AS category</ex>\n");
    xml.push_str("      <ex desc=\"simple\">RETURN CASE n.status WHEN 'PRODUCING' THEN 'active' WHEN 'SHUT DOWN' THEN 'closed' ELSE 'other' END</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </CASE>\n");
}

fn write_topic_create(xml: &mut String) {
    xml.push_str("  <CREATE>\n");
    xml.push_str("    <desc>Create new nodes and relationships with properties.</desc>\n");
    xml.push_str("    <examples>\n");
    xml.push_str(
        "      <ex desc=\"node\">CREATE (:Field {name: 'Troll', status: 'PRODUCING'})</ex>\n",
    );
    xml.push_str("      <ex desc=\"relationship\">MATCH (a:Field {name: 'Troll'}), (b:Company {name: 'Equinor'}) CREATE (a)-[:OPERATED_BY]-&gt;(b)</ex>\n");
    xml.push_str("      <ex desc=\"with properties\">MATCH (a:Field), (b:Well) WHERE a.name = b.field CREATE (b)-[:BELONGS_TO {since: 2020}]-&gt;(a)</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </CREATE>\n");
}

fn write_topic_set(xml: &mut String) {
    xml.push_str("  <SET>\n");
    xml.push_str("    <desc>Set or update properties on existing nodes/relationships.</desc>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"set property\">MATCH (n:Field {name: 'Troll'}) SET n.status = 'SHUT DOWN'</ex>\n");
    xml.push_str("      <ex desc=\"set multiple\">MATCH (n:Field {name: 'Troll'}) SET n.status = 'SHUT DOWN', n.end_year = 2025</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </SET>\n");
}

fn write_topic_delete(xml: &mut String) {
    xml.push_str("  <DELETE>\n");
    xml.push_str("    <desc>Delete nodes or relationships. REMOVE drops individual properties from a node.</desc>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"delete node\">MATCH (n:Field {name: 'Test'}) DELETE n</ex>\n");
    xml.push_str(
        "      <ex desc=\"delete relationship\">MATCH (a)-[r:OLD_REL]-&gt;(b) DELETE r</ex>\n",
    );
    xml.push_str("      <ex desc=\"remove property\">MATCH (n:Field {name: 'Troll'}) REMOVE n.temp_flag</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </DELETE>\n");
}

fn write_topic_merge(xml: &mut String) {
    xml.push_str("  <MERGE>\n");
    xml.push_str("    <desc>Match existing node/relationship or create if it doesn't exist (upsert). ON CREATE SET and ON MATCH SET for conditional property updates.</desc>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic\">MERGE (n:Field {name: 'Troll'})</ex>\n");
    xml.push_str("      <ex desc=\"on create\">MERGE (n:Field {name: 'Troll'}) ON CREATE SET n.created = 2025</ex>\n");
    xml.push_str("      <ex desc=\"on match\">MERGE (n:Field {name: 'Troll'}) ON MATCH SET n.updated = 2025</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </MERGE>\n");
}

fn write_topic_operators(xml: &mut String) {
    xml.push_str("  <operators>\n");
    xml.push_str("    <desc>All supported operators with semantics.</desc>\n");
    xml.push_str("    <group name=\"math\" desc=\"Arithmetic\">+ (add), - (subtract), * (multiply), / (divide)</group>\n");
    xml.push_str("    <group name=\"string\" desc=\"String concatenation\">|| — null propagates: 'a' || null = null. Auto-converts numbers: 'v' || 42 = 'v42'.</group>\n");
    xml.push_str("    <group name=\"comparison\" desc=\"Comparison\">= (equal), &lt;&gt; (not equal), &lt;, &gt;, &lt;=, &gt;=, IN (list membership)</group>\n");
    xml.push_str("    <group name=\"logical\" desc=\"Boolean\">AND, OR, NOT, XOR</group>\n");
    xml.push_str("    <group name=\"null\" desc=\"Null checks\">IS NULL, IS NOT NULL</group>\n");
    xml.push_str("    <group name=\"regex\" desc=\"Regex match\">=~ 'pattern' — Java-style regex, case-sensitive by default. Use (?i) for case-insensitive.</group>\n");
    xml.push_str("    <group name=\"predicates\" desc=\"String predicates\">CONTAINS, STARTS WITH, ENDS WITH — case-sensitive substring checks.</group>\n");
    xml.push_str("    <examples>\n");
    xml.push_str(
        "      <ex desc=\"concat with number\">RETURN n.name || '-' || n.block AS label</ex>\n",
    );
    xml.push_str("      <ex desc=\"regex case-insensitive\">WHERE n.name =~ '(?i)troll.*'</ex>\n");
    xml.push_str("      <ex desc=\"IN list\">WHERE n.status IN ['PRODUCING', 'SHUT DOWN']</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </operators>\n");
}

fn write_topic_functions(xml: &mut String) {
    xml.push_str("  <functions>\n");
    xml.push_str("    <desc>All built-in functions grouped by category.</desc>\n");
    xml.push_str("    <group name=\"math\">abs(x), ceil(x)/ceiling(x), floor(x), round(x [,decimals]), sqrt(x), sign(x), log(x)/ln(x), log10(x), exp(x), pow(x,y), pi(), rand(), toInteger(x)/toInt(x), toFloat(x)</group>\n");
    xml.push_str("    <group name=\"string\">toString(x), toUpper(s), toLower(s), trim(s), lTrim(s), rTrim(s), replace(s,from,to), substring(s,start[,len]), left(s,n), right(s,n), split(s,delim), reverse(s), size(s)</group>\n");
    xml.push_str("    <group name=\"aggregate\">count(*)/count(expr), sum(expr), avg(expr), min(expr), max(expr), collect(expr), stDev(expr)/std(expr)</group>\n");
    xml.push_str("    <group name=\"graph\">size(list), length(path), id(node), labels(node), type(rel), coalesce(expr,...) — first non-null, range(start,end[,step]), keys(node)</group>\n");
    xml.push_str("    <examples>\n");
    xml.push_str(
        "      <ex desc=\"round precision\">RETURN round(n.depth / 1000.0, 1) AS depth_km</ex>\n",
    );
    xml.push_str("      <ex desc=\"coalesce\">RETURN coalesce(n.nickname, n.name) AS label</ex>\n");
    xml.push_str("      <ex desc=\"string\">RETURN toLower(n.name) AS lower_name</ex>\n");
    xml.push_str("      <ex desc=\"aggregate\">RETURN n.status, count(*) AS n, avg(n.depth) AS avg_depth</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("    <group name=\"temporal\">date(str)/datetime(str), date_diff(d1,d2), date ± N (add/sub days), date - date → days (int), d.year/d.month/d.day</group>\n");
    xml.push_str("    <group name=\"window\">row_number() OVER (...), rank() OVER (...), dense_rank() OVER (...). Syntax: func() OVER (PARTITION BY expr ORDER BY expr [DESC]). PARTITION BY optional.</group>\n");
    xml.push_str("    <group name=\"semantic\">text_score(n, 'col', 'query' [, metric]) — similarity score (metrics: 'cosine', 'poincare', 'dot_product', 'euclidean'); embedding_norm(n, 'col') — L2 norm of embedding vector (hierarchy depth in Poincaré space, 0=root, ~1=leaf)</group>\n");
    xml.push_str("  </functions>\n");
}

fn write_topic_patterns(xml: &mut String) {
    xml.push_str("  <patterns>\n");
    xml.push_str("    <desc>Pattern syntax for matching graph structures.</desc>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"labeled node\">(n:Field)</ex>\n");
    xml.push_str("      <ex desc=\"inline properties\">(n:Field {status: 'active'})</ex>\n");
    xml.push_str("      <ex desc=\"directed relationship\">(a)-[:BELONGS_TO]-&gt;(b)</ex>\n");
    xml.push_str(
        "      <ex desc=\"variable-length\">(a)-[:KNOWS*1..3]-&gt;(b) — path length 1 to 3</ex>\n",
    );
    xml.push_str("      <ex desc=\"any relationship\">(a)--&gt;(b) or (a)-[r]-&gt;(b)</ex>\n");
    xml.push_str("      <ex desc=\"list comprehension\">[x IN collect(n.name) WHERE x STARTS WITH '35']</ex>\n");
    xml.push_str("      <ex desc=\"map projection\">n {.name, .status} — returns {name: ..., status: ...}</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </patterns>\n");
}

// ── Procedure deep-dive functions ──────────────────────────────────────────

fn write_topic_pagerank(xml: &mut String) {
    xml.push_str("  <pagerank>\n");
    xml.push_str("    <desc>Compute PageRank centrality for all nodes. Higher score = more influential.</desc>\n");
    xml.push_str("    <syntax>CALL pagerank({params}) YIELD node, score</syntax>\n");
    xml.push_str("    <params>\n");
    xml.push_str("      <param name=\"damping_factor\" type=\"float\" default=\"0.85\">Probability of following a link vs random jump.</param>\n");
    xml.push_str("      <param name=\"max_iterations\" type=\"int\" default=\"100\">Convergence iteration limit.</param>\n");
    xml.push_str("      <param name=\"tolerance\" type=\"float\" default=\"1e-6\">Convergence threshold.</param>\n");
    xml.push_str("      <param name=\"connection_types\" type=\"string|list\">Filter to specific relationship types.</param>\n");
    xml.push_str("    </params>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic\">CALL pagerank() YIELD node, score RETURN node.name, score ORDER BY score DESC LIMIT 10</ex>\n");
    xml.push_str("      <ex desc=\"filtered\">CALL pagerank({connection_types: 'CITES'}) YIELD node, score RETURN node.name, score ORDER BY score DESC</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </pagerank>\n");
}

fn write_topic_betweenness(xml: &mut String) {
    xml.push_str("  <betweenness>\n");
    xml.push_str("    <desc>Compute betweenness centrality. High score = node lies on many shortest paths (bridge/broker).</desc>\n");
    xml.push_str("    <syntax>CALL betweenness({params}) YIELD node, score</syntax>\n");
    xml.push_str("    <params>\n");
    xml.push_str("      <param name=\"normalized\" type=\"bool\" default=\"true\">Normalize scores to 0..1 range.</param>\n");
    xml.push_str("      <param name=\"sample_size\" type=\"int\" optional=\"true\">Approximate by sampling N source nodes (faster for large graphs).</param>\n");
    xml.push_str("      <param name=\"connection_types\" type=\"string|list\">Filter to specific relationship types.</param>\n");
    xml.push_str("    </params>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic\">CALL betweenness() YIELD node, score RETURN node.name, score ORDER BY score DESC LIMIT 10</ex>\n");
    xml.push_str("      <ex desc=\"sampled\">CALL betweenness({sample_size: 100}) YIELD node, score RETURN node.name, round(score, 4) ORDER BY score DESC</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </betweenness>\n");
}

fn write_topic_degree(xml: &mut String) {
    xml.push_str("  <degree>\n");
    xml.push_str("    <desc>Compute degree centrality (number of connections per node, optionally normalized).</desc>\n");
    xml.push_str("    <syntax>CALL degree({params}) YIELD node, score</syntax>\n");
    xml.push_str("    <params>\n");
    xml.push_str("      <param name=\"normalized\" type=\"bool\" default=\"true\">Normalize by max possible degree.</param>\n");
    xml.push_str("      <param name=\"connection_types\" type=\"string|list\">Filter to specific relationship types.</param>\n");
    xml.push_str("    </params>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic\">CALL degree() YIELD node, score RETURN node.name, score ORDER BY score DESC LIMIT 10</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </degree>\n");
}

fn write_topic_closeness(xml: &mut String) {
    xml.push_str("  <closeness>\n");
    xml.push_str("    <desc>Compute closeness centrality (inverse of average shortest path distance). High = close to all others.</desc>\n");
    xml.push_str("    <syntax>CALL closeness({params}) YIELD node, score</syntax>\n");
    xml.push_str("    <params>\n");
    xml.push_str("      <param name=\"normalized\" type=\"bool\" default=\"true\">Normalize scores.</param>\n");
    xml.push_str("      <param name=\"sample_size\" type=\"int\" optional=\"true\">Approximate by sampling N source nodes (faster for large graphs).</param>\n");
    xml.push_str("      <param name=\"connection_types\" type=\"string|list\">Filter to specific relationship types.</param>\n");
    xml.push_str("    </params>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic\">CALL closeness() YIELD node, score RETURN node.name, score ORDER BY score DESC LIMIT 10</ex>\n");
    xml.push_str("      <ex desc=\"sampled\">CALL closeness({sample_size: 100}) YIELD node, score RETURN node.name, round(score, 4) ORDER BY score DESC</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </closeness>\n");
}

fn write_topic_louvain(xml: &mut String) {
    xml.push_str("  <louvain>\n");
    xml.push_str("    <desc>Community detection using the Louvain algorithm. Assigns each node a community ID.</desc>\n");
    xml.push_str("    <syntax>CALL louvain({params}) YIELD node, community</syntax>\n");
    xml.push_str("    <params>\n");
    xml.push_str("      <param name=\"resolution\" type=\"float\" default=\"1.0\">Higher = more/smaller communities, lower = fewer/larger.</param>\n");
    xml.push_str("      <param name=\"weight_property\" type=\"string\" optional=\"true\">Edge property to use as weight.</param>\n");
    xml.push_str("      <param name=\"connection_types\" type=\"string|list\">Filter to specific relationship types.</param>\n");
    xml.push_str("    </params>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic\">CALL louvain() YIELD node, community RETURN community, count(*) AS size, collect(node.name) AS members ORDER BY size DESC</ex>\n");
    xml.push_str("      <ex desc=\"high resolution\">CALL louvain({resolution: 2.0}) YIELD node, community RETURN community, count(*) AS size ORDER BY size DESC</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </louvain>\n");
}

fn write_topic_label_propagation(xml: &mut String) {
    xml.push_str("  <label_propagation>\n");
    xml.push_str("    <desc>Community detection using label propagation. Fast, non-deterministic. Each node adopts its neighbors' majority label.</desc>\n");
    xml.push_str("    <syntax>CALL label_propagation({params}) YIELD node, community</syntax>\n");
    xml.push_str("    <params>\n");
    xml.push_str("      <param name=\"max_iterations\" type=\"int\" default=\"100\">Iteration limit.</param>\n");
    xml.push_str("      <param name=\"connection_types\" type=\"string|list\">Filter to specific relationship types.</param>\n");
    xml.push_str("    </params>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic\">CALL label_propagation() YIELD node, community RETURN community, count(*) AS size ORDER BY size DESC</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </label_propagation>\n");
}

fn write_topic_connected_components(xml: &mut String) {
    xml.push_str("  <connected_components>\n");
    xml.push_str("    <desc>Find weakly connected components. Nodes in the same component can reach each other ignoring edge direction.</desc>\n");
    xml.push_str("    <syntax>CALL connected_components() YIELD node, component</syntax>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic\">CALL connected_components() YIELD node, component RETURN component, count(*) AS size ORDER BY size DESC</ex>\n");
    xml.push_str("      <ex desc=\"find isolated\">CALL connected_components() YIELD node, component WITH component, count(*) AS size WHERE size = 1 RETURN count(*) AS isolated_nodes</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </connected_components>\n");
}

fn write_topic_cluster(xml: &mut String) {
    xml.push_str("  <cluster>\n");
    xml.push_str("    <desc>Cluster nodes using DBSCAN or K-means. Reads nodes from preceding MATCH clause.</desc>\n");
    xml.push_str("    <syntax>MATCH (n:Type) CALL cluster({params}) YIELD node, cluster RETURN ...</syntax>\n");
    xml.push_str("    <modes>\n");
    xml.push_str("      <spatial>Omit 'properties' — auto-detects lat/lon from set_spatial() config. Uses haversine distance. eps is in meters. Geometry centroids used as fallback for WKT types.</spatial>\n");
    xml.push_str("      <property>Specify properties: ['col1','col2'] — euclidean distance on numeric values. Use normalize: true when feature scales differ.</property>\n");
    xml.push_str("    </modes>\n");
    xml.push_str("    <params>\n");
    xml.push_str("      <param name=\"method\" type=\"string\" default=\"dbscan\">'dbscan' or 'kmeans'.</param>\n");
    xml.push_str("      <param name=\"eps\" type=\"float\" default=\"0.5\">DBSCAN: max neighborhood distance. In meters for spatial mode.</param>\n");
    xml.push_str("      <param name=\"min_points\" type=\"int\" default=\"3\">DBSCAN: min neighbors to form a core point.</param>\n");
    xml.push_str(
        "      <param name=\"k\" type=\"int\" default=\"5\">K-means: number of clusters.</param>\n",
    );
    xml.push_str("      <param name=\"max_iterations\" type=\"int\" default=\"100\">K-means: iteration limit.</param>\n");
    xml.push_str("      <param name=\"normalize\" type=\"bool\" default=\"false\">Property mode: scale features to [0,1] before clustering.</param>\n");
    xml.push_str("      <param name=\"properties\" type=\"list\" optional=\"true\">Numeric property names for property mode. Omit for spatial mode.</param>\n");
    xml.push_str("    </params>\n");
    xml.push_str("    <yields>node (the matched node), cluster (int — cluster ID; -1 = noise for DBSCAN)</yields>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"spatial DBSCAN\">MATCH (f:Field) CALL cluster({method: 'dbscan', eps: 50000, min_points: 2}) YIELD node, cluster RETURN cluster, count(*) AS n, collect(node.name) AS fields ORDER BY n DESC</ex>\n");
    xml.push_str("      <ex desc=\"property K-means\">MATCH (w:Well) CALL cluster({properties: ['depth', 'temperature'], method: 'kmeans', k: 3, normalize: true}) YIELD node, cluster RETURN cluster, collect(node.name) AS wells</ex>\n");
    xml.push_str("      <ex desc=\"spatial K-means\">MATCH (s:Station) CALL cluster({method: 'kmeans', k: 4}) YIELD node, cluster RETURN cluster, count(*) AS n</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </cluster>\n");
}

fn write_topic_explain(xml: &mut String) {
    xml.push_str("  <EXPLAIN>\n");
    xml.push_str("    <desc>Show query plan without executing. Returns a ResultView with columns [step, operation, estimated_rows].</desc>\n");
    xml.push_str("    <syntax>EXPLAIN &lt;any Cypher query&gt;</syntax>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic plan\">EXPLAIN MATCH (n:Person) WHERE n.age &gt; 30 RETURN n.name</ex>\n");
    xml.push_str("      <ex desc=\"inspect fused optimization\">EXPLAIN MATCH (n:Person) RETURN count(n)</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("    <notes>Cardinality estimates use type_indices counts. Fused optimizations shown as single steps.</notes>\n");
    xml.push_str("  </EXPLAIN>\n");
}

fn write_topic_profile(xml: &mut String) {
    xml.push_str("  <PROFILE>\n");
    xml.push_str("    <desc>Execute query AND collect per-clause statistics. Returns normal results with a .profile property.</desc>\n");
    xml.push_str("    <syntax>PROFILE &lt;any Cypher query&gt;</syntax>\n");
    xml.push_str("    <profile_columns>clause (str), rows_in (int), rows_out (int), elapsed_us (int)</profile_columns>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"profile read query\">PROFILE MATCH (n:Person) WHERE n.age &gt; 30 RETURN n.name</ex>\n");
    xml.push_str("      <ex desc=\"profile mutation\">PROFILE CREATE (n:Temp {val: 1})</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("    <notes>Access stats via result.profile (list of dicts). None for non-profiled queries.</notes>\n");
    xml.push_str("  </PROFILE>\n");
}

fn write_topic_spatial(xml: &mut String) {
    xml.push_str("  <spatial>\n");
    xml.push_str("    <desc>Spatial functions for geographic queries. Requires set_spatial() config on the node type (location or geometry). All distance/area/perimeter results are in meters.</desc>\n");
    xml.push_str("    <setup>Python: g.set_spatial('Field', location=('lat', 'lon')) or g.set_spatial('Area', geometry='wkt')</setup>\n");
    xml.push_str("    <note>WKT uses (longitude latitude) order per OGC standard. point(lat, lon) uses latitude-first. These conventions differ — be careful when mixing them.</note>\n");
    xml.push_str("    <functions>\n");
    xml.push_str("      <fn name=\"distance(a, b)\">Geodesic distance in meters between two spatial nodes. Returns Null if either node has no location.</fn>\n");
    xml.push_str("      <fn name=\"contains(a, b)\">True if geometry a fully contains geometry b (or point b).</fn>\n");
    xml.push_str("      <fn name=\"intersects(a, b)\">True if geometries a and b overlap.</fn>\n");
    xml.push_str(
        "      <fn name=\"centroid(n)\">Returns {lat, lon} centroid of node's geometry.</fn>\n",
    );
    xml.push_str("      <fn name=\"area(n)\">Area of node's geometry in m².</fn>\n");
    xml.push_str("      <fn name=\"perimeter(n)\">Perimeter of node's geometry in meters.</fn>\n");
    xml.push_str("    </functions>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"distance between nodes\">MATCH (a:Field {name: 'Troll'}), (b:Field {name: 'Ekofisk'}) RETURN distance(a, b) / 1000.0 AS km</ex>\n");
    xml.push_str("      <ex desc=\"nearest neighbors\">MATCH (a:Field {name: 'Troll'}), (b:Field) WHERE a &lt;&gt; b RETURN b.name, round(distance(a, b) / 1000.0, 1) AS km ORDER BY km LIMIT 5</ex>\n");
    xml.push_str("      <ex desc=\"contains check\">MATCH (area:Block), (w:Well) WHERE contains(area, w) RETURN area.name, collect(w.name) AS wells</ex>\n");
    xml.push_str("      <ex desc=\"area calculation\">MATCH (b:Block) RETURN b.name, round(area(b) / 1e6, 1) AS km2</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </spatial>\n");
}

fn write_topic_temporal(xml: &mut String) {
    xml.push_str("  <temporal>\n");
    xml.push_str("    <desc>Temporal filtering functions for date-range validity checks on nodes and relationships. Works with any date/datetime string or DateTime properties. NULL fields are treated as open-ended boundaries.</desc>\n");
    xml.push_str("    <functions>\n");
    xml.push_str("      <fn name=\"date(str) / datetime(str)\">Parse date string to DateTime value. Supports 'YYYY-MM-DD' format.</fn>\n");
    xml.push_str("      <fn name=\"date_diff(d1, d2)\">Days between two dates (d1 - d2). Same as date subtraction.</fn>\n");
    xml.push_str("      <fn name=\"date + N / date - N\">Add/subtract N days from a date.</fn>\n");
    xml.push_str("      <fn name=\"date - date\">Days between two dates (returns integer).</fn>\n");
    xml.push_str("      <fn name=\"d.year / d.month / d.day\">Extract year, month, or day from a DateTime value.</fn>\n");
    xml.push_str("      <fn name=\"valid_at(entity, date, 'from_field', 'to_field')\">True if entity.from_field &lt;= date &lt;= entity.to_field. NULL from_field = valid since beginning. NULL to_field = still valid.</fn>\n");
    xml.push_str("      <fn name=\"valid_during(entity, start, end, 'from_field', 'to_field')\">True if entity's validity period overlaps [start, end]. Overlap: entity.from_field &lt;= end AND entity.to_field &gt;= start. NULL = open-ended.</fn>\n");
    xml.push_str("    </functions>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"node valid at date\">MATCH (e:Estimate) WHERE valid_at(e, '2020-06-15', 'date_from', 'date_to') RETURN e.title, e.value</ex>\n");
    xml.push_str("      <ex desc=\"edge valid at date\">MATCH (a)-[r:EMPLOYED_AT]->(b) WHERE valid_at(r, '2023-01-01', 'start_date', 'end_date') RETURN a.name, b.name</ex>\n");
    xml.push_str("      <ex desc=\"range overlap\">MATCH (p:Prospect) WHERE valid_during(p, '2021-01-01', '2022-12-31', 'date_from', 'date_to') RETURN p.title</ex>\n");
    xml.push_str("      <ex desc=\"with date()\">MATCH (e:Estimate) WHERE valid_at(e, date('2020-06-15'), 'date_from', 'date_to') RETURN e.title</ex>\n");
    xml.push_str("      <ex desc=\"open-ended\">MATCH (c:Contract) WHERE valid_at(c, '2025-01-01', 'start_date', 'end_date') RETURN c.title -- NULL end_date = still valid</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("    <null_semantics>\n");
    xml.push_str("      <rule>NULL from_field = valid since the beginning (always passes the from check)</rule>\n");
    xml.push_str("      <rule>NULL to_field = still valid / open-ended (always passes the to check)</rule>\n");
    xml.push_str("      <rule>Both NULL = always valid (returns true)</rule>\n");
    xml.push_str("    </null_semantics>\n");
    xml.push_str("  </temporal>\n");
}

/// Write full detail for a single node type: properties, connections,
/// timeseries/spatial/embedding config, and sample nodes.
fn write_type_detail(
    xml: &mut String,
    graph: &DirGraph,
    node_type: &str,
    caps: &TypeCapabilities,
    indent: &str,
    neighbors_cache: Option<&HashMap<String, NeighborsSchema>>,
) {
    let count = graph
        .type_indices
        .get(node_type)
        .map(|v| v.len())
        .unwrap_or(0);

    let mut alias_attrs = String::new();
    if let Some(id_alias) = graph.id_field_aliases.get(node_type) {
        alias_attrs.push_str(&format!(" id_alias=\"{}\"", xml_escape(id_alias)));
    }
    if let Some(title_alias) = graph.title_field_aliases.get(node_type) {
        alias_attrs.push_str(&format!(" title_alias=\"{}\"", xml_escape(title_alias)));
    }
    if let Some(tc) = graph.temporal_node_configs.get(node_type) {
        alias_attrs.push_str(&format!(
            " temporal_from=\"{}\" temporal_to=\"{}\"",
            xml_escape(&tc.valid_from),
            xml_escape(&tc.valid_to)
        ));
    }

    xml.push_str(&format!(
        "{}<type name=\"{}\" count=\"{}\"{}>\n",
        indent,
        xml_escape(node_type),
        count,
        alias_attrs
    ));

    // Properties (exclude builtins: type, title, id)
    if let Ok(stats) = compute_property_stats(graph, node_type, 15, Some(200)) {
        let filtered: Vec<&PropertyStatInfo> = stats
            .iter()
            .filter(|p| !matches!(p.property_name.as_str(), "type" | "title" | "id"))
            .filter(|p| p.non_null > 0)
            .collect();
        if !filtered.is_empty() {
            xml.push_str(&format!("{}  <properties>\n", indent));
            for prop in &filtered {
                let mut attrs = format!(
                    "name=\"{}\" type=\"{}\" unique=\"{}\"",
                    xml_escape(&prop.property_name),
                    xml_escape(&prop.type_string),
                    prop.unique
                );
                if let Some(ref vals) = prop.values {
                    if !vals.is_empty() {
                        let val_strs: Vec<String> =
                            vals.iter().map(value_display_compact).collect();
                        attrs.push_str(&format!(" vals=\"{}\"", xml_escape(&val_strs.join("|"))));
                    }
                }
                xml.push_str(&format!("{}    <prop {}/>\n", indent, attrs));
            }
            xml.push_str(&format!("{}  </properties>\n", indent));
        }
    }

    // Connections (neighbors) — use pre-computed cache if available
    let computed;
    let neighbors_opt = if let Some(cache) = neighbors_cache {
        cache.get(node_type)
    } else {
        computed = compute_neighbors_schema(graph, node_type).ok();
        computed.as_ref()
    };
    if let Some(neighbors) = neighbors_opt {
        if !neighbors.outgoing.is_empty() || !neighbors.incoming.is_empty() {
            xml.push_str(&format!("{}  <connections>\n", indent));
            for nc in &neighbors.outgoing {
                xml.push_str(&format!(
                    "{}    <out type=\"{}\" target=\"{}\" count=\"{}\"/>\n",
                    indent,
                    xml_escape(&nc.connection_type),
                    xml_escape(&nc.other_type),
                    nc.count
                ));
            }
            for nc in &neighbors.incoming {
                xml.push_str(&format!(
                    "{}    <in type=\"{}\" source=\"{}\" count=\"{}\"/>\n",
                    indent,
                    xml_escape(&nc.connection_type),
                    xml_escape(&nc.other_type),
                    nc.count
                ));
            }
            xml.push_str(&format!("{}  </connections>\n", indent));
        }
    }

    // Timeseries config
    if caps.has_timeseries {
        if let Some(config) = graph.timeseries_configs.get(node_type) {
            let mut attrs = format!("resolution=\"{}\"", xml_escape(&config.resolution));
            if !config.channels.is_empty() {
                attrs.push_str(&format!(
                    " channels=\"{}\"",
                    config
                        .channels
                        .iter()
                        .map(|c| xml_escape(c))
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
            if !config.units.is_empty() {
                let units_str: Vec<String> = config
                    .units
                    .iter()
                    .map(|(k, v)| format!("{}={}", xml_escape(k), xml_escape(v)))
                    .collect();
                attrs.push_str(&format!(" units=\"{}\"", units_str.join(",")));
            }
            xml.push_str(&format!("{}  <timeseries {}/>\n", indent, attrs));
        }
    }

    // Spatial config
    if caps.has_location || caps.has_geometry {
        if let Some(config) = graph.spatial_configs.get(node_type) {
            let mut attrs = String::new();
            if let Some((lat, lon)) = &config.location {
                attrs.push_str(&format!(
                    "location=\"{},{}\"",
                    xml_escape(lat),
                    xml_escape(lon)
                ));
            }
            if let Some(geom) = &config.geometry {
                if !attrs.is_empty() {
                    attrs.push(' ');
                }
                attrs.push_str(&format!("geometry=\"{}\"", xml_escape(geom)));
            }
            if !attrs.is_empty() {
                xml.push_str(&format!("{}  <spatial {}/>\n", indent, attrs));
            }
        }
    }

    // Embedding config
    if caps.has_embeddings {
        for ((nt, prop_name), store) in &graph.embeddings {
            if nt == node_type {
                let text_col = prop_name.strip_suffix("_emb").unwrap_or(prop_name.as_str());
                xml.push_str(&format!(
                    "{}  <embeddings text_col=\"{}\" dim=\"{}\" count=\"{}\"/>\n",
                    indent,
                    xml_escape(text_col),
                    store.dimension,
                    store.len()
                ));
            }
        }
    }

    // Supporting children (if this is a core type with children)
    {
        let children: Vec<&String> = graph
            .parent_types
            .iter()
            .filter(|(_, parent)| parent.as_str() == node_type)
            .map(|(child, _)| child)
            .collect();
        if !children.is_empty() {
            let empty_caps = TypeCapabilities {
                has_timeseries: false,
                has_location: false,
                has_geometry: false,
                has_embeddings: false,
            };
            // Compute caps for children (direct, not bubbled)
            let child_caps = compute_type_capabilities(graph);
            let mut child_strs: Vec<(usize, String)> = children
                .iter()
                .map(|child| {
                    let count = graph.type_indices.get(*child).map(|v| v.len()).unwrap_or(0);
                    let prop_count = graph
                        .node_type_metadata
                        .get(*child)
                        .map(|m| m.len())
                        .unwrap_or(0);
                    let tc = child_caps.get(*child).unwrap_or(&empty_caps);
                    (count, format_type_descriptor(child, count, prop_count, tc))
                })
                .collect();
            child_strs.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
            let strs: Vec<&str> = child_strs.iter().map(|(_, s)| s.as_str()).collect();
            xml.push_str(&format!(
                "{}  <supporting>{}</supporting>\n",
                indent,
                strs.join(", ")
            ));
        }
    }

    // Sample nodes (2 samples)
    if let Ok(samples) = compute_sample(graph, node_type, 2) {
        if !samples.is_empty() {
            xml.push_str(&format!("{}  <samples>\n", indent));
            for node in samples {
                let mut attrs = format!(
                    "id=\"{}\" title=\"{}\"",
                    xml_escape(&value_display_compact(&node.id)),
                    xml_escape(&value_display_compact(&node.title))
                );
                // Include up to 4 non-null custom properties
                let mut prop_count = 0;
                let mut sorted_props: Vec<(&str, &Value)> =
                    node.property_iter(&graph.interner).collect();
                sorted_props.sort_by_key(|(k, _)| *k);
                for (k, v) in sorted_props {
                    if !is_null_value(v) && prop_count < 4 {
                        attrs.push_str(&format!(
                            " {}=\"{}\"",
                            xml_escape(k),
                            xml_escape(&value_display_compact(v))
                        ));
                        prop_count += 1;
                    }
                }
                xml.push_str(&format!("{}    <node {}/>\n", indent, attrs));
            }
            xml.push_str(&format!("{}  </samples>\n", indent));
        }
    }

    xml.push_str(&format!("{}</type>\n", indent));
}

// ── Describe: builders ─────────────────────────────────────────────────────

/// Build inventory for complex graphs (>15 types): size bands with
/// complexity markers and capability flags.
fn build_inventory(graph: &DirGraph) -> String {
    let mut caps = compute_type_capabilities(graph);
    bubble_capabilities(&mut caps, &graph.parent_types);
    let child_counts = children_counts(&graph.parent_types);
    let has_tiers = !graph.parent_types.is_empty();
    let empty_caps = TypeCapabilities {
        has_timeseries: false,
        has_location: false,
        has_geometry: false,
        has_embeddings: false,
    };

    let mut xml = String::with_capacity(2048);

    xml.push_str(&format!(
        "<graph nodes=\"{}\" edges=\"{}\">\n",
        graph.graph.node_count(),
        graph.graph.edge_count()
    ));

    write_conventions(&mut xml, &caps);
    write_read_only_notice(&mut xml, graph);

    // Collect types: if tiers active, only core types; otherwise all types
    let mut entries: Vec<(String, usize, usize)> = graph
        .type_indices
        .iter()
        .filter(|(nt, _)| !has_tiers || !graph.parent_types.contains_key(*nt))
        .map(|(nt, indices)| {
            let prop_count = graph
                .node_type_metadata
                .get(nt)
                .map(|m| m.len())
                .unwrap_or(0);
            (nt.clone(), indices.len(), prop_count)
        })
        .collect();
    // Sort by count descending, then alphabetically
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let core_count = entries.len();
    let supporting_count = graph.parent_types.len();
    if has_tiers {
        xml.push_str(&format!(
            "  <types core=\"{}\" supporting=\"{}\">\n    ",
            core_count, supporting_count
        ));
    } else {
        xml.push_str(&format!("  <types count=\"{}\">\n    ", core_count));
    }

    let type_strs: Vec<String> = entries
        .iter()
        .map(|(nt, count, prop_count)| {
            let tc = caps.get(nt).unwrap_or(&empty_caps);
            let desc = format_type_descriptor(nt, *count, *prop_count, tc);
            let children = child_counts.get(nt).copied().unwrap_or(0);
            if children > 0 {
                format!("{} +{}", desc, children)
            } else {
                desc
            }
        })
        .collect();
    xml.push_str(&type_strs.join(", "));
    xml.push_str("\n  </types>\n");

    let conn_stats = compute_connection_type_stats(graph);
    write_connection_map(&mut xml, graph, &conn_stats);
    write_extensions(&mut xml, graph);
    write_exploration_hints(&mut xml, graph, &conn_stats);

    xml.push_str(
        "  <hint>Use describe(types=['TypeName']) for properties, samples. Use describe(connections=['CONN_TYPE']) for edge property stats and samples.</hint>\n",
    );
    xml.push_str("</graph>");
    xml
}

/// Build inventory with inline detail for simple graphs (≤15 types).
fn build_inventory_with_detail(graph: &DirGraph) -> String {
    let mut caps = compute_type_capabilities(graph);
    bubble_capabilities(&mut caps, &graph.parent_types);
    let mut xml = String::with_capacity(4096);

    xml.push_str(&format!(
        "<graph nodes=\"{}\" edges=\"{}\">\n",
        graph.graph.node_count(),
        graph.graph.edge_count()
    ));

    write_conventions(&mut xml, &caps);
    write_read_only_notice(&mut xml, graph);

    // Full detail for each type (core only if tiers active)
    let has_tiers = !graph.parent_types.is_empty();
    let mut type_names: Vec<&String> = graph
        .type_indices
        .keys()
        .filter(|nt| !has_tiers || !graph.parent_types.contains_key(*nt))
        .collect();
    type_names.sort();

    xml.push_str("  <types>\n");
    let empty_caps = TypeCapabilities {
        has_timeseries: false,
        has_location: false,
        has_geometry: false,
        has_embeddings: false,
    };
    // Pre-compute all neighbor schemas in a single edge pass
    let all_neighbors = compute_all_neighbors_schemas(graph);
    for nt in type_names {
        let tc = caps.get(nt).unwrap_or(&empty_caps);
        write_type_detail(&mut xml, graph, nt, tc, "    ", Some(&all_neighbors));
    }
    xml.push_str("  </types>\n");

    let conn_stats = compute_connection_type_stats(graph);
    write_connection_map(&mut xml, graph, &conn_stats);
    write_extensions(&mut xml, graph);
    write_exploration_hints(&mut xml, graph, &conn_stats);

    xml.push_str("</graph>");
    xml
}

/// Build focused detail for specific requested types.
fn build_focused_detail(graph: &DirGraph, types: &[String]) -> Result<String, String> {
    // Validate all types exist
    for t in types {
        if !graph.type_indices.contains_key(t) {
            return Err(format!("Node type '{}' not found. Available: {}", t, {
                let mut names: Vec<&String> = graph.type_indices.keys().collect();
                names.sort();
                names
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            }));
        }
    }

    let caps = compute_type_capabilities(graph);
    let empty_caps = TypeCapabilities {
        has_timeseries: false,
        has_location: false,
        has_geometry: false,
        has_embeddings: false,
    };
    let mut xml = String::with_capacity(2048);
    xml.push_str("<graph>\n");
    write_read_only_notice(&mut xml, graph);

    for t in types {
        let tc = caps.get(t).unwrap_or(&empty_caps);
        write_type_detail(&mut xml, graph, t, tc, "  ", None);
    }

    xml.push_str("</graph>");
    Ok(xml)
}

// ── Fluent API reference ──────────────────────────────────────────────────

const FLUENT_TOPIC_LIST: &str = "select, where, traverse, compare, spatial, temporal, \
    retrieval, statistics, algorithms, vectors, timeseries, mutation, \
    loading, export, indexes, set_operations, subgraph, schema, transactions";

/// Tier 2: compact fluent API reference grouped by functional area.
fn write_fluent_overview(xml: &mut String) {
    xml.push_str("<fluent_api>\n");
    xml.push_str("  <note>Selection model: most methods return a new KnowledgeGraph with updated selection. Data is materialised only on retrieval (collect, to_df, etc.).</note>\n");

    // Selection & filtering
    xml.push_str("  <group name=\"selection\">\n");
    xml.push_str("    <method sig=\"select(type, sort=None, limit=None)\">Select all nodes of a type. Returns lazy selection.</method>\n");
    xml.push_str("    <method sig=\"where({prop: value})\">Filter by property: exact, comparison (&gt;,&lt;,&gt;=,&lt;=), string (contains, starts_with, ends_with, regex), in, is_null, is_not_null, negated variants.</method>\n");
    xml.push_str(
        "    <method sig=\"where_any([{...}, {...}])\">OR logic across condition sets.</method>\n",
    );
    xml.push_str("    <method sig=\"where_connected(conn_type, direction='any')\">Keep nodes that have a specific connection.</method>\n");
    xml.push_str("    <method sig=\"where_orphans(include_orphans=True)\">Filter by connectivity: orphans only or connected only.</method>\n");
    xml.push_str("    <method sig=\"sort(prop, ascending=True)\">Sort selection. Multi-col: sort([('a', True), ('b', False)]).</method>\n");
    xml.push_str("    <method sig=\"limit(n)\">Limit to first n results.</method>\n");
    xml.push_str("    <method sig=\"offset(n)\">Skip first n results (for pagination).</method>\n");
    xml.push_str("    <method sig=\"expand(hops=1)\">BFS expansion — include all nodes within n hops.</method>\n");
    xml.push_str("  </group>\n");

    // Traversal
    xml.push_str("  <group name=\"traversal\">\n");
    xml.push_str("    <method sig=\"traverse(conn_type, direction=None, target_type=None, where=None, where_connection=None, sort=None, limit=None)\">Follow graph edges. Returns target nodes as new selection level.</method>\n");
    xml.push_str("    <method sig=\"compare(target_type, method, filter=None, sort=None, limit=None)\">Spatial, semantic, or clustering comparison against a target type.</method>\n");
    xml.push_str("    <method sig=\"add_properties({Type: [props]})\">Enrich leaf nodes with properties from ancestor levels (copy, rename, aggregate, spatial).</method>\n");
    xml.push_str("    <method sig=\"create_connections(conn_type)\">Materialise direct edges from traversal chain.</method>\n");
    xml.push_str("  </group>\n");

    // Spatial
    xml.push_str("  <group name=\"spatial\">\n");
    xml.push_str("    <method sig=\"set_spatial(type, lat_field, lon_field, geometry_field=None)\">Declare spatial fields for a node type.</method>\n");
    xml.push_str("    <method sig=\"near_point(lat, lon, max_distance_deg)\">Filter by distance in degrees (fast, approximate).</method>\n");
    xml.push_str("    <method sig=\"near_point_m(lat, lon, max_distance_m)\">Filter by geodesic distance in meters (WGS84).</method>\n");
    xml.push_str("    <method sig=\"within_bounds(min_lat, min_lon, max_lat, max_lon)\">Bounding-box filter.</method>\n");
    xml.push_str("    <method sig=\"contains_point(lat, lon)\">Point-in-polygon test (requires WKT geometry).</method>\n");
    xml.push_str("    <method sig=\"intersects_geometry(wkt)\">Geometry overlap test.</method>\n");
    xml.push_str("    <method sig=\"bounds()\">Geographic bounding box of selection.</method>\n");
    xml.push_str("    <method sig=\"centroid()\">Average lat/lon of selection.</method>\n");
    xml.push_str("  </group>\n");

    // Temporal
    xml.push_str("  <group name=\"temporal\">\n");
    xml.push_str("    <method sig=\"valid_at(date, from_col='valid_from', to_col='valid_to')\">Point-in-time filter: keep nodes valid at a specific date.</method>\n");
    xml.push_str("    <method sig=\"valid_during(start, end, from_col='valid_from', to_col='valid_to')\">Range overlap filter: keep nodes valid during a period.</method>\n");
    xml.push_str("  </group>\n");

    // Retrieval
    xml.push_str("  <group name=\"retrieval\">\n");
    xml.push_str("    <method sig=\"collect(limit=None)\">Materialise selected nodes as a flat ResultView.</method>\n");
    xml.push_str("    <method sig=\"collect_grouped(group_by, parent_info=False)\">Materialise nodes grouped by parent type as dict.</method>\n");
    xml.push_str("    <method sig=\"to_df()\">Export selection as pandas DataFrame.</method>\n");
    xml.push_str(
        "    <method sig=\"to_gdf()\">Export as GeoDataFrame (requires WKT geometry).</method>\n",
    );
    xml.push_str(
        "    <method sig=\"ids()\">Lightweight retrieval: id + type + title only.</method>\n",
    );
    xml.push_str("    <method sig=\"node(type, id)\">O(1) lookup by type + id. Returns dict or None.</method>\n");
    xml.push_str("    <method sig=\"count(group_by=None)\">Count nodes, optionally grouped by property.</method>\n");
    xml.push_str("    <method sig=\"len()\">O(1) count of selected nodes.</method>\n");
    xml.push_str("    <method sig=\"sample(n)\">Random sample as ResultView.</method>\n");
    xml.push_str("    <method sig=\"titles()\">Title-only retrieval.</method>\n");
    xml.push_str("    <method sig=\"get_properties(props)\">Specific properties as list of tuples.</method>\n");
    xml.push_str("  </group>\n");

    // Statistics
    xml.push_str("  <group name=\"statistics\">\n");
    xml.push_str("    <method sig=\"statistics(properties=None, group_by=None)\">Descriptive stats: count, mean, std, min, max, sum.</method>\n");
    xml.push_str("    <method sig=\"calculate(expression, store_as=None)\">Math expressions on properties. store_as saves result as new property.</method>\n");
    xml.push_str("    <method sig=\"unique_values(property, store_as=None)\">Distinct values for a property.</method>\n");
    xml.push_str(
        "    <method sig=\"degrees(connection_type=None)\">Node degree counts.</method>\n",
    );
    xml.push_str("  </group>\n");

    // Graph algorithms
    xml.push_str("  <group name=\"algorithms\">\n");
    xml.push_str("    <method sig=\"shortest_path(source_type, source_id, target_type, target_id, connection_type=None, directed=True)\">Full path with node details.</method>\n");
    xml.push_str("    <method sig=\"shortest_path_length(...)\">Hop count only.</method>\n");
    xml.push_str("    <method sig=\"all_paths(source_type, source_id, target_type, target_id, max_hops=5)\">Enumerate all paths.</method>\n");
    xml.push_str("    <method sig=\"pagerank(damping_factor=0.85, connection_type=None)\">PageRank centrality.</method>\n");
    xml.push_str("    <method sig=\"betweenness_centrality(connection_type=None)\">Betweenness centrality.</method>\n");
    xml.push_str("    <method sig=\"louvain_communities(resolution=1.0, connection_type=None)\">Community detection (Louvain).</method>\n");
    xml.push_str("    <method sig=\"connected_components(mode='weak', connection_type=None)\">Connected component analysis.</method>\n");
    xml.push_str("  </group>\n");

    // Vector search
    xml.push_str("  <group name=\"vectors\">\n");
    xml.push_str("    <method sig=\"set_embedder(model_name_or_callable)\">Register embedding model for text search.</method>\n");
    xml.push_str("    <method sig=\"embed_texts(type, column)\">Compute and store embeddings for a text column.</method>\n");
    xml.push_str("    <method sig=\"search_text(query, type, column=None, top_k=10, min_score=None)\">Semantic text search (auto-embeds query).</method>\n");
    xml.push_str("    <method sig=\"vector_search(vector, type, column=None, top_k=10, min_score=None)\">Search with pre-computed query vector.</method>\n");
    xml.push_str("  </group>\n");

    // Timeseries
    xml.push_str("  <group name=\"timeseries\">\n");
    xml.push_str("    <method sig=\"set_timeseries(type, resolution, channels, units=None)\">Declare timeseries schema for a node type.</method>\n");
    xml.push_str("    <method sig=\"add_timeseries(df, type, fk_field, time_key, channels)\">Bulk load timeseries data from DataFrame.</method>\n");
    xml.push_str("    <method sig=\"timeseries(type, id, channel=None)\">Retrieve timeseries for a node (all channels or specific).</method>\n");
    xml.push_str("  </group>\n");

    // Mutation
    xml.push_str("  <group name=\"mutation\">\n");
    xml.push_str("    <method sig=\"update({prop: value}, conflict_handling='update')\">Batch property update on selected nodes.</method>\n");
    xml.push_str("  </group>\n");

    // Data loading
    xml.push_str("  <group name=\"loading\">\n");
    xml.push_str("    <method sig=\"add_nodes(df, type, id_field, title_field, columns=None, column_types=None, timeseries=None)\">Load nodes from DataFrame.</method>\n");
    xml.push_str("    <method sig=\"add_connections(df, conn_type, source_type, source_id, target_type, target_id)\">Load edges from DataFrame.</method>\n");
    xml.push_str("    <method sig=\"kglite.from_blueprint(path, verbose=False)\">Build graph from JSON blueprint + CSVs.</method>\n");
    xml.push_str("  </group>\n");

    // Export & persistence
    xml.push_str("  <group name=\"export\">\n");
    xml.push_str("    <method sig=\"export(path, format='graphml')\">Export as GraphML, GEXF, JSON (D3), or CSV.</method>\n");
    xml.push_str("    <method sig=\"export_csv(directory)\">CSV tree + blueprint.json (round-trips with from_blueprint).</method>\n");
    xml.push_str("    <method sig=\"save(path)\">Binary .kgl v3 file (auto-columnar, supports larger-than-RAM loading).</method>\n");
    xml.push_str("    <method sig=\"kglite.load(path)\">Restore from .kgl file.</method>\n");
    xml.push_str("  </group>\n");

    // Columnar storage
    xml.push_str("  <group name=\"columnar\">\n");
    xml.push_str("    <method sig=\"enable_columnar()\">Convert properties to per-type columnar stores (lower memory).</method>\n");
    xml.push_str("    <method sig=\"disable_columnar()\">Convert back to compact per-node storage.</method>\n");
    xml.push_str(
        "    <method sig=\"is_columnar\">Property: True if columnar storage is active.</method>\n",
    );
    xml.push_str("  </group>\n");

    // Set operations
    xml.push_str("  <group name=\"set_operations\">\n");
    xml.push_str("    <method sig=\"union(other)\">Nodes in either selection.</method>\n");
    xml.push_str("    <method sig=\"intersection(other)\">Nodes in both selections.</method>\n");
    xml.push_str("    <method sig=\"difference(other)\">Nodes in first but not second.</method>\n");
    xml.push_str("  </group>\n");

    // Indexes
    xml.push_str("  <group name=\"indexes\">\n");
    xml.push_str("    <method sig=\"create_index(type, property)\">Equality index for fast lookup.</method>\n");
    xml.push_str("    <method sig=\"create_range_index(type, property)\">B-tree for range queries.</method>\n");
    xml.push_str("    <method sig=\"create_composite_index(type, [prop1, prop2])\">Multi-column index.</method>\n");
    xml.push_str("  </group>\n");

    // Transactions
    xml.push_str("  <group name=\"transactions\">\n");
    xml.push_str(
        "    <method sig=\"begin()\">Read-write transaction (context manager).</method>\n",
    );
    xml.push_str("    <method sig=\"begin_read()\">Read-only transaction, O(1) cost (context manager).</method>\n");
    xml.push_str("  </group>\n");

    xml.push_str("  <hint>Use describe(fluent=['traverse','where','spatial',...]) for detailed docs with examples.</hint>\n");
    xml.push_str("</fluent_api>\n");
}

/// Tier 3: detailed fluent API docs for specific topics with params and examples.
fn write_fluent_topics(xml: &mut String, topics: &[String]) -> Result<(), String> {
    if topics.is_empty() {
        write_fluent_overview(xml);
        return Ok(());
    }

    xml.push_str("<fluent_api>\n");
    for topic in topics {
        let key = topic.to_lowercase();
        match key.as_str() {
            "select" | "selection" | "where" | "filtering" => write_fluent_topic_selection(xml),
            "traverse" | "traversal" => write_fluent_topic_traversal(xml),
            "compare" | "comparison" => write_fluent_topic_compare(xml),
            "spatial" => write_fluent_topic_spatial(xml),
            "temporal" => write_fluent_topic_temporal(xml),
            "retrieval" | "collect" => write_fluent_topic_retrieval(xml),
            "statistics" | "calculate" => write_fluent_topic_statistics(xml),
            "algorithms" | "graph_algorithms" => write_fluent_topic_algorithms(xml),
            "vectors" | "embeddings" | "search" => write_fluent_topic_vectors(xml),
            "timeseries" => write_fluent_topic_timeseries(xml),
            "mutation" | "update" => write_fluent_topic_mutation(xml),
            "loading" | "data_loading" => write_fluent_topic_loading(xml),
            "export" | "persistence" => write_fluent_topic_export(xml),
            "indexes" => write_fluent_topic_indexes(xml),
            "set_operations" => write_fluent_topic_set_operations(xml),
            "subgraph" => write_fluent_topic_subgraph(xml),
            "schema" => write_fluent_topic_schema(xml),
            "transactions" => write_fluent_topic_transactions(xml),
            _ => {
                return Err(format!(
                    "Unknown fluent API topic '{}'. Available: {}",
                    topic, FLUENT_TOPIC_LIST
                ));
            }
        }
    }
    xml.push_str("</fluent_api>\n");
    Ok(())
}

// ── Fluent tier 3: topic detail functions ──────────────────────────────────

fn write_fluent_topic_selection(xml: &mut String) {
    xml.push_str("  <selection>\n");
    xml.push_str("    <desc>Select and filter nodes using method chaining. All filter methods return a new lazy selection.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"select(type, sort=None, limit=None)\">Start a selection on a node type.</m>\n");
    xml.push_str("      <m sig=\"where({prop: value})\">Exact match, comparison (&gt;, &lt;, &gt;=, &lt;=), string predicates (contains, starts_with, ends_with, regex), in-list, null checks, negated variants (not_in, not_contains).</m>\n");
    xml.push_str("      <m sig=\"where_any([{...}, {...}])\">OR logic: keep nodes matching any condition set.</m>\n");
    xml.push_str("      <m sig=\"where_connected(conn_type, direction='any')\">Keep only nodes that have a specific connection.</m>\n");
    xml.push_str(
        "      <m sig=\"where_orphans(include_orphans=True)\">Filter by connectivity.</m>\n",
    );
    xml.push_str("      <m sig=\"sort(prop, ascending=True)\">Sort by property. Multi-col: sort([('a', True), ('b', False)]).</m>\n");
    xml.push_str("      <m sig=\"limit(n) / offset(n)\">Pagination.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str(
        "      <ex desc=\"exact match\">graph.select('Person').where({'city': 'Oslo'})</ex>\n",
    );
    xml.push_str("      <ex desc=\"comparison\">graph.select('Product').where({'price': {'&gt;=': 100, '&lt;=': 500}})</ex>\n");
    xml.push_str("      <ex desc=\"string search\">graph.select('Person').where({'name': {'contains': 'ali'}})</ex>\n");
    xml.push_str("      <ex desc=\"IN list\">graph.select('Person').where({'city': {'in': ['Oslo', 'Bergen']}})</ex>\n");
    xml.push_str("      <ex desc=\"null check\">graph.select('Person').where({'email': {'is_not_null': True}})</ex>\n");
    xml.push_str(
        "      <ex desc=\"regex\">graph.select('Person').where({'name': {'regex': '^A.*'}})</ex>\n",
    );
    xml.push_str("      <ex desc=\"OR logic\">graph.select('Person').where_any([{'city': 'Oslo'}, {'age': {'&gt;': 60}}])</ex>\n");
    xml.push_str("      <ex desc=\"pagination\">graph.select('Person').sort('name').offset(20).limit(10)</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </selection>\n");
}

fn write_fluent_topic_traversal(xml: &mut String) {
    xml.push_str("  <traversal>\n");
    xml.push_str("    <desc>Follow graph edges to navigate the graph. traverse() adds target nodes as a new hierarchy level. For spatial/semantic/clustering operations, use compare() instead.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"traverse(conn_type, direction=None, target_type=None, where=None, where_connection=None, sort=None, limit=None)\">Follow edges. direction: 'outgoing', 'incoming', or None (both).</m>\n");
    xml.push_str("      <m sig=\"add_properties({Type: [props]})\">Enrich leaf nodes with properties from ancestor levels. Supports copy, rename, Agg helpers (count, sum, mean, min, max, std, collect), and Spatial helpers (distance, area, perimeter, centroid_lat, centroid_lon).</m>\n");
    xml.push_str("      <m sig=\"create_connections(conn_type)\">Materialise direct edges from a traversal chain.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic outgoing\">graph.select('Person').traverse('WORKS_AT').collect()</ex>\n");
    xml.push_str("      <ex desc=\"incoming with filter\">graph.select('Company').traverse('WORKS_AT', direction='incoming', where={'age': {'&gt;': 30}})</ex>\n");
    xml.push_str("      <ex desc=\"target type filter\">graph.select('Well').traverse('OF_FIELD', direction='incoming', target_type='ProductionProfile')</ex>\n");
    xml.push_str("      <ex desc=\"multi-hop chain\">graph.select('Person').traverse('WORKS_AT').traverse('LOCATED_IN').collect()</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </traversal>\n");
}

fn write_fluent_topic_compare(xml: &mut String) {
    xml.push_str("  <compare>\n");
    xml.push_str("    <desc>Compare selected nodes against a target type using spatial, semantic, or clustering methods. Results are added as a new hierarchy level.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"compare(target_type, 'contains')\">Spatial: keep targets whose geometry contains the source point.</m>\n");
    xml.push_str("      <m sig=\"compare(target_type, 'intersects')\">Spatial: keep targets whose geometry intersects the source.</m>\n");
    xml.push_str("      <m sig=\"compare(target_type, {'type': 'distance', 'max_m': N})\">Spatial: keep targets within N meters.</m>\n");
    xml.push_str("      <m sig=\"compare(target_type, {'type': 'text_score', 'property': 'col', 'metric': 'cosine'|'poincare'})\">Semantic: rank by embedding similarity (default cosine; use 'poincare' for hierarchical data).</m>\n");
    xml.push_str("      <m sig=\"compare(target_type, {'type': 'cluster', 'k': N})\">Cluster targets by features (K-means or DBSCAN).</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"spatial containment\">graph.select('Structure').compare('Well', 'contains').collect()</ex>\n");
    xml.push_str("      <ex desc=\"distance\">graph.select('Well').compare('Well', {'type': 'distance', 'max_m': 5000})</ex>\n");
    xml.push_str("      <ex desc=\"semantic\">graph.select('Doc').compare('Doc', {'type': 'text_score', 'property': 'summary', 'threshold': 0.7})</ex>\n");
    xml.push_str("      <ex desc=\"clustering\">graph.select('Well').compare('Well', {'type': 'cluster', 'k': 5, 'features': ['lat', 'lon']})</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </compare>\n");
}

fn write_fluent_topic_spatial(xml: &mut String) {
    xml.push_str("  <spatial>\n");
    xml.push_str("    <desc>Spatial filtering and aggregation. Requires set_spatial() or column_types during add_nodes().</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"set_spatial(type, lat_field, lon_field, geometry_field=None)\">Declare spatial fields for a node type.</m>\n");
    xml.push_str("      <m sig=\"near_point(lat, lon, max_distance_deg)\">Filter by distance in degrees (fast, approximate). ~111km per degree at equator.</m>\n");
    xml.push_str("      <m sig=\"near_point_m(lat, lon, max_distance_m)\">Geodesic distance filter in meters (WGS84, Vincenty).</m>\n");
    xml.push_str("      <m sig=\"within_bounds(min_lat, min_lon, max_lat, max_lon)\">Bounding-box filter.</m>\n");
    xml.push_str("      <m sig=\"contains_point(lat, lon)\">Point-in-polygon test (requires WKT geometry).</m>\n");
    xml.push_str("      <m sig=\"intersects_geometry(wkt)\">Geometry overlap test.</m>\n");
    xml.push_str("      <m sig=\"bounds()\">Bounding box of current selection: {min_lat, min_lon, max_lat, max_lon}.</m>\n");
    xml.push_str("      <m sig=\"centroid()\">Average lat/lon: {lat, lon}.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str(
        "      <ex desc=\"setup\">graph.set_spatial('City', 'latitude', 'longitude')</ex>\n",
    );
    xml.push_str("      <ex desc=\"near point (degrees)\">graph.select('City').near_point(59.91, 10.75, 0.5)</ex>\n");
    xml.push_str("      <ex desc=\"near point (meters)\">graph.select('City').near_point_m(59.91, 10.75, 50000)</ex>\n");
    xml.push_str("      <ex desc=\"bounding box\">graph.select('Field').within_bounds(55.0, 0.0, 65.0, 15.0)</ex>\n");
    xml.push_str("      <ex desc=\"point in polygon\">graph.select('Block').contains_point(60.5, 4.2)</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </spatial>\n");
}

fn write_fluent_topic_temporal(xml: &mut String) {
    xml.push_str("  <temporal>\n");
    xml.push_str("    <desc>Temporal validity filtering. Nodes must have valid_from / valid_to (or custom-named) date properties.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"valid_at(date, from_col='valid_from', to_col='valid_to')\">Keep nodes valid at a specific date. date can be 'YYYY-MM-DD' string or datetime.</m>\n");
    xml.push_str("      <m sig=\"valid_during(start, end, from_col='valid_from', to_col='valid_to')\">Keep nodes whose validity overlaps a date range.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str(
        "      <ex desc=\"point in time\">graph.select('Licence').valid_at('2020-06-15')</ex>\n",
    );
    xml.push_str("      <ex desc=\"range overlap\">graph.select('Licence').valid_during('2020-01-01', '2020-12-31')</ex>\n");
    xml.push_str("      <ex desc=\"custom columns\">graph.select('Contract').valid_at('2023-01-01', from_col='start_date', to_col='end_date')</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </temporal>\n");
}

fn write_fluent_topic_retrieval(xml: &mut String) {
    xml.push_str("  <retrieval>\n");
    xml.push_str("    <desc>Materialise selected nodes. Most selectors are lazy — these methods trigger data retrieval.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"collect(limit=None)\">Flat ResultView (iterable, indexable, .to_list(), .to_df()).</m>\n");
    xml.push_str("      <m sig=\"collect_grouped(group_by, parent_info=False)\">Nodes grouped by parent type as dict.</m>\n");
    xml.push_str("      <m sig=\"to_df()\">Pandas DataFrame with all properties as columns.</m>\n");
    xml.push_str("      <m sig=\"to_gdf()\">GeoDataFrame with geometry column (requires spatial config).</m>\n");
    xml.push_str("      <m sig=\"ids()\">Lightweight: id + type + title only.</m>\n");
    xml.push_str(
        "      <m sig=\"node(type, id)\">O(1) single-node lookup. Returns dict or None.</m>\n",
    );
    xml.push_str(
        "      <m sig=\"count(group_by=None)\">Count, optionally grouped by property.</m>\n",
    );
    xml.push_str("      <m sig=\"len()\">O(1) selection size.</m>\n");
    xml.push_str("      <m sig=\"sample(n)\">Random n nodes as ResultView.</m>\n");
    xml.push_str("      <m sig=\"titles()\">Title-only list.</m>\n");
    xml.push_str("      <m sig=\"get_properties(props)\">Specific properties as tuples.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"collect all\">results = graph.select('Person').where({'city': 'Oslo'}).collect()</ex>\n");
    xml.push_str("      <ex desc=\"to dataframe\">df = graph.select('Person').to_df()</ex>\n");
    xml.push_str("      <ex desc=\"single lookup\">node = graph.node('Person', 42)</ex>\n");
    xml.push_str(
        "      <ex desc=\"count by group\">graph.select('Person').count(group_by='city')</ex>\n",
    );
    xml.push_str("      <ex desc=\"random sample\">graph.select('Person').sample(5)</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </retrieval>\n");
}

fn write_fluent_topic_statistics(xml: &mut String) {
    xml.push_str("  <statistics>\n");
    xml.push_str("    <desc>Descriptive statistics, calculations, and aggregations on selected nodes.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"statistics(properties=None, group_by=None)\">Count, mean, std, min, max, sum for numeric properties.</m>\n");
    xml.push_str("      <m sig=\"calculate(expression, store_as=None)\">Math expression on properties. store_as persists result.</m>\n");
    xml.push_str("      <m sig=\"unique_values(property, store_as=None)\">Distinct values for a property.</m>\n");
    xml.push_str(
        "      <m sig=\"degrees(connection_type=None)\">In/out/total degree counts per node.</m>\n",
    );
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"basic stats\">graph.select('Product').statistics(['price', 'quantity'])</ex>\n");
    xml.push_str("      <ex desc=\"grouped stats\">graph.select('Product').statistics(['price'], group_by='category')</ex>\n");
    xml.push_str("      <ex desc=\"calculate\">graph.select('Product').calculate('price * quantity', store_as='revenue')</ex>\n");
    xml.push_str("      <ex desc=\"unique\">graph.select('Person').unique_values('city')</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </statistics>\n");
}

fn write_fluent_topic_algorithms(xml: &mut String) {
    xml.push_str("  <algorithms>\n");
    xml.push_str("    <desc>Graph algorithms: paths, centrality, community detection.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"shortest_path(src_type, src_id, tgt_type, tgt_id, connection_type=None, directed=True)\">Full path with node details.</m>\n");
    xml.push_str("      <m sig=\"shortest_path_length(src_type, src_id, tgt_type, tgt_id, ...)\">Hop count only (integer).</m>\n");
    xml.push_str("      <m sig=\"shortest_path_ids(src_type, src_id, tgt_type, tgt_id, ...)\">Path as list of (type, id) tuples.</m>\n");
    xml.push_str("      <m sig=\"all_paths(src_type, src_id, tgt_type, tgt_id, max_hops=5)\">All paths up to max_hops.</m>\n");
    xml.push_str("      <m sig=\"pagerank(damping_factor=0.85, connection_type=None)\">PageRank centrality → ResultView.</m>\n");
    xml.push_str("      <m sig=\"betweenness_centrality(connection_type=None)\">Betweenness centrality → ResultView.</m>\n");
    xml.push_str("      <m sig=\"degree_centrality(connection_type=None, normalized=True)\">Degree centrality → dict.</m>\n");
    xml.push_str("      <m sig=\"closeness_centrality(connection_type=None)\">Closeness centrality → ResultView.</m>\n");
    xml.push_str("      <m sig=\"louvain_communities(resolution=1.0, connection_type=None)\">Community detection → ResultView.</m>\n");
    xml.push_str("      <m sig=\"label_propagation(max_iterations=100)\">Label propagation communities → ResultView.</m>\n");
    xml.push_str("      <m sig=\"connected_components(mode='weak', connection_type=None)\">Component analysis → ResultView.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str(
        "      <ex desc=\"shortest path\">graph.shortest_path('Person', 1, 'Person', 42)</ex>\n",
    );
    xml.push_str("      <ex desc=\"path length\">graph.shortest_path_length('City', 'Oslo', 'City', 'Bergen', connection_type='ROAD')</ex>\n");
    xml.push_str("      <ex desc=\"pagerank\">graph.pagerank(connection_type='CITES')</ex>\n");
    xml.push_str("      <ex desc=\"communities\">graph.louvain_communities(resolution=1.5)</ex>\n");
    xml.push_str("      <ex desc=\"components\">graph.connected_components(mode='weak')</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </algorithms>\n");
}

fn write_fluent_topic_vectors(xml: &mut String) {
    xml.push_str("  <vectors>\n");
    xml.push_str("    <desc>Embedding storage and semantic search. Requires set_embedder() or pre-computed vectors.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"set_embedder(model_name_or_callable)\">Register embedding model (sentence-transformers name or callable).</m>\n");
    xml.push_str("      <m sig=\"embed_texts(type, column)\">Compute and store embeddings for a text column.</m>\n");
    xml.push_str("      <m sig=\"set_embeddings(type, column, embeddings_dict)\">Provide pre-computed embeddings {id: vector}.</m>\n");
    xml.push_str("      <m sig=\"search_text(query, type, column=None, top_k=10, min_score=None)\">Semantic search — auto-embeds query string.</m>\n");
    xml.push_str("      <m sig=\"vector_search(vector, type, column=None, top_k=10, min_score=None)\">Search with explicit query vector.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"setup\">graph.set_embedder('all-MiniLM-L6-v2')</ex>\n");
    xml.push_str("      <ex desc=\"embed\">graph.embed_texts('Paper', 'abstract')</ex>\n");
    xml.push_str("      <ex desc=\"text search\">graph.search_text('machine learning for graphs', 'Paper', top_k=5)</ex>\n");
    xml.push_str(
        "      <ex desc=\"min score\">graph.search_text('NLP', 'Paper', min_score=0.7)</ex>\n",
    );
    xml.push_str("    </examples>\n");
    xml.push_str("  </vectors>\n");
}

fn write_fluent_topic_timeseries(xml: &mut String) {
    xml.push_str("  <timeseries>\n");
    xml.push_str("    <desc>Time-indexed data per node. Declare schema, bulk-load from DataFrame, retrieve per node.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"set_timeseries(type, resolution, channels, units=None)\">Declare timeseries schema. resolution: 'day'|'month'|'year'.</m>\n");
    xml.push_str("      <m sig=\"add_timeseries(df, type, fk_field, time_key, channels)\">Bulk load from DataFrame with foreign key to nodes.</m>\n");
    xml.push_str("      <m sig=\"timeseries(type, id, channel=None)\">Retrieve all channels or a specific channel for one node.</m>\n");
    xml.push_str("      <m sig=\"timeseries_config(type)\">Query timeseries metadata (resolution, channels, units).</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"schema\">graph.set_timeseries('Field', resolution='month', channels=['oil', 'gas'], units={'oil': 'MSm3'})</ex>\n");
    xml.push_str("      <ex desc=\"bulk load\">graph.add_timeseries(prod_df, 'Field', fk_field='field_id', time_key='date', channels=['oil', 'gas'])</ex>\n");
    xml.push_str(
        "      <ex desc=\"retrieve\">ts = graph.timeseries('Field', 123, channel='oil')</ex>\n",
    );
    xml.push_str("      <ex desc=\"inline loading\">graph.add_nodes(df, 'Prod', 'id', 'name', timeseries={'time': 'date', 'channels': ['oil', 'gas']})</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </timeseries>\n");
}

fn write_fluent_topic_mutation(xml: &mut String) {
    xml.push_str("  <mutation>\n");
    xml.push_str("    <desc>Update properties on selected nodes.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"update({prop: value}, conflict_handling='update')\">Batch property update. conflict_handling: 'update'|'preserve'|'replace'.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"set property\">graph.select('Person').where({'city': 'Oslo'}).update({'country': 'Norway'})</ex>\n");
    xml.push_str("      <ex desc=\"preserve existing\">graph.select('Person').update({'status': 'active'}, conflict_handling='preserve')</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </mutation>\n");
}

fn write_fluent_topic_loading(xml: &mut String) {
    xml.push_str("  <loading>\n");
    xml.push_str(
        "    <desc>Load nodes and connections from DataFrames or blueprint files.</desc>\n",
    );
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"add_nodes(df, type, id_field, title_field, columns=None, column_types=None, conflict_handling='skip', timeseries=None)\">Load nodes. conflict_handling: 'update'|'replace'|'skip'|'preserve'|'sum'. column_types maps columns to spatial/temporal types.</m>\n");
    xml.push_str("      <m sig=\"add_connections(data, conn_type, source_type, source_id_field, target_type, target_id_field, columns=None, conflict_handling='update', query=None, extra_properties=None)\">Load edges from DataFrame (data=df) or Cypher query (data=None, query='MATCH...RETURN...'). conflict_handling: 'update'|'replace'|'skip'|'preserve'|'sum'. extra_properties stamps static props onto query-mode edges.</m>\n");
    xml.push_str("      <m sig=\"add_nodes_bulk(specs)\">Bulk load multiple node types: [{'node_type': ..., 'data': df, ...}].</m>\n");
    xml.push_str(
        "      <m sig=\"add_connections_bulk(specs)\">Bulk load multiple connection types.</m>\n",
    );
    xml.push_str("      <m sig=\"kglite.from_blueprint(path, verbose=False)\">Build graph from JSON blueprint + CSVs.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str(
        "      <ex desc=\"basic nodes\">graph.add_nodes(df, 'Person', 'id', 'name')</ex>\n",
    );
    xml.push_str("      <ex desc=\"with spatial\">graph.add_nodes(df, 'City', 'id', 'name', column_types={'lat': 'location.lat', 'lon': 'location.lon'})</ex>\n");
    xml.push_str("      <ex desc=\"edges\">graph.add_connections(df, 'WORKS_AT', 'Person', 'person_id', 'Company', 'company_id')</ex>\n");
    xml.push_str("      <ex desc=\"edges from query\">graph.add_connections(None, 'ENCLOSES', 'Play', 'play_id', 'Area', 'area_id', query='MATCH (p:Play), (a:Area) WHERE contains(p, a) RETURN DISTINCT p.id AS play_id, a.id AS area_id')</ex>\n");
    xml.push_str("      <ex desc=\"blueprint\">graph = kglite.from_blueprint('blueprint.json', verbose=True)</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </loading>\n");
}

fn write_fluent_topic_export(xml: &mut String) {
    xml.push_str("  <export>\n");
    xml.push_str("    <desc>Export graph data and persist to disk.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"export(path, format='graphml')\">Export as 'graphml', 'gexf', 'json' (D3), or 'csv'.</m>\n");
    xml.push_str(
        "      <m sig=\"export_string(format='graphml')\">Export to string (no file).</m>\n",
    );
    xml.push_str("      <m sig=\"export_csv(directory)\">CSV directory tree + blueprint.json (round-trips with from_blueprint).</m>\n");
    xml.push_str("      <m sig=\"save(path)\">Binary .kgl v3 file (auto-columnar, supports larger-than-RAM loading).</m>\n");
    xml.push_str("      <m sig=\"kglite.load(path)\">Restore from .kgl file.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str(
        "      <ex desc=\"graphml\">graph.export('graph.graphml', format='graphml')</ex>\n",
    );
    xml.push_str("      <ex desc=\"csv roundtrip\">graph.export_csv('output/'); g2 = kglite.from_blueprint('output/blueprint.json')</ex>\n");
    xml.push_str(
        "      <ex desc=\"binary\">graph.save('graph.kgl'); g2 = kglite.load('graph.kgl')</ex>\n",
    );
    xml.push_str("    </examples>\n");
    xml.push_str("  </export>\n");
}

fn write_fluent_topic_indexes(xml: &mut String) {
    xml.push_str("  <indexes>\n");
    xml.push_str("    <desc>Create property indexes for faster lookups. Type indices are automatic.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"create_index(type, property)\">Equality index: fast exact-match lookup.</m>\n");
    xml.push_str("      <m sig=\"create_range_index(type, property)\">B-tree index: fast range queries (&gt;, &lt;, &gt;=, &lt;=).</m>\n");
    xml.push_str("      <m sig=\"create_composite_index(type, [prop1, prop2, ...])\">Multi-property index.</m>\n");
    xml.push_str("      <m sig=\"drop_index(type, property) / drop_range_index / drop_composite_index\">Remove indexes.</m>\n");
    xml.push_str("      <m sig=\"list_indexes() / list_composite_indexes()\">Enumerate existing indexes.</m>\n");
    xml.push_str(
        "      <m sig=\"index_stats(type, property)\">Index metadata and hit count.</m>\n",
    );
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"equality\">graph.create_index('Person', 'email')</ex>\n");
    xml.push_str("      <ex desc=\"range\">graph.create_range_index('Product', 'price')</ex>\n");
    xml.push_str("      <ex desc=\"composite\">graph.create_composite_index('Person', ['city', 'age'])</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </indexes>\n");
}

fn write_fluent_topic_set_operations(xml: &mut String) {
    xml.push_str("  <set_operations>\n");
    xml.push_str("    <desc>Combine selections using set logic.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"union(other)\">Nodes in either selection.</m>\n");
    xml.push_str("      <m sig=\"intersection(other)\">Nodes in both selections.</m>\n");
    xml.push_str("      <m sig=\"difference(other)\">In first but not second.</m>\n");
    xml.push_str("      <m sig=\"symmetric_difference(other)\">In exactly one selection.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"union\">oslo_or_young = graph.select('Person').where({'city': 'Oslo'}).union(graph.select('Person').where({'age': {'&lt;': 25}}))</ex>\n");
    xml.push_str(
        "      <ex desc=\"intersection\">oslo_and_young = oslo.intersection(young)</ex>\n",
    );
    xml.push_str("    </examples>\n");
    xml.push_str("  </set_operations>\n");
}

fn write_fluent_topic_subgraph(xml: &mut String) {
    xml.push_str("  <subgraph>\n");
    xml.push_str(
        "    <desc>Extract a subset of the graph into a new independent KnowledgeGraph.</desc>\n",
    );
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"to_subgraph()\">Extract selected nodes + inter-edges into a new graph.</m>\n");
    xml.push_str("      <m sig=\"subgraph_stats()\">Preview extraction: node/edge counts without materialising.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"extract\">sub = graph.select('Person').where({'city': 'Oslo'}).to_subgraph()</ex>\n");
    xml.push_str("      <ex desc=\"preview\">graph.select('Person').subgraph_stats()</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </subgraph>\n");
}

fn write_fluent_topic_schema(xml: &mut String) {
    xml.push_str("  <schema>\n");
    xml.push_str("    <desc>Inspect and enforce graph schema.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"schema()\">Full schema dict: node types, connections, indexes, counts.</m>\n");
    xml.push_str("      <m sig=\"schema_text()\">Human-readable schema summary.</m>\n");
    xml.push_str("      <m sig=\"properties(type)\">Per-property statistics: type, non_null, unique, samples.</m>\n");
    xml.push_str(
        "      <m sig=\"connection_types()\">All connection types with counts and endpoints.</m>\n",
    );
    xml.push_str(
        "      <m sig=\"describe(types=['...'])\">AI-optimised XML for specific types.</m>\n",
    );
    xml.push_str("      <m sig=\"define_schema(schema_dict)\">Enforce schema constraints on future loads.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"full schema\">graph.schema()</ex>\n");
    xml.push_str("      <ex desc=\"text overview\">print(graph.schema_text())</ex>\n");
    xml.push_str("      <ex desc=\"property detail\">graph.properties('Person')</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </schema>\n");
}

fn write_fluent_topic_transactions(xml: &mut String) {
    xml.push_str("  <transactions>\n");
    xml.push_str("    <desc>Transactional access with automatic rollback on error.</desc>\n");
    xml.push_str("    <methods>\n");
    xml.push_str("      <m sig=\"begin()\">Read-write transaction. Use as context manager.</m>\n");
    xml.push_str("      <m sig=\"begin_read()\">Read-only transaction (O(1) cost, no copy). Use as context manager.</m>\n");
    xml.push_str("    </methods>\n");
    xml.push_str("    <examples>\n");
    xml.push_str("      <ex desc=\"read-write\">with graph.begin() as tx: tx.select('Person').update({'verified': True})</ex>\n");
    xml.push_str("      <ex desc=\"read-only\">with graph.begin_read() as ro: count = ro.select('Person').len()</ex>\n");
    xml.push_str("    </examples>\n");
    xml.push_str("  </transactions>\n");
}

// ── Describe: entry point ──────────────────────────────────────────────────

/// Build an XML description of the graph for AI agents (progressive disclosure).
///
/// Four independent axes:
/// - `types` → Node type deep-dive (None=inventory, Some=focused detail).
/// - `connections` → Connection type docs (Off=in inventory, Overview=all, Topics=specific).
/// - `cypher` → Cypher language reference (Off=hint, Overview=compact, Topics=detailed).
/// - `fluent` → Fluent API reference (Off=hint, Overview=compact, Topics=detailed).
///
/// When `connections`, `cypher`, or `fluent` is not Off, only those tracks are returned.
pub fn compute_description(
    graph: &DirGraph,
    types: Option<&[String]>,
    connections: &ConnectionDetail,
    cypher: &CypherDetail,
    fluent: &FluentDetail,
) -> Result<String, String> {
    // If connections, cypher, or fluent is requested, return only those tracks
    let standalone = !matches!(connections, ConnectionDetail::Off)
        || !matches!(cypher, CypherDetail::Off)
        || !matches!(fluent, FluentDetail::Off);

    if standalone {
        let mut result = String::with_capacity(4096);
        match connections {
            ConnectionDetail::Off => {}
            ConnectionDetail::Overview => write_connections_overview(&mut result, graph),
            ConnectionDetail::Topics(ref topics) => {
                write_connections_detail(&mut result, graph, topics)?;
            }
        }
        match cypher {
            CypherDetail::Off => {}
            CypherDetail::Overview => write_cypher_overview(&mut result),
            CypherDetail::Topics(ref topics) => {
                write_cypher_topics(&mut result, topics)?;
            }
        }
        match fluent {
            FluentDetail::Off => {}
            FluentDetail::Overview => write_fluent_overview(&mut result),
            FluentDetail::Topics(ref topics) => {
                write_fluent_topics(&mut result, topics)?;
            }
        }
        return Ok(result);
    }

    // Normal describe — inventory or focused detail
    let result = match types {
        Some(requested) if !requested.is_empty() => build_focused_detail(graph, requested)?,
        _ => {
            // Count core types only (exclude supporting types)
            let core_count = graph
                .type_indices
                .keys()
                .filter(|nt| !graph.parent_types.contains_key(*nt))
                .count();
            if core_count <= 15 {
                build_inventory_with_detail(graph)
            } else {
                build_inventory(graph)
            }
        }
    };
    Ok(result)
}

/// Minimal XML escaping for attribute values.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

// ── MCP quickstart ──────────────────────────────────────────────────────────

/// Return a self-contained XML quickstart for setting up a KGLite MCP server.
///
/// Static content — no graph instance needed.
pub fn mcp_quickstart() -> String {
    format!(
        r##"<mcp_quickstart version="{version}">

  <setup>
    <install>pip install kglite fastmcp</install>
    <server><![CDATA[
import kglite
from fastmcp import FastMCP

graph = kglite.load("your_graph.kgl")
mcp = FastMCP("my-graph", instructions="Knowledge graph. Call graph_overview first.")

@mcp.tool()
def graph_overview(
    types: list[str] | None = None,
    connections: bool | list[str] | None = None,
    cypher: bool | list[str] | None = None,
) -> str:
    """Get graph schema, connection details, or Cypher language reference.

    Three independent axes — call with no args first for the overview:
      graph_overview()                            — inventory + connections with property names
      graph_overview(types=["Field"])             — property schemas, samples
      graph_overview(connections=True)            — all connection types with properties
      graph_overview(connections=["BELONGS_TO"])  — deep-dive: property stats, sample edges
      graph_overview(cypher=True)                 — Cypher clauses, functions, procedures
      graph_overview(cypher=["cluster","MATCH"])  — detailed docs with examples"""
    return graph.describe(types=types, connections=connections, cypher=cypher)

@mcp.tool()
def cypher_query(query: str) -> str:
    """Run a Cypher query against the knowledge graph.

    Supports MATCH, WHERE, RETURN, ORDER BY, LIMIT, aggregations,
    path traversals, CREATE, SET, DELETE, and CALL procedures.
    Append FORMAT CSV for compact CSV output (good for larger data transfers).
    Returns up to 200 rows."""
    result = graph.cypher(query)
    if isinstance(result, str):
        return result  # FORMAT CSV already returned a string
    if len(result) == 0:
        return "Query returned no results."
    rows = [str(dict(row)) for row in result[:200]]
    header = f"Returned {{len(result)}} row(s)"
    if len(result) > 200:
        header += " (showing first 200)"
    return header + ":\n" + "\n".join(rows)

@mcp.tool()
def bug_report(query: str, result: str, expected: str, description: str) -> str:
    """File a Cypher bug report to reported_bugs.md.

    Writes a timestamped, version-tagged entry (newest first).
    Use when a query returns incorrect or unexpected results."""
    return graph.bug_report(query, result, expected, description)

if __name__ == "__main__":
    mcp.run(transport="stdio")
]]></server>
  </setup>

  <core_tools desc="Essential — include all three in every MCP server">
    <tool name="graph_overview" method="graph.describe()" args="types, connections, cypher">
      Schema introspection with 3-tier progressive disclosure.
      The agent's entry point — always expose this.
    </tool>
    <tool name="cypher_query" method="graph.cypher()" args="query">
      Execute Cypher queries. MATCH/WHERE/RETURN/CREATE/SET/DELETE,
      aggregations, CALL procedures (pagerank, cluster, etc.).
      Append FORMAT CSV for compact CSV output (good for larger data transfers).
    </tool>
    <tool name="bug_report" method="graph.bug_report()" args="query, result, expected, description">
      File bug reports to reported_bugs.md. Input is sanitised.
    </tool>
  </core_tools>

  <optional_tools desc="Add based on your use case">
    <tool name="find_entity" method="graph.find()" args="name, node_type?, match_type?">
      Search nodes by name. match_type: 'exact' (default), 'contains', 'starts_with'.
      Useful for code graphs where entities have qualified names.
    </tool>
    <tool name="read_source" method="graph.source()" args="names, node_type?">
      Resolve entity names to file paths and line ranges.
      Returns source code locations for code navigation.
    </tool>
    <tool name="entity_context" method="graph.context()" args="name, node_type?, hops?">
      Get neighborhood of a node — related entities within N hops.
      Good for understanding how entities connect.
    </tool>
    <tool name="file_toc" method="graph.toc()" args="file_path">
      Table of contents for a file — lists all entities sorted by line.
      Only relevant for code-tree graphs.
    </tool>
    <tool name="grep_source" custom="true">
      Text search across source files. Not built-in — implement with
      your own file-reading logic or expose graph.cypher() with
      CONTAINS/STARTS WITH/=~ for in-graph text search.
    </tool>
  </optional_tools>

  <register_with_claude>
    <claude_desktop desc="Add to Claude Desktop config">
      <file>~/Library/Application Support/Claude/claude_desktop_config.json</file>
      <config><![CDATA[
{{
  "mcpServers": {{
    "my-graph": {{
      "command": "python",
      "args": ["/absolute/path/to/mcp_server.py"]
    }}
  }}
}}
]]></config>
    </claude_desktop>
    <claude_code desc="Add to Claude Code config">
      <file>.claude/settings.json (project) or ~/.claude/settings.json (global)</file>
      <config><![CDATA[
{{
  "mcpServers": {{
    "my-graph": {{
      "command": "python",
      "args": ["/absolute/path/to/mcp_server.py"]
    }}
  }}
}}
]]></config>
    </claude_code>
    <note>Restart Claude after editing config. The server appears as an MCP tool provider.</note>
  </register_with_claude>

</mcp_quickstart>
"##,
        version = env!("CARGO_PKG_VERSION"),
    )
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::values::Value;
    use crate::graph::schema::{ConnectionTypeInfo, DirGraph, EdgeData, NodeData};
    use std::collections::{HashMap, HashSet};

    // ── Test helpers ───────────────────────────────────────────────────────

    /// Build a minimal graph with one or more node types.
    /// Returns the DirGraph with nodes registered in type_indices.
    fn make_graph_with_nodes(types: &[(&str, Vec<(&str, &str, Vec<(&str, Value)>)>)]) -> DirGraph {
        let mut g = DirGraph::new();
        for (node_type, nodes) in types {
            for (id, title, props) in nodes {
                let mut prop_map: HashMap<String, Value> = HashMap::new();
                for (k, v) in props {
                    prop_map.insert(k.to_string(), v.clone());
                }
                let node = NodeData::new(
                    Value::String(id.to_string()),
                    Value::String(title.to_string()),
                    node_type.to_string(),
                    prop_map,
                    &mut g.interner,
                );
                let idx = g.graph.add_node(node);
                g.type_indices
                    .entry(node_type.to_string())
                    .or_default()
                    .push(idx);
            }
        }
        g
    }

    /// Add an edge between the first node of `src_type` and the first node of `tgt_type`.
    fn add_edge(
        g: &mut DirGraph,
        src_type: &str,
        tgt_type: &str,
        conn_type: &str,
        props: Vec<(&str, Value)>,
    ) {
        let src_idx = g.type_indices[src_type][0];
        let tgt_idx = g.type_indices[tgt_type][0];
        let mut prop_map: HashMap<String, Value> = HashMap::new();
        for (k, v) in props {
            prop_map.insert(k.to_string(), v.clone());
        }
        let edge = EdgeData::new(conn_type.to_string(), prop_map, &mut g.interner);
        g.graph.add_edge(src_idx, tgt_idx, edge);
    }

    /// Add an edge between specific node indices within types.
    fn add_edge_indexed(
        g: &mut DirGraph,
        src_type: &str,
        src_idx: usize,
        tgt_type: &str,
        tgt_idx: usize,
        conn_type: &str,
    ) {
        let src = g.type_indices[src_type][src_idx];
        let tgt = g.type_indices[tgt_type][tgt_idx];
        let edge = EdgeData::new(conn_type.to_string(), HashMap::new(), &mut g.interner);
        g.graph.add_edge(src, tgt, edge);
    }

    // ── xml_escape ─────────────────────────────────────────────────────────

    #[test]
    fn test_xml_escape_plain_string() {
        assert_eq!(xml_escape("hello"), "hello");
    }

    #[test]
    fn test_xml_escape_ampersand() {
        assert_eq!(xml_escape("a&b"), "a&amp;b");
    }

    #[test]
    fn test_xml_escape_angle_brackets() {
        assert_eq!(xml_escape("<tag>"), "&lt;tag&gt;");
    }

    #[test]
    fn test_xml_escape_quotes() {
        assert_eq!(xml_escape("say \"hi\""), "say &quot;hi&quot;");
    }

    #[test]
    fn test_xml_escape_all_special() {
        assert_eq!(xml_escape("<a & \"b\">"), "&lt;a &amp; &quot;b&quot;&gt;");
    }

    #[test]
    fn test_xml_escape_empty_string() {
        assert_eq!(xml_escape(""), "");
    }

    // ── property_complexity ────────────────────────────────────────────────

    #[test]
    fn test_property_complexity_ranges() {
        assert_eq!(property_complexity(0), "vl");
        assert_eq!(property_complexity(3), "vl");
        assert_eq!(property_complexity(4), "l");
        assert_eq!(property_complexity(8), "l");
        assert_eq!(property_complexity(9), "m");
        assert_eq!(property_complexity(15), "m");
        assert_eq!(property_complexity(16), "h");
        assert_eq!(property_complexity(30), "h");
        assert_eq!(property_complexity(31), "vh");
        assert_eq!(property_complexity(100), "vh");
    }

    // ── size_tier ──────────────────────────────────────────────────────────

    #[test]
    fn test_size_tier_ranges() {
        assert_eq!(size_tier(0), "vs");
        assert_eq!(size_tier(9), "vs");
        assert_eq!(size_tier(10), "s");
        assert_eq!(size_tier(99), "s");
        assert_eq!(size_tier(100), "m");
        assert_eq!(size_tier(999), "m");
        assert_eq!(size_tier(1000), "l");
        assert_eq!(size_tier(9999), "l");
        assert_eq!(size_tier(10000), "vl");
        assert_eq!(size_tier(100000), "vl");
    }

    // ── TypeCapabilities ───────────────────────────────────────────────────

    #[test]
    fn test_flags_csv_empty() {
        let tc = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        assert_eq!(tc.flags_csv(), "");
    }

    #[test]
    fn test_flags_csv_all() {
        let tc = TypeCapabilities {
            has_timeseries: true,
            has_location: true,
            has_geometry: true,
            has_embeddings: true,
        };
        // When geometry is present, location is suppressed
        assert_eq!(tc.flags_csv(), "ts,geo,vec");
    }

    #[test]
    fn test_flags_csv_location_only() {
        let tc = TypeCapabilities {
            has_timeseries: false,
            has_location: true,
            has_geometry: false,
            has_embeddings: false,
        };
        assert_eq!(tc.flags_csv(), "loc");
    }

    #[test]
    fn test_flags_csv_location_suppressed_by_geometry() {
        let tc = TypeCapabilities {
            has_timeseries: false,
            has_location: true,
            has_geometry: true,
            has_embeddings: false,
        };
        // location is suppressed when geometry is present
        assert_eq!(tc.flags_csv(), "geo");
    }

    #[test]
    fn test_merge_capabilities() {
        let mut parent = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: true,
        };
        let child = TypeCapabilities {
            has_timeseries: true,
            has_location: true,
            has_geometry: false,
            has_embeddings: false,
        };
        parent.merge(&child);
        assert!(parent.has_timeseries);
        assert!(parent.has_location);
        assert!(!parent.has_geometry);
        assert!(parent.has_embeddings);
    }

    // ── format_type_descriptor ─────────────────────────────────────────────

    #[test]
    fn test_format_type_descriptor_no_flags() {
        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        let result = format_type_descriptor("Person", 50, 5, &caps);
        assert_eq!(result, "Person[s,l]");
    }

    #[test]
    fn test_format_type_descriptor_with_flags() {
        let caps = TypeCapabilities {
            has_timeseries: true,
            has_location: false,
            has_geometry: false,
            has_embeddings: true,
        };
        let result = format_type_descriptor("Sensor", 1500, 20, &caps);
        assert_eq!(result, "Sensor[l,h,ts,vec]");
    }

    #[test]
    fn test_format_type_descriptor_special_chars() {
        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        let result = format_type_descriptor("Type<A>", 5, 2, &caps);
        assert_eq!(result, "Type&lt;A&gt;[vs,vl]");
    }

    // ── is_null_value ──────────────────────────────────────────────────────

    #[test]
    fn test_is_null_value() {
        assert!(is_null_value(&Value::Null));
        assert!(is_null_value(&Value::Float64(f64::NAN)));
        assert!(!is_null_value(&Value::Int64(0)));
        assert!(!is_null_value(&Value::String(String::new())));
        assert!(!is_null_value(&Value::Float64(0.0)));
        assert!(!is_null_value(&Value::Boolean(false)));
    }

    // ── value_type_name ────────────────────────────────────────────────────

    #[test]
    fn test_value_type_name() {
        assert_eq!(value_type_name(&Value::String("hi".into())), "str");
        assert_eq!(value_type_name(&Value::Int64(42)), "int");
        assert_eq!(value_type_name(&Value::Float64(3.14)), "float");
        assert_eq!(value_type_name(&Value::Boolean(true)), "bool");
        assert_eq!(value_type_name(&Value::Null), "unknown");
        assert_eq!(
            value_type_name(&Value::Point { lat: 0.0, lon: 0.0 }),
            "point"
        );
    }

    // ── value_display_compact ──────────────────────────────────────────────

    #[test]
    fn test_value_display_compact_string_short() {
        let v = Value::String("hello".into());
        assert_eq!(value_display_compact(&v), "hello");
    }

    #[test]
    fn test_value_display_compact_string_truncation() {
        let long_str = "a".repeat(50);
        let v = Value::String(long_str);
        let result = value_display_compact(&v);
        assert!(result.ends_with("..."));
        // 37 chars + "..." = 40 chars
        assert_eq!(result.len(), 40);
    }

    #[test]
    fn test_value_display_compact_int() {
        assert_eq!(value_display_compact(&Value::Int64(42)), "42");
    }

    #[test]
    fn test_value_display_compact_float() {
        let result = value_display_compact(&Value::Float64(3.14));
        assert!(result.contains("3.14"));
    }

    #[test]
    fn test_value_display_compact_bool() {
        assert_eq!(value_display_compact(&Value::Boolean(true)), "true");
        assert_eq!(value_display_compact(&Value::Boolean(false)), "false");
    }

    #[test]
    fn test_value_display_compact_point() {
        let v = Value::Point {
            lat: 59.9,
            lon: 10.7,
        };
        assert_eq!(value_display_compact(&v), "(59.9,10.7)");
    }

    #[test]
    fn test_value_display_compact_null() {
        assert_eq!(value_display_compact(&Value::Null), "");
    }

    // ── types_compatible ───────────────────────────────────────────────────

    #[test]
    fn test_types_compatible_strings() {
        assert!(types_compatible("String", "String"));
        assert!(types_compatible("String", "UniqueId"));
        assert!(types_compatible("UniqueId", "str"));
        assert!(types_compatible("str", "String"));
    }

    #[test]
    fn test_types_compatible_numbers() {
        assert!(types_compatible("Int64", "Float64"));
        assert!(types_compatible("float", "int"));
        assert!(types_compatible("Int64", "int"));
    }

    #[test]
    fn test_types_compatible_mismatches() {
        assert!(!types_compatible("String", "Int64"));
        assert!(!types_compatible("float", "str"));
        assert!(!types_compatible("bool", "int"));
        assert!(!types_compatible("unknown", "String"));
    }

    // ── children_counts ────────────────────────────────────────────────────

    #[test]
    fn test_children_counts_empty() {
        let parent_types: HashMap<String, String> = HashMap::new();
        let counts = children_counts(&parent_types);
        assert!(counts.is_empty());
    }

    #[test]
    fn test_children_counts_multiple() {
        let mut parent_types: HashMap<String, String> = HashMap::new();
        parent_types.insert("ChildA".into(), "Parent".into());
        parent_types.insert("ChildB".into(), "Parent".into());
        parent_types.insert("ChildC".into(), "Other".into());
        let counts = children_counts(&parent_types);
        assert_eq!(counts["Parent"], 2);
        assert_eq!(counts["Other"], 1);
    }

    // ── compute_connected_types ────────────────────────────────────────────

    #[test]
    fn test_compute_connected_types_empty() {
        let stats: Vec<ConnectionTypeStats> = Vec::new();
        let connected = compute_connected_types(&stats);
        assert!(connected.is_empty());
    }

    #[test]
    fn test_compute_connected_types() {
        let stats = vec![ConnectionTypeStats {
            connection_type: "KNOWS".into(),
            count: 5,
            source_types: vec!["Person".into()],
            target_types: vec!["Person".into(), "Company".into()],
            property_names: vec![],
        }];
        let connected = compute_connected_types(&stats);
        assert!(connected.contains("Person"));
        assert!(connected.contains("Company"));
        assert_eq!(connected.len(), 2);
    }

    // ── compute_connected_type_pairs ───────────────────────────────────────

    #[test]
    fn test_compute_connected_type_pairs() {
        let stats = vec![ConnectionTypeStats {
            connection_type: "BELONGS_TO".into(),
            count: 10,
            source_types: vec!["Well".into()],
            target_types: vec!["Field".into()],
            property_names: vec![],
        }];
        let pairs = compute_connected_type_pairs(&stats);
        assert!(pairs.contains(&("Well".into(), "Field".into())));
        assert!(pairs.contains(&("Field".into(), "Well".into())));
    }

    // ── bubble_capabilities ────────────────────────────────────────────────

    #[test]
    fn test_bubble_capabilities() {
        let mut caps: HashMap<String, TypeCapabilities> = HashMap::new();
        caps.insert(
            "Parent".into(),
            TypeCapabilities {
                has_timeseries: false,
                has_location: false,
                has_geometry: false,
                has_embeddings: false,
            },
        );
        caps.insert(
            "Child".into(),
            TypeCapabilities {
                has_timeseries: true,
                has_location: false,
                has_geometry: false,
                has_embeddings: true,
            },
        );

        let mut parent_types: HashMap<String, String> = HashMap::new();
        parent_types.insert("Child".into(), "Parent".into());

        bubble_capabilities(&mut caps, &parent_types);
        let parent = &caps["Parent"];
        assert!(parent.has_timeseries);
        assert!(parent.has_embeddings);
        assert!(!parent.has_location);
    }

    // ── Empty graph ────────────────────────────────────────────────────────

    #[test]
    fn test_compute_schema_empty_graph() {
        let g = DirGraph::new();
        let schema = compute_schema(&g);
        assert_eq!(schema.node_count, 0);
        assert_eq!(schema.edge_count, 0);
        assert!(schema.node_types.is_empty());
        assert!(schema.connection_types.is_empty());
        assert!(schema.indexes.is_empty());
    }

    #[test]
    fn test_compute_connection_type_stats_empty() {
        let g = DirGraph::new();
        let stats = compute_connection_type_stats(&g);
        assert!(stats.is_empty());
    }

    // ── Graph with nodes only ──────────────────────────────────────────────

    #[test]
    fn test_compute_schema_nodes_only() {
        let g = make_graph_with_nodes(&[(
            "Person",
            vec![
                ("p1", "Alice", vec![("age", Value::Int64(30))]),
                ("p2", "Bob", vec![("age", Value::Int64(25))]),
            ],
        )]);
        let schema = compute_schema(&g);
        assert_eq!(schema.node_count, 2);
        assert_eq!(schema.edge_count, 0);
        assert_eq!(schema.node_types.len(), 1);
        assert_eq!(schema.node_types[0].0, "Person");
        assert_eq!(schema.node_types[0].1.count, 2);
    }

    // ── Graph with edges ───────────────────────────────────────────────────

    #[test]
    fn test_compute_connection_type_stats_fallback() {
        // Without connection_type_metadata, uses the fallback scan path
        let mut g = make_graph_with_nodes(&[
            (
                "Person",
                vec![("p1", "Alice", vec![]), ("p2", "Bob", vec![])],
            ),
            ("City", vec![("c1", "London", vec![])]),
        ]);
        add_edge(&mut g, "Person", "City", "LIVES_IN", vec![]);
        add_edge_indexed(&mut g, "Person", 1, "City", 0, "LIVES_IN");

        let stats = compute_connection_type_stats(&g);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].connection_type, "LIVES_IN");
        assert_eq!(stats[0].count, 2);
        assert_eq!(stats[0].source_types, vec!["Person".to_string()]);
        assert_eq!(stats[0].target_types, vec!["City".to_string()]);
    }

    #[test]
    fn test_compute_connection_type_stats_fast_path() {
        // With connection_type_metadata populated, uses the fast path
        let mut g = make_graph_with_nodes(&[
            ("Person", vec![("p1", "Alice", vec![])]),
            ("City", vec![("c1", "London", vec![])]),
        ]);
        add_edge(
            &mut g,
            "Person",
            "City",
            "LIVES_IN",
            vec![("since", Value::Int64(2020))],
        );

        // Populate metadata
        let mut sources = HashSet::new();
        sources.insert("Person".to_string());
        let mut targets = HashSet::new();
        targets.insert("City".to_string());
        let mut prop_types = HashMap::new();
        prop_types.insert("since".to_string(), "Int64".to_string());
        g.connection_type_metadata.insert(
            "LIVES_IN".to_string(),
            ConnectionTypeInfo {
                source_types: sources,
                target_types: targets,
                property_types: prop_types,
            },
        );

        let stats = compute_connection_type_stats(&g);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].connection_type, "LIVES_IN");
        assert_eq!(stats[0].property_names, vec!["since".to_string()]);
    }

    // ── compute_property_stats ─────────────────────────────────────────────

    #[test]
    fn test_compute_property_stats_basic() {
        let g = make_graph_with_nodes(&[(
            "Person",
            vec![
                ("p1", "Alice", vec![("age", Value::Int64(30))]),
                (
                    "p2",
                    "Bob",
                    vec![
                        ("age", Value::Int64(25)),
                        ("city", Value::String("NYC".into())),
                    ],
                ),
            ],
        )]);
        let stats = compute_property_stats(&g, "Person", 15, None).unwrap();

        // Should contain type, title, id, age, city
        let names: Vec<&str> = stats.iter().map(|s| s.property_name.as_str()).collect();
        assert!(names.contains(&"type"));
        assert!(names.contains(&"title"));
        assert!(names.contains(&"id"));
        assert!(names.contains(&"age"));
        assert!(names.contains(&"city"));

        // The "type" property should always be synthetic with count = total nodes
        let type_stat = stats.iter().find(|s| s.property_name == "type").unwrap();
        assert_eq!(type_stat.non_null, 2);
        assert_eq!(type_stat.unique, 1);
        assert!(type_stat.values.is_some());
    }

    #[test]
    fn test_compute_property_stats_unknown_type() {
        let g = DirGraph::new();
        let result = compute_property_stats(&g, "NonExistent", 15, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_compute_property_stats_with_nulls() {
        let g = make_graph_with_nodes(&[(
            "Item",
            vec![
                ("i1", "A", vec![("val", Value::Int64(1))]),
                ("i2", "B", vec![("val", Value::Null)]),
            ],
        )]);
        let stats = compute_property_stats(&g, "Item", 15, None).unwrap();
        let val_stat = stats.iter().find(|s| s.property_name == "val").unwrap();
        assert_eq!(val_stat.non_null, 1);
        assert_eq!(val_stat.unique, 1);
    }

    #[test]
    fn test_compute_property_stats_with_sampling() {
        // Create enough nodes that sampling kicks in
        let nodes: Vec<(&str, &str, Vec<(&str, Value)>)> = (0..20)
            .map(|i| {
                // Leak the strings so they have 'static lifetime for the tuple
                let id: &'static str = Box::leak(format!("id{}", i).into_boxed_str());
                let title: &'static str = Box::leak(format!("title{}", i).into_boxed_str());
                let props: Vec<(&str, Value)> = vec![("x", Value::Int64(i))];
                (id, title, props)
            })
            .collect();
        let g = make_graph_with_nodes(&[("Batch", nodes)]);

        // Sample 5 out of 20
        let stats = compute_property_stats(&g, "Batch", 0, Some(5)).unwrap();
        let x_stat = stats.iter().find(|s| s.property_name == "x").unwrap();
        // Sampled non_null should be scaled up to approximate total
        assert!(x_stat.non_null >= 15); // 5 * 4.0 = 20, or close
    }

    // ── compute_neighbors_schema ───────────────────────────────────────────

    #[test]
    fn test_compute_neighbors_schema() {
        let mut g = make_graph_with_nodes(&[
            ("Person", vec![("p1", "Alice", vec![])]),
            ("City", vec![("c1", "London", vec![])]),
        ]);
        add_edge(&mut g, "Person", "City", "LIVES_IN", vec![]);

        let schema = compute_neighbors_schema(&g, "Person").unwrap();
        assert_eq!(schema.outgoing.len(), 1);
        assert_eq!(schema.outgoing[0].connection_type, "LIVES_IN");
        assert_eq!(schema.outgoing[0].other_type, "City");
        assert_eq!(schema.outgoing[0].count, 1);
        assert!(schema.incoming.is_empty());

        let city_schema = compute_neighbors_schema(&g, "City").unwrap();
        assert!(city_schema.outgoing.is_empty());
        assert_eq!(city_schema.incoming.len(), 1);
        assert_eq!(city_schema.incoming[0].connection_type, "LIVES_IN");
        assert_eq!(city_schema.incoming[0].other_type, "Person");
    }

    #[test]
    fn test_compute_neighbors_schema_unknown_type() {
        let g = DirGraph::new();
        let result = compute_neighbors_schema(&g, "NonExistent");
        assert!(result.is_err());
    }

    // ── compute_all_neighbors_schemas ──────────────────────────────────────

    #[test]
    fn test_compute_all_neighbors_schemas() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "a", vec![])]),
            ("B", vec![("b1", "b", vec![])]),
            ("C", vec![("c1", "c", vec![])]),
        ]);
        add_edge(&mut g, "A", "B", "REL1", vec![]);
        add_edge(&mut g, "B", "C", "REL2", vec![]);

        let all = compute_all_neighbors_schemas(&g);
        assert_eq!(all.len(), 3);

        let a_schema = &all["A"];
        assert_eq!(a_schema.outgoing.len(), 1);
        assert!(a_schema.incoming.is_empty());

        let b_schema = &all["B"];
        assert_eq!(b_schema.outgoing.len(), 1);
        assert_eq!(b_schema.incoming.len(), 1);

        let c_schema = &all["C"];
        assert!(c_schema.outgoing.is_empty());
        assert_eq!(c_schema.incoming.len(), 1);
    }

    // ── compute_sample ─────────────────────────────────────────────────────

    #[test]
    fn test_compute_sample_basic() {
        let g = make_graph_with_nodes(&[(
            "Item",
            vec![
                ("i1", "A", vec![]),
                ("i2", "B", vec![]),
                ("i3", "C", vec![]),
            ],
        )]);
        let samples = compute_sample(&g, "Item", 2).unwrap();
        assert_eq!(samples.len(), 2);
    }

    #[test]
    fn test_compute_sample_more_than_available() {
        let g = make_graph_with_nodes(&[("Item", vec![("i1", "A", vec![])])]);
        let samples = compute_sample(&g, "Item", 10).unwrap();
        assert_eq!(samples.len(), 1);
    }

    #[test]
    fn test_compute_sample_unknown_type() {
        let g = DirGraph::new();
        let result = compute_sample(&g, "X", 5);
        assert!(result.is_err());
    }

    // ── compute_schema with indexes ────────────────────────────────────────

    #[test]
    fn test_compute_schema_with_indexes() {
        let mut g = make_graph_with_nodes(&[(
            "Person",
            vec![("p1", "Alice", vec![("age", Value::Int64(30))])],
        )]);
        // Add a property index key
        g.property_indices
            .insert(("Person".to_string(), "age".to_string()), HashMap::new());
        // Add a range index key
        g.range_indices.insert(
            ("Person".to_string(), "age".to_string()),
            std::collections::BTreeMap::new(),
        );
        // Add a composite index key
        g.composite_indices.insert(
            (
                "Person".to_string(),
                vec!["age".to_string(), "name".to_string()],
            ),
            HashMap::new(),
        );

        let schema = compute_schema(&g);
        assert!(schema.indexes.len() >= 2);
        let idx_strs: Vec<&str> = schema.indexes.iter().map(|s| s.as_str()).collect();
        assert!(idx_strs.iter().any(|s| s.contains("Person.age")));
        assert!(idx_strs.iter().any(|s| s.contains("[range]")));
        assert!(idx_strs.iter().any(|s| s.contains("(age, name)")));
    }

    // ── write_conventions ──────────────────────────────────────────────────

    #[test]
    fn test_write_conventions_no_capabilities() {
        let caps: HashMap<String, TypeCapabilities> = HashMap::new();
        let mut xml = String::new();
        write_conventions(&mut xml, &caps);
        assert!(xml.contains("All nodes have .id and .title</conventions>"));
        assert!(!xml.contains("Some have:"));
    }

    #[test]
    fn test_write_conventions_with_capabilities() {
        let mut caps: HashMap<String, TypeCapabilities> = HashMap::new();
        caps.insert(
            "Sensor".into(),
            TypeCapabilities {
                has_timeseries: true,
                has_location: true,
                has_geometry: false,
                has_embeddings: false,
            },
        );
        let mut xml = String::new();
        write_conventions(&mut xml, &caps);
        assert!(xml.contains("Some have:"));
        assert!(xml.contains("location"));
        assert!(xml.contains("timeseries"));
    }

    // ── write_read_only_notice ─────────────────────────────────────────────

    #[test]
    fn test_write_read_only_notice_off() {
        let g = DirGraph::new();
        let mut xml = String::new();
        write_read_only_notice(&mut xml, &g);
        assert!(xml.is_empty());
    }

    #[test]
    fn test_write_read_only_notice_on() {
        let mut g = DirGraph::new();
        g.read_only = true;
        let mut xml = String::new();
        write_read_only_notice(&mut xml, &g);
        assert!(xml.contains("<read-only>"));
        assert!(xml.contains("mutations disabled"));
    }

    // ── write_connection_map ───────────────────────────────────────────────

    #[test]
    fn test_write_connection_map_empty() {
        let g = DirGraph::new();
        let stats: Vec<ConnectionTypeStats> = Vec::new();
        let mut xml = String::new();
        write_connection_map(&mut xml, &g, &stats);
        assert!(xml.contains("<connections/>"));
    }

    #[test]
    fn test_write_connection_map_with_stats() {
        let g = DirGraph::new();
        let stats = vec![ConnectionTypeStats {
            connection_type: "KNOWS".into(),
            count: 42,
            source_types: vec!["Person".into()],
            target_types: vec!["Person".into()],
            property_names: vec!["since".into()],
        }];
        let mut xml = String::new();
        write_connection_map(&mut xml, &g, &stats);
        assert!(xml.contains("type=\"KNOWS\""));
        assert!(xml.contains("count=\"42\""));
        assert!(xml.contains("from=\"Person\""));
        assert!(xml.contains("to=\"Person\""));
        assert!(xml.contains("properties=\"since\""));
    }

    // ── write_extensions ───────────────────────────────────────────────────

    #[test]
    fn test_write_extensions_basic() {
        let g = DirGraph::new();
        let mut xml = String::new();
        write_extensions(&mut xml, &g);
        assert!(xml.contains("<extensions>"));
        assert!(xml.contains("</extensions>"));
        assert!(xml.contains("<algorithms"));
        assert!(xml.contains("<cypher"));
        assert!(xml.contains("<fluent_api"));
        // No timeseries, spatial, or embeddings
        assert!(!xml.contains("<timeseries"));
        assert!(!xml.contains("<spatial"));
        assert!(!xml.contains("<semantic"));
    }

    #[test]
    fn test_write_extensions_with_timeseries() {
        let mut g = DirGraph::new();
        g.timeseries_configs.insert(
            "Sensor".to_string(),
            super::super::timeseries::TimeseriesConfig {
                resolution: "daily".to_string(),
                channels: vec!["temp".to_string()],
                units: HashMap::new(),
                bin_type: None,
            },
        );
        let mut xml = String::new();
        write_extensions(&mut xml, &g);
        assert!(xml.contains("<timeseries"));
    }

    // ── write_exploration_hints ─────────────────────────────────────────────

    #[test]
    fn test_write_exploration_hints_trivial_graph() {
        // < 2 types → no hints
        let g = make_graph_with_nodes(&[("Only", vec![("o1", "x", vec![])])]);
        let stats: Vec<ConnectionTypeStats> = Vec::new();
        let mut xml = String::new();
        write_exploration_hints(&mut xml, &g, &stats);
        assert!(xml.is_empty());
    }

    #[test]
    fn test_write_exploration_hints_disconnected_types() {
        let mut g = make_graph_with_nodes(&[
            ("TypeA", vec![("a1", "a", vec![])]),
            ("TypeB", vec![("b1", "b", vec![])]),
            ("TypeC", vec![("c1", "c", vec![])]),
        ]);
        // Connect A to B, leave C disconnected
        add_edge(&mut g, "TypeA", "TypeB", "REL", vec![]);

        let stats = compute_connection_type_stats(&g);
        let mut xml = String::new();
        write_exploration_hints(&mut xml, &g, &stats);
        assert!(xml.contains("<disconnected>"));
        assert!(xml.contains("TypeC"));
    }

    // ── write_cypher_overview ──────────────────────────────────────────────

    #[test]
    fn test_write_cypher_overview_structure() {
        let mut xml = String::new();
        write_cypher_overview(&mut xml);
        assert!(xml.starts_with("<cypher>"));
        assert!(xml.ends_with("</cypher>\n"));
        assert!(xml.contains("<clauses>"));
        assert!(xml.contains("<operators>"));
        assert!(xml.contains("<functions>"));
        assert!(xml.contains("<procedures>"));
        assert!(xml.contains("<patterns>"));
        assert!(xml.contains("<limitations>"));
    }

    // ── write_cypher_topics ────────────────────────────────────────────────

    #[test]
    fn test_write_cypher_topics_unknown() {
        let mut xml = String::new();
        let result = write_cypher_topics(&mut xml, &["NONEXISTENT".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown Cypher topic"));
    }

    #[test]
    fn test_write_cypher_topics_empty_falls_back_to_overview() {
        let mut xml = String::new();
        let result = write_cypher_topics(&mut xml, &[]);
        assert!(result.is_ok());
        assert!(xml.contains("<clauses>"));
    }

    #[test]
    fn test_write_cypher_topics_match() {
        let mut xml = String::new();
        let result = write_cypher_topics(&mut xml, &["MATCH".to_string()]);
        assert!(result.is_ok());
        assert!(xml.contains("<MATCH>"));
        assert!(xml.contains("</MATCH>"));
    }

    #[test]
    fn test_write_cypher_topics_case_insensitive() {
        let mut xml = String::new();
        let result = write_cypher_topics(&mut xml, &["where".to_string()]);
        assert!(result.is_ok());
        assert!(xml.contains("<WHERE>"));
    }

    #[test]
    fn test_write_cypher_topics_multiple() {
        let mut xml = String::new();
        let result = write_cypher_topics(&mut xml, &["MATCH".to_string(), "RETURN".to_string()]);
        assert!(result.is_ok());
        assert!(xml.contains("<MATCH>"));
        assert!(xml.contains("<RETURN>"));
    }

    #[test]
    fn test_write_cypher_topics_order_by_aliases() {
        for alias in &["ORDER BY", "ORDERBY", "ORDER_BY"] {
            let mut xml = String::new();
            let result = write_cypher_topics(&mut xml, &[alias.to_string()]);
            assert!(result.is_ok(), "Failed for alias: {}", alias);
            assert!(xml.contains("<ORDER_BY>"));
        }
    }

    #[test]
    fn test_write_cypher_topics_all_known() {
        // Test that every topic in the known topic list can be rendered
        let topics: Vec<String> = CYPHER_TOPIC_LIST
            .split(", ")
            .map(|s| s.trim().to_string())
            .collect();
        for topic in &topics {
            let mut xml = String::new();
            let result = write_cypher_topics(&mut xml, &[topic.clone()]);
            assert!(result.is_ok(), "Failed for topic: {}", topic);
            assert!(!xml.is_empty(), "Empty output for topic: {}", topic);
        }
    }

    // ── write_fluent_overview ──────────────────────────────────────────────

    #[test]
    fn test_write_fluent_overview_structure() {
        let mut xml = String::new();
        write_fluent_overview(&mut xml);
        assert!(xml.starts_with("<fluent_api>"));
        assert!(xml.ends_with("</fluent_api>\n"));
        assert!(xml.contains("selection"));
        assert!(xml.contains("traversal"));
    }

    // ── write_fluent_topics ────────────────────────────────────────────────

    #[test]
    fn test_write_fluent_topics_unknown() {
        let mut xml = String::new();
        let result = write_fluent_topics(&mut xml, &["NONEXISTENT".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_fluent_topics_empty_falls_back() {
        let mut xml = String::new();
        let result = write_fluent_topics(&mut xml, &[]);
        assert!(result.is_ok());
        assert!(xml.contains("<fluent_api>"));
    }

    #[test]
    fn test_write_fluent_topics_all_known() {
        let topics: Vec<String> = FLUENT_TOPIC_LIST
            .split(", ")
            .map(|s| s.trim().to_string())
            .collect();
        for topic in &topics {
            let mut xml = String::new();
            let result = write_fluent_topics(&mut xml, &[topic.clone()]);
            assert!(result.is_ok(), "Failed for fluent topic: {}", topic);
            assert!(!xml.is_empty(), "Empty output for fluent topic: {}", topic);
        }
    }

    // ── compute_description ────────────────────────────────────────────────

    #[test]
    fn test_compute_description_empty_graph() {
        let g = DirGraph::new();
        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Off,
            &CypherDetail::Off,
            &FluentDetail::Off,
        )
        .unwrap();
        assert!(result.contains("<graph"));
        assert!(result.contains("nodes=\"0\""));
        assert!(result.contains("edges=\"0\""));
    }

    #[test]
    fn test_compute_description_with_nodes() {
        let g = make_graph_with_nodes(&[
            (
                "Person",
                vec![("p1", "Alice", vec![("age", Value::Int64(30))])],
            ),
            ("City", vec![("c1", "London", vec![])]),
        ]);
        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Off,
            &CypherDetail::Off,
            &FluentDetail::Off,
        )
        .unwrap();
        assert!(result.contains("Person"));
        assert!(result.contains("City"));
    }

    #[test]
    fn test_compute_description_focused_types() {
        let g = make_graph_with_nodes(&[
            (
                "Person",
                vec![("p1", "Alice", vec![("age", Value::Int64(30))])],
            ),
            ("City", vec![("c1", "London", vec![])]),
        ]);
        let types = vec!["Person".to_string()];
        let result = compute_description(
            &g,
            Some(&types),
            &ConnectionDetail::Off,
            &CypherDetail::Off,
            &FluentDetail::Off,
        )
        .unwrap();
        assert!(result.contains("Person"));
    }

    #[test]
    fn test_compute_description_focused_type_not_found() {
        let g = DirGraph::new();
        let types = vec!["NonExistent".to_string()];
        let result = compute_description(
            &g,
            Some(&types),
            &ConnectionDetail::Off,
            &CypherDetail::Off,
            &FluentDetail::Off,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_compute_description_cypher_overview() {
        let g = DirGraph::new();
        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Off,
            &CypherDetail::Overview,
            &FluentDetail::Off,
        )
        .unwrap();
        assert!(result.contains("<cypher>"));
        // Standalone mode — should NOT contain <graph>
        assert!(!result.contains("<graph"));
    }

    #[test]
    fn test_compute_description_cypher_topics() {
        let g = DirGraph::new();
        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Off,
            &CypherDetail::Topics(vec!["MATCH".to_string()]),
            &FluentDetail::Off,
        )
        .unwrap();
        assert!(result.contains("<MATCH>"));
    }

    #[test]
    fn test_compute_description_fluent_overview() {
        let g = DirGraph::new();
        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Off,
            &CypherDetail::Off,
            &FluentDetail::Overview,
        )
        .unwrap();
        assert!(result.contains("<fluent_api>"));
    }

    #[test]
    fn test_compute_description_connections_overview() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "a", vec![])]),
            ("B", vec![("b1", "b", vec![])]),
        ]);
        add_edge(&mut g, "A", "B", "REL", vec![]);

        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Overview,
            &CypherDetail::Off,
            &FluentDetail::Off,
        )
        .unwrap();
        assert!(result.contains("<connections>"));
        assert!(result.contains("REL"));
    }

    // ── build_inventory ────────────────────────────────────────────────────

    #[test]
    fn test_build_inventory_basic() {
        let g = make_graph_with_nodes(&[
            ("Person", vec![("p1", "Alice", vec![])]),
            ("City", vec![("c1", "London", vec![])]),
        ]);
        let result = build_inventory(&g);
        assert!(result.starts_with("<graph"));
        assert!(result.ends_with("</graph>"));
        assert!(result.contains("Person"));
        assert!(result.contains("City"));
    }

    #[test]
    fn test_build_inventory_with_parent_types() {
        let mut g = make_graph_with_nodes(&[
            ("Core", vec![("c1", "main", vec![])]),
            ("Sub", vec![("s1", "child", vec![])]),
        ]);
        g.parent_types.insert("Sub".to_string(), "Core".to_string());

        let result = build_inventory(&g);
        // Supporting types should be noted
        assert!(result.contains("core="));
        assert!(result.contains("supporting="));
    }

    // ── build_inventory_with_detail ────────────────────────────────────────

    #[test]
    fn test_build_inventory_with_detail() {
        let g = make_graph_with_nodes(&[(
            "Person",
            vec![
                ("p1", "Alice", vec![("age", Value::Int64(30))]),
                ("p2", "Bob", vec![("age", Value::Int64(25))]),
            ],
        )]);
        let result = build_inventory_with_detail(&g);
        assert!(result.contains("<type name=\"Person\""));
        assert!(result.contains("<samples>"));
    }

    // ── write_connections_overview ──────────────────────────────────────────

    #[test]
    fn test_write_connections_overview_empty() {
        let g = DirGraph::new();
        let mut xml = String::new();
        write_connections_overview(&mut xml, &g);
        assert!(xml.contains("<connections/>"));
    }

    #[test]
    fn test_write_connections_overview_with_edges() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "a", vec![])]),
            ("B", vec![("b1", "b", vec![])]),
        ]);
        add_edge(&mut g, "A", "B", "LINKS", vec![]);
        let mut xml = String::new();
        write_connections_overview(&mut xml, &g);
        assert!(xml.contains("type=\"LINKS\""));
    }

    // ── write_connections_detail ────────────────────────────────────────────

    #[test]
    fn test_write_connections_detail_unknown_type() {
        let g = DirGraph::new();
        let mut xml = String::new();
        let result = write_connections_detail(&mut xml, &g, &["NOPE".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_write_connections_detail_valid() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "nodeA", vec![])]),
            ("B", vec![("b1", "nodeB", vec![])]),
        ]);
        add_edge(
            &mut g,
            "A",
            "B",
            "LINKS",
            vec![("weight", Value::Float64(0.5))],
        );
        let mut xml = String::new();
        let result = write_connections_detail(&mut xml, &g, &["LINKS".to_string()]);
        assert!(result.is_ok());
        assert!(xml.contains("<LINKS"));
        assert!(xml.contains("<endpoints>"));
        assert!(xml.contains("<samples>"));
    }

    // ── compute_edge_property_stats ────────────────────────────────────────

    #[test]
    fn test_compute_edge_property_stats_no_edges() {
        let g = DirGraph::new();
        let stats = compute_edge_property_stats(&g, "NONEXISTENT", 10);
        assert!(stats.is_empty());
    }

    #[test]
    fn test_compute_edge_property_stats() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "a", vec![])]),
            ("B", vec![("b1", "b", vec![])]),
        ]);
        add_edge(
            &mut g,
            "A",
            "B",
            "REL",
            vec![("weight", Value::Float64(1.5))],
        );
        let stats = compute_edge_property_stats(&g, "REL", 10);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].property_name, "weight");
        assert_eq!(stats[0].non_null, 1);
        assert_eq!(stats[0].type_string, "float");
    }

    // ── mcp_quickstart ─────────────────────────────────────────────────────

    #[test]
    fn test_mcp_quickstart() {
        let result = mcp_quickstart();
        assert!(result.contains("<mcp_quickstart"));
        assert!(result.contains("</mcp_quickstart>"));
        assert!(result.contains("pip install kglite"));
    }

    // ── compute_type_capabilities ──────────────────────────────────────────

    #[test]
    fn test_compute_type_capabilities_empty() {
        let g = DirGraph::new();
        let caps = compute_type_capabilities(&g);
        assert!(caps.is_empty());
    }

    #[test]
    fn test_compute_type_capabilities_with_timeseries() {
        let mut g = make_graph_with_nodes(&[("Sensor", vec![("s1", "s", vec![])])]);
        g.timeseries_configs.insert(
            "Sensor".to_string(),
            super::super::timeseries::TimeseriesConfig {
                resolution: "daily".to_string(),
                channels: vec![],
                units: HashMap::new(),
                bin_type: None,
            },
        );
        let caps = compute_type_capabilities(&g);
        assert!(caps["Sensor"].has_timeseries);
        assert!(!caps["Sensor"].has_location);
    }

    #[test]
    fn test_compute_type_capabilities_with_point_metadata() {
        let mut g = make_graph_with_nodes(&[("Place", vec![("pl1", "here", vec![])])]);
        let mut meta = HashMap::new();
        meta.insert("coords".to_string(), "Point".to_string());
        g.node_type_metadata.insert("Place".to_string(), meta);
        let caps = compute_type_capabilities(&g);
        assert!(caps["Place"].has_location);
    }

    // ── Large graph inventory (>15 types) ──────────────────────────────────

    #[test]
    fn test_compute_description_large_graph_uses_inventory() {
        // Create a graph with >15 core types to trigger the inventory path
        let types: Vec<(&str, Vec<(&str, &str, Vec<(&str, Value)>)>)> = (0..20)
            .map(|i| {
                let type_name: &'static str = Box::leak(format!("Type{}", i).into_boxed_str());
                let id: &'static str = Box::leak(format!("id{}", i).into_boxed_str());
                let title: &'static str = Box::leak(format!("t{}", i).into_boxed_str());
                let nodes = vec![(id, title, vec![])];
                (type_name, nodes)
            })
            .collect();
        let g = make_graph_with_nodes(&types);

        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Off,
            &CypherDetail::Off,
            &FluentDetail::Off,
        )
        .unwrap();
        // Should use build_inventory (compact) path
        assert!(result.contains("<types count="));
        // Should have the hint
        assert!(result.contains("describe(types="));
    }

    // ── write_type_detail ──────────────────────────────────────────────────

    #[test]
    fn test_write_type_detail_basic() {
        let g = make_graph_with_nodes(&[(
            "Person",
            vec![
                ("p1", "Alice", vec![("age", Value::Int64(30))]),
                ("p2", "Bob", vec![("age", Value::Int64(25))]),
            ],
        )]);
        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        let mut xml = String::new();
        write_type_detail(&mut xml, &g, "Person", &caps, "  ", None);
        assert!(xml.contains("<type name=\"Person\" count=\"2\""));
        assert!(xml.contains("</type>"));
        assert!(xml.contains("<samples>"));
    }

    #[test]
    fn test_write_type_detail_with_aliases() {
        let mut g = make_graph_with_nodes(&[("Well", vec![("w1", "Well-1", vec![])])]);
        g.id_field_aliases
            .insert("Well".to_string(), "npdid".to_string());
        g.title_field_aliases
            .insert("Well".to_string(), "well_name".to_string());

        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        let mut xml = String::new();
        write_type_detail(&mut xml, &g, "Well", &caps, "", None);
        assert!(xml.contains("id_alias=\"npdid\""));
        assert!(xml.contains("title_alias=\"well_name\""));
    }

    // ── sample_unique_values ───────────────────────────────────────────────

    #[test]
    fn test_sample_unique_values_basic() {
        let g = make_graph_with_nodes(&[(
            "Item",
            vec![
                ("i1", "A", vec![("color", Value::String("red".into()))]),
                ("i2", "B", vec![("color", Value::String("blue".into()))]),
                ("i3", "C", vec![("color", Value::String("red".into()))]),
            ],
        )]);
        let vals = sample_unique_values(&g, "Item", "color", 100);
        assert_eq!(vals.len(), 2);
        assert!(vals.contains("red"));
        assert!(vals.contains("blue"));
    }

    #[test]
    fn test_sample_unique_values_max_limit() {
        let g = make_graph_with_nodes(&[(
            "Item",
            vec![
                ("i1", "A", vec![("x", Value::Int64(1))]),
                ("i2", "B", vec![("x", Value::Int64(2))]),
                ("i3", "C", vec![("x", Value::Int64(3))]),
            ],
        )]);
        let vals = sample_unique_values(&g, "Item", "x", 2);
        assert_eq!(vals.len(), 2);
    }

    #[test]
    fn test_sample_unique_values_nonexistent_type() {
        let g = DirGraph::new();
        let vals = sample_unique_values(&g, "Nothing", "x", 10);
        assert!(vals.is_empty());
    }

    #[test]
    fn test_sample_unique_values_with_nulls() {
        let g = make_graph_with_nodes(&[(
            "Item",
            vec![
                ("i1", "A", vec![("x", Value::Int64(1))]),
                ("i2", "B", vec![("x", Value::Null)]),
            ],
        )]);
        let vals = sample_unique_values(&g, "Item", "x", 10);
        assert_eq!(vals.len(), 1);
    }

    // ── Special characters in node names ───────────────────────────────────

    #[test]
    fn test_description_with_special_chars_in_names() {
        let g = make_graph_with_nodes(&[("Type<A>&B", vec![("id&1", "title\"quoted\"", vec![])])]);
        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Off,
            &CypherDetail::Off,
            &FluentDetail::Off,
        )
        .unwrap();
        // Should be escaped in XML
        assert!(result.contains("&amp;"));
        assert!(result.contains("&lt;"));
        assert!(result.contains("&gt;"));
    }

    // ── Connections with edge properties ───────────────────────────────────

    #[test]
    fn test_connection_map_with_edge_properties() {
        let g = DirGraph::new();
        let stats = vec![ConnectionTypeStats {
            connection_type: "HAS".into(),
            count: 10,
            source_types: vec!["A".into()],
            target_types: vec!["B".into()],
            property_names: vec!["weight".into(), "type".into()],
        }];
        let mut xml = String::new();
        write_connection_map(&mut xml, &g, &stats);
        assert!(xml.contains("properties=\"weight,type\""));
    }

    // ── Multiple connection types ──────────────────────────────────────────

    #[test]
    fn test_compute_schema_multiple_edge_types() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "a", vec![])]),
            ("B", vec![("b1", "b", vec![])]),
            ("C", vec![("c1", "c", vec![])]),
        ]);
        add_edge(&mut g, "A", "B", "KNOWS", vec![]);
        add_edge(&mut g, "B", "C", "MANAGES", vec![]);

        let schema = compute_schema(&g);
        assert_eq!(schema.node_count, 3);
        assert_eq!(schema.edge_count, 2);
        assert_eq!(schema.connection_types.len(), 2);
        let conn_names: Vec<&str> = schema
            .connection_types
            .iter()
            .map(|c| c.connection_type.as_str())
            .collect();
        assert!(conn_names.contains(&"KNOWS"));
        assert!(conn_names.contains(&"MANAGES"));
    }

    // ── write_connection_map filters supporting types ──────────────────────

    #[test]
    fn test_write_connection_map_filters_supporting_type_edges() {
        let mut g = DirGraph::new();
        // Set up parent_types so "Sub" is a child of "Core"
        g.parent_types.insert("Sub".to_string(), "Core".to_string());

        // A connection where all sources are children of the target → should be filtered
        let stats = vec![ConnectionTypeStats {
            connection_type: "OF_CORE".into(),
            count: 5,
            source_types: vec!["Sub".into()],
            target_types: vec!["Core".into()],
            property_names: vec![],
        }];
        let mut xml = String::new();
        write_connection_map(&mut xml, &g, &stats);
        // The entire connection should be filtered out
        assert!(xml.contains("<connections/>"));
    }

    // ── compute_join_candidates ─────────────────────────────────────────────

    #[test]
    fn test_compute_join_candidates_with_overlap() {
        let mut g = make_graph_with_nodes(&[
            (
                "TypeX",
                vec![
                    ("x1", "A", vec![("name", Value::String("Alice".into()))]),
                    ("x2", "B", vec![("name", Value::String("Bob".into()))]),
                ],
            ),
            (
                "TypeY",
                vec![
                    ("y1", "C", vec![("name", Value::String("Alice".into()))]),
                    ("y2", "D", vec![("name", Value::String("Charlie".into()))]),
                ],
            ),
        ]);
        // Add metadata so types_compatible works
        let mut meta_x = HashMap::new();
        meta_x.insert("name".to_string(), "String".to_string());
        g.node_type_metadata.insert("TypeX".to_string(), meta_x);
        let mut meta_y = HashMap::new();
        meta_y.insert("name".to_string(), "String".to_string());
        g.node_type_metadata.insert("TypeY".to_string(), meta_y);

        let connected_pairs = HashSet::new(); // No existing connections
        let candidates = compute_join_candidates(&g, &connected_pairs, 10, 100);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].overlap, 1); // "Alice" overlaps
        assert_eq!(candidates[0].left_prop, "name");
    }

    #[test]
    fn test_compute_join_candidates_skips_connected_pairs() {
        let mut g = make_graph_with_nodes(&[
            (
                "TypeX",
                vec![("x1", "A", vec![("name", Value::String("Alice".into()))])],
            ),
            (
                "TypeY",
                vec![("y1", "B", vec![("name", Value::String("Alice".into()))])],
            ),
        ]);
        let mut meta = HashMap::new();
        meta.insert("name".to_string(), "String".to_string());
        g.node_type_metadata
            .insert("TypeX".to_string(), meta.clone());
        g.node_type_metadata.insert("TypeY".to_string(), meta);

        // Mark them as already connected
        let mut connected_pairs = HashSet::new();
        connected_pairs.insert(("TypeX".to_string(), "TypeY".to_string()));
        connected_pairs.insert(("TypeY".to_string(), "TypeX".to_string()));

        let candidates = compute_join_candidates(&g, &connected_pairs, 10, 100);
        assert!(candidates.is_empty());
    }

    // ── Additional coverage tests ─────────────────────────────────────────

    // ── value_type_name extended ──────────────────────────────────────────

    #[test]
    fn test_value_type_name_datetime() {
        let dt = chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        assert_eq!(value_type_name(&Value::DateTime(dt)), "datetime");
    }

    #[test]
    fn test_value_type_name_uniqueid() {
        assert_eq!(value_type_name(&Value::UniqueId(42)), "uniqueid");
    }

    #[test]
    fn test_value_type_name_noderef() {
        assert_eq!(value_type_name(&Value::NodeRef(0)), "noderef");
    }

    #[test]
    fn test_value_type_name_edgeref() {
        assert_eq!(
            value_type_name(&Value::EdgeRef {
                edge_idx: 0,
                src_idx: 0,
                dst_idx: 1,
            }),
            "edgeref"
        );
    }

    // ── value_display_compact extended ────────────────────────────────────

    #[test]
    fn test_value_display_compact_datetime() {
        let dt = chrono::NaiveDate::from_ymd_opt(2020, 6, 15).unwrap();
        let result = value_display_compact(&Value::DateTime(dt));
        assert!(result.contains("2020"));
    }

    #[test]
    fn test_value_display_compact_uniqueid() {
        let result = value_display_compact(&Value::UniqueId(12345));
        assert_eq!(result, "12345");
    }

    #[test]
    fn test_value_display_compact_noderef() {
        assert_eq!(value_display_compact(&Value::NodeRef(42)), "node#42");
    }

    #[test]
    fn test_value_display_compact_edgeref() {
        assert_eq!(
            value_display_compact(&Value::EdgeRef {
                edge_idx: 7,
                src_idx: 0,
                dst_idx: 1,
            }),
            "edge#7"
        );
    }

    #[test]
    fn test_value_display_compact_string_exactly_40_chars() {
        // 40 chars should NOT be truncated
        let s = "a".repeat(40);
        let v = Value::String(s.clone());
        assert_eq!(value_display_compact(&v), s);
    }

    #[test]
    fn test_value_display_compact_string_41_chars_truncated() {
        // 41 chars should be truncated
        let s = "a".repeat(41);
        let result = value_display_compact(&Value::String(s));
        assert!(result.ends_with("..."));
        assert_eq!(result.len(), 40);
    }

    // ── is_null_value extended ────────────────────────────────────────────

    #[test]
    fn test_is_null_value_nan_negative() {
        assert!(is_null_value(&Value::Float64(f64::NEG_INFINITY * 0.0))); // NaN
    }

    #[test]
    fn test_is_null_value_point_not_null() {
        assert!(!is_null_value(&Value::Point { lat: 0.0, lon: 0.0 }));
    }

    // ── compute_property_stats max_values=0 ──────────────────────────────

    #[test]
    fn test_compute_property_stats_max_values_zero() {
        let g = make_graph_with_nodes(&[(
            "Item",
            vec![
                ("i1", "A", vec![("color", Value::String("red".into()))]),
                ("i2", "B", vec![("color", Value::String("blue".into()))]),
            ],
        )]);
        let stats = compute_property_stats(&g, "Item", 0, None).unwrap();
        let color_stat = stats.iter().find(|s| s.property_name == "color").unwrap();
        // max_values=0 means values should be None
        assert!(color_stat.values.is_none());
        assert_eq!(color_stat.unique, 2);
        assert_eq!(color_stat.non_null, 2);
    }

    // ── compute_property_stats with metadata ─────────────────────────────

    #[test]
    fn test_compute_property_stats_uses_metadata_type() {
        let mut g = make_graph_with_nodes(&[(
            "Item",
            vec![("i1", "A", vec![("score", Value::Float64(3.14))])],
        )]);
        let mut meta = HashMap::new();
        meta.insert("score".to_string(), "Float64".to_string());
        g.node_type_metadata.insert("Item".to_string(), meta);

        let stats = compute_property_stats(&g, "Item", 15, None).unwrap();
        let score_stat = stats.iter().find(|s| s.property_name == "score").unwrap();
        // Should use metadata type string "Float64" instead of "float"
        assert_eq!(score_stat.type_string, "Float64");
    }

    // ── compute_property_stats value_cap exceeded ────────────────────────

    #[test]
    fn test_compute_property_stats_value_cap_exceeded() {
        // Create nodes with many unique values exceeding max_values
        let nodes: Vec<(&str, &str, Vec<(&str, Value)>)> = (0..20)
            .map(|i| {
                let id: &'static str = Box::leak(format!("id{}", i).into_boxed_str());
                let title: &'static str = Box::leak(format!("t{}", i).into_boxed_str());
                let props: Vec<(&str, Value)> =
                    vec![("val", Value::String(format!("v{}", i).into()))];
                (id, title, props)
            })
            .collect();
        let g = make_graph_with_nodes(&[("Many", nodes)]);

        // max_values=5, but there are 20 unique values
        let stats = compute_property_stats(&g, "Many", 5, None).unwrap();
        let val_stat = stats.iter().find(|s| s.property_name == "val").unwrap();
        // When unique > max_values, values should be None
        assert!(val_stat.values.is_none());
    }

    // ── write_extensions with spatial ─────────────────────────────────────

    #[test]
    fn test_write_extensions_with_spatial() {
        let mut g = DirGraph::new();
        g.spatial_configs.insert(
            "City".to_string(),
            crate::graph::schema::SpatialConfig {
                location: Some(("lat".to_string(), "lon".to_string())),
                geometry: None,
                points: HashMap::new(),
                shapes: HashMap::new(),
            },
        );
        let mut xml = String::new();
        write_extensions(&mut xml, &g);
        assert!(xml.contains("<spatial"));
    }

    #[test]
    fn test_write_extensions_with_spatial_from_metadata() {
        let mut g = DirGraph::new();
        let mut meta = HashMap::new();
        meta.insert("coords".to_string(), "point".to_string());
        g.node_type_metadata.insert("Place".to_string(), meta);
        let mut xml = String::new();
        write_extensions(&mut xml, &g);
        assert!(xml.contains("<spatial"));
    }

    // ── write_extensions with edges (connections hint) ────────────────────

    #[test]
    fn test_write_extensions_with_edges() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "a", vec![])]),
            ("B", vec![("b1", "b", vec![])]),
        ]);
        add_edge(&mut g, "A", "B", "REL", vec![]);
        let mut xml = String::new();
        write_extensions(&mut xml, &g);
        assert!(xml.contains("<connections hint="));
    }

    #[test]
    fn test_write_extensions_no_edges_no_connections_hint() {
        let g = DirGraph::new();
        let mut xml = String::new();
        write_extensions(&mut xml, &g);
        assert!(!xml.contains("<connections hint="));
    }

    // ── write_type_detail with spatial config ────────────────────────────

    #[test]
    fn test_write_type_detail_with_spatial_location() {
        let mut g = make_graph_with_nodes(&[(
            "City",
            vec![("c1", "Oslo", vec![("lat", Value::Float64(59.9))])],
        )]);
        g.spatial_configs.insert(
            "City".to_string(),
            crate::graph::schema::SpatialConfig {
                location: Some(("lat".to_string(), "lon".to_string())),
                geometry: None,
                points: HashMap::new(),
                shapes: HashMap::new(),
            },
        );
        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: true,
            has_geometry: false,
            has_embeddings: false,
        };
        let mut xml = String::new();
        write_type_detail(&mut xml, &g, "City", &caps, "", None);
        assert!(xml.contains("<spatial"));
        assert!(xml.contains("location=\"lat,lon\""));
    }

    #[test]
    fn test_write_type_detail_with_spatial_geometry() {
        let mut g = make_graph_with_nodes(&[("Block", vec![("b1", "Block1", vec![])])]);
        g.spatial_configs.insert(
            "Block".to_string(),
            crate::graph::schema::SpatialConfig {
                location: None,
                geometry: Some("wkt_col".to_string()),
                points: HashMap::new(),
                shapes: HashMap::new(),
            },
        );
        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: true,
            has_embeddings: false,
        };
        let mut xml = String::new();
        write_type_detail(&mut xml, &g, "Block", &caps, "", None);
        assert!(xml.contains("geometry=\"wkt_col\""));
    }

    // ── write_type_detail with timeseries ─────────────────────────────────

    #[test]
    fn test_write_type_detail_with_timeseries() {
        let mut g = make_graph_with_nodes(&[("Sensor", vec![("s1", "Sensor1", vec![])])]);
        let mut units = HashMap::new();
        units.insert("temp".to_string(), "°C".to_string());
        g.timeseries_configs.insert(
            "Sensor".to_string(),
            super::super::timeseries::TimeseriesConfig {
                resolution: "daily".to_string(),
                channels: vec!["temp".to_string(), "pressure".to_string()],
                units,
                bin_type: None,
            },
        );
        let caps = TypeCapabilities {
            has_timeseries: true,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        let mut xml = String::new();
        write_type_detail(&mut xml, &g, "Sensor", &caps, "", None);
        assert!(xml.contains("<timeseries"));
        assert!(xml.contains("resolution=\"daily\""));
        assert!(xml.contains("channels=\"temp,pressure\""));
        assert!(xml.contains("units=\"temp=°C\""));
    }

    // ── write_type_detail with supporting children ────────────────────────

    #[test]
    fn test_write_type_detail_with_supporting_children() {
        let mut g = make_graph_with_nodes(&[
            ("Parent", vec![("p1", "main", vec![])]),
            ("ChildA", vec![("ca1", "ca", vec![])]),
            ("ChildB", vec![("cb1", "cb", vec![])]),
        ]);
        g.parent_types
            .insert("ChildA".to_string(), "Parent".to_string());
        g.parent_types
            .insert("ChildB".to_string(), "Parent".to_string());

        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        let mut xml = String::new();
        write_type_detail(&mut xml, &g, "Parent", &caps, "", None);
        assert!(xml.contains("<supporting>"));
        assert!(xml.contains("ChildA"));
        assert!(xml.contains("ChildB"));
    }

    // ── write_type_detail with temporal config ────────────────────────────

    #[test]
    fn test_write_type_detail_with_temporal_config() {
        let mut g = make_graph_with_nodes(&[("Contract", vec![("c1", "Contract1", vec![])])]);
        g.temporal_node_configs.insert(
            "Contract".to_string(),
            crate::graph::schema::TemporalConfig {
                valid_from: "start_date".to_string(),
                valid_to: "end_date".to_string(),
            },
        );
        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        let mut xml = String::new();
        write_type_detail(&mut xml, &g, "Contract", &caps, "", None);
        assert!(xml.contains("temporal_from=\"start_date\""));
        assert!(xml.contains("temporal_to=\"end_date\""));
    }

    // ── write_type_detail with pre-computed neighbor cache ────────────────

    #[test]
    fn test_write_type_detail_with_neighbor_cache() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "nodeA", vec![])]),
            ("B", vec![("b1", "nodeB", vec![])]),
        ]);
        add_edge(&mut g, "A", "B", "LINKS", vec![]);

        let all_neighbors = compute_all_neighbors_schemas(&g);
        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        let mut xml = String::new();
        write_type_detail(&mut xml, &g, "A", &caps, "", Some(&all_neighbors));
        assert!(xml.contains("<connections>"));
        assert!(xml.contains("type=\"LINKS\""));
        assert!(xml.contains("target=\"B\""));
    }

    // ── write_type_detail with no properties ──────────────────────────────

    #[test]
    fn test_write_type_detail_no_custom_properties() {
        let g = make_graph_with_nodes(&[("Empty", vec![("e1", "E", vec![])])]);
        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        let mut xml = String::new();
        write_type_detail(&mut xml, &g, "Empty", &caps, "  ", None);
        assert!(xml.contains("<type name=\"Empty\" count=\"1\""));
        // Should still have samples
        assert!(xml.contains("<samples>"));
    }

    // ── build_focused_detail ──────────────────────────────────────────────

    #[test]
    fn test_build_focused_detail_multiple_types() {
        let g = make_graph_with_nodes(&[
            ("Person", vec![("p1", "Alice", vec![])]),
            ("City", vec![("c1", "London", vec![])]),
            ("Other", vec![("o1", "X", vec![])]),
        ]);
        let types = vec!["Person".to_string(), "City".to_string()];
        let result = build_focused_detail(&g, &types).unwrap();
        assert!(result.contains("Person"));
        assert!(result.contains("City"));
        // "Other" should NOT be included
        assert!(!result.contains("Other"));
    }

    #[test]
    fn test_build_focused_detail_type_not_found() {
        let g = make_graph_with_nodes(&[("Person", vec![("p1", "Alice", vec![])])]);
        let types = vec!["NonExistent".to_string()];
        let result = build_focused_detail(&g, &types);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("not found"));
        assert!(err.contains("Person")); // Should list available types
    }

    // ── build_inventory_with_detail with edges ───────────────────────────

    #[test]
    fn test_build_inventory_with_detail_with_edges() {
        let mut g = make_graph_with_nodes(&[
            (
                "Person",
                vec![("p1", "Alice", vec![("age", Value::Int64(30))])],
            ),
            ("City", vec![("c1", "London", vec![])]),
        ]);
        add_edge(&mut g, "Person", "City", "LIVES_IN", vec![]);

        let result = build_inventory_with_detail(&g);
        assert!(result.contains("Person"));
        assert!(result.contains("City"));
        assert!(result.contains("LIVES_IN"));
        assert!(result.contains("<connections>"));
    }

    // ── build_inventory with edges ────────────────────────────────────────

    #[test]
    fn test_build_inventory_with_edges() {
        let mut g = make_graph_with_nodes(&[
            ("Person", vec![("p1", "Alice", vec![])]),
            ("City", vec![("c1", "London", vec![])]),
        ]);
        add_edge(&mut g, "Person", "City", "LIVES_IN", vec![]);

        let result = build_inventory(&g);
        assert!(result.contains("LIVES_IN"));
    }

    // ── compute_description standalone modes ─────────────────────────────

    #[test]
    fn test_compute_description_connections_topics() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "nodeA", vec![])]),
            ("B", vec![("b1", "nodeB", vec![])]),
        ]);
        add_edge(
            &mut g,
            "A",
            "B",
            "LINKS",
            vec![("weight", Value::Float64(0.9))],
        );

        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Topics(vec!["LINKS".to_string()]),
            &CypherDetail::Off,
            &FluentDetail::Off,
        )
        .unwrap();
        assert!(result.contains("<LINKS"));
        assert!(result.contains("<endpoints>"));
        // Standalone mode — should NOT contain <graph>
        assert!(!result.contains("<graph"));
    }

    #[test]
    fn test_compute_description_connections_topics_not_found() {
        let g = DirGraph::new();
        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Topics(vec!["NOPE".to_string()]),
            &CypherDetail::Off,
            &FluentDetail::Off,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_compute_description_fluent_topics() {
        let g = DirGraph::new();
        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Off,
            &CypherDetail::Off,
            &FluentDetail::Topics(vec!["select".to_string()]),
        )
        .unwrap();
        assert!(result.contains("<selection>"));
    }

    #[test]
    fn test_compute_description_multiple_standalone_axes() {
        let g = DirGraph::new();
        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Off,
            &CypherDetail::Overview,
            &FluentDetail::Overview,
        )
        .unwrap();
        assert!(result.contains("<cypher>"));
        assert!(result.contains("<fluent_api>"));
        assert!(!result.contains("<graph"));
    }

    // ── write_exploration_hints with join candidates ──────────────────────

    #[test]
    fn test_write_exploration_hints_with_join_candidates() {
        let mut g = make_graph_with_nodes(&[
            (
                "TypeX",
                vec![("x1", "A", vec![("name", Value::String("Shared".into()))])],
            ),
            (
                "TypeY",
                vec![("y1", "B", vec![("name", Value::String("Shared".into()))])],
            ),
            ("TypeZ", vec![("z1", "C", vec![])]),
        ]);
        // Connect Z to X, leave Y disconnected from X
        add_edge(&mut g, "TypeZ", "TypeX", "REL", vec![]);

        // Add metadata for join candidate detection
        let mut meta = HashMap::new();
        meta.insert("name".to_string(), "String".to_string());
        g.node_type_metadata
            .insert("TypeX".to_string(), meta.clone());
        g.node_type_metadata.insert("TypeY".to_string(), meta);

        let stats = compute_connection_type_stats(&g);
        let mut xml = String::new();
        write_exploration_hints(&mut xml, &g, &stats);
        // TypeY is disconnected — should appear
        assert!(xml.contains("TypeY"));
    }

    // ── write_exploration_hints no edges ──────────────────────────────────

    #[test]
    fn test_write_exploration_hints_no_edges() {
        // >= 2 types but 0 edges → no hints (all disconnected = not useful)
        let g = make_graph_with_nodes(&[
            ("TypeA", vec![("a1", "a", vec![])]),
            ("TypeB", vec![("b1", "b", vec![])]),
        ]);
        let stats: Vec<ConnectionTypeStats> = Vec::new();
        let mut xml = String::new();
        write_exploration_hints(&mut xml, &g, &stats);
        assert!(xml.is_empty());
    }

    // ── write_connection_map with temporal edges ─────────────────────────

    #[test]
    fn test_write_connection_map_with_temporal_edges() {
        let mut g = DirGraph::new();
        g.temporal_edge_configs.insert(
            "EMPLOYED".to_string(),
            vec![crate::graph::schema::TemporalConfig {
                valid_from: "start".to_string(),
                valid_to: "end".to_string(),
            }],
        );
        let stats = vec![ConnectionTypeStats {
            connection_type: "EMPLOYED".into(),
            count: 10,
            source_types: vec!["Person".into()],
            target_types: vec!["Company".into()],
            property_names: vec![],
        }];
        let mut xml = String::new();
        write_connection_map(&mut xml, &g, &stats);
        assert!(xml.contains("temporal_from=\"start\""));
        assert!(xml.contains("temporal_to=\"end\""));
    }

    // ── compute_connection_type_stats multiple edge types ─────────────────

    #[test]
    fn test_compute_connection_type_stats_multiple_types_fallback() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "a", vec![])]),
            ("B", vec![("b1", "b", vec![])]),
            ("C", vec![("c1", "c", vec![])]),
        ]);
        add_edge(&mut g, "A", "B", "KNOWS", vec![]);
        add_edge(&mut g, "B", "C", "WORKS_AT", vec![]);

        let stats = compute_connection_type_stats(&g);
        assert_eq!(stats.len(), 2);
        // Should be sorted by connection type
        assert_eq!(stats[0].connection_type, "KNOWS");
        assert_eq!(stats[1].connection_type, "WORKS_AT");
    }

    // ── sample_unique_values with different value types ──────────────────

    #[test]
    fn test_sample_unique_values_float() {
        let g = make_graph_with_nodes(&[(
            "Item",
            vec![
                ("i1", "A", vec![("x", Value::Float64(1.5))]),
                ("i2", "B", vec![("x", Value::Float64(2.5))]),
            ],
        )]);
        let vals = sample_unique_values(&g, "Item", "x", 100);
        assert_eq!(vals.len(), 2);
    }

    #[test]
    fn test_sample_unique_values_uniqueid() {
        let g = make_graph_with_nodes(&[(
            "Item",
            vec![
                ("i1", "A", vec![("uid", Value::UniqueId(1))]),
                ("i2", "B", vec![("uid", Value::UniqueId(2))]),
            ],
        )]);
        let vals = sample_unique_values(&g, "Item", "uid", 100);
        assert_eq!(vals.len(), 2);
    }

    #[test]
    fn test_sample_unique_values_missing_property() {
        let g = make_graph_with_nodes(&[("Item", vec![("i1", "A", vec![("x", Value::Int64(1))])])]);
        let vals = sample_unique_values(&g, "Item", "nonexistent", 100);
        assert!(vals.is_empty());
    }

    // ── compute_neighbors_schema with multiple edges ─────────────────────

    #[test]
    fn test_compute_neighbors_schema_multiple_targets() {
        let mut g = make_graph_with_nodes(&[
            (
                "Person",
                vec![("p1", "Alice", vec![]), ("p2", "Bob", vec![])],
            ),
            ("City", vec![("c1", "London", vec![])]),
            ("Country", vec![("co1", "UK", vec![])]),
        ]);
        add_edge(&mut g, "Person", "City", "LIVES_IN", vec![]);
        add_edge_indexed(&mut g, "Person", 0, "Country", 0, "BORN_IN");

        let schema = compute_neighbors_schema(&g, "Person").unwrap();
        assert_eq!(schema.outgoing.len(), 2);
        // Should be sorted by (connection_type, other_type)
        assert_eq!(schema.outgoing[0].connection_type, "BORN_IN");
        assert_eq!(schema.outgoing[1].connection_type, "LIVES_IN");
    }

    // ── write_conventions with all cap types ──────────────────────────────

    #[test]
    fn test_write_conventions_with_all_capabilities() {
        let mut caps: HashMap<String, TypeCapabilities> = HashMap::new();
        caps.insert(
            "Full".into(),
            TypeCapabilities {
                has_timeseries: true,
                has_location: true,
                has_geometry: true,
                has_embeddings: true,
            },
        );
        let mut xml = String::new();
        write_conventions(&mut xml, &caps);
        assert!(xml.contains("location"));
        assert!(xml.contains("geometry"));
        assert!(xml.contains("timeseries"));
        assert!(xml.contains("embeddings"));
    }

    // ── compute_schema node types are sorted ─────────────────────────────

    #[test]
    fn test_compute_schema_sorted_types() {
        let g = make_graph_with_nodes(&[
            ("Zebra", vec![("z1", "z", vec![])]),
            ("Alpha", vec![("a1", "a", vec![])]),
            ("Middle", vec![("m1", "m", vec![])]),
        ]);
        let schema = compute_schema(&g);
        let type_names: Vec<&str> = schema.node_types.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(type_names, vec!["Alpha", "Middle", "Zebra"]);
    }

    // ── write_connections_detail with multiple properties and samples ─────

    #[test]
    fn test_write_connections_detail_with_properties() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "nodeA", vec![]), ("a2", "nodeA2", vec![])]),
            ("B", vec![("b1", "nodeB", vec![])]),
        ]);
        add_edge(
            &mut g,
            "A",
            "B",
            "HAS",
            vec![
                ("weight", Value::Float64(0.5)),
                ("label", Value::String("test".into())),
            ],
        );
        add_edge_indexed(&mut g, "A", 1, "B", 0, "HAS");

        let mut xml = String::new();
        let result = write_connections_detail(&mut xml, &g, &["HAS".to_string()]);
        assert!(result.is_ok());
        assert!(xml.contains("<HAS"));
        assert!(xml.contains("count=\"2\""));
        assert!(xml.contains("<properties>"));
    }

    // ── compute_edge_property_stats with mixed types ─────────────────────

    #[test]
    fn test_compute_edge_property_stats_multiple_props() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "a", vec![])]),
            ("B", vec![("b1", "b", vec![])]),
        ]);
        add_edge(
            &mut g,
            "A",
            "B",
            "REL",
            vec![
                ("w", Value::Float64(1.0)),
                ("label", Value::String("x".into())),
            ],
        );
        let stats = compute_edge_property_stats(&g, "REL", 10);
        assert_eq!(stats.len(), 2);
        // Should be sorted by property name
        assert_eq!(stats[0].property_name, "label");
        assert_eq!(stats[1].property_name, "w");
    }

    // ── compute_description with read_only graph ─────────────────────────

    #[test]
    fn test_compute_description_read_only_graph() {
        let mut g = make_graph_with_nodes(&[("A", vec![("a1", "a", vec![])])]);
        g.read_only = true;

        let result = compute_description(
            &g,
            None,
            &ConnectionDetail::Off,
            &CypherDetail::Off,
            &FluentDetail::Off,
        )
        .unwrap();
        assert!(result.contains("<read-only>"));
    }

    // ── write_connections_overview with properties ────────────────────────

    #[test]
    fn test_write_connections_overview_with_properties() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "a", vec![])]),
            ("B", vec![("b1", "b", vec![])]),
        ]);
        add_edge(
            &mut g,
            "A",
            "B",
            "HAS",
            vec![("weight", Value::Float64(1.0))],
        );
        let mut xml = String::new();
        write_connections_overview(&mut xml, &g);
        assert!(xml.contains("type=\"HAS\""));
    }

    // ── fluent topic aliases ──────────────────────────────────────────────

    #[test]
    fn test_write_fluent_topics_aliases() {
        // Test that aliases route to the correct topics
        let aliases = vec![
            ("selection", "<selection>"),
            ("filtering", "<selection>"),
            ("traversal", "<traversal>"),
            ("comparison", "<compare>"),
            ("collect", "<retrieval>"),
            ("calculate", "<statistics>"),
            ("graph_algorithms", "<algorithms>"),
            ("embeddings", "<vectors>"),
            ("search", "<vectors>"),
            ("update", "<mutation>"),
            ("data_loading", "<loading>"),
            ("persistence", "<export>"),
        ];
        for (alias, expected_tag) in aliases {
            let mut xml = String::new();
            let result = write_fluent_topics(&mut xml, &[alias.to_string()]);
            assert!(result.is_ok(), "Failed for fluent alias: {}", alias);
            assert!(
                xml.contains(expected_tag),
                "Alias '{}' did not produce expected tag '{}'",
                alias,
                expected_tag
            );
        }
    }

    // ── cypher topic aliases ──────────────────────────────────────────────

    #[test]
    fn test_write_cypher_topics_delete_alias() {
        let mut xml = String::new();
        let result = write_cypher_topics(&mut xml, &["REMOVE".to_string()]);
        assert!(result.is_ok());
        assert!(xml.contains("<DELETE>"));
    }

    #[test]
    fn test_write_cypher_topics_label_propagation_aliases() {
        for alias in &["LABEL_PROPAGATION", "LABELPROPAGATION"] {
            let mut xml = String::new();
            let result = write_cypher_topics(&mut xml, &[alias.to_string()]);
            assert!(result.is_ok(), "Failed for alias: {}", alias);
            assert!(xml.contains("<label_propagation>"));
        }
    }

    #[test]
    fn test_write_cypher_topics_connected_components_aliases() {
        for alias in &["CONNECTED_COMPONENTS", "CONNECTEDCOMPONENTS"] {
            let mut xml = String::new();
            let result = write_cypher_topics(&mut xml, &[alias.to_string()]);
            assert!(result.is_ok(), "Failed for alias: {}", alias);
            assert!(xml.contains("<connected_components>"));
        }
    }

    // ── compute_description with empty types slice ────────────────────────

    #[test]
    fn test_compute_description_empty_types_slice() {
        let g = make_graph_with_nodes(&[("Person", vec![("p1", "Alice", vec![])])]);
        // Empty slice should fall through to inventory
        let types: Vec<String> = vec![];
        let result = compute_description(
            &g,
            Some(&types),
            &ConnectionDetail::Off,
            &CypherDetail::Off,
            &FluentDetail::Off,
        )
        .unwrap();
        assert!(result.contains("<graph"));
    }

    // ── write_connection_map with source filtering ───────────────────────

    #[test]
    fn test_write_connection_map_filters_empty_sources_after_tier_filter() {
        let mut g = DirGraph::new();
        g.parent_types
            .insert("Child".to_string(), "Parent".to_string());

        // Connection where only source is a supporting type, target is unrelated
        // After filtering supporting types from sources, sources would be empty
        let stats = vec![ConnectionTypeStats {
            connection_type: "SOME_REL".into(),
            count: 3,
            source_types: vec!["Child".into()],
            target_types: vec!["Unrelated".into()],
            property_names: vec![],
        }];
        let mut xml = String::new();
        write_connection_map(&mut xml, &g, &stats);
        // Sources become empty after tier filter → connection should be skipped
        // (the `continue` in the loop handles this)
        assert!(!xml.contains("SOME_REL"));
    }

    // ── write_type_detail indentation ─────────────────────────────────────

    #[test]
    fn test_write_type_detail_uses_correct_indent() {
        let g = make_graph_with_nodes(&[("T", vec![("t1", "test", vec![("x", Value::Int64(1))])])]);
        let caps = TypeCapabilities {
            has_timeseries: false,
            has_location: false,
            has_geometry: false,
            has_embeddings: false,
        };
        let mut xml = String::new();
        write_type_detail(&mut xml, &g, "T", &caps, ">>", None);
        assert!(xml.starts_with(">><type"));
        assert!(xml.contains(">>  <properties>"));
        assert!(xml.contains(">>  <samples>"));
    }

    // ── compute_all_neighbors_schemas empty graph ─────────────────────────

    #[test]
    fn test_compute_all_neighbors_schemas_empty() {
        let g = DirGraph::new();
        let all = compute_all_neighbors_schemas(&g);
        assert!(all.is_empty());
    }

    // ── build_inventory_with_detail parent types ──────────────────────────

    #[test]
    fn test_build_inventory_with_detail_filters_supporting_types() {
        let mut g = make_graph_with_nodes(&[
            ("Core", vec![("c1", "main", vec![])]),
            ("Supporting", vec![("s1", "sub", vec![])]),
        ]);
        g.parent_types
            .insert("Supporting".to_string(), "Core".to_string());

        let result = build_inventory_with_detail(&g);
        // Should contain Core but not list Supporting as a top-level type
        assert!(result.contains("Core"));
        // Supporting should only appear in the <supporting> child element
    }

    // ── types_compatible edge cases ──────────────────────────────────────

    #[test]
    fn test_types_compatible_empty_strings() {
        assert!(!types_compatible("", ""));
        assert!(!types_compatible("", "String"));
    }

    #[test]
    fn test_types_compatible_case_insensitive() {
        assert!(types_compatible("STRING", "string"));
        assert!(types_compatible("int64", "INT64"));
        assert!(types_compatible("Float64", "FLOAT64"));
    }

    // ── compute_schema with edge metadata ─────────────────────────────────

    #[test]
    fn test_compute_schema_with_edges() {
        let mut g = make_graph_with_nodes(&[
            ("A", vec![("a1", "a", vec![])]),
            ("B", vec![("b1", "b", vec![])]),
        ]);
        add_edge(&mut g, "A", "B", "REL", vec![]);

        let schema = compute_schema(&g);
        assert_eq!(schema.node_count, 2);
        assert_eq!(schema.edge_count, 1);
        assert_eq!(schema.connection_types.len(), 1);
    }

    // ── property_complexity boundary values ───────────────────────────────

    #[test]
    fn test_property_complexity_exact_boundaries() {
        assert_eq!(property_complexity(3), "vl");
        assert_eq!(property_complexity(4), "l");
        assert_eq!(property_complexity(8), "l");
        assert_eq!(property_complexity(9), "m");
        assert_eq!(property_complexity(15), "m");
        assert_eq!(property_complexity(16), "h");
        assert_eq!(property_complexity(30), "h");
        assert_eq!(property_complexity(31), "vh");
    }

    // ── size_tier boundary values ─────────────────────────────────────────

    #[test]
    fn test_size_tier_exact_boundaries() {
        assert_eq!(size_tier(9), "vs");
        assert_eq!(size_tier(10), "s");
        assert_eq!(size_tier(99), "s");
        assert_eq!(size_tier(100), "m");
        assert_eq!(size_tier(999), "m");
        assert_eq!(size_tier(1000), "l");
        assert_eq!(size_tier(9999), "l");
        assert_eq!(size_tier(10000), "vl");
    }

    // ── xml_escape with unicode ──────────────────────────────────────────

    #[test]
    fn test_xml_escape_unicode() {
        assert_eq!(xml_escape("café"), "café");
        assert_eq!(xml_escape("日本語"), "日本語");
        assert_eq!(xml_escape("hello<世界>"), "hello&lt;世界&gt;");
    }

    // ── mcp_quickstart content ────────────────────────────────────────────

    #[test]
    fn test_mcp_quickstart_contains_tools() {
        let result = mcp_quickstart();
        assert!(result.contains("graph_overview"));
        assert!(result.contains("cypher_query"));
        assert!(result.contains("bug_report"));
        assert!(result.contains("core_tools"));
    }

    // ── compute_join_candidates with supporting types excluded ────────────

    #[test]
    fn test_compute_join_candidates_excludes_supporting_types() {
        let mut g = make_graph_with_nodes(&[
            (
                "Core",
                vec![("c1", "A", vec![("name", Value::String("Shared".into()))])],
            ),
            (
                "Supporting",
                vec![("s1", "B", vec![("name", Value::String("Shared".into()))])],
            ),
        ]);
        g.parent_types
            .insert("Supporting".to_string(), "Core".to_string());
        let mut meta = HashMap::new();
        meta.insert("name".to_string(), "String".to_string());
        g.node_type_metadata
            .insert("Core".to_string(), meta.clone());
        g.node_type_metadata.insert("Supporting".to_string(), meta);

        let connected_pairs = HashSet::new();
        let candidates = compute_join_candidates(&g, &connected_pairs, 10, 100);
        // Supporting type should be excluded from join candidate search
        assert!(candidates.is_empty());
    }

    // ── compute_property_stats ordering ───────────────────────────────────

    #[test]
    fn test_compute_property_stats_ordering() {
        let g = make_graph_with_nodes(&[(
            "Item",
            vec![(
                "i1",
                "A",
                vec![("zebra", Value::Int64(1)), ("alpha", Value::Int64(2))],
            )],
        )]);
        let stats = compute_property_stats(&g, "Item", 15, None).unwrap();
        let names: Vec<&str> = stats.iter().map(|s| s.property_name.as_str()).collect();
        // Order should be: type, title, id, then remaining sorted
        assert_eq!(names[0], "type");
        assert_eq!(names[1], "title");
        assert_eq!(names[2], "id");
        // Remaining properties should be alphabetically sorted
        let remaining = &names[3..];
        assert!(remaining.windows(2).all(|w| w[0] <= w[1]));
    }
}
