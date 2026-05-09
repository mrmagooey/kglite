// src/graph/batch_operations.rs
use crate::datatypes::Value;
use crate::graph::schema::{DirGraph, EdgeData, InternedKey, NodeData};
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

// Constants for batch size optimization
const SMALL_BATCH_THRESHOLD: usize = 100;
const MEDIUM_BATCH_THRESHOLD: usize = 1000;
const LARGE_BATCH_CHUNK_SIZE: usize = 1000;

#[derive(Debug)]
enum BatchType {
    Small,
    Medium,
    Large,
}

#[derive(Debug, Default)]
pub struct BatchMetrics {
    pub processing_time: f64,
    pub memory_used: usize,
    pub batch_count: usize,
}

// Node Processing
#[derive(Debug)]
#[allow(dead_code)]
pub enum NodeAction {
    Update {
        node_idx: NodeIndex,
        title: Option<Value>, // Changed to Option to indicate if title should be updated
        properties: HashMap<String, Value>,
        conflict_mode: ConflictHandling, // Added conflict mode
    },
    Create {
        node_type: String,
        id: Value,
        title: Value,
        properties: HashMap<String, Value>,
    },
    /// Create with pre-interned property keys (avoids re-interning per row)
    CreateInterned {
        node_type: String,
        id: Value,
        title: Value,
        properties: Vec<(InternedKey, Value)>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ConflictHandling {
    Replace, // Replace all properties and title
    Skip,    // Don't update existing nodes/edges
    #[default]
    Update, // Merge properties, new values overwrite existing
    Preserve, // Merge properties, existing values take precedence
    Sum,     // Merge properties, add numeric values (edges); acts as Update for nodes
}

/// Add two Values if both are numeric. Mixed Int64+Float64 promotes to Float64.
/// Non-numeric values fall back to Update behavior (new value overwrites).
fn sum_values(existing: &Value, new: &Value) -> Value {
    match (existing, new) {
        (Value::Int64(a), Value::Int64(b)) => Value::Int64(a.wrapping_add(*b)),
        (Value::Float64(a), Value::Float64(b)) => Value::Float64(a + b),
        (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 + b),
        (Value::Float64(a), Value::Int64(b)) => Value::Float64(a + *b as f64),
        _ => new.clone(),
    }
}

#[derive(Debug)]
struct NodeCreation {
    node_type: String,
    id: Value,
    title: Value,
    properties: HashMap<String, Value>,
}

#[derive(Debug)]
struct NodeCreationInterned {
    node_type: String,
    id: Value,
    title: Value,
    properties: Vec<(InternedKey, Value)>,
}

#[derive(Debug)]
struct NodeUpdate {
    node_idx: NodeIndex,
    title: Option<Value>, // Changed to Option
    properties: HashMap<String, Value>,
    conflict_mode: ConflictHandling,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BatchStats {
    pub creates: usize,
    pub updates: usize,
}

impl BatchStats {
    fn combine(&mut self, other: &BatchStats) {
        self.creates += other.creates;
        self.updates += other.updates;
    }
}

#[derive(Debug)]
pub struct BatchProcessor {
    creates: Vec<NodeCreation>,
    creates_interned: Vec<NodeCreationInterned>,
    updates: Vec<NodeUpdate>,
    capacity: usize,
    batch_type: BatchType,
    metrics: BatchMetrics,
    accumulated_stats: BatchStats, // Track stats across intermediate flushes
}

impl BatchProcessor {
    pub fn new(estimated_size: usize) -> Self {
        let (capacity, batch_type) = match estimated_size {
            n if n < SMALL_BATCH_THRESHOLD => (n, BatchType::Small),
            n if n < MEDIUM_BATCH_THRESHOLD => (n, BatchType::Medium),
            _ => (LARGE_BATCH_CHUNK_SIZE, BatchType::Large),
        };

        BatchProcessor {
            creates: Vec::with_capacity(capacity),
            creates_interned: Vec::with_capacity(capacity),
            updates: Vec::with_capacity(capacity),
            capacity,
            batch_type,
            metrics: BatchMetrics::default(),
            accumulated_stats: BatchStats::default(),
        }
    }

    pub fn add_action(&mut self, action: NodeAction, graph: &mut DirGraph) -> Result<(), String> {
        match action {
            NodeAction::Create {
                node_type,
                id,
                title,
                properties,
            } => {
                self.creates.push(NodeCreation {
                    node_type,
                    id,
                    title,
                    properties,
                });
            }
            NodeAction::CreateInterned {
                node_type,
                id,
                title,
                properties,
            } => {
                self.creates_interned.push(NodeCreationInterned {
                    node_type,
                    id,
                    title,
                    properties,
                });
            }
            NodeAction::Update {
                node_idx,
                title,
                properties,
                conflict_mode,
            } => {
                self.updates.push(NodeUpdate {
                    node_idx,
                    title,
                    properties,
                    conflict_mode, // Add this field
                });
            }
        }

        // For large batches, flush if we hit capacity
        if let BatchType::Large = self.batch_type {
            if self.creates.len() + self.creates_interned.len() >= self.capacity {
                let stats = self.flush_chunk(graph)?;
                self.accumulated_stats.combine(&stats); // Accumulate stats from intermediate flushes
            }
        }

        Ok(())
    }

    fn flush_chunk(&mut self, graph: &mut DirGraph) -> Result<BatchStats, String> {
        let start = Instant::now();
        let mut stats = BatchStats::default();

        // Process creates in current chunk
        for creation in self.creates.drain(..) {
            let id_for_index = creation.id.clone();
            let node_type_for_index = creation.node_type.clone();

            // Use compact storage if a TypeSchema exists for this node type
            let schema: Option<Arc<_>> = graph.type_schemas.get(&creation.node_type).cloned();
            let node_data = if let Some(ref ts) = schema {
                NodeData::new_compact(
                    creation.id,
                    creation.title,
                    creation.node_type.clone(),
                    creation.properties,
                    &mut graph.interner,
                    ts,
                )
            } else {
                NodeData::new(
                    creation.id,
                    creation.title,
                    creation.node_type.clone(),
                    creation.properties,
                    &mut graph.interner,
                )
            };
            let node_idx = graph.graph.add_node(node_data);
            // Add to type index
            graph
                .type_indices
                .entry(creation.node_type)
                .or_default()
                .push(node_idx);
            // Add to ID index for O(1) lookups
            graph
                .id_indices
                .entry(node_type_for_index)
                .or_default()
                .insert(id_for_index, node_idx);
            stats.creates += 1;
        }

        // Process pre-interned creates (fast path — no string interning needed)
        for creation in self.creates_interned.drain(..) {
            let id_for_index = creation.id.clone();
            let node_type_for_index = creation.node_type.clone();

            let schema: Option<Arc<_>> = graph.type_schemas.get(&creation.node_type).cloned();
            let node_data = if let Some(ref ts) = schema {
                NodeData::new_compact_preinterned(
                    creation.id,
                    creation.title,
                    creation.node_type.clone(),
                    creation.properties,
                    ts,
                )
            } else {
                NodeData::new_preinterned(
                    creation.id,
                    creation.title,
                    creation.node_type.clone(),
                    creation.properties,
                )
            };
            let node_idx = graph.graph.add_node(node_data);
            graph
                .type_indices
                .entry(creation.node_type)
                .or_default()
                .push(node_idx);
            graph
                .id_indices
                .entry(node_type_for_index)
                .or_default()
                .insert(id_for_index, node_idx);
            stats.creates += 1;
        }

        // Process updates in current chunk
        for update in self.updates.drain(..) {
            if update.conflict_mode == ConflictHandling::Skip {
                // Skip this node entirely
                continue;
            }

            // Pre-intern property keys before borrowing graph.graph mutably
            let interned_props: Vec<(InternedKey, Value)> = update
                .properties
                .into_iter()
                .map(|(k, v)| {
                    let key = graph.interner.get_or_intern(&k);
                    (key, v)
                })
                .collect();

            if let Some(node) = graph.graph.node_weight_mut(update.node_idx) {
                match update.conflict_mode {
                    ConflictHandling::Skip => unreachable!(), // handled above
                    ConflictHandling::Replace => {
                        // Current behavior - complete replacement
                        if let Some(new_title) = update.title {
                            node.title = new_title;
                        }
                        node.properties.replace_all(interned_props);
                    }
                    ConflictHandling::Update | ConflictHandling::Sum => {
                        // Update only if provided (Sum acts as Update for nodes)
                        if let Some(new_title) = update.title {
                            node.title = new_title;
                        }
                        for (k, v) in interned_props {
                            node.properties.insert(k, v);
                        }
                    }
                    ConflictHandling::Preserve => {
                        // Update only if provided, but preserve existing values
                        if let Some(new_title) = update.title {
                            if node.title == Value::Null {
                                node.title = new_title;
                            }
                        }
                        // Merge properties with preference to existing values
                        for (k, v) in interned_props {
                            node.properties.insert_if_absent(k, v);
                        }
                    }
                }
                stats.updates += 1;
            }
        }

        // Update metrics
        self.metrics.processing_time += start.elapsed().as_secs_f64();
        self.metrics.batch_count += 1;
        self.metrics.memory_used = self.creates.capacity() + self.updates.capacity();

        Ok(stats)
    }

    pub fn execute(mut self, graph: &mut DirGraph) -> Result<(BatchStats, BatchMetrics), String> {
        // Start with accumulated stats from intermediate flushes (for large batches)
        let mut total_stats = self.accumulated_stats;

        match self.batch_type {
            BatchType::Small | BatchType::Medium => {
                // Process in a single batch
                let stats = self.flush_chunk(graph)?;
                total_stats.combine(&stats);
            }
            BatchType::Large => {
                // Process any remaining items
                if !self.creates.is_empty()
                    || !self.creates_interned.is_empty()
                    || !self.updates.is_empty()
                {
                    let stats = self.flush_chunk(graph)?;
                    total_stats.combine(&stats);
                }
            }
        }

        Ok((total_stats, self.metrics))
    }
}

// Connection Processing
#[derive(Debug)]
struct ConnectionCreation {
    source_idx: NodeIndex,
    target_idx: NodeIndex,
    properties: HashMap<String, Value>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ConnectionBatchStats {
    pub connections_created: usize,
    pub properties_tracked: usize,
}

impl ConnectionBatchStats {
    fn combine(&mut self, other: &ConnectionBatchStats) {
        self.connections_created += other.connections_created;
        self.properties_tracked = self.properties_tracked.max(other.properties_tracked);
    }
}

#[derive(Debug)]
pub struct ConnectionBatchProcessor {
    connections: Vec<ConnectionCreation>,
    schema_properties: HashSet<String>,
    capacity: usize,
    batch_type: BatchType,
    metrics: BatchMetrics,
    conflict_mode: ConflictHandling,
    accumulated_stats: ConnectionBatchStats, // Track stats across intermediate flushes
    skip_existence_check: bool,              // Skip find_edge() on initial load
}

impl ConnectionBatchProcessor {
    pub fn new(estimated_size: usize) -> Self {
        let (capacity, batch_type) = match estimated_size {
            n if n < SMALL_BATCH_THRESHOLD => (n, BatchType::Small),
            n if n < MEDIUM_BATCH_THRESHOLD => (n, BatchType::Medium),
            _ => (LARGE_BATCH_CHUNK_SIZE, BatchType::Large),
        };

        ConnectionBatchProcessor {
            connections: Vec::with_capacity(capacity),
            schema_properties: HashSet::new(),
            capacity,
            batch_type,
            metrics: BatchMetrics::default(),
            conflict_mode: ConflictHandling::Update,
            accumulated_stats: ConnectionBatchStats::default(),
            skip_existence_check: false,
        }
    }

    // Add setter for conflict mode
    pub fn set_conflict_mode(&mut self, mode: ConflictHandling) {
        self.conflict_mode = mode;
    }

    /// Skip edge existence checks (safe when this connection type has no existing edges)
    pub fn set_skip_existence_check(&mut self, skip: bool) {
        self.skip_existence_check = skip;
    }

    pub fn add_connection(
        &mut self,
        source_idx: NodeIndex,
        target_idx: NodeIndex,
        properties: HashMap<String, Value>,
        graph: &mut DirGraph,
        connection_type: &str,
    ) -> Result<(), String> {
        // Skip existence check on initial load (no existing edges of this type).
        // For Skip-mode we still short-circuit here so we don't even buffer the
        // duplicate; for Update/Replace/Preserve/Sum the lookup is amortised by
        // the per-source index built once in flush_chunk (see below). Doing the
        // O(degree(src)) lookup *both* here and in flush_chunk was the root
        // cause of the analysis post-processing hang on large Azure datasets.
        if !self.skip_existence_check && self.conflict_mode == ConflictHandling::Skip {
            let conn_type_key = graph.interner.get_or_intern(connection_type);
            let existing_edge = graph
                .graph
                .edges_connecting(source_idx, target_idx)
                .find(|e| e.weight().connection_type == conn_type_key)
                .map(|e| e.id());

            if existing_edge.is_some() {
                return Ok(());
            }
        }

        // Track property names for schema
        for key in properties.keys() {
            self.schema_properties.insert(key.clone());
        }

        self.connections.push(ConnectionCreation {
            source_idx,
            target_idx,
            properties,
        });

        // For large batches, flush if we hit capacity
        if let BatchType::Large = self.batch_type {
            if self.connections.len() >= self.capacity {
                let stats = self.flush_chunk(graph, connection_type)?;
                self.accumulated_stats.combine(&stats); // Accumulate stats from intermediate flushes
            }
        }

        Ok(())
    }

    fn flush_chunk(
        &mut self,
        graph: &mut DirGraph,
        connection_type: &str,
    ) -> Result<ConnectionBatchStats, String> {
        let start = Instant::now();
        let mut stats = ConnectionBatchStats::default();

        // Pre-intern the connection type for edge type comparison
        let conn_type_key = graph.interner.get_or_intern(connection_type);

        // Build a per-source index of existing edges of `conn_type_key` so
        // that the per-edge existence check below is O(1) instead of
        // O(degree(src)). The previous implementation called
        // `edges_connecting(src, dst)` for every edge, which scaled
        // quadratically when post-processing repeatedly added edges from a
        // hub source (e.g. Azure AZMG* edges from a service-principal source
        // to many targets), causing analysis to hang on large datasets.
        //
        // We collect the set of unique source NodeIndexes referenced by this
        // chunk, then iterate each source's outgoing edges once to fill the
        // (src, dst) -> edge_id map. After that, the per-edge lookup is a
        // single hash probe.
        //
        // Fast path: when the graph contains no edges of `conn_type_key` yet
        // (common during analysis post-processing, where each derived edge
        // type is brand-new), the index would be empty after walking every
        // source's full adjacency list — pure waste. Skip the build; the
        // per-loop `existing_edges.insert` after each create still keeps
        // within-chunk dedup correct. We register the type with the graph
        // after the first add so subsequent flush_chunks within the same
        // execute() call see it as existing and resume building the index.
        let type_existed = graph.has_connection_type(connection_type);
        let mut existing_edges: HashMap<(NodeIndex, NodeIndex), petgraph::graph::EdgeIndex> =
            HashMap::new();
        if !self.skip_existence_check && type_existed && !self.connections.is_empty() {
            let mut sources: HashSet<NodeIndex> = HashSet::with_capacity(self.connections.len());
            for conn in &self.connections {
                sources.insert(conn.source_idx);
            }
            existing_edges.reserve(self.connections.len());
            for src in sources {
                for edge_ref in graph.graph.edges(src) {
                    if edge_ref.weight().connection_type == conn_type_key {
                        existing_edges.insert((src, edge_ref.target()), edge_ref.id());
                    }
                }
            }
        }

        // Create or update edges in current chunk
        for conn in self.connections.drain(..) {
            // On initial load, skip existence check for performance (no existing edges).
            // Otherwise look up via the precomputed index — O(1).
            let existing_edge = if self.skip_existence_check {
                None
            } else {
                existing_edges
                    .get(&(conn.source_idx, conn.target_idx))
                    .copied()
            };

            if let Some(edge_idx) = existing_edge {
                match self.conflict_mode {
                    ConflictHandling::Skip => {
                        // Skip this edge (should already be filtered in add_connection)
                        continue;
                    }
                    ConflictHandling::Replace => {
                        // Remove the existing edge and create a new one.
                        // conn_type_key is already interned for this chunk;
                        // use new_interned to skip a redundant intern lookup.
                        graph.graph.remove_edge(edge_idx);
                        let interned_props: Vec<(InternedKey, Value)> = conn
                            .properties
                            .into_iter()
                            .map(|(k, v)| (graph.interner.get_or_intern(&k), v))
                            .collect();
                        let edge_data = EdgeData::new_interned(conn_type_key, interned_props);
                        graph
                            .graph
                            .add_edge(conn.source_idx, conn.target_idx, edge_data);
                        stats.connections_created += 1;
                    }
                    ConflictHandling::Update => {
                        // Update existing edge properties
                        // Pre-intern keys before getting mutable edge reference
                        let interned_props: Vec<(InternedKey, Value)> = conn
                            .properties
                            .into_iter()
                            .map(|(k, v)| {
                                let key = graph.interner.get_or_intern(&k);
                                (key, v)
                            })
                            .collect();
                        if let Some(EdgeData {
                            properties: edge_props,
                            ..
                        }) = graph.graph.edge_weight_mut(edge_idx)
                        {
                            // Merge properties, preferring new values
                            for (k, v) in interned_props {
                                if let Some((_, existing)) =
                                    edge_props.iter_mut().find(|(ek, _)| *ek == k)
                                {
                                    *existing = v;
                                } else {
                                    edge_props.push((k, v));
                                }
                            }
                            stats.connections_created += 1;
                        }
                    }
                    ConflictHandling::Preserve => {
                        // Update but preserve existing values
                        // Pre-intern keys before getting mutable edge reference
                        let interned_props: Vec<(InternedKey, Value)> = conn
                            .properties
                            .into_iter()
                            .map(|(k, v)| {
                                let key = graph.interner.get_or_intern(&k);
                                (key, v)
                            })
                            .collect();
                        if let Some(EdgeData {
                            properties: edge_props,
                            ..
                        }) = graph.graph.edge_weight_mut(edge_idx)
                        {
                            // Merge properties, preserving existing values
                            for (k, v) in interned_props {
                                if !edge_props.iter().any(|(ek, _)| *ek == k) {
                                    edge_props.push((k, v));
                                }
                            }
                            stats.connections_created += 1;
                        }
                    }
                    ConflictHandling::Sum => {
                        // Sum numeric properties, overwrite non-numeric
                        let interned_props: Vec<(InternedKey, Value)> = conn
                            .properties
                            .into_iter()
                            .map(|(k, v)| {
                                let key = graph.interner.get_or_intern(&k);
                                (key, v)
                            })
                            .collect();
                        if let Some(EdgeData {
                            properties: edge_props,
                            ..
                        }) = graph.graph.edge_weight_mut(edge_idx)
                        {
                            for (k, v) in interned_props {
                                if let Some((_, existing)) =
                                    edge_props.iter_mut().find(|(ek, _)| *ek == k)
                                {
                                    *existing = sum_values(existing, &v);
                                } else {
                                    edge_props.push((k, v));
                                }
                            }
                            stats.connections_created += 1;
                        }
                    }
                }
            } else {
                // Create new edge. Reuse pre-interned conn_type_key.
                let interned_props: Vec<(InternedKey, Value)> = conn
                    .properties
                    .into_iter()
                    .map(|(k, v)| (graph.interner.get_or_intern(&k), v))
                    .collect();
                let edge_data = EdgeData::new_interned(conn_type_key, interned_props);
                let new_id = graph
                    .graph
                    .add_edge(conn.source_idx, conn.target_idx, edge_data);
                // Keep the per-chunk index in sync so a later duplicate
                // (src,dst) within the same chunk hits the update branch
                // instead of creating a parallel edge of the same type.
                if !self.skip_existence_check {
                    existing_edges.insert((conn.source_idx, conn.target_idx), new_id);
                }
                stats.connections_created += 1;
            }
        }

        // Invalidate edge type count cache after edge mutations
        graph.invalidate_edge_type_counts_cache();

        // If we created any edges of this type and the type wasn't registered
        // before this chunk, register it now so a subsequent flush_chunk in
        // the same execute() call sees the type as existing and rebuilds the
        // (src,dst) index against the edges we just added.
        if !type_existed && stats.connections_created > 0 {
            graph.connection_types.insert(conn_type_key);
        }

        // Update metrics
        self.metrics.processing_time += start.elapsed().as_secs_f64();
        self.metrics.batch_count += 1;
        self.metrics.memory_used = self.connections.capacity();

        stats.properties_tracked = self.schema_properties.len();
        Ok(stats)
    }

    pub fn execute(
        mut self,
        graph: &mut DirGraph,
        connection_type: String,
    ) -> Result<(ConnectionBatchStats, BatchMetrics), String> {
        // Register connection type for O(1) lookups
        graph.register_connection_type(connection_type.clone());

        // Start with accumulated stats from intermediate flushes (for large batches)
        let mut total_stats = self.accumulated_stats;

        match self.batch_type {
            BatchType::Small | BatchType::Medium => {
                // Process in a single batch
                let stats = self.flush_chunk(graph, &connection_type)?;
                total_stats.combine(&stats);
            }
            BatchType::Large => {
                // Process any remaining items
                if !self.connections.is_empty() {
                    let stats = self.flush_chunk(graph, &connection_type)?;
                    total_stats.combine(&stats);
                }
            }
        }

        Ok((total_stats, self.metrics))
    }

    pub fn get_schema_properties(&self) -> &HashSet<String> {
        &self.schema_properties
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_values_int_int() {
        assert_eq!(
            sum_values(&Value::Int64(10), &Value::Int64(5)),
            Value::Int64(15)
        );
    }

    #[test]
    fn test_sum_values_int_negative() {
        assert_eq!(
            sum_values(&Value::Int64(10), &Value::Int64(-3)),
            Value::Int64(7)
        );
    }

    #[test]
    fn test_sum_values_float_float() {
        match sum_values(&Value::Float64(1.5), &Value::Float64(2.5)) {
            Value::Float64(v) => assert!((v - 4.0).abs() < 1e-10),
            other => panic!("Expected Float64, got {:?}", other),
        }
    }

    #[test]
    fn test_sum_values_int_float_promotion() {
        match sum_values(&Value::Int64(10), &Value::Float64(2.5)) {
            Value::Float64(v) => assert!((v - 12.5).abs() < 1e-10),
            other => panic!("Expected Float64, got {:?}", other),
        }
    }

    #[test]
    fn test_sum_values_float_int_promotion() {
        match sum_values(&Value::Float64(3.5), &Value::Int64(2)) {
            Value::Float64(v) => assert!((v - 5.5).abs() < 1e-10),
            other => panic!("Expected Float64, got {:?}", other),
        }
    }

    #[test]
    fn test_sum_values_non_numeric_overwrites() {
        assert_eq!(
            sum_values(&Value::String("old".into()), &Value::String("new".into())),
            Value::String("new".into()),
        );
    }

    #[test]
    fn test_sum_values_null_cases() {
        assert_eq!(sum_values(&Value::Null, &Value::Int64(5)), Value::Int64(5));
        assert_eq!(sum_values(&Value::Int64(5), &Value::Null), Value::Null);
    }

    /// When the connection type is brand-new, flush_chunk skips the
    /// per-source adjacency scan but still de-dups duplicates created within
    /// the same chunk via the existing_edges insert-on-add path.
    #[test]
    fn test_connection_batch_new_type_dedups_within_chunk() {
        use crate::graph::schema::NodeData;

        let mut g = DirGraph::new();
        let src = g.graph.add_node(NodeData::new(
            Value::Int64(1),
            Value::String("Hub".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut g.interner,
        ));
        let dst = g.graph.add_node(NodeData::new(
            Value::Int64(2),
            Value::String("Target".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut g.interner,
        ));

        let mut p = ConnectionBatchProcessor::new(2);
        p.add_connection(src, dst, HashMap::new(), &mut g, "FreshType")
            .unwrap();
        p.add_connection(src, dst, HashMap::new(), &mut g, "FreshType")
            .unwrap();
        p.execute(&mut g, "FreshType".to_string()).unwrap();

        // Same (src,dst,FreshType) entered twice — should yield exactly one
        // edge after dedup-within-chunk.
        assert_eq!(g.graph.edge_count(), 1);
    }

    /// When the connection type already has edges in the graph, flush_chunk
    /// builds the per-source index and matches an incoming duplicate to the
    /// pre-existing edge (Update mode merges properties rather than creating
    /// a parallel edge).
    #[test]
    fn test_connection_batch_existing_type_uses_index() {
        use crate::graph::schema::{EdgeData, NodeData};

        let mut g = DirGraph::new();
        let src = g.graph.add_node(NodeData::new(
            Value::Int64(1),
            Value::String("Hub".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut g.interner,
        ));
        let dst = g.graph.add_node(NodeData::new(
            Value::Int64(2),
            Value::String("Target".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut g.interner,
        ));

        // Pre-seed an edge of type "Existing" so flush_chunk takes the
        // index-build path.
        let edge_data = EdgeData::new("Existing".to_string(), HashMap::new(), &mut g.interner);
        g.graph.add_edge(src, dst, edge_data);
        g.register_connection_type("Existing".to_string());
        assert_eq!(g.graph.edge_count(), 1);

        // Add a duplicate (src,dst,Existing) via the batch processor — should
        // hit the Update branch (merge), not create a second parallel edge.
        let mut props = HashMap::new();
        props.insert("isacl".to_string(), Value::Boolean(true));
        let mut p = ConnectionBatchProcessor::new(1);
        p.add_connection(src, dst, props, &mut g, "Existing")
            .unwrap();
        p.execute(&mut g, "Existing".to_string()).unwrap();

        assert_eq!(g.graph.edge_count(), 1);
        // The merged property should be present.
        let edge = g.graph.edge_weights().next().unwrap();
        let isacl_key = g.interner.get_or_intern("isacl");
        let found = edge.properties.iter().any(|(k, _)| *k == isacl_key);
        assert!(found, "expected merged property on existing edge");
    }

    /// Two consecutive flush_chunks of the same brand-new type within one
    /// execute() must dedup against each other: the second chunk needs to
    /// see edges added by the first.
    #[test]
    fn test_connection_batch_new_type_registers_after_first_chunk() {
        use crate::graph::schema::NodeData;

        let mut g = DirGraph::new();
        let src = g.graph.add_node(NodeData::new(
            Value::Int64(1),
            Value::String("Hub".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut g.interner,
        ));
        let dst = g.graph.add_node(NodeData::new(
            Value::Int64(2),
            Value::String("Target".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut g.interner,
        ));

        // Force two flush_chunks by exceeding LARGE_BATCH_CHUNK_SIZE.
        // First chunk: type doesn't exist → skip-build path.
        // After the first chunk's edge is created, the type is registered.
        // Second chunk: type now exists → index-build picks up the edge from
        // chunk 1, so a duplicate (src,dst) hits the Update branch.
        let mut p =
            ConnectionBatchProcessor::new(LARGE_BATCH_CHUNK_SIZE + LARGE_BATCH_CHUNK_SIZE / 2);
        // First chunk: 1000 distinct dst-shifted edges (none duplicate)
        // We only have one dst so just push (src,dst) over and over —
        // within-chunk dedup keeps just one in chunk 1.
        for _ in 0..LARGE_BATCH_CHUNK_SIZE {
            p.add_connection(src, dst, HashMap::new(), &mut g, "AcrossChunks")
                .unwrap();
        }
        // Second chunk attempts another (src,dst) of the same type.
        for _ in 0..(LARGE_BATCH_CHUNK_SIZE / 2) {
            p.add_connection(src, dst, HashMap::new(), &mut g, "AcrossChunks")
                .unwrap();
        }
        p.execute(&mut g, "AcrossChunks".to_string()).unwrap();

        // Across both chunks, exactly one edge of (src,dst,AcrossChunks)
        // should exist.
        assert_eq!(g.graph.edge_count(), 1);
    }
}
