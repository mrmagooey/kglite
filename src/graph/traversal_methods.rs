// src/graph/traversal_methods.rs
use crate::datatypes::values::FilterCondition;
use crate::datatypes::values::Value;
use crate::graph::filtering_methods;
use crate::graph::schema::{
    CurrentSelection, DirGraph, InternedKey, NodeData, SelectionOperation, SpatialConfig,
    TemporalConfig,
};
use crate::graph::spatial;
use crate::graph::temporal;
use crate::graph::vector_search;
use chrono::NaiveDate;
use geo::geometry::Geometry;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use std::collections::{HashMap, HashSet};

/// Temporal filter for edge traversal.
/// Carries multiple TemporalConfig entries to support shared connection type names
/// across source types (e.g., HAS_LICENSEE used by Field, Licence, BusinessArrangement).
pub enum TemporalEdgeFilter {
    /// Point-in-time: valid_from <= date AND (valid_to IS NULL OR valid_to >= date)
    At(Vec<TemporalConfig>, NaiveDate),
    /// Range overlap: valid_from <= end AND (valid_to IS NULL OR valid_to >= start)
    During(Vec<TemporalConfig>, NaiveDate, NaiveDate),
}

// ── Comparison-based traversal types ─────────────────────────────────────────

/// How polygon nodes should be spatially resolved.
/// When set, overrides the default "location → centroid fallback" behavior.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SpatialResolve {
    /// Use geometry centroid (ignoring location fields)
    Centroid,
    /// Use closest point on geometry boundary (for distance calculations)
    Closest,
    /// Use full polygon geometry (for containment checks)
    Geometry,
}

/// Parsed configuration from the Python `method=` parameter (str or dict).
pub struct MethodConfig {
    pub method_type: String,
    pub resolve: Option<SpatialResolve>,
    pub max_distance_m: Option<f64>,
    pub geometry_field: Option<String>,
    pub property: Option<String>,
    pub threshold: Option<f64>,
    pub metric: Option<String>,
    pub algorithm: Option<String>,
    pub features: Option<Vec<String>>,
    pub k: Option<usize>,
    pub eps: Option<f64>,
    pub min_samples: Option<usize>,
}

impl MethodConfig {
    /// Build from a string shorthand (no extra settings).
    pub fn from_string(method_type: String) -> Self {
        Self {
            method_type,
            resolve: None,
            max_distance_m: None,
            geometry_field: None,
            property: None,
            threshold: None,
            metric: None,
            algorithm: None,
            features: None,
            k: None,
            eps: None,
            min_samples: None,
        }
    }

    /// Parse `resolve` string to enum.
    pub fn parse_resolve(s: &str) -> Result<SpatialResolve, String> {
        match s {
            "centroid" => Ok(SpatialResolve::Centroid),
            "closest" => Ok(SpatialResolve::Closest),
            "geometry" => Ok(SpatialResolve::Geometry),
            _ => Err(format!(
                "Unknown resolve mode: '{}'. Valid: 'centroid', 'closest', 'geometry'",
                s
            )),
        }
    }
}

/// Check if edge properties match all given filter conditions
fn edge_matches_conditions(
    properties: &[(InternedKey, Value)],
    conditions: &HashMap<String, FilterCondition>,
) -> bool {
    conditions.iter().all(|(field, condition)| {
        let ik = InternedKey::from_str(field);
        match properties.iter().find(|(k, _)| *k == ik).map(|(_, v)| v) {
            Some(value) => filtering_methods::matches_condition(value, condition),
            None => {
                // Missing field is treated as null
                matches!(condition, FilterCondition::IsNull)
            }
        }
    })
}

/// Check if edge properties pass a temporal filter.
/// Tries multiple configs to find one matching the edge's field names.
fn edge_passes_temporal(properties: &[(InternedKey, Value)], filter: &TemporalEdgeFilter) -> bool {
    match filter {
        TemporalEdgeFilter::At(configs, date) => {
            temporal::is_temporally_valid_multi(properties, configs, date)
        }
        TemporalEdgeFilter::During(configs, start, end) => {
            temporal::overlaps_range_multi(properties, configs, start, end)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn make_traversal(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    connection_type: String,
    level_index: Option<usize>,
    direction: Option<String>,
    filter_target: Option<&HashMap<String, FilterCondition>>,
    filter_connection: Option<&HashMap<String, FilterCondition>>,
    sort_target: Option<&Vec<(String, bool)>>,
    max_nodes: Option<usize>,
    new_level: Option<bool>,
    temporal_filter: Option<&TemporalEdgeFilter>,
    target_type: Option<&[String]>,
) -> Result<(), String> {
    // Validate connection type exists
    if !graph.has_connection_type(&connection_type) {
        return Err(format!(
            "Connection type '{}' does not exist in graph",
            connection_type
        ));
    }

    // First get the source level index
    let source_level_index =
        level_index.unwrap_or_else(|| selection.get_level_count().saturating_sub(1));

    let create_new_level = new_level.unwrap_or(true);

    // Get source level
    let source_level = selection
        .get_level(source_level_index)
        .ok_or_else(|| "No valid source level found for traversal".to_string())?;

    // Early empty check
    if source_level.is_empty() {
        return Err("No source nodes available for traversal".to_string());
    }

    // Set up traversal directions
    let dir = match direction.as_deref() {
        Some("incoming") => Some(Direction::Incoming),
        Some("outgoing") => Some(Direction::Outgoing),
        Some(d) => {
            return Err(format!(
                "Invalid direction: {}. Must be 'incoming' or 'outgoing'",
                d
            ))
        }
        None => None, // Both directions
    };

    // FAST PATH: No filtering, sorting, or limits - optimized for common case
    // target_type is kept in fast path since it's a cheap string comparison
    let use_fast_path = filter_target.is_none()
        && filter_connection.is_none()
        && sort_target.is_none()
        && max_nodes.is_none()
        && create_new_level
        && temporal_filter.is_none();

    if use_fast_path {
        return make_traversal_fast(
            graph,
            selection,
            &connection_type,
            source_level_index,
            dir,
            target_type,
        );
    }

    // SLOW PATH: Full processing with filtering/sorting/limits
    make_traversal_full(
        graph,
        selection,
        connection_type,
        source_level_index,
        dir,
        filter_target,
        filter_connection,
        sort_target,
        max_nodes,
        create_new_level,
        temporal_filter,
        target_type,
    )
}

/// Fast traversal path for the common case: no filtering, no sorting, no limits.
/// Avoids HashMap overhead by collecting all targets directly.
fn make_traversal_fast(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    connection_type: &str,
    source_level_index: usize,
    direction: Option<Direction>,
    target_type: Option<&[String]>,
) -> Result<(), String> {
    // Get source nodes using iterator to avoid allocation
    let source_level = selection
        .get_level(source_level_index)
        .ok_or_else(|| "No valid source level found for traversal".to_string())?;

    // Collect source nodes (we need this twice - once for iteration, once for the parent map)
    let source_nodes: Vec<NodeIndex> = source_level.iter_node_indices().collect();

    // Create new level
    selection.add_level();
    let target_level_index = selection.get_level_count() - 1;

    // Pre-intern connection type for fast u64 == u64 comparison in inner loop
    let conn_key = InternedKey::from_str(connection_type);

    // Pre-allocate targets HashSet with estimated capacity
    let mut all_targets_per_parent: HashMap<NodeIndex, Vec<NodeIndex>> =
        HashMap::with_capacity(source_nodes.len());

    // Process each source node
    for &source_node in &source_nodes {
        let mut targets: HashSet<NodeIndex> = HashSet::new();

        // Helper: check if a target node passes the type filter
        let type_ok = |idx: petgraph::graph::NodeIndex| -> bool {
            match target_type {
                None => true,
                Some(types) => {
                    let nt = &graph.graph[idx].node_type;
                    types.iter().any(|t| t == nt)
                }
            }
        };

        // Process edges based on direction
        match direction {
            Some(Direction::Outgoing) => {
                for edge in graph.graph.edges_directed(source_node, Direction::Outgoing) {
                    if edge.weight().connection_type == conn_key {
                        let t = edge.target();
                        if type_ok(t) {
                            targets.insert(t);
                        }
                    }
                }
            }
            Some(Direction::Incoming) => {
                for edge in graph.graph.edges_directed(source_node, Direction::Incoming) {
                    if edge.weight().connection_type == conn_key {
                        let t = edge.source();
                        if type_ok(t) {
                            targets.insert(t);
                        }
                    }
                }
            }
            None => {
                // Both directions
                for edge in graph.graph.edges_directed(source_node, Direction::Outgoing) {
                    if edge.weight().connection_type == conn_key {
                        let t = edge.target();
                        if type_ok(t) {
                            targets.insert(t);
                        }
                    }
                }
                for edge in graph.graph.edges_directed(source_node, Direction::Incoming) {
                    if edge.weight().connection_type == conn_key {
                        let t = edge.source();
                        if type_ok(t) {
                            targets.insert(t);
                        }
                    }
                }
            }
        }

        // Store targets for this parent
        if !targets.is_empty() {
            all_targets_per_parent.insert(source_node, targets.into_iter().collect());
        }
    }

    // Get target level and populate it
    let level = selection
        .get_level_mut(target_level_index)
        .ok_or_else(|| "Failed to access target selection level".to_string())?;

    // Set up operation
    level.operations = vec![SelectionOperation::Traverse {
        connection_type: connection_type.to_string(),
        direction: direction.map(|d| {
            if d == Direction::Incoming {
                "incoming"
            } else {
                "outgoing"
            }
            .to_string()
        }),
        max_nodes: None,
    }];

    // Add all parent->children mappings
    for (parent, children) in all_targets_per_parent {
        level.add_selection(Some(parent), children);
    }

    Ok(())
}

/// Full traversal path with filtering, sorting, and limits support.
#[allow(clippy::too_many_arguments)]
fn make_traversal_full(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    connection_type: String,
    source_level_index: usize,
    direction: Option<Direction>,
    filter_target: Option<&HashMap<String, FilterCondition>>,
    filter_connection: Option<&HashMap<String, FilterCondition>>,
    sort_target: Option<&Vec<(String, bool)>>,
    max_nodes: Option<usize>,
    create_new_level: bool,
    temporal_filter: Option<&TemporalEdgeFilter>,
    target_type: Option<&[String]>,
) -> Result<(), String> {
    // Get source level
    let source_level = selection
        .get_level(source_level_index)
        .ok_or_else(|| "No valid source level found for traversal".to_string())?;

    // Collect all necessary data from source level
    let parents: Vec<NodeIndex> = if create_new_level {
        source_level.iter_node_indices().collect()
    } else {
        source_level.selections.keys().filter_map(|k| *k).collect()
    };

    // Create a mapping of parent nodes to their source nodes
    let source_nodes_map: HashMap<NodeIndex, Vec<NodeIndex>> = if create_new_level {
        parents
            .iter()
            .map(|&parent| (parent, vec![parent]))
            .collect()
    } else {
        source_level
            .selections
            .iter()
            .filter_map(|(parent, children)| parent.map(|p| (p, children.clone())))
            .collect()
    };

    // Now we can safely modify the selection
    if create_new_level {
        selection.add_level();
    }

    let target_level_index = if create_new_level {
        selection.get_level_count() - 1
    } else {
        source_level_index
    };

    // Get and initialize target level
    let level = selection
        .get_level_mut(target_level_index)
        .ok_or_else(|| "Failed to access target selection level".to_string())?;

    // Set up operation
    let operation = SelectionOperation::Traverse {
        connection_type: connection_type.clone(),
        direction: direction.map(|d| {
            if d == Direction::Incoming {
                "incoming"
            } else {
                "outgoing"
            }
            .to_string()
        }),
        max_nodes,
    };
    level.operations = vec![operation];

    // Define an empty vector to use when no source nodes exist
    let empty_vec: Vec<NodeIndex> = Vec::new();

    // Process each parent node once
    for &parent in &parents {
        // Use a reference to an existing empty vector to avoid temporary lifetime issues
        let source_nodes = source_nodes_map.get(&parent).unwrap_or(&empty_vec);

        if !create_new_level {
            // Clear existing selection for this parent
            level.selections.entry(Some(parent)).or_default().clear();
        }

        // Collect all targets for this parent in one pass
        let mut targets = HashSet::new();

        // Pre-intern connection type for fast u64 == u64 comparison
        let conn_key = InternedKey::from_str(&connection_type);

        // Helper: check if a target node passes the type filter
        let type_ok = |idx: NodeIndex| -> bool {
            match target_type {
                None => true,
                Some(types) => {
                    let nt = &graph.graph[idx].node_type;
                    types.iter().any(|t| t == nt)
                }
            }
        };

        // Process edges based on direction
        for &source_node in source_nodes {
            match direction {
                Some(Direction::Outgoing) => {
                    for edge in graph.graph.edges_directed(source_node, Direction::Outgoing) {
                        if edge.weight().connection_type == conn_key {
                            if let Some(conn_filter) = filter_connection {
                                if !edge_matches_conditions(&edge.weight().properties, conn_filter)
                                {
                                    continue;
                                }
                            }
                            if let Some(tf) = &temporal_filter {
                                if !edge_passes_temporal(&edge.weight().properties, tf) {
                                    continue;
                                }
                            }
                            let t = edge.target();
                            if type_ok(t) {
                                targets.insert(t);
                            }
                        }
                    }
                }
                Some(Direction::Incoming) => {
                    for edge in graph.graph.edges_directed(source_node, Direction::Incoming) {
                        if edge.weight().connection_type == conn_key {
                            if let Some(conn_filter) = filter_connection {
                                if !edge_matches_conditions(&edge.weight().properties, conn_filter)
                                {
                                    continue;
                                }
                            }
                            if let Some(tf) = &temporal_filter {
                                if !edge_passes_temporal(&edge.weight().properties, tf) {
                                    continue;
                                }
                            }
                            let t = edge.source();
                            if type_ok(t) {
                                targets.insert(t);
                            }
                        }
                    }
                }
                None => {
                    // Both directions
                    for edge in graph.graph.edges_directed(source_node, Direction::Outgoing) {
                        if edge.weight().connection_type == conn_key {
                            if let Some(conn_filter) = filter_connection {
                                if !edge_matches_conditions(&edge.weight().properties, conn_filter)
                                {
                                    continue;
                                }
                            }
                            if let Some(tf) = &temporal_filter {
                                if !edge_passes_temporal(&edge.weight().properties, tf) {
                                    continue;
                                }
                            }
                            let t = edge.target();
                            if type_ok(t) {
                                targets.insert(t);
                            }
                        }
                    }
                    for edge in graph.graph.edges_directed(source_node, Direction::Incoming) {
                        if edge.weight().connection_type == conn_key {
                            if let Some(conn_filter) = filter_connection {
                                if !edge_matches_conditions(&edge.weight().properties, conn_filter)
                                {
                                    continue;
                                }
                            }
                            if let Some(tf) = &temporal_filter {
                                if !edge_passes_temporal(&edge.weight().properties, tf) {
                                    continue;
                                }
                            }
                            let t = edge.source();
                            if type_ok(t) {
                                targets.insert(t);
                            }
                        }
                    }
                }
            }
        }

        // Convert to Vec for processing
        let target_vec: Vec<NodeIndex> = targets.into_iter().collect();

        // Apply filtering and sorting in one pass
        let processed_nodes = filtering_methods::process_nodes(
            graph,
            target_vec,
            filter_target,
            sort_target,
            max_nodes,
        );

        // Add the processed nodes to the selection
        level.add_selection(Some(parent), processed_nodes);
    }

    Ok(())
}

// ── Comparison-based traversal ───────────────────────────────────────────────

/// Dispatcher for comparison-based traversal methods.
/// When `method` is specified, traverse() switches from edge-based to comparison-based mode:
/// the first arg becomes the target node type, and matches are discovered via spatial,
/// semantic, or clustering comparisons rather than pre-existing edges.
pub fn make_comparison_traversal(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    target_type: Option<&str>,
    config: &MethodConfig,
    filter_target: Option<&HashMap<String, FilterCondition>>,
    sort_target: Option<&Vec<(String, bool)>>,
    max_nodes: Option<usize>,
) -> Result<(), String> {
    match config.method_type.as_str() {
        "contains" => {
            let tt = target_type
                .ok_or("method 'contains' requires a target_type (first arg to traverse)")?;
            spatial_contains_traversal(
                graph,
                selection,
                tt,
                config.resolve,
                config.geometry_field.as_deref(),
                filter_target,
                sort_target,
                max_nodes,
            )
        }
        "intersects" => {
            let tt = target_type
                .ok_or("method 'intersects' requires a target_type (first arg to traverse)")?;
            spatial_intersects_traversal(
                graph,
                selection,
                tt,
                config.geometry_field.as_deref(),
                filter_target,
                sort_target,
                max_nodes,
            )
        }
        "distance" => {
            let tt = target_type
                .ok_or("method 'distance' requires a target_type (first arg to traverse)")?;
            let max_dist = config.max_distance_m.ok_or(
                "method 'distance' requires 'max_m' (dict) or max_distance_m parameter",
            )?;
            spatial_distance_traversal(
                graph,
                selection,
                tt,
                max_dist,
                config.resolve,
                config.geometry_field.as_deref(),
                filter_target,
                sort_target,
                max_nodes,
            )
        }
        "text_score" => {
            let tt = target_type
                .ok_or("method 'text_score' requires a target_type (first arg to traverse)")?;
            let prop = config
                .property
                .as_deref()
                .ok_or("method 'text_score' requires 'property'")?;
            let thresh = config.threshold.unwrap_or(0.0);
            let dist_metric = match config.metric.as_deref() {
                Some("dot_product") => vector_search::DistanceMetric::DotProduct,
                Some("euclidean") => vector_search::DistanceMetric::Euclidean,
                Some("poincare") => vector_search::DistanceMetric::Poincare,
                _ => vector_search::DistanceMetric::Cosine,
            };
            semantic_score_traversal(
                graph,
                selection,
                tt,
                prop,
                thresh,
                dist_metric,
                filter_target,
                sort_target,
                max_nodes,
            )
        }
        "cluster" => {
            let algo = config
                .algorithm
                .as_deref()
                .ok_or("method 'cluster' requires 'algorithm' (e.g. 'kmeans')")?;
            let feats = config
                .features
                .as_deref()
                .ok_or("method 'cluster' requires 'features'")?;
            cluster_traversal(
                graph, selection, target_type, algo, feats, config.k, config.eps,
                config.min_samples,
            )
        }
        _ => Err(format!(
            "Unknown traversal method: '{}'. Valid: 'contains', 'intersects', 'distance', 'text_score', 'cluster'",
            config.method_type
        )),
    }
}

// ── Spatial helpers ─────────────────────────────────────────────────────────

/// Resolve the geometry field name for a node type, checking override then SpatialConfig.
fn resolve_geometry_field<'a>(
    spatial_config: Option<&'a SpatialConfig>,
    geometry_field_override: Option<&'a str>,
) -> Option<&'a str> {
    geometry_field_override.or_else(|| spatial_config.and_then(|sc| sc.geometry.as_deref()))
}

/// Extract a parsed WKT geometry from a node's properties.
fn node_geometry(node: &NodeData, geom_field: &str) -> Option<Geometry<f64>> {
    match node.get_property(geom_field).as_deref() {
        Some(Value::String(wkt)) => spatial::parse_wkt(wkt).ok(),
        _ => None,
    }
}

/// Extract (lat, lon) from a node using SpatialConfig (location fields + geometry centroid fallback).
fn node_lat_lon(node: &NodeData, spatial_config: Option<&SpatialConfig>) -> Option<(f64, f64)> {
    let sc = spatial_config?;
    if let Some((ref lat_f, ref lon_f)) = sc.location {
        if let Some((lat, lon)) = extract_lat_lon(node, lat_f, lon_f) {
            return Some((lat, lon));
        }
    }
    // Fallback to geometry centroid
    if let Some(ref geom_f) = sc.geometry {
        if let Some(geom) = node_geometry(node, geom_f) {
            return spatial::geometry_centroid(&geom).ok();
        }
    }
    None
}

/// Resolve a node to a (lat, lon) point respecting the `resolve` mode.
/// - None: default (location → geometry centroid fallback)
/// - Centroid: force geometry centroid (skip location fields)
/// - Closest/Geometry: also resolve to geometry centroid as a point
///   (actual geometry usage is handled by the caller)
fn resolve_node_point(
    node: &NodeData,
    spatial_config: Option<&SpatialConfig>,
    resolve: Option<SpatialResolve>,
    geometry_field_override: Option<&str>,
) -> Option<(f64, f64)> {
    match resolve {
        Some(SpatialResolve::Centroid)
        | Some(SpatialResolve::Closest)
        | Some(SpatialResolve::Geometry) => {
            // Force geometry centroid — skip location fields
            let geom_field = geometry_field_override
                .or_else(|| spatial_config.and_then(|sc| sc.geometry.as_deref()))?;
            let geom = node_geometry(node, geom_field)?;
            spatial::geometry_centroid(&geom).ok()
        }
        None => {
            // Default: location → geometry centroid fallback
            node_lat_lon(node, spatial_config)
        }
    }
}

/// Get the parsed geometry for a node (for resolve='geometry' or 'closest' mode).
fn resolve_node_geom(
    node: &NodeData,
    spatial_config: Option<&SpatialConfig>,
    geometry_field_override: Option<&str>,
) -> Option<Geometry<f64>> {
    let geom_field =
        geometry_field_override.or_else(|| spatial_config.and_then(|sc| sc.geometry.as_deref()))?;
    node_geometry(node, geom_field)
}

fn extract_lat_lon(node: &NodeData, lat_field: &str, lon_field: &str) -> Option<(f64, f64)> {
    let lat = node
        .get_property(lat_field)
        .as_deref()
        .and_then(value_to_f64)?;
    let lon = node
        .get_property(lon_field)
        .as_deref()
        .and_then(value_to_f64)?;
    Some((lat, lon))
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Float64(f) => Some(*f),
        Value::Int64(i) => Some(*i as f64),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Collect source nodes and determine source type from the current selection.
fn get_source_info(
    graph: &DirGraph,
    selection: &CurrentSelection,
) -> Result<(Vec<NodeIndex>, String), String> {
    let level_idx = selection.get_level_count().saturating_sub(1);
    let level = selection
        .get_level(level_idx)
        .ok_or("No source level for comparison traversal")?;
    let source_nodes: Vec<NodeIndex> = level.iter_node_indices().collect();
    if source_nodes.is_empty() {
        return Err("No source nodes for comparison traversal".into());
    }
    let source_type = graph
        .get_node(source_nodes[0])
        .map(|n| n.node_type.clone())
        .ok_or("Cannot determine source node type")?;
    Ok((source_nodes, source_type))
}

/// Get all candidate target nodes from type_indices.
fn get_target_candidates(graph: &DirGraph, target_type: &str) -> Result<Vec<NodeIndex>, String> {
    graph.type_indices.get(target_type).cloned().ok_or_else(|| {
        let available: Vec<&String> = graph.type_indices.keys().collect();
        format!(
            "Target type '{}' not found in graph. Available: {:?}",
            target_type, available
        )
    })
}

/// Insert matched pairs into a new selection level, applying optional filter/sort/limit.
fn insert_matches_into_selection(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    matches: HashMap<NodeIndex, Vec<NodeIndex>>,
    method: &str,
    filter_target: Option<&HashMap<String, FilterCondition>>,
    sort_target: Option<&Vec<(String, bool)>>,
    max_nodes: Option<usize>,
) {
    selection.add_level();
    let target_level_idx = selection.get_level_count() - 1;
    let level = selection.get_level_mut(target_level_idx).unwrap();

    level.operations = vec![SelectionOperation::Custom(format!(
        "compare(method='{}')",
        method
    ))];

    for (parent, children) in matches {
        let processed = filtering_methods::process_nodes(
            graph,
            children,
            filter_target,
            sort_target,
            max_nodes,
        );
        if !processed.is_empty() {
            level.add_selection(Some(parent), processed);
        }
    }
}

// ── Spatial: contains ───────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn spatial_contains_traversal(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    target_type: &str,
    resolve: Option<SpatialResolve>,
    geometry_field: Option<&str>,
    filter_target: Option<&HashMap<String, FilterCondition>>,
    sort_target: Option<&Vec<(String, bool)>>,
    max_nodes: Option<usize>,
) -> Result<(), String> {
    let (source_nodes, source_type) = get_source_info(graph, selection)?;
    let target_candidates = get_target_candidates(graph, target_type)?;

    let source_spatial = graph.get_spatial_config(&source_type);
    let target_spatial = graph.get_spatial_config(target_type);

    // Source needs a geometry field (polygon) for containment
    let src_geom_field =
        resolve_geometry_field(source_spatial, geometry_field).ok_or_else(|| {
            format!(
                "method 'contains' requires source type '{}' to have a geometry. \
             Set via set_spatial() or pass geometry='field' in method dict",
                source_type
            )
        })?;

    let use_full_geometry = resolve == Some(SpatialResolve::Geometry);

    // Build matches: for each source geometry, find targets contained within it
    let mut matches: HashMap<NodeIndex, Vec<NodeIndex>> =
        HashMap::with_capacity(source_nodes.len());

    for &src_idx in &source_nodes {
        let src_node = match graph.get_node(src_idx) {
            Some(n) => n,
            None => continue,
        };
        let src_geom = match node_geometry(src_node, src_geom_field) {
            Some(g) => g,
            None => continue,
        };

        // Compute bounding box for pre-filter
        let src_bbox = geo::BoundingRect::bounding_rect(&src_geom);

        let mut matched = Vec::new();
        for &tgt_idx in &target_candidates {
            let tgt_node = match graph.get_node(tgt_idx) {
                Some(n) => n,
                None => continue,
            };

            if use_full_geometry {
                // resolve='geometry': polygon-in-polygon containment
                if let Some(tgt_geom) = resolve_node_geom(tgt_node, target_spatial, geometry_field)
                {
                    if spatial::geometry_contains_geometry(&src_geom, &tgt_geom) {
                        matched.push(tgt_idx);
                    }
                }
            } else {
                // Default / resolve='centroid': target as point → point-in-polygon
                if let Some((lat, lon)) =
                    resolve_node_point(tgt_node, target_spatial, resolve, geometry_field)
                {
                    // Bounding box pre-filter
                    if let Some(ref bbox) = src_bbox {
                        if lat < bbox.min().y
                            || lat > bbox.max().y
                            || lon < bbox.min().x
                            || lon > bbox.max().x
                        {
                            continue;
                        }
                    }
                    let pt = geo::geometry::Point::new(lon, lat);
                    if spatial::geometry_contains_point(&src_geom, &pt) {
                        matched.push(tgt_idx);
                    }
                }
            }
        }

        if !matched.is_empty() {
            matches.insert(src_idx, matched);
        }
    }

    insert_matches_into_selection(
        graph,
        selection,
        matches,
        "contains",
        filter_target,
        sort_target,
        max_nodes,
    );
    Ok(())
}

// ── Spatial: intersects ─────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn spatial_intersects_traversal(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    target_type: &str,
    geometry_field: Option<&str>,
    filter_target: Option<&HashMap<String, FilterCondition>>,
    sort_target: Option<&Vec<(String, bool)>>,
    max_nodes: Option<usize>,
) -> Result<(), String> {
    let (source_nodes, source_type) = get_source_info(graph, selection)?;
    let target_candidates = get_target_candidates(graph, target_type)?;

    let source_spatial = graph.get_spatial_config(&source_type);
    let target_spatial = graph.get_spatial_config(target_type);

    let src_geom_field =
        resolve_geometry_field(source_spatial, geometry_field).ok_or_else(|| {
            format!(
                "method 'intersects' requires source type '{}' to have a geometry. \
             Set via set_spatial() or pass geometry='field' in method dict",
                source_type
            )
        })?;
    let tgt_geom_field =
        resolve_geometry_field(target_spatial, geometry_field).ok_or_else(|| {
            format!(
                "method 'intersects' requires target type '{}' to have a geometry. \
             Set via set_spatial() or pass geometry='field' in method dict",
                target_type
            )
        })?;

    let mut matches: HashMap<NodeIndex, Vec<NodeIndex>> =
        HashMap::with_capacity(source_nodes.len());

    for &src_idx in &source_nodes {
        let src_node = match graph.get_node(src_idx) {
            Some(n) => n,
            None => continue,
        };
        let src_geom = match node_geometry(src_node, src_geom_field) {
            Some(g) => g,
            None => continue,
        };

        let mut matched = Vec::new();
        for &tgt_idx in &target_candidates {
            let tgt_node = match graph.get_node(tgt_idx) {
                Some(n) => n,
                None => continue,
            };
            if let Some(tgt_geom) = node_geometry(tgt_node, tgt_geom_field) {
                if spatial::geometries_intersect(&src_geom, &tgt_geom) {
                    matched.push(tgt_idx);
                }
            }
        }

        if !matched.is_empty() {
            matches.insert(src_idx, matched);
        }
    }

    insert_matches_into_selection(
        graph,
        selection,
        matches,
        "intersects",
        filter_target,
        sort_target,
        max_nodes,
    );
    Ok(())
}

// ── Spatial: distance ───────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn spatial_distance_traversal(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    target_type: &str,
    max_distance_m: f64,
    resolve: Option<SpatialResolve>,
    geometry_field: Option<&str>,
    filter_target: Option<&HashMap<String, FilterCondition>>,
    sort_target: Option<&Vec<(String, bool)>>,
    max_nodes: Option<usize>,
) -> Result<(), String> {
    let (source_nodes, source_type) = get_source_info(graph, selection)?;
    let target_candidates = get_target_candidates(graph, target_type)?;

    let source_spatial = graph.get_spatial_config(&source_type);
    let target_spatial = graph.get_spatial_config(target_type);

    let use_closest = resolve == Some(SpatialResolve::Closest);

    if use_closest {
        // ── resolve='closest': use geometry boundaries for minimum distance ──
        distance_closest_mode(
            graph,
            selection,
            &source_nodes,
            &target_candidates,
            max_distance_m,
            source_spatial,
            target_spatial,
            geometry_field,
            filter_target,
            sort_target,
            max_nodes,
        )
    } else {
        // ── Default / resolve='centroid': point-to-point geodesic distance ──
        distance_point_mode(
            graph,
            selection,
            &source_nodes,
            &target_candidates,
            max_distance_m,
            resolve,
            source_spatial,
            target_spatial,
            geometry_field,
            filter_target,
            sort_target,
            max_nodes,
        )
    }
}

/// Distance using point-to-point (default or centroid resolve).
#[allow(clippy::too_many_arguments)]
fn distance_point_mode(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    source_nodes: &[NodeIndex],
    target_candidates: &[NodeIndex],
    max_distance_m: f64,
    resolve: Option<SpatialResolve>,
    source_spatial: Option<&SpatialConfig>,
    target_spatial: Option<&SpatialConfig>,
    geometry_field: Option<&str>,
    filter_target: Option<&HashMap<String, FilterCondition>>,
    sort_target: Option<&Vec<(String, bool)>>,
    max_nodes: Option<usize>,
) -> Result<(), String> {
    struct TargetLoc {
        idx: NodeIndex,
        lat: f64,
        lon: f64,
    }

    // Pre-compute target points
    let mut target_locs: Vec<TargetLoc> = Vec::with_capacity(target_candidates.len());
    for &tgt_idx in target_candidates {
        if let Some(tgt_node) = graph.get_node(tgt_idx) {
            if let Some((lat, lon)) =
                resolve_node_point(tgt_node, target_spatial, resolve, geometry_field)
            {
                target_locs.push(TargetLoc {
                    idx: tgt_idx,
                    lat,
                    lon,
                });
            }
        }
    }

    let mut matches: HashMap<NodeIndex, Vec<NodeIndex>> =
        HashMap::with_capacity(source_nodes.len());

    for &src_idx in source_nodes {
        let src_node = match graph.get_node(src_idx) {
            Some(n) => n,
            None => continue,
        };

        let (src_lat, src_lon) =
            match resolve_node_point(src_node, source_spatial, resolve, geometry_field) {
                Some(loc) => loc,
                None => continue,
            };

        let mut matched = Vec::new();
        for tgt in &target_locs {
            let dist = spatial::geodesic_distance(src_lat, src_lon, tgt.lat, tgt.lon);
            if dist <= max_distance_m {
                matched.push(tgt.idx);
            }
        }

        if !matched.is_empty() {
            matches.insert(src_idx, matched);
        }
    }

    insert_matches_into_selection(
        graph,
        selection,
        matches,
        "distance",
        filter_target,
        sort_target,
        max_nodes,
    );
    Ok(())
}

/// Distance using closest boundary points (resolve='closest').
#[allow(clippy::too_many_arguments)]
fn distance_closest_mode(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    source_nodes: &[NodeIndex],
    target_candidates: &[NodeIndex],
    max_distance_m: f64,
    source_spatial: Option<&SpatialConfig>,
    target_spatial: Option<&SpatialConfig>,
    geometry_field: Option<&str>,
    filter_target: Option<&HashMap<String, FilterCondition>>,
    sort_target: Option<&Vec<(String, bool)>>,
    max_nodes: Option<usize>,
) -> Result<(), String> {
    let mut matches: HashMap<NodeIndex, Vec<NodeIndex>> =
        HashMap::with_capacity(source_nodes.len());

    for &src_idx in source_nodes {
        let src_node = match graph.get_node(src_idx) {
            Some(n) => n,
            None => continue,
        };

        let src_geom = resolve_node_geom(src_node, source_spatial, geometry_field);
        // Fallback to centroid point if no geometry
        let src_point = resolve_node_point(
            src_node,
            source_spatial,
            Some(SpatialResolve::Centroid),
            geometry_field,
        );

        if src_geom.is_none() && src_point.is_none() {
            continue;
        }

        let mut matched = Vec::new();
        for &tgt_idx in target_candidates {
            let tgt_node = match graph.get_node(tgt_idx) {
                Some(n) => n,
                None => continue,
            };

            let tgt_geom = resolve_node_geom(tgt_node, target_spatial, geometry_field);
            let tgt_point = resolve_node_point(
                tgt_node,
                target_spatial,
                Some(SpatialResolve::Centroid),
                geometry_field,
            );

            // Compute minimum boundary distance using best available info
            let dist = match (&src_geom, &tgt_geom) {
                (Some(sg), Some(tg)) => {
                    // Both have geometry: use point_to_geometry for better approximation
                    // (try both directions, take minimum)
                    let d1 = src_point.and_then(|(lat, lon)| {
                        spatial::point_to_geometry_distance_m(lat, lon, tg).ok()
                    });
                    let d2 = tgt_point.and_then(|(lat, lon)| {
                        spatial::point_to_geometry_distance_m(lat, lon, sg).ok()
                    });
                    match (d1, d2) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (Some(a), None) => Some(a),
                        (None, Some(b)) => Some(b),
                        (None, None) => {
                            // Last resort: centroid-to-centroid
                            spatial::geometry_to_geometry_distance_m(sg, tg).ok()
                        }
                    }
                }
                (Some(sg), None) => {
                    // Source has geometry, target is a point
                    tgt_point.and_then(|(lat, lon)| {
                        spatial::point_to_geometry_distance_m(lat, lon, sg).ok()
                    })
                }
                (None, Some(tg)) => {
                    // Source is a point, target has geometry
                    src_point.and_then(|(lat, lon)| {
                        spatial::point_to_geometry_distance_m(lat, lon, tg).ok()
                    })
                }
                (None, None) => {
                    // Both are points — fallback to geodesic
                    match (src_point, tgt_point) {
                        (Some((lat1, lon1)), Some((lat2, lon2))) => {
                            Some(spatial::geodesic_distance(lat1, lon1, lat2, lon2))
                        }
                        _ => None,
                    }
                }
            };

            if let Some(d) = dist {
                if d <= max_distance_m {
                    matched.push(tgt_idx);
                }
            }
        }

        if !matched.is_empty() {
            matches.insert(src_idx, matched);
        }
    }

    insert_matches_into_selection(
        graph,
        selection,
        matches,
        "distance",
        filter_target,
        sort_target,
        max_nodes,
    );
    Ok(())
}

// ── Semantic: text_score ────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn semantic_score_traversal(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    target_type: &str,
    embedding_property: &str,
    threshold: f64,
    metric: vector_search::DistanceMetric,
    filter_target: Option<&HashMap<String, FilterCondition>>,
    sort_target: Option<&Vec<(String, bool)>>,
    max_nodes: Option<usize>,
) -> Result<(), String> {
    let (source_nodes, source_type) = get_source_info(graph, selection)?;
    let target_candidates = get_target_candidates(graph, target_type)?;

    // Get embedding stores for source and target types
    let src_store = graph
        .embeddings
        .get(&(source_type.clone(), embedding_property.to_string()))
        .ok_or_else(|| {
            format!(
                "No embeddings found for type '{}', property '{}'. Use set_embedder() first.",
                source_type, embedding_property
            )
        })?;

    let tgt_store = graph
        .embeddings
        .get(&(target_type.to_string(), embedding_property.to_string()))
        .ok_or_else(|| {
            format!(
                "No embeddings found for type '{}', property '{}'. Use set_embedder() first.",
                target_type, embedding_property
            )
        })?;

    let similarity_fn = match metric {
        vector_search::DistanceMetric::Cosine => vector_search::cosine_similarity,
        vector_search::DistanceMetric::DotProduct => vector_search::dot_product,
        vector_search::DistanceMetric::Euclidean => vector_search::neg_euclidean_distance,
        vector_search::DistanceMetric::Poincare => vector_search::neg_poincare_distance,
    };
    let threshold_f32 = threshold as f32;

    let mut matches: HashMap<NodeIndex, Vec<NodeIndex>> =
        HashMap::with_capacity(source_nodes.len());

    for &src_idx in &source_nodes {
        let src_embedding = match src_store.get_embedding(src_idx.index()) {
            Some(e) => e,
            None => continue,
        };

        let mut matched = Vec::new();
        for &tgt_idx in &target_candidates {
            // Skip self-matches
            if tgt_idx == src_idx {
                continue;
            }
            if let Some(tgt_embedding) = tgt_store.get_embedding(tgt_idx.index()) {
                let score = similarity_fn(src_embedding, tgt_embedding);
                if score >= threshold_f32 {
                    matched.push(tgt_idx);
                }
            }
        }

        if !matched.is_empty() {
            matches.insert(src_idx, matched);
        }
    }

    insert_matches_into_selection(
        graph,
        selection,
        matches,
        "text_score",
        filter_target,
        sort_target,
        max_nodes,
    );
    Ok(())
}

// ── Clustering ──────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn cluster_traversal(
    graph: &DirGraph,
    selection: &mut CurrentSelection,
    target_type: Option<&str>,
    algorithm: &str,
    features: &[String],
    k: Option<usize>,
    eps: Option<f64>,
    min_samples: Option<usize>,
) -> Result<(), String> {
    use crate::graph::clustering;

    let level_idx = selection.get_level_count().saturating_sub(1);
    let level = selection
        .get_level(level_idx)
        .ok_or("No source level for cluster traversal")?;
    let source_nodes: Vec<NodeIndex> = level.iter_node_indices().collect();
    if source_nodes.is_empty() {
        return Err("No source nodes for cluster traversal".into());
    }

    // Optionally filter by target_type
    let nodes: Vec<NodeIndex> = if let Some(tt) = target_type {
        source_nodes
            .into_iter()
            .filter(|&idx| {
                graph
                    .get_node(idx)
                    .map(|n| n.node_type == tt)
                    .unwrap_or(false)
            })
            .collect()
    } else {
        source_nodes
    };

    if nodes.is_empty() {
        return Err("No nodes remain after type filter for clustering".into());
    }

    // Check if features are spatial (latitude, longitude) — use haversine distance matrix
    let source_type = graph
        .get_node(nodes[0])
        .map(|n| n.node_type.clone())
        .unwrap_or_default();
    let spatial_cfg = graph.get_spatial_config(&source_type);
    let is_spatial = features.len() >= 2 && {
        if let Some(sc) = spatial_cfg {
            if let Some((ref lat_f, ref lon_f)) = sc.location {
                features.contains(&lat_f.to_string()) && features.contains(&lon_f.to_string())
            } else {
                false
            }
        } else {
            false
        }
    };

    // Extract feature matrix
    let mut feature_matrix: Vec<Vec<f64>> = Vec::with_capacity(nodes.len());
    for &idx in &nodes {
        let node = graph.get_node(idx).unwrap();
        let mut row = Vec::with_capacity(features.len());
        for feat in features {
            let val = node
                .get_property(feat)
                .as_deref()
                .and_then(value_to_f64)
                .unwrap_or(0.0);
            row.push(val);
        }
        feature_matrix.push(row);
    }

    let assignments = match algorithm {
        "kmeans" => {
            let k_val = k.ok_or("method='cluster' with algorithm='kmeans' requires k parameter")?;
            clustering::kmeans(&feature_matrix, k_val, 100)
        }
        "dbscan" => {
            let eps_val =
                eps.ok_or("method='cluster' with algorithm='dbscan' requires eps parameter")?;
            let min_pts = min_samples.unwrap_or(5);
            let distances = if is_spatial {
                // Extract lat/lon columns for haversine
                let lat_idx = features
                    .iter()
                    .position(|f| {
                        spatial_cfg
                            .and_then(|sc| sc.location.as_ref())
                            .map(|(lat_f, _)| f == lat_f)
                            .unwrap_or(false)
                    })
                    .unwrap_or(0);
                let lon_idx = features
                    .iter()
                    .position(|f| {
                        spatial_cfg
                            .and_then(|sc| sc.location.as_ref())
                            .map(|(_, lon_f)| f == lon_f)
                            .unwrap_or(false)
                    })
                    .unwrap_or(1);
                let coords: Vec<(f64, f64)> = feature_matrix
                    .iter()
                    .map(|row| (row[lat_idx], row[lon_idx]))
                    .collect();
                clustering::haversine_distance_matrix(&coords)
            } else {
                let mut feat_clone = feature_matrix.clone();
                clustering::normalize_features(&mut feat_clone);
                clustering::euclidean_distance_matrix(&feat_clone)
            };
            clustering::dbscan(&distances, eps_val, min_pts)
        }
        _ => {
            return Err(format!(
                "Unknown clustering algorithm: '{}'. Valid: 'kmeans', 'dbscan'",
                algorithm
            ))
        }
    };

    // Build selection hierarchy: cluster_id -> member nodes
    // Use a synthetic parent = None for each cluster group
    selection.add_level();
    let target_level_idx = selection.get_level_count() - 1;
    let level = selection.get_level_mut(target_level_idx).unwrap();
    level.operations = vec![SelectionOperation::Custom(format!(
        "compare(method='cluster', algorithm='{}')",
        algorithm
    ))];

    // Group nodes by cluster
    let mut clusters: HashMap<i64, Vec<NodeIndex>> = HashMap::new();
    for assign in &assignments {
        clusters
            .entry(assign.cluster)
            .or_default()
            .push(nodes[assign.index]);
    }

    // For clustering, we don't have natural parent nodes. We insert each cluster
    // group with parent=None. The first node of each cluster serves as a representative parent.
    // This allows downstream methods like statistics() to group by cluster.
    for members in clusters.values() {
        if members.is_empty() {
            continue;
        }
        // Use first member as the "parent" representative for this cluster group
        let representative = members[0];
        let children: Vec<NodeIndex> = members[1..].to_vec();
        if children.is_empty() {
            // Single-member cluster: insert with None parent
            level.add_selection(None, vec![representative]);
        } else {
            level.add_selection(Some(representative), children);
        }
    }

    Ok(())
}

pub struct ChildPropertyGroup {
    pub parent_idx: NodeIndex,
    pub parent_title: String,
    pub values: Vec<String>,
}

pub fn get_children_properties(
    graph: &DirGraph,
    selection: &CurrentSelection,
    property: &str,
) -> Vec<ChildPropertyGroup> {
    let mut result = Vec::new();

    // Get the current level index
    let level_index = selection.get_level_count().saturating_sub(1);

    // Get all parents with their children
    if let Some(level) = selection.get_level(level_index) {
        for (&parent_opt, children) in &level.selections {
            if let Some(parent) = parent_opt {
                // Get parent title
                let parent_title = if let Some(node) = graph.get_node(parent) {
                    match node.get_field_ref("title").as_deref() {
                        Some(Value::String(s)) => s.clone(),
                        _ => format!("node_{}", parent.index()),
                    }
                } else {
                    format!("node_{}", parent.index())
                };

                // For each parent, collect property values from children
                let mut values_list = Vec::new();

                for &child_idx in children {
                    if let Some(node) = graph.get_node(child_idx) {
                        let value = match node.get_field_ref(property).as_deref() {
                            Some(Value::String(s)) => s.clone(),
                            Some(Value::Int64(i)) => i.to_string(),
                            Some(Value::Float64(f)) => f.to_string(),
                            Some(Value::Boolean(b)) => b.to_string(),
                            Some(Value::UniqueId(u)) => u.to_string(),
                            Some(Value::DateTime(d)) => d.format("%Y-%m-%d").to_string(),
                            Some(Value::Point { lat, lon }) => {
                                format!("point({}, {})", lat, lon)
                            }
                            Some(Value::Null) => "null".to_string(),
                            Some(Value::NodeRef(idx)) => format!("node#{}", idx),
                            Some(Value::EdgeRef { edge_idx, .. }) => {
                                format!("edge#{}", edge_idx)
                            }
                            None => continue,
                        };

                        values_list.push(value);
                    }
                }

                result.push(ChildPropertyGroup {
                    parent_idx: parent,
                    parent_title,
                    values: values_list,
                });
            }
        }
    }

    result
}

/// Helper to format a list of values with optional truncation
fn format_property_list(values: &[String], max_length: Option<usize>) -> String {
    let joined = values.join(", ");
    match max_length {
        Some(max) if joined.len() > max => {
            format!("{}...", &joined[..max.saturating_sub(3)])
        }
        _ => joined,
    }
}

pub fn format_for_storage(
    property_groups: &[ChildPropertyGroup],
    max_length: Option<usize>,
) -> Vec<(Option<NodeIndex>, Value)> {
    property_groups
        .iter()
        .map(|group| {
            let formatted = format_property_list(&group.values, max_length);
            (Some(group.parent_idx), Value::String(formatted))
        })
        .collect()
}

pub fn format_for_dictionary(
    property_groups: &[ChildPropertyGroup],
    max_length: Option<usize>,
) -> Vec<(String, String)> {
    property_groups
        .iter()
        .map(|group| {
            let formatted = format_property_list(&group.values, max_length);
            (group.parent_title.clone(), formatted)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::values::Value;
    use crate::graph::schema::{DirGraph, EdgeData, NodeData, TemporalConfig};
    use petgraph::graph::NodeIndex;
    use std::collections::HashMap;

    // ── Test-graph builders ────────────────────────────────────────────────

    /// A -> B -> C, all "Person" type, edge type "KNOWS"
    fn build_linear_graph() -> (DirGraph, Vec<NodeIndex>) {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();
        for i in 0..3 {
            let node = NodeData::new(
                Value::Int64(i),
                Value::String(format!("Person_{}", i)),
                "Person".to_string(),
                HashMap::new(),
                &mut graph.interner,
            );
            let idx = graph.graph.add_node(node);
            graph
                .type_indices
                .entry("Person".to_string())
                .or_default()
                .push(idx);
            indices.push(idx);
        }
        for i in 0..2 {
            let edge = EdgeData::new("KNOWS".to_string(), HashMap::new(), &mut graph.interner);
            graph.graph.add_edge(indices[i], indices[i + 1], edge);
        }
        graph.register_connection_type("KNOWS".to_string());
        (graph, indices)
    }

    /// A -> B, A -> C, B -> D  (tree shape)
    /// All "Person" type, edge type "MANAGES"
    fn build_tree_graph() -> (DirGraph, Vec<NodeIndex>) {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();
        let names = ["Alice", "Bob", "Carol", "Dave"];
        for (i, name) in names.iter().enumerate() {
            let node = NodeData::new(
                Value::Int64(i as i64),
                Value::String(name.to_string()),
                "Person".to_string(),
                HashMap::new(),
                &mut graph.interner,
            );
            let idx = graph.graph.add_node(node);
            graph
                .type_indices
                .entry("Person".to_string())
                .or_default()
                .push(idx);
            indices.push(idx);
        }
        // A->B, A->C, B->D
        let pairs = [(0, 1), (0, 2), (1, 3)];
        for (from, to) in pairs {
            let edge = EdgeData::new("MANAGES".to_string(), HashMap::new(), &mut graph.interner);
            graph.graph.add_edge(indices[from], indices[to], edge);
        }
        graph.register_connection_type("MANAGES".to_string());
        (graph, indices)
    }

    /// A -> B (KNOWS), A -> C (LIKES)  — two different edge types
    fn build_multi_edge_type_graph() -> (DirGraph, Vec<NodeIndex>) {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();
        for i in 0..3 {
            let node = NodeData::new(
                Value::Int64(i),
                Value::String(format!("Node_{}", i)),
                "Person".to_string(),
                HashMap::new(),
                &mut graph.interner,
            );
            let idx = graph.graph.add_node(node);
            graph
                .type_indices
                .entry("Person".to_string())
                .or_default()
                .push(idx);
            indices.push(idx);
        }
        let edge_knows = EdgeData::new("KNOWS".to_string(), HashMap::new(), &mut graph.interner);
        graph.graph.add_edge(indices[0], indices[1], edge_knows);
        let edge_likes = EdgeData::new("LIKES".to_string(), HashMap::new(), &mut graph.interner);
        graph.graph.add_edge(indices[0], indices[2], edge_likes);
        graph.register_connection_type("KNOWS".to_string());
        graph.register_connection_type("LIKES".to_string());
        (graph, indices)
    }

    /// Build a graph with two node types: Person and City, connected by LIVES_IN
    fn build_heterogeneous_graph() -> (DirGraph, Vec<NodeIndex>) {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();
        // Person nodes
        for i in 0..2 {
            let node = NodeData::new(
                Value::Int64(i),
                Value::String(format!("Person_{}", i)),
                "Person".to_string(),
                HashMap::new(),
                &mut graph.interner,
            );
            let idx = graph.graph.add_node(node);
            graph
                .type_indices
                .entry("Person".to_string())
                .or_default()
                .push(idx);
            indices.push(idx);
        }
        // City node
        let city = NodeData::new(
            Value::Int64(10),
            Value::String("Melbourne".to_string()),
            "City".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let city_idx = graph.graph.add_node(city);
        graph
            .type_indices
            .entry("City".to_string())
            .or_default()
            .push(city_idx);
        indices.push(city_idx);

        // Person_0 -> Melbourne, Person_1 -> Melbourne
        for i in 0..2 {
            let edge = EdgeData::new("LIVES_IN".to_string(), HashMap::new(), &mut graph.interner);
            graph.graph.add_edge(indices[i], city_idx, edge);
        }
        graph.register_connection_type("LIVES_IN".to_string());
        (graph, indices)
    }

    /// A -> A (self-loop)
    fn build_self_loop_graph() -> (DirGraph, Vec<NodeIndex>) {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("Self".to_string()),
            "Node".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let idx = graph.graph.add_node(node);
        graph
            .type_indices
            .entry("Node".to_string())
            .or_default()
            .push(idx);
        let edge = EdgeData::new("LOOP".to_string(), HashMap::new(), &mut graph.interner);
        graph.graph.add_edge(idx, idx, edge);
        graph.register_connection_type("LOOP".to_string());
        (graph, vec![idx])
    }

    /// A <-> B (cycle of length 2)
    fn build_cycle_graph() -> (DirGraph, Vec<NodeIndex>) {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();
        for i in 0..2 {
            let node = NodeData::new(
                Value::Int64(i),
                Value::String(format!("Node_{}", i)),
                "Node".to_string(),
                HashMap::new(),
                &mut graph.interner,
            );
            let idx = graph.graph.add_node(node);
            graph
                .type_indices
                .entry("Node".to_string())
                .or_default()
                .push(idx);
            indices.push(idx);
        }
        let edge1 = EdgeData::new("LINK".to_string(), HashMap::new(), &mut graph.interner);
        graph.graph.add_edge(indices[0], indices[1], edge1);
        let edge2 = EdgeData::new("LINK".to_string(), HashMap::new(), &mut graph.interner);
        graph.graph.add_edge(indices[1], indices[0], edge2);
        graph.register_connection_type("LINK".to_string());
        (graph, indices)
    }

    /// Two disconnected components: {A->B} and {C->D}, edge type "LINK"
    fn build_disconnected_graph() -> (DirGraph, Vec<NodeIndex>) {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();
        for i in 0..4 {
            let node = NodeData::new(
                Value::Int64(i),
                Value::String(format!("Node_{}", i)),
                "Node".to_string(),
                HashMap::new(),
                &mut graph.interner,
            );
            let idx = graph.graph.add_node(node);
            graph
                .type_indices
                .entry("Node".to_string())
                .or_default()
                .push(idx);
            indices.push(idx);
        }
        let edge1 = EdgeData::new("LINK".to_string(), HashMap::new(), &mut graph.interner);
        graph.graph.add_edge(indices[0], indices[1], edge1);
        let edge2 = EdgeData::new("LINK".to_string(), HashMap::new(), &mut graph.interner);
        graph.graph.add_edge(indices[2], indices[3], edge2);
        graph.register_connection_type("LINK".to_string());
        (graph, indices)
    }

    /// Build a graph with edge properties (weight: i64)
    fn build_weighted_edge_graph() -> (DirGraph, Vec<NodeIndex>) {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();
        for i in 0..3 {
            let node = NodeData::new(
                Value::Int64(i),
                Value::String(format!("Node_{}", i)),
                "Node".to_string(),
                HashMap::new(),
                &mut graph.interner,
            );
            let idx = graph.graph.add_node(node);
            graph
                .type_indices
                .entry("Node".to_string())
                .or_default()
                .push(idx);
            indices.push(idx);
        }
        // A->B with weight=10, A->C with weight=20
        let mut props1 = HashMap::new();
        props1.insert("weight".to_string(), Value::Int64(10));
        let edge1 = EdgeData::new("LINK".to_string(), props1, &mut graph.interner);
        graph.graph.add_edge(indices[0], indices[1], edge1);

        let mut props2 = HashMap::new();
        props2.insert("weight".to_string(), Value::Int64(20));
        let edge2 = EdgeData::new("LINK".to_string(), props2, &mut graph.interner);
        graph.graph.add_edge(indices[0], indices[2], edge2);
        graph.register_connection_type("LINK".to_string());
        (graph, indices)
    }

    /// Helper: set up a selection with given nodes at level 0
    fn selection_with_nodes(nodes: &[NodeIndex]) -> CurrentSelection {
        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(None, nodes.to_vec());
        sel
    }

    /// Helper: create a SpatialConfig with all required fields
    fn spatial_config(
        location: Option<(String, String)>,
        geometry: Option<String>,
    ) -> SpatialConfig {
        SpatialConfig {
            location,
            geometry,
            points: HashMap::new(),
            shapes: HashMap::new(),
        }
    }

    // ── MethodConfig tests ────────────────────────────────────────────────

    #[test]
    fn test_method_config_from_string() {
        let cfg = MethodConfig::from_string("distance".to_string());
        assert_eq!(cfg.method_type, "distance");
        assert!(cfg.resolve.is_none());
        assert!(cfg.max_distance_m.is_none());
        assert!(cfg.geometry_field.is_none());
        assert!(cfg.property.is_none());
        assert!(cfg.threshold.is_none());
        assert!(cfg.metric.is_none());
        assert!(cfg.algorithm.is_none());
        assert!(cfg.features.is_none());
        assert!(cfg.k.is_none());
        assert!(cfg.eps.is_none());
        assert!(cfg.min_samples.is_none());
    }

    #[test]
    fn test_method_config_all_fields() {
        let mut config = MethodConfig::from_string("distance".to_string());
        config.resolve = Some(SpatialResolve::Closest);
        config.max_distance_m = Some(1000.0);
        config.geometry_field = Some("geom".to_string());
        config.property = Some("embedding".to_string());
        config.threshold = Some(0.8);
        config.metric = Some("cosine".to_string());
        config.algorithm = Some("kmeans".to_string());
        config.features = Some(vec!["lat".to_string(), "lon".to_string()]);
        config.k = Some(5);
        config.eps = Some(0.5);
        config.min_samples = Some(3);

        assert_eq!(config.method_type, "distance");
        assert_eq!(config.resolve, Some(SpatialResolve::Closest));
        assert_eq!(config.max_distance_m, Some(1000.0));
        assert_eq!(config.geometry_field, Some("geom".to_string()));
        assert_eq!(config.property, Some("embedding".to_string()));
        assert_eq!(config.threshold, Some(0.8));
        assert_eq!(config.metric, Some("cosine".to_string()));
        assert_eq!(config.algorithm, Some("kmeans".to_string()));
        assert_eq!(config.features.as_ref().unwrap().len(), 2);
        assert_eq!(config.k, Some(5));
        assert_eq!(config.eps, Some(0.5));
        assert_eq!(config.min_samples, Some(3));
    }

    // ── parse_resolve tests ───────────────────────────────────────────────

    #[test]
    fn test_parse_resolve_centroid() {
        assert_eq!(
            MethodConfig::parse_resolve("centroid").unwrap(),
            SpatialResolve::Centroid
        );
    }

    #[test]
    fn test_parse_resolve_closest() {
        assert_eq!(
            MethodConfig::parse_resolve("closest").unwrap(),
            SpatialResolve::Closest
        );
    }

    #[test]
    fn test_parse_resolve_geometry() {
        assert_eq!(
            MethodConfig::parse_resolve("geometry").unwrap(),
            SpatialResolve::Geometry
        );
    }

    #[test]
    fn test_parse_resolve_invalid() {
        let result = MethodConfig::parse_resolve("invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown resolve mode"));
    }

    // ── edge_matches_conditions tests ──────────────────────────────────────

    #[test]
    fn test_edge_matches_conditions_eq() {
        let props = vec![(InternedKey::from_str("weight"), Value::Int64(10))];
        let mut conditions = HashMap::new();
        conditions.insert(
            "weight".to_string(),
            FilterCondition::Equals(Value::Int64(10)),
        );
        assert!(edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_no_match() {
        let props = vec![(InternedKey::from_str("weight"), Value::Int64(5))];
        let mut conditions = HashMap::new();
        conditions.insert(
            "weight".to_string(),
            FilterCondition::Equals(Value::Int64(10)),
        );
        assert!(!edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_missing_field_is_null() {
        let props: Vec<(InternedKey, Value)> = vec![];
        let mut conditions = HashMap::new();
        conditions.insert("missing_field".to_string(), FilterCondition::IsNull);
        assert!(edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_missing_field_not_eq() {
        let props: Vec<(InternedKey, Value)> = vec![];
        let mut conditions = HashMap::new();
        conditions.insert(
            "missing_field".to_string(),
            FilterCondition::Equals(Value::Int64(10)),
        );
        assert!(!edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_multiple_all_pass() {
        let props = vec![
            (InternedKey::from_str("weight"), Value::Int64(10)),
            (
                InternedKey::from_str("status"),
                Value::String("active".to_string()),
            ),
        ];
        let mut conditions = HashMap::new();
        conditions.insert(
            "weight".to_string(),
            FilterCondition::Equals(Value::Int64(10)),
        );
        conditions.insert(
            "status".to_string(),
            FilterCondition::Equals(Value::String("active".to_string())),
        );
        assert!(edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_multiple_one_fails() {
        let props = vec![
            (InternedKey::from_str("weight"), Value::Int64(10)),
            (
                InternedKey::from_str("status"),
                Value::String("inactive".to_string()),
            ),
        ];
        let mut conditions = HashMap::new();
        conditions.insert(
            "weight".to_string(),
            FilterCondition::Equals(Value::Int64(10)),
        );
        conditions.insert(
            "status".to_string(),
            FilterCondition::Equals(Value::String("active".to_string())),
        );
        assert!(!edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_empty_conditions() {
        let props = vec![(InternedKey::from_str("weight"), Value::Int64(10))];
        let conditions: HashMap<String, FilterCondition> = HashMap::new();
        assert!(edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_greater_than() {
        let props = vec![(InternedKey::from_str("weight"), Value::Int64(15))];
        let mut conditions = HashMap::new();
        conditions.insert(
            "weight".to_string(),
            FilterCondition::GreaterThan(Value::Int64(10)),
        );
        assert!(edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_less_than() {
        let props = vec![(InternedKey::from_str("weight"), Value::Int64(5))];
        let mut conditions = HashMap::new();
        conditions.insert(
            "weight".to_string(),
            FilterCondition::LessThan(Value::Int64(10)),
        );
        assert!(edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_is_not_null() {
        let props = vec![(InternedKey::from_str("weight"), Value::Int64(10))];
        let mut conditions = HashMap::new();
        conditions.insert("weight".to_string(), FilterCondition::IsNotNull);
        assert!(edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_is_not_null_missing() {
        let props: Vec<(InternedKey, Value)> = vec![];
        let mut conditions = HashMap::new();
        conditions.insert("weight".to_string(), FilterCondition::IsNotNull);
        assert!(!edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_in_list() {
        let props = vec![(
            InternedKey::from_str("status"),
            Value::String("active".to_string()),
        )];
        let mut conditions = HashMap::new();
        conditions.insert(
            "status".to_string(),
            FilterCondition::In(vec![
                Value::String("active".to_string()),
                Value::String("pending".to_string()),
            ]),
        );
        assert!(edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_in_list_no_match() {
        let props = vec![(
            InternedKey::from_str("status"),
            Value::String("closed".to_string()),
        )];
        let mut conditions = HashMap::new();
        conditions.insert(
            "status".to_string(),
            FilterCondition::In(vec![
                Value::String("active".to_string()),
                Value::String("pending".to_string()),
            ]),
        );
        assert!(!edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_contains_string() {
        let props = vec![(
            InternedKey::from_str("description"),
            Value::String("hello world".to_string()),
        )];
        let mut conditions = HashMap::new();
        conditions.insert(
            "description".to_string(),
            FilterCondition::Contains(Value::String("world".to_string())),
        );
        assert!(edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_starts_with() {
        let props = vec![(
            InternedKey::from_str("name"),
            Value::String("Alice Smith".to_string()),
        )];
        let mut conditions = HashMap::new();
        conditions.insert(
            "name".to_string(),
            FilterCondition::StartsWith(Value::String("Alice".to_string())),
        );
        assert!(edge_matches_conditions(&props, &conditions));
    }

    #[test]
    fn test_edge_matches_conditions_ends_with() {
        let props = vec![(
            InternedKey::from_str("name"),
            Value::String("Alice Smith".to_string()),
        )];
        let mut conditions = HashMap::new();
        conditions.insert(
            "name".to_string(),
            FilterCondition::EndsWith(Value::String("Smith".to_string())),
        );
        assert!(edge_matches_conditions(&props, &conditions));
    }

    // ── edge_passes_temporal tests ────────────────────────────────────────

    #[test]
    fn test_edge_passes_temporal_at_valid() {
        let configs = vec![TemporalConfig {
            valid_from: "start_date".to_string(),
            valid_to: "end_date".to_string(),
        }];
        let props = vec![
            (
                InternedKey::from_str("start_date"),
                Value::DateTime(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            ),
            (
                InternedKey::from_str("end_date"),
                Value::DateTime(NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
            ),
        ];
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let filter = TemporalEdgeFilter::At(configs, date);
        assert!(edge_passes_temporal(&props, &filter));
    }

    #[test]
    fn test_edge_passes_temporal_at_outside_range() {
        let configs = vec![TemporalConfig {
            valid_from: "start_date".to_string(),
            valid_to: "end_date".to_string(),
        }];
        let props = vec![
            (
                InternedKey::from_str("start_date"),
                Value::DateTime(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            ),
            (
                InternedKey::from_str("end_date"),
                Value::DateTime(NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()),
            ),
        ];
        let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let filter = TemporalEdgeFilter::At(configs, date);
        assert!(!edge_passes_temporal(&props, &filter));
    }

    #[test]
    fn test_edge_passes_temporal_at_null_end_date() {
        // Null end date means still active (unbounded)
        let configs = vec![TemporalConfig {
            valid_from: "start_date".to_string(),
            valid_to: "end_date".to_string(),
        }];
        let props = vec![
            (
                InternedKey::from_str("start_date"),
                Value::DateTime(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            ),
            (InternedKey::from_str("end_date"), Value::Null),
        ];
        let date = NaiveDate::from_ymd_opt(2030, 1, 1).unwrap();
        let filter = TemporalEdgeFilter::At(configs, date);
        assert!(edge_passes_temporal(&props, &filter));
    }

    #[test]
    fn test_edge_passes_temporal_no_matching_config() {
        let configs = vec![TemporalConfig {
            valid_from: "other_from".to_string(),
            valid_to: "other_to".to_string(),
        }];
        let props = vec![(InternedKey::from_str("weight"), Value::Int64(10))];
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let filter = TemporalEdgeFilter::At(configs, date);
        // No matching config => treated as non-temporal => passes
        assert!(edge_passes_temporal(&props, &filter));
    }

    #[test]
    fn test_edge_passes_temporal_during_overlap() {
        let configs = vec![TemporalConfig {
            valid_from: "start_date".to_string(),
            valid_to: "end_date".to_string(),
        }];
        let props = vec![
            (
                InternedKey::from_str("start_date"),
                Value::DateTime(NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()),
            ),
            (
                InternedKey::from_str("end_date"),
                Value::DateTime(NaiveDate::from_ymd_opt(2024, 9, 30).unwrap()),
            ),
        ];
        let start = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let filter = TemporalEdgeFilter::During(configs, start, end);
        assert!(edge_passes_temporal(&props, &filter));
    }

    #[test]
    fn test_edge_passes_temporal_during_no_overlap() {
        let configs = vec![TemporalConfig {
            valid_from: "start_date".to_string(),
            valid_to: "end_date".to_string(),
        }];
        let props = vec![
            (
                InternedKey::from_str("start_date"),
                Value::DateTime(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            ),
            (
                InternedKey::from_str("end_date"),
                Value::DateTime(NaiveDate::from_ymd_opt(2024, 3, 31).unwrap()),
            ),
        ];
        let start = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let filter = TemporalEdgeFilter::During(configs, start, end);
        assert!(!edge_passes_temporal(&props, &filter));
    }

    // ── value_to_f64 tests ─────────────────────────────────────────────────

    #[test]
    fn test_value_to_f64_float() {
        assert_eq!(value_to_f64(&Value::Float64(3.14)), Some(3.14));
    }

    #[test]
    fn test_value_to_f64_int() {
        assert_eq!(value_to_f64(&Value::Int64(42)), Some(42.0));
    }

    #[test]
    fn test_value_to_f64_string_valid() {
        assert_eq!(value_to_f64(&Value::String("2.5".to_string())), Some(2.5));
    }

    #[test]
    fn test_value_to_f64_string_invalid() {
        assert_eq!(value_to_f64(&Value::String("abc".to_string())), None);
    }

    #[test]
    fn test_value_to_f64_bool() {
        assert_eq!(value_to_f64(&Value::Boolean(true)), None);
    }

    #[test]
    fn test_value_to_f64_null() {
        assert_eq!(value_to_f64(&Value::Null), None);
    }

    // ── format_property_list tests ─────────────────────────────────────────

    #[test]
    fn test_format_property_list_no_truncation() {
        let values = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(format_property_list(&values, None), "a, b, c");
    }

    #[test]
    fn test_format_property_list_with_truncation() {
        let values = vec!["hello".to_string(), "world".to_string(), "foo".to_string()];
        // "hello, world, foo" = 17 chars, truncate at 10
        let result = format_property_list(&values, Some(10));
        assert!(result.ends_with("..."));
        assert!(result.len() <= 10);
    }

    #[test]
    fn test_format_property_list_empty() {
        let values: Vec<String> = vec![];
        assert_eq!(format_property_list(&values, None), "");
    }

    #[test]
    fn test_format_property_list_single() {
        let values = vec!["only".to_string()];
        assert_eq!(format_property_list(&values, None), "only");
    }

    #[test]
    fn test_format_property_list_no_truncation_needed() {
        let values = vec!["hi".to_string()];
        assert_eq!(format_property_list(&values, Some(100)), "hi");
    }

    // ── node_geometry tests ───────────────────────────────────────────────

    #[test]
    fn test_node_geometry_valid_wkt() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert(
            "geom".to_string(),
            Value::String("POINT (144.9 -37.8)".to_string()),
        );
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let result = node_geometry(&node, "geom");
        assert!(result.is_some());
    }

    #[test]
    fn test_node_geometry_invalid_wkt() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert(
            "geom".to_string(),
            Value::String("NOT_VALID_WKT".to_string()),
        );
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let result = node_geometry(&node, "geom");
        assert!(result.is_none());
    }

    #[test]
    fn test_node_geometry_missing_field() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let result = node_geometry(&node, "geom");
        assert!(result.is_none());
    }

    #[test]
    fn test_node_geometry_non_string_value() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert("geom".to_string(), Value::Int64(42));
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let result = node_geometry(&node, "geom");
        assert!(result.is_none());
    }

    // ── resolve_geometry_field tests ────────────────────────────────────────

    #[test]
    fn test_resolve_geometry_field_override() {
        let sc = spatial_config(None, Some("default_geom".to_string()));
        let result = resolve_geometry_field(Some(&sc), Some("override_geom"));
        assert_eq!(result, Some("override_geom"));
    }

    #[test]
    fn test_resolve_geometry_field_from_config() {
        let sc = spatial_config(None, Some("config_geom".to_string()));
        let result = resolve_geometry_field(Some(&sc), None);
        assert_eq!(result, Some("config_geom"));
    }

    #[test]
    fn test_resolve_geometry_field_none() {
        let result = resolve_geometry_field(None, None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_resolve_geometry_field_config_no_geometry() {
        let sc = spatial_config(Some(("lat".to_string(), "lon".to_string())), None);
        let result = resolve_geometry_field(Some(&sc), None);
        assert_eq!(result, None);
    }

    // ── extract_lat_lon tests ──────────────────────────────────────────────

    #[test]
    fn test_extract_lat_lon_floats() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert("lat".to_string(), Value::Float64(-37.8));
        props.insert("lon".to_string(), Value::Float64(144.9));
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let result = extract_lat_lon(&node, "lat", "lon");
        assert_eq!(result, Some((-37.8, 144.9)));
    }

    #[test]
    fn test_extract_lat_lon_ints() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert("lat".to_string(), Value::Int64(-38));
        props.insert("lon".to_string(), Value::Int64(145));
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let result = extract_lat_lon(&node, "lat", "lon");
        assert_eq!(result, Some((-38.0, 145.0)));
    }

    #[test]
    fn test_extract_lat_lon_strings() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert("lat".to_string(), Value::String("-37.8".to_string()));
        props.insert("lon".to_string(), Value::String("144.9".to_string()));
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let result = extract_lat_lon(&node, "lat", "lon");
        assert_eq!(result, Some((-37.8, 144.9)));
    }

    #[test]
    fn test_extract_lat_lon_missing_field() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let result = extract_lat_lon(&node, "lat", "lon");
        assert_eq!(result, None);
    }

    // ── node_lat_lon tests ─────────────────────────────────────────────────

    #[test]
    fn test_node_lat_lon_no_spatial_config() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let result = node_lat_lon(&node, None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_node_lat_lon_with_location_fields() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert("latitude".to_string(), Value::Float64(-37.8));
        props.insert("longitude".to_string(), Value::Float64(144.9));
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let sc = spatial_config(
            Some(("latitude".to_string(), "longitude".to_string())),
            None,
        );
        let result = node_lat_lon(&node, Some(&sc));
        assert_eq!(result, Some((-37.8, 144.9)));
    }

    #[test]
    fn test_node_lat_lon_fallback_to_geometry_centroid() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert(
            "geom".to_string(),
            Value::String("POINT (144.9 -37.8)".to_string()),
        );
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let sc = spatial_config(
            Some(("lat".to_string(), "lon".to_string())), // these fields don't exist
            Some("geom".to_string()),
        );
        let result = node_lat_lon(&node, Some(&sc));
        assert!(result.is_some());
        let (lat, lon) = result.unwrap();
        assert!((lat - (-37.8)).abs() < 0.01);
        assert!((lon - 144.9).abs() < 0.01);
    }

    #[test]
    fn test_node_lat_lon_no_location_no_geometry() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let sc = spatial_config(None, None);
        let result = node_lat_lon(&node, Some(&sc));
        assert_eq!(result, None);
    }

    // ── resolve_node_point tests ──────────────────────────────────────────

    #[test]
    fn test_resolve_node_point_default_uses_location() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert("lat".to_string(), Value::Float64(-37.8));
        props.insert("lon".to_string(), Value::Float64(144.9));
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let sc = spatial_config(Some(("lat".to_string(), "lon".to_string())), None);
        let result = resolve_node_point(&node, Some(&sc), None, None);
        assert_eq!(result, Some((-37.8, 144.9)));
    }

    #[test]
    fn test_resolve_node_point_centroid_mode_needs_geometry() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert("lat".to_string(), Value::Float64(-37.8));
        props.insert("lon".to_string(), Value::Float64(144.9));
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let sc = spatial_config(Some(("lat".to_string(), "lon".to_string())), None);
        // Centroid mode skips location fields, requires geometry
        let result = resolve_node_point(&node, Some(&sc), Some(SpatialResolve::Centroid), None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_resolve_node_point_centroid_mode_with_geometry() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert(
            "geom".to_string(),
            Value::String("POINT (144.9 -37.8)".to_string()),
        );
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let sc = spatial_config(None, Some("geom".to_string()));
        let result = resolve_node_point(&node, Some(&sc), Some(SpatialResolve::Centroid), None);
        assert!(result.is_some());
        let (lat, lon) = result.unwrap();
        assert!((lat - (-37.8)).abs() < 0.01);
        assert!((lon - 144.9).abs() < 0.01);
    }

    #[test]
    fn test_resolve_node_point_no_spatial_config() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let result = resolve_node_point(&node, None, None, None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_resolve_node_point_geometry_field_override() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert(
            "custom_geom".to_string(),
            Value::String("POINT (144.9 -37.8)".to_string()),
        );
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let result = resolve_node_point(
            &node,
            None,
            Some(SpatialResolve::Centroid),
            Some("custom_geom"),
        );
        assert!(result.is_some());
    }

    // ── resolve_node_geom tests ──────────────────────────────────────────

    #[test]
    fn test_resolve_node_geom_with_spatial_config() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert(
            "geom".to_string(),
            Value::String("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))".to_string()),
        );
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let sc = spatial_config(None, Some("geom".to_string()));
        let result = resolve_node_geom(&node, Some(&sc), None);
        assert!(result.is_some());
    }

    #[test]
    fn test_resolve_node_geom_with_override() {
        let mut graph = DirGraph::new();
        let mut props = HashMap::new();
        props.insert(
            "my_geom".to_string(),
            Value::String("POINT (1.0 2.0)".to_string()),
        );
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            props,
            &mut graph.interner,
        );
        let result = resolve_node_geom(&node, None, Some("my_geom"));
        assert!(result.is_some());
    }

    #[test]
    fn test_resolve_node_geom_no_config_no_override() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::Int64(0),
            Value::String("N".to_string()),
            "Place".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let result = resolve_node_geom(&node, None, None);
        assert!(result.is_none());
    }

    // ── insert_matches_into_selection tests ─────────────────────────────────

    #[test]
    fn test_insert_matches_into_selection_basic() {
        let (graph, indices) = build_linear_graph();
        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(None, vec![indices[0]]);

        let mut matches = HashMap::new();
        matches.insert(indices[0], vec![indices[1], indices[2]]);

        insert_matches_into_selection(&graph, &mut sel, matches, "test_method", None, None, None);

        assert_eq!(sel.get_level_count(), 2);
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_insert_matches_into_selection_with_max_nodes() {
        let (graph, indices) = build_linear_graph();
        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(None, vec![indices[0]]);

        let mut matches = HashMap::new();
        matches.insert(indices[0], vec![indices[1], indices[2]]);

        insert_matches_into_selection(
            &graph,
            &mut sel,
            matches,
            "test_method",
            None,
            None,
            Some(1),
        );

        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn test_insert_matches_empty() {
        let (graph, _indices) = build_linear_graph();
        let mut sel = CurrentSelection::new();
        let matches: HashMap<NodeIndex, Vec<NodeIndex>> = HashMap::new();
        insert_matches_into_selection(&graph, &mut sel, matches, "test_method", None, None, None);
        assert_eq!(sel.get_level_count(), 2);
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 0);
    }

    // ── get_source_info tests ──────────────────────────────────────────────

    #[test]
    fn test_get_source_info_success() {
        let (graph, indices) = build_linear_graph();
        let sel = selection_with_nodes(&[indices[0], indices[1]]);
        let (nodes, node_type) = get_source_info(&graph, &sel).unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(node_type, "Person");
    }

    #[test]
    fn test_get_source_info_empty() {
        let (graph, _indices) = build_linear_graph();
        let sel = CurrentSelection::new();
        let result = get_source_info(&graph, &sel);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No source nodes"));
    }

    #[test]
    fn test_get_source_info_uses_last_level() {
        let (graph, indices) = build_linear_graph();
        let mut sel = CurrentSelection::new();
        sel.add_level();
        let level1 = sel.get_level_mut(1).unwrap();
        level1.add_selection(None, vec![indices[1]]);

        let (nodes, node_type) = get_source_info(&graph, &sel).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[1]);
        assert_eq!(node_type, "Person");
    }

    // ── get_target_candidates tests ────────────────────────────────────────

    #[test]
    fn test_get_target_candidates_success() {
        let (graph, _indices) = build_linear_graph();
        let candidates = get_target_candidates(&graph, "Person").unwrap();
        assert_eq!(candidates.len(), 3);
    }

    #[test]
    fn test_get_target_candidates_not_found() {
        let (graph, _indices) = build_linear_graph();
        let result = get_target_candidates(&graph, "NonExistentType");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found in graph"));
    }

    #[test]
    fn test_get_target_candidates_heterogeneous() {
        let (graph, _indices) = build_heterogeneous_graph();
        let persons = get_target_candidates(&graph, "Person").unwrap();
        assert_eq!(persons.len(), 2);
        let cities = get_target_candidates(&graph, "City").unwrap();
        assert_eq!(cities.len(), 1);
    }

    // ── make_traversal tests ───────────────────────────────────────────────

    #[test]
    fn test_make_traversal_invalid_connection_type() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "NONEXISTENT".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_make_traversal_empty_source() {
        let (graph, _indices) = build_linear_graph();
        let mut sel = CurrentSelection::new();
        let result = make_traversal(
            &graph,
            &mut sel,
            "KNOWS".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No source nodes"));
    }

    #[test]
    fn test_make_traversal_invalid_direction() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "KNOWS".to_string(),
            None,
            Some("sideways".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid direction"));
    }

    #[test]
    fn test_make_traversal_outgoing() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "KNOWS".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        assert_eq!(sel.get_level_count(), 2);
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[1]);
    }

    #[test]
    fn test_make_traversal_incoming() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[1]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "KNOWS".to_string(),
            None,
            Some("incoming".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[0]);
    }

    #[test]
    fn test_make_traversal_both_directions() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[1]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "KNOWS".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 2); // A and C
    }

    #[test]
    fn test_make_traversal_tree_outgoing() {
        let (graph, indices) = build_tree_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "MANAGES".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&indices[1]));
        assert!(nodes.contains(&indices[2]));
    }

    #[test]
    fn test_make_traversal_only_matching_edge_type() {
        let (graph, indices) = build_multi_edge_type_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "KNOWS".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[1]); // Only B, not C
    }

    #[test]
    fn test_make_traversal_self_loop() {
        let (graph, indices) = build_self_loop_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LOOP".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[0]);
    }

    #[test]
    fn test_make_traversal_self_loop_both_directions() {
        let (graph, indices) = build_self_loop_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LOOP".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[0]);
    }

    #[test]
    fn test_make_traversal_self_loop_incoming() {
        let (graph, indices) = build_self_loop_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LOOP".to_string(),
            None,
            Some("incoming".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[0]);
    }

    #[test]
    fn test_make_traversal_cycle() {
        let (graph, indices) = build_cycle_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[1]);
    }

    #[test]
    fn test_make_traversal_cycle_incoming() {
        let (graph, indices) = build_cycle_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("incoming".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[1]);
    }

    #[test]
    fn test_make_traversal_no_outgoing_edges() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[2]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "KNOWS".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 0);
    }

    #[test]
    fn test_make_traversal_disconnected_no_results() {
        let (graph, indices) = build_disconnected_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[1]); // Only B, not C or D
    }

    // ── make_traversal with target_type filter ────────────────────────────

    #[test]
    fn test_make_traversal_target_type_filter() {
        let (graph, indices) = build_heterogeneous_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LIVES_IN".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&["City".to_string()]),
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[2]); // Melbourne
    }

    #[test]
    fn test_make_traversal_target_type_filter_no_match() {
        let (graph, indices) = build_heterogeneous_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LIVES_IN".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&["Company".to_string()]),
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 0);
    }

    // ── make_traversal_full with connection filter ─────────────────────────

    #[test]
    fn test_make_traversal_full_with_edge_filter() {
        let (graph, indices) = build_weighted_edge_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let mut conn_filter = HashMap::new();
        conn_filter.insert(
            "weight".to_string(),
            FilterCondition::Equals(Value::Int64(10)),
        );
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            Some(&conn_filter),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[1]); // Only B (weight=10)
    }

    #[test]
    fn test_make_traversal_full_incoming_with_filter() {
        let (graph, indices) = build_weighted_edge_graph();
        let mut sel = selection_with_nodes(&[indices[1]]);
        let mut conn_filter = HashMap::new();
        conn_filter.insert(
            "weight".to_string(),
            FilterCondition::Equals(Value::Int64(10)),
        );
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("incoming".to_string()),
            None,
            Some(&conn_filter),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[0]); // A
    }

    #[test]
    fn test_make_traversal_full_incoming_filter_no_match() {
        let (graph, indices) = build_weighted_edge_graph();
        let mut sel = selection_with_nodes(&[indices[1]]);
        let mut conn_filter = HashMap::new();
        conn_filter.insert(
            "weight".to_string(),
            FilterCondition::Equals(Value::Int64(99)),
        );
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("incoming".to_string()),
            None,
            Some(&conn_filter),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 0);
    }

    #[test]
    fn test_make_traversal_full_both_directions_with_filter() {
        let (graph, indices) = build_weighted_edge_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let mut conn_filter = HashMap::new();
        conn_filter.insert(
            "weight".to_string(),
            FilterCondition::GreaterThanEquals(Value::Int64(15)),
        );
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            None,
            None,
            Some(&conn_filter),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[2]); // Only C (weight=20)
    }

    #[test]
    fn test_make_traversal_full_with_max_nodes() {
        let (graph, indices) = build_tree_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "MANAGES".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            Some(1),
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn test_make_traversal_full_new_level_false() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "KNOWS".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            Some(10),
            Some(false),
            None,
            None,
        );
        assert!(result.is_ok());
        assert_eq!(sel.get_level_count(), 1);
    }

    // ── make_traversal with temporal filter ───────────────────────────────

    #[test]
    fn test_make_traversal_with_temporal_filter() {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();
        for i in 0..3 {
            let node = NodeData::new(
                Value::Int64(i),
                Value::String(format!("Node_{}", i)),
                "Node".to_string(),
                HashMap::new(),
                &mut graph.interner,
            );
            let idx = graph.graph.add_node(node);
            graph
                .type_indices
                .entry("Node".to_string())
                .or_default()
                .push(idx);
            indices.push(idx);
        }

        // Edge A->B valid 2024-01-01 to 2024-06-30
        let mut props1 = HashMap::new();
        props1.insert(
            "valid_from".to_string(),
            Value::DateTime(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
        );
        props1.insert(
            "valid_to".to_string(),
            Value::DateTime(NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()),
        );
        let edge1 = EdgeData::new("LINK".to_string(), props1, &mut graph.interner);
        graph.graph.add_edge(indices[0], indices[1], edge1);

        // Edge A->C valid 2024-06-01 to 2025-12-31
        let mut props2 = HashMap::new();
        props2.insert(
            "valid_from".to_string(),
            Value::DateTime(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap()),
        );
        props2.insert(
            "valid_to".to_string(),
            Value::DateTime(NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
        );
        let edge2 = EdgeData::new("LINK".to_string(), props2, &mut graph.interner);
        graph.graph.add_edge(indices[0], indices[2], edge2);
        graph.register_connection_type("LINK".to_string());

        let configs = vec![TemporalConfig {
            valid_from: "valid_from".to_string(),
            valid_to: "valid_to".to_string(),
        }];
        // Query at 2024-08-01 => only A->C edge should pass
        let date = NaiveDate::from_ymd_opt(2024, 8, 1).unwrap();
        let temporal = TemporalEdgeFilter::At(configs, date);

        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            Some(100), // max_nodes to force slow path
            None,
            Some(&temporal),
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[2]); // Only C
    }

    // ── make_traversal with node property filter ─────────────────────────

    #[test]
    fn test_make_traversal_with_node_filter() {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();

        let source = NodeData::new(
            Value::Int64(0),
            Value::String("Source".to_string()),
            "Node".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let src_idx = graph.graph.add_node(source);
        graph
            .type_indices
            .entry("Node".to_string())
            .or_default()
            .push(src_idx);
        indices.push(src_idx);

        // Target A with age=25
        let mut props_a = HashMap::new();
        props_a.insert("age".to_string(), Value::Int64(25));
        let target_a = NodeData::new(
            Value::Int64(1),
            Value::String("TargetA".to_string()),
            "Node".to_string(),
            props_a,
            &mut graph.interner,
        );
        let a_idx = graph.graph.add_node(target_a);
        graph
            .type_indices
            .entry("Node".to_string())
            .or_default()
            .push(a_idx);
        indices.push(a_idx);

        // Target B with age=35
        let mut props_b = HashMap::new();
        props_b.insert("age".to_string(), Value::Int64(35));
        let target_b = NodeData::new(
            Value::Int64(2),
            Value::String("TargetB".to_string()),
            "Node".to_string(),
            props_b,
            &mut graph.interner,
        );
        let b_idx = graph.graph.add_node(target_b);
        graph
            .type_indices
            .entry("Node".to_string())
            .or_default()
            .push(b_idx);
        indices.push(b_idx);

        let edge1 = EdgeData::new("LINK".to_string(), HashMap::new(), &mut graph.interner);
        graph.graph.add_edge(src_idx, a_idx, edge1);
        let edge2 = EdgeData::new("LINK".to_string(), HashMap::new(), &mut graph.interner);
        graph.graph.add_edge(src_idx, b_idx, edge2);
        graph.register_connection_type("LINK".to_string());

        let mut filter = HashMap::new();
        filter.insert(
            "age".to_string(),
            FilterCondition::GreaterThan(Value::Int64(30)),
        );

        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("outgoing".to_string()),
            Some(&filter),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[2]); // Only TargetB (age=35)
    }

    // ── make_traversal with sort and limit ───────────────────────────────

    #[test]
    fn test_make_traversal_full_with_sort_and_limit() {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();

        let source = NodeData::new(
            Value::Int64(0),
            Value::String("Source".to_string()),
            "Node".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let src_idx = graph.graph.add_node(source);
        graph
            .type_indices
            .entry("Node".to_string())
            .or_default()
            .push(src_idx);
        indices.push(src_idx);

        for (i, priority) in [30, 10, 20].iter().enumerate() {
            let mut props = HashMap::new();
            props.insert("priority".to_string(), Value::Int64(*priority));
            let target = NodeData::new(
                Value::Int64((i + 1) as i64),
                Value::String(format!("Target_{}", i)),
                "Node".to_string(),
                props,
                &mut graph.interner,
            );
            let t_idx = graph.graph.add_node(target);
            graph
                .type_indices
                .entry("Node".to_string())
                .or_default()
                .push(t_idx);
            indices.push(t_idx);

            let edge = EdgeData::new("LINK".to_string(), HashMap::new(), &mut graph.interner);
            graph.graph.add_edge(src_idx, t_idx, edge);
        }
        graph.register_connection_type("LINK".to_string());

        let sort = vec![("priority".to_string(), true)]; // ascending
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            Some(&sort),
            Some(2),
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 2);
    }

    // ── make_traversal_fast tests ──────────────────────────────────────────

    #[test]
    fn test_make_traversal_fast_multiple_sources() {
        let (graph, indices) = build_tree_graph();
        let mut sel = selection_with_nodes(&[indices[1], indices[2]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "MANAGES".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[3]); // Only D
    }

    #[test]
    fn test_make_traversal_fast_both_directions_dedup() {
        let (graph, indices) = build_cycle_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let result = make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[1]);
    }

    // ── Two-hop traversal (chained) ────────────────────────────────────────

    #[test]
    fn test_two_hop_traversal() {
        let (graph, indices) = build_tree_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        make_traversal(
            &graph,
            &mut sel,
            "MANAGES".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        make_traversal(
            &graph,
            &mut sel,
            "MANAGES".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(sel.get_level_count(), 3);
        let level2 = sel.get_level(2).unwrap();
        let nodes: Vec<NodeIndex> = level2.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[3]); // D
    }

    #[test]
    fn test_traversal_disconnected_two_hops() {
        let (graph, indices) = build_disconnected_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        make_traversal(
            &graph,
            &mut sel,
            "LINK".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let level2 = sel.get_level(2).unwrap();
        let nodes: Vec<NodeIndex> = level2.iter_node_indices().collect();
        assert_eq!(nodes.len(), 0);
    }

    // ── Multiple parents sharing a target ─────────────────────────────────

    #[test]
    fn test_traversal_multiple_parents_same_target() {
        let (graph, indices) = build_heterogeneous_graph();
        let mut sel = selection_with_nodes(&[indices[0], indices[1]]);
        make_traversal(
            &graph,
            &mut sel,
            "LIVES_IN".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let level1 = sel.get_level(1).unwrap();
        let total_nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(total_nodes.len(), 2);
        assert!(total_nodes.iter().all(|&n| n == indices[2]));
    }

    // ── Traversal with specified level_index ──────────────────────────────

    #[test]
    fn test_make_traversal_with_level_index() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        sel.add_level();
        let level1 = sel.get_level_mut(1).unwrap();
        level1.add_selection(None, vec![indices[1]]);

        let result = make_traversal(
            &graph,
            &mut sel,
            "KNOWS".to_string(),
            Some(0),
            Some("outgoing".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        let level2 = sel.get_level(2).unwrap();
        let nodes: Vec<NodeIndex> = level2.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[1]); // B, traversed from A
    }

    // ── get_children_properties tests ──────────────────────────────────────

    #[test]
    fn test_get_children_properties_basic() {
        let mut graph = DirGraph::new();
        let mut props_parent = HashMap::new();
        props_parent.insert("role".to_string(), Value::String("manager".to_string()));
        let parent_node = NodeData::new(
            Value::Int64(0),
            Value::String("ParentTitle".to_string()),
            "Person".to_string(),
            props_parent,
            &mut graph.interner,
        );
        let parent_idx = graph.graph.add_node(parent_node);

        let mut child_props1 = HashMap::new();
        child_props1.insert("name".to_string(), Value::String("Alice".to_string()));
        let child1 = NodeData::new(
            Value::Int64(1),
            Value::String("Child1".to_string()),
            "Person".to_string(),
            child_props1,
            &mut graph.interner,
        );
        let child1_idx = graph.graph.add_node(child1);

        let mut child_props2 = HashMap::new();
        child_props2.insert("name".to_string(), Value::String("Bob".to_string()));
        let child2 = NodeData::new(
            Value::Int64(2),
            Value::String("Child2".to_string()),
            "Person".to_string(),
            child_props2,
            &mut graph.interner,
        );
        let child2_idx = graph.graph.add_node(child2);

        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(Some(parent_idx), vec![child1_idx, child2_idx]);

        let result = get_children_properties(&graph, &sel, "name");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].parent_title, "ParentTitle");
        assert_eq!(result[0].values.len(), 2);
        assert!(result[0].values.contains(&"Alice".to_string()));
        assert!(result[0].values.contains(&"Bob".to_string()));
    }

    #[test]
    fn test_get_children_properties_missing_property() {
        let mut graph = DirGraph::new();
        let parent_node = NodeData::new(
            Value::Int64(0),
            Value::String("Parent".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let parent_idx = graph.graph.add_node(parent_node);

        let child = NodeData::new(
            Value::Int64(1),
            Value::String("Child".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let child_idx = graph.graph.add_node(child);

        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(Some(parent_idx), vec![child_idx]);

        let result = get_children_properties(&graph, &sel, "nonexistent");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 0);
    }

    #[test]
    fn test_get_children_properties_various_value_types() {
        let mut graph = DirGraph::new();
        let parent_node = NodeData::new(
            Value::Int64(0),
            Value::String("Parent".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let parent_idx = graph.graph.add_node(parent_node);

        let mut props = HashMap::new();
        props.insert("age".to_string(), Value::Int64(25));
        let child = NodeData::new(
            Value::Int64(1),
            Value::String("Child".to_string()),
            "Person".to_string(),
            props,
            &mut graph.interner,
        );
        let child_idx = graph.graph.add_node(child);

        let mut props2 = HashMap::new();
        props2.insert("age".to_string(), Value::Float64(30.5));
        let child2 = NodeData::new(
            Value::Int64(2),
            Value::String("Child2".to_string()),
            "Person".to_string(),
            props2,
            &mut graph.interner,
        );
        let child2_idx = graph.graph.add_node(child2);

        let mut props3 = HashMap::new();
        props3.insert("age".to_string(), Value::Boolean(true));
        let child3 = NodeData::new(
            Value::Int64(3),
            Value::String("Child3".to_string()),
            "Person".to_string(),
            props3,
            &mut graph.interner,
        );
        let child3_idx = graph.graph.add_node(child3);

        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(Some(parent_idx), vec![child_idx, child2_idx, child3_idx]);

        let result = get_children_properties(&graph, &sel, "age");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 3);
        assert!(result[0].values.contains(&"25".to_string()));
        assert!(result[0].values.contains(&"30.5".to_string()));
        assert!(result[0].values.contains(&"true".to_string()));
    }

    #[test]
    fn test_get_children_properties_no_parent() {
        let graph = DirGraph::new();
        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(None, vec![NodeIndex::new(0)]);

        let result = get_children_properties(&graph, &sel, "name");
        assert_eq!(result.len(), 0); // None parents are skipped
    }

    #[test]
    fn test_get_children_properties_null_value() {
        let mut graph = DirGraph::new();
        let parent = NodeData::new(
            Value::Int64(0),
            Value::String("Parent".to_string()),
            "Node".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let parent_idx = graph.graph.add_node(parent);

        let mut props = HashMap::new();
        props.insert("status".to_string(), Value::Null);
        let child = NodeData::new(
            Value::Int64(1),
            Value::String("Child".to_string()),
            "Node".to_string(),
            props,
            &mut graph.interner,
        );
        let child_idx = graph.graph.add_node(child);

        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(Some(parent_idx), vec![child_idx]);

        let result = get_children_properties(&graph, &sel, "status");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 1);
        assert_eq!(result[0].values[0], "null");
    }

    #[test]
    fn test_get_children_properties_unique_id_value() {
        let mut graph = DirGraph::new();
        let parent = NodeData::new(
            Value::Int64(0),
            Value::String("Parent".to_string()),
            "Node".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let parent_idx = graph.graph.add_node(parent);

        let mut props = HashMap::new();
        props.insert("uid".to_string(), Value::UniqueId(42));
        let child = NodeData::new(
            Value::Int64(1),
            Value::String("Child".to_string()),
            "Node".to_string(),
            props,
            &mut graph.interner,
        );
        let child_idx = graph.graph.add_node(child);

        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(Some(parent_idx), vec![child_idx]);

        let result = get_children_properties(&graph, &sel, "uid");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 1);
        assert_eq!(result[0].values[0], "42");
    }

    #[test]
    fn test_get_children_properties_point_value() {
        let mut graph = DirGraph::new();
        let parent = NodeData::new(
            Value::Int64(0),
            Value::String("Parent".to_string()),
            "Node".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let parent_idx = graph.graph.add_node(parent);

        let mut props = HashMap::new();
        props.insert(
            "location".to_string(),
            Value::Point {
                lat: -37.8,
                lon: 144.9,
            },
        );
        let child = NodeData::new(
            Value::Int64(1),
            Value::String("Child".to_string()),
            "Node".to_string(),
            props,
            &mut graph.interner,
        );
        let child_idx = graph.graph.add_node(child);

        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(Some(parent_idx), vec![child_idx]);

        let result = get_children_properties(&graph, &sel, "location");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 1);
        assert_eq!(result[0].values[0], "point(-37.8, 144.9)");
    }

    #[test]
    fn test_get_children_properties_parent_without_string_title() {
        let mut graph = DirGraph::new();
        let parent = NodeData::new(
            Value::Int64(0),
            Value::Int64(999), // title is an Int64, not a String
            "Node".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let parent_idx = graph.graph.add_node(parent);

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Test".to_string()));
        let child = NodeData::new(
            Value::Int64(1),
            Value::String("Child".to_string()),
            "Node".to_string(),
            props,
            &mut graph.interner,
        );
        let child_idx = graph.graph.add_node(child);

        let mut sel = CurrentSelection::new();
        let level = sel.get_level_mut(0).unwrap();
        level.add_selection(Some(parent_idx), vec![child_idx]);

        let result = get_children_properties(&graph, &sel, "name");
        assert_eq!(result.len(), 1);
        assert!(result[0].parent_title.starts_with("node_"));
    }

    // ── format_for_storage tests ───────────────────────────────────────────

    #[test]
    fn test_format_for_storage() {
        let groups = vec![
            ChildPropertyGroup {
                parent_idx: NodeIndex::new(0),
                parent_title: "P1".to_string(),
                values: vec!["a".to_string(), "b".to_string()],
            },
            ChildPropertyGroup {
                parent_idx: NodeIndex::new(1),
                parent_title: "P2".to_string(),
                values: vec!["c".to_string()],
            },
        ];
        let result = format_for_storage(&groups, None);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, Some(NodeIndex::new(0)));
        assert_eq!(result[0].1, Value::String("a, b".to_string()));
        assert_eq!(result[1].0, Some(NodeIndex::new(1)));
        assert_eq!(result[1].1, Value::String("c".to_string()));
    }

    #[test]
    fn test_format_for_storage_with_truncation() {
        let groups = vec![ChildPropertyGroup {
            parent_idx: NodeIndex::new(0),
            parent_title: "P".to_string(),
            values: vec!["hello".to_string(), "world".to_string(), "foo".to_string()],
        }];
        let result = format_for_storage(&groups, Some(10));
        match &result[0].1 {
            Value::String(s) => assert!(s.ends_with("...")),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_format_for_storage_empty_groups() {
        let groups: Vec<ChildPropertyGroup> = vec![];
        let result = format_for_storage(&groups, None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_format_for_storage_empty_values() {
        let groups = vec![ChildPropertyGroup {
            parent_idx: NodeIndex::new(0),
            parent_title: "P".to_string(),
            values: vec![],
        }];
        let result = format_for_storage(&groups, None);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, Value::String("".to_string()));
    }

    // ── format_for_dictionary tests ────────────────────────────────────────

    #[test]
    fn test_format_for_dictionary() {
        let groups = vec![ChildPropertyGroup {
            parent_idx: NodeIndex::new(0),
            parent_title: "ParentA".to_string(),
            values: vec!["x".to_string(), "y".to_string()],
        }];
        let result = format_for_dictionary(&groups, None);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "ParentA");
        assert_eq!(result[0].1, "x, y");
    }

    #[test]
    fn test_format_for_dictionary_with_truncation() {
        let groups = vec![ChildPropertyGroup {
            parent_idx: NodeIndex::new(0),
            parent_title: "P".to_string(),
            values: vec!["abcdefgh".to_string(), "ijklmnop".to_string()],
        }];
        let result = format_for_dictionary(&groups, Some(10));
        assert!(result[0].1.ends_with("..."));
    }

    #[test]
    fn test_format_for_dictionary_empty_groups() {
        let groups: Vec<ChildPropertyGroup> = vec![];
        let result = format_for_dictionary(&groups, None);
        assert!(result.is_empty());
    }

    // ── make_comparison_traversal dispatch tests ──────────────────────────

    #[test]
    fn test_make_comparison_traversal_unknown_method() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let config = MethodConfig::from_string("unknown_method".to_string());
        let result =
            make_comparison_traversal(&graph, &mut sel, Some("Person"), &config, None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown traversal method"));
    }

    #[test]
    fn test_make_comparison_traversal_contains_no_target_type() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let config = MethodConfig::from_string("contains".to_string());
        let result = make_comparison_traversal(&graph, &mut sel, None, &config, None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires a target_type"));
    }

    #[test]
    fn test_make_comparison_traversal_intersects_no_target_type() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let config = MethodConfig::from_string("intersects".to_string());
        let result = make_comparison_traversal(&graph, &mut sel, None, &config, None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires a target_type"));
    }

    #[test]
    fn test_make_comparison_traversal_distance_no_target_type() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let mut config = MethodConfig::from_string("distance".to_string());
        config.max_distance_m = Some(1000.0);
        let result = make_comparison_traversal(&graph, &mut sel, None, &config, None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires a target_type"));
    }

    #[test]
    fn test_make_comparison_traversal_distance_no_max_distance() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let config = MethodConfig::from_string("distance".to_string());
        let result =
            make_comparison_traversal(&graph, &mut sel, Some("Person"), &config, None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires 'max_m'"));
    }

    #[test]
    fn test_make_comparison_traversal_text_score_no_target_type() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let mut config = MethodConfig::from_string("text_score".to_string());
        config.property = Some("embedding".to_string());
        let result = make_comparison_traversal(&graph, &mut sel, None, &config, None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires a target_type"));
    }

    #[test]
    fn test_make_comparison_traversal_text_score_no_property() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let config = MethodConfig::from_string("text_score".to_string());
        let result =
            make_comparison_traversal(&graph, &mut sel, Some("Person"), &config, None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires 'property'"));
    }

    #[test]
    fn test_make_comparison_traversal_cluster_no_algorithm() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let config = MethodConfig::from_string("cluster".to_string());
        let result =
            make_comparison_traversal(&graph, &mut sel, Some("Person"), &config, None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires 'algorithm'"));
    }

    #[test]
    fn test_make_comparison_traversal_cluster_no_features() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let mut config = MethodConfig::from_string("cluster".to_string());
        config.algorithm = Some("kmeans".to_string());
        let result =
            make_comparison_traversal(&graph, &mut sel, Some("Person"), &config, None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires 'features'"));
    }

    #[test]
    fn test_cluster_traversal_unknown_algorithm() {
        let (graph, indices) = build_linear_graph();
        let mut sel = selection_with_nodes(&indices);
        let mut config = MethodConfig::from_string("cluster".to_string());
        config.algorithm = Some("spectral".to_string());
        config.features = Some(vec!["id".to_string()]);
        let result =
            make_comparison_traversal(&graph, &mut sel, Some("Person"), &config, None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown clustering algorithm"));
    }

    // ── make_traversal_full with target_type + connection filter ─────────

    #[test]
    fn test_make_traversal_full_target_type_with_conn_filter() {
        let (graph, indices) = build_heterogeneous_graph();
        let mut sel = selection_with_nodes(&[indices[0]]);
        let conn_filter: HashMap<String, FilterCondition> = HashMap::new();

        let result = make_traversal(
            &graph,
            &mut sel,
            "LIVES_IN".to_string(),
            None,
            Some("outgoing".to_string()),
            None,
            Some(&conn_filter),
            None,
            None,
            None,
            None,
            Some(&["City".to_string()]),
        );
        assert!(result.is_ok());
        let level1 = sel.get_level(1).unwrap();
        let nodes: Vec<NodeIndex> = level1.iter_node_indices().collect();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], indices[2]); // Melbourne
    }

    #[test]
    fn test_child_property_group_creation() {
        let group = ChildPropertyGroup {
            parent_idx: NodeIndex::new(0),
            parent_title: "Parent".to_string(),
            values: vec!["val1".to_string(), "val2".to_string()],
        };
        assert_eq!(group.parent_title, "Parent");
        assert_eq!(group.values.len(), 2);
    }

    #[test]
    fn test_child_property_group_empty_values() {
        let group = ChildPropertyGroup {
            parent_idx: NodeIndex::new(5),
            parent_title: "Test".to_string(),
            values: vec![],
        };
        assert_eq!(group.values.len(), 0);
        assert_eq!(group.parent_idx.index(), 5);
    }

    #[test]
    fn test_format_for_dictionary_basic() {
        let group = ChildPropertyGroup {
            parent_idx: NodeIndex::new(0),
            parent_title: "ParentTitle".to_string(),
            values: vec!["val1".to_string(), "val2".to_string()],
        };
        let groups = vec![group];
        let result = format_for_dictionary(&groups, None);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "ParentTitle");
        assert_eq!(result[0].1, "val1, val2");
    }

    #[test]
    fn test_format_for_dictionary_multiple_groups() {
        let group1 = ChildPropertyGroup {
            parent_idx: NodeIndex::new(0),
            parent_title: "Parent1".to_string(),
            values: vec!["a".to_string(), "b".to_string()],
        };
        let group2 = ChildPropertyGroup {
            parent_idx: NodeIndex::new(1),
            parent_title: "Parent2".to_string(),
            values: vec!["x".to_string(), "y".to_string()],
        };
        let groups = vec![group1, group2];
        let result = format_for_dictionary(&groups, None);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "Parent1");
        assert_eq!(result[1].0, "Parent2");
    }

    #[test]
    fn test_format_for_storage_basic() {
        let group = ChildPropertyGroup {
            parent_idx: NodeIndex::new(0),
            parent_title: "Parent".to_string(),
            values: vec!["val1".to_string(), "val2".to_string()],
        };
        let groups = vec![group];
        let result = format_for_storage(&groups, None);
        assert_eq!(result.len(), 1);
        if let Value::String(s) = &result[0].1 {
            assert_eq!(s, "val1, val2");
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_format_property_list_basic() {
        let values = vec![
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
        ];
        let result = format_property_list(&values, None);
        assert_eq!(result, "Alice, Bob, Charlie");
    }

    #[test]
    fn test_method_config_full_construction() {
        let mut config = MethodConfig::from_string("distance".to_string());
        config.max_distance_m = Some(100.0);
        config.k = Some(5);
        config.eps = Some(0.5);
        config.min_samples = Some(3);

        assert_eq!(config.max_distance_m, Some(100.0));
        assert_eq!(config.k, Some(5));
        assert_eq!(config.eps, Some(0.5));
        assert_eq!(config.min_samples, Some(3));
    }

    #[test]
    fn test_resolve_geometry_field_no_override_no_config() {
        let result = resolve_geometry_field(None, None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_resolve_geometry_field_with_override() {
        let result = resolve_geometry_field(None, Some("custom_geom"));
        assert_eq!(result, Some("custom_geom"));
    }

    #[test]
    fn test_spatial_resolve_debug_format() {
        let resolve = SpatialResolve::Centroid;
        assert_eq!(format!("{:?}", resolve), "Centroid");
    }

    #[test]
    fn test_spatial_resolve_enum_equality() {
        assert_eq!(SpatialResolve::Centroid, SpatialResolve::Centroid);
        assert_ne!(SpatialResolve::Centroid, SpatialResolve::Closest);
    }

    #[test]
    fn test_spatial_resolve_parse_centroid() {
        let result = MethodConfig::parse_resolve("centroid");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SpatialResolve::Centroid);
    }

    #[test]
    fn test_spatial_resolve_parse_closest() {
        let result = MethodConfig::parse_resolve("closest");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SpatialResolve::Closest);
    }

    #[test]
    fn test_spatial_resolve_parse_geometry() {
        let result = MethodConfig::parse_resolve("geometry");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), SpatialResolve::Geometry);
    }

    #[test]
    fn test_spatial_resolve_parse_invalid() {
        let result = MethodConfig::parse_resolve("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_temporal_edge_filter_at_variant() {
        let configs = vec![];
        let date = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let _filter = TemporalEdgeFilter::At(configs, date);
    }

    #[test]
    fn test_temporal_edge_filter_during_variant() {
        let configs = vec![];
        let start = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = chrono::NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let _filter = TemporalEdgeFilter::During(configs, start, end);
    }

    #[test]
    fn test_value_to_f64_boolean() {
        let v = Value::Boolean(true);
        assert_eq!(value_to_f64(&v), None);
    }

    #[test]
    fn test_value_to_f64_invalid_string() {
        let v = Value::String("not_a_number".to_string());
        assert_eq!(value_to_f64(&v), None);
    }

    #[test]
    fn test_value_to_f64_large_int() {
        let v = Value::Int64(999999);
        assert_eq!(value_to_f64(&v), Some(999999.0));
    }

    #[test]
    fn test_value_to_f64_negative_float() {
        let v = Value::Float64(-3.14);
        assert_eq!(value_to_f64(&v), Some(-3.14));
    }

    #[test]
    fn test_value_to_f64_negative_int() {
        let v = Value::Int64(-42);
        assert_eq!(value_to_f64(&v), Some(-42.0));
    }

    #[test]
    fn test_value_to_f64_string() {
        let v = Value::String("2.5".to_string());
        assert_eq!(value_to_f64(&v), Some(2.5));
    }
}
