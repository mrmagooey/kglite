// src/graph/maintain_graph.rs
use crate::datatypes::{DataFrame, Value};
use crate::graph::batch_operations::{
    BatchProcessor, ConflictHandling, ConnectionBatchProcessor, NodeAction,
};
use crate::graph::lookups::{CombinedTypeLookup, TypeLookup};
use crate::graph::reporting::{ConnectionOperationReport, NodeOperationReport};
use crate::graph::schema::{CurrentSelection, DirGraph, InternedKey, TypeSchema};
use crate::graph::spatial;
use petgraph::graph::NodeIndex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

fn check_data_validity(df_data: &DataFrame, unique_id_field: &str) -> Result<(), String> {
    // Remove strict UniqueId type verification to allow nulls
    if !df_data.verify_column(unique_id_field) {
        let available_cols: Vec<_> = df_data.get_column_names();
        return Err(format!(
            "Column '{}' not found in DataFrame. Available columns: [{}]",
            unique_id_field,
            available_cols.join(", ")
        ));
    }
    Ok(())
}

fn get_column_types(df_data: &DataFrame) -> HashMap<String, String> {
    let mut types = HashMap::new();
    for col_name in df_data.get_column_names() {
        let col_type = df_data.get_column_type(&col_name);
        types.insert(col_name.clone(), col_type.to_string());
    }
    types
}

pub fn add_nodes(
    graph: &mut DirGraph,
    df_data: DataFrame,
    node_type: String,
    unique_id_field: String,
    node_title_field: Option<String>,
    conflict_handling: Option<String>,
) -> Result<NodeOperationReport, String> {
    // Parse conflict handling option
    let conflict_mode = match conflict_handling.as_deref() {
        Some("replace") => ConflictHandling::Replace,
        Some("skip") => ConflictHandling::Skip,
        Some("preserve") => ConflictHandling::Preserve,
        Some("sum") => ConflictHandling::Sum,
        Some("update") | None => ConflictHandling::Update, // Default
        Some(other) => return Err(format!(
            "Unknown conflict handling mode: '{}'. Valid options: 'update' (default), 'replace', 'skip', 'preserve', 'sum'",
            other
        )),
    };

    let should_update_title = node_title_field.is_some();
    let title_field = node_title_field.unwrap_or_else(|| unique_id_field.clone());
    check_data_validity(&df_data, &unique_id_field)?;

    // Track errors
    let mut errors = Vec::new();

    let df_column_types = get_column_types(&df_data);

    // Check for type mismatches if metadata already exists
    if let Some(existing_meta) = graph.get_node_type_metadata(&node_type) {
        for (col_name, col_type) in &df_column_types {
            if let Some(existing_type) = existing_meta.get(col_name) {
                if existing_type != col_type {
                    errors.push(format!(
                        "Type mismatch for property '{}': existing schema has '{}', but data has '{}'",
                        col_name, existing_type, col_type
                    ));
                }
            }
        }
    }

    // Upsert node type metadata (merges new column types into existing)
    graph.upsert_node_type_metadata(&node_type, df_column_types);

    // Record original field name aliases so users can query by original column name
    if unique_id_field != "id" {
        graph
            .id_field_aliases
            .insert(node_type.clone(), unique_id_field.clone());
    }
    if title_field != "title" {
        graph
            .title_field_aliases
            .insert(node_type.clone(), title_field.clone());
    }

    let type_lookup =
        TypeLookup::from_id_indices(&graph.id_indices, &graph.graph, node_type.clone())?;
    let id_idx = df_data
        .get_column_index(&unique_id_field)
        .ok_or_else(|| format!("Column '{}' not found", unique_id_field))?;
    let title_idx = df_data
        .get_column_index(&title_field)
        .ok_or_else(|| format!("Column '{}' not found", title_field))?;

    // OPTIMIZATION: Pre-compute property column info (name + index) to avoid repeated lookups
    // This avoids: 1) string comparisons in the loop, 2) HashMap lookups per property
    let property_columns: Vec<(String, usize)> = df_data
        .get_column_names()
        .into_iter()
        .filter_map(|col_name| {
            if col_name != unique_id_field && col_name != title_field {
                df_data
                    .get_column_index(&col_name)
                    .map(|idx| (col_name, idx))
            } else {
                None
            }
        })
        .collect();

    // Build TypeSchema from DataFrame columns for compact storage
    let schema_keys: Vec<InternedKey> = property_columns
        .iter()
        .map(|(col_name, _)| graph.interner.get_or_intern(col_name))
        .collect();
    let type_schema = Arc::new(TypeSchema::from_keys(schema_keys));

    // Store or extend the schema for this node type
    let existing = graph.type_schemas.get(&node_type).cloned();
    if let Some(existing_schema) = existing {
        // Extend the existing schema with any new keys
        let mut merged = (*existing_schema).clone();
        for (_, key) in type_schema.iter() {
            merged.add_key(key);
        }
        let merged_arc = Arc::new(merged);
        graph.type_schemas.insert(node_type.clone(), merged_arc);
    } else {
        graph.type_schemas.insert(node_type.clone(), type_schema);
    }

    // Pre-intern property column keys once (avoids re-interning per row)
    let interned_columns: Vec<(InternedKey, usize)> = property_columns
        .iter()
        .map(|(col_name, col_idx)| (graph.interner.get_or_intern(col_name), *col_idx))
        .collect();
    let property_count = property_columns.len();
    let mut batch = BatchProcessor::new(df_data.row_count());
    let mut skipped_count = 0;
    let mut skipped_null_id = 0;
    let mut skipped_parse_fail = 0;

    for row_idx in 0..df_data.row_count() {
        let id = match df_data.get_value_by_index(row_idx, id_idx) {
            Some(Value::Null) => {
                skipped_count += 1;
                skipped_null_id += 1;
                continue;
            }
            Some(id) => id,
            None => {
                skipped_count += 1;
                skipped_parse_fail += 1;
                continue;
            }
        };

        let title = df_data
            .get_value_by_index(row_idx, title_idx)
            .unwrap_or(Value::Null);

        // Use pre-interned keys — avoids HashMap allocation and string cloning per row
        let mut properties_interned = Vec::with_capacity(property_count);
        for (interned_key, col_idx) in &interned_columns {
            let value = df_data
                .get_value_by_index(row_idx, *col_idx)
                .unwrap_or(Value::Null);
            if !matches!(value, Value::Null) {
                properties_interned.push((*interned_key, value));
            }
        }

        let action = match type_lookup.check_uid(&id) {
            Some(node_idx) => {
                // Determine if we should update the title
                let title_update = if should_update_title {
                    Some(title)
                } else {
                    None
                };

                // Update path still uses HashMap (less frequent, interning handled in batch)
                let mut properties = HashMap::with_capacity(properties_interned.len());
                for (ik, v) in properties_interned {
                    let name = graph.interner.resolve(ik);
                    properties.insert(name.to_string(), v);
                }

                NodeAction::Update {
                    node_idx,
                    title: title_update,
                    properties,
                    conflict_mode,
                }
            }
            None => NodeAction::CreateInterned {
                node_type: node_type.clone(),
                id,
                title,
                properties: properties_interned,
            },
        };
        batch.add_action(action, graph)?;
    }

    // Report skip reasons
    if skipped_null_id > 0 {
        errors.push(format!(
            "Skipped {} rows: null values in ID field '{}'",
            skipped_null_id, unique_id_field
        ));
    }
    if skipped_parse_fail > 0 {
        errors.push(format!(
            "Skipped {} rows: could not parse ID field '{}'. If IDs are strings, pass column_types={{'{}'
: 'string'}}",
            skipped_parse_fail, unique_id_field, unique_id_field
        ));
    }

    // Execute the batch and get the statistics
    let (stats, metrics) = batch.execute(graph)?;

    // Calculate elapsed time
    let elapsed_ms = metrics.processing_time * 1000.0; // Convert to milliseconds

    // Create and return the operation report with timestamp and errors
    let mut report = NodeOperationReport::new(
        "add_nodes".to_string(),
        stats.creates,
        stats.updates,
        skipped_count,
        elapsed_ms,
    );

    // Add errors if we found any
    if !errors.is_empty() {
        report = report.with_errors(errors);
    }

    Ok(report)
}

#[allow(clippy::too_many_arguments)]
pub fn add_connections(
    graph: &mut DirGraph,
    df_data: DataFrame,
    connection_type: String,
    source_type: String,
    source_id_field: String,
    target_type: String,
    target_id_field: String,
    source_title_field: Option<String>,
    target_title_field: Option<String>,
    conflict_handling: Option<String>,
) -> Result<ConnectionOperationReport, String> {
    // Parse conflict handling option
    let conflict_mode = match conflict_handling.as_deref() {
        Some("replace") => ConflictHandling::Replace,
        Some("skip") => ConflictHandling::Skip,
        Some("preserve") => ConflictHandling::Preserve,
        Some("sum") => ConflictHandling::Sum,
        Some("update") | None => ConflictHandling::Update, // Default
        Some(other) => return Err(format!(
            "Unknown conflict handling mode: '{}'. Valid options: 'update' (default), 'replace', 'skip', 'preserve', 'sum'",
            other
        )),
    };

    // Track errors
    let mut errors = Vec::new();

    let available_cols: Vec<_> = df_data.get_column_names();
    if !df_data.verify_column(&source_id_field) {
        return Err(format!(
            "Source ID column '{}' not found in DataFrame. Available columns: [{}]",
            source_id_field,
            available_cols.join(", ")
        ));
    }
    if !df_data.verify_column(&target_id_field) {
        return Err(format!(
            "Target ID column '{}' not found in DataFrame. Available columns: [{}]",
            target_id_field,
            available_cols.join(", ")
        ));
    }

    // Check if source and target types exist
    if !graph.has_node_type(&source_type) {
        errors.push(format!(
            "Source node type '{}' does not exist in the graph",
            source_type
        ));
    }

    if !graph.has_node_type(&target_type) {
        errors.push(format!(
            "Target node type '{}' does not exist in the graph",
            target_type
        ));
    }

    let source_id_idx = df_data
        .get_column_index(&source_id_field)
        .ok_or_else(|| format!("Source ID column '{}' not found", source_id_field))?;
    let target_id_idx = df_data
        .get_column_index(&target_id_field)
        .ok_or_else(|| format!("Target ID column '{}' not found", target_id_field))?;

    // Use as_ref() to borrow rather than move
    let source_title_idx = source_title_field
        .as_ref()
        .and_then(|field| df_data.get_column_index(field));
    let target_title_idx = target_title_field
        .as_ref()
        .and_then(|field| df_data.get_column_index(field));

    let lookup = CombinedTypeLookup::from_id_indices(
        &graph.id_indices,
        &graph.graph,
        source_type.clone(),
        target_type.clone(),
    )?;
    let mut batch = ConnectionBatchProcessor::new(df_data.row_count());
    // Set the conflict handling mode
    batch.set_conflict_mode(conflict_mode);
    // Skip edge existence checks on initial load (no existing edges of this type)
    let is_initial_load = !graph
        .connection_type_metadata
        .contains_key(&connection_type);
    batch.set_skip_existence_check(is_initial_load);

    let mut skipped_count = 0;
    let mut skipped_null_source = 0;
    let mut skipped_null_target = 0;
    // Instead of tracking ids directly, track counts of missing items
    let mut missing_source_count = 0;
    let mut missing_target_count = 0;

    // Cache column names and pre-compute which columns are property columns (not ID or title fields)
    // This avoids repeated allocations and string comparisons in the loop
    let property_columns: Vec<String> = df_data
        .get_column_names()
        .into_iter()
        .filter(|col_name| {
            let is_id_field = *col_name == source_id_field || *col_name == target_id_field;
            let is_source_title = source_title_field
                .as_ref()
                .is_some_and(|field| *col_name == *field);
            let is_target_title = target_title_field
                .as_ref()
                .is_some_and(|field| *col_name == *field);
            !is_id_field && !is_source_title && !is_target_title
        })
        .collect();

    for row_idx in 0..df_data.row_count() {
        let source_id = match df_data.get_value_by_index(row_idx, source_id_idx) {
            Some(Value::Null) => {
                skipped_count += 1;
                skipped_null_source += 1;
                continue;
            }
            None => {
                skipped_count += 1;
                skipped_null_source += 1;
                continue;
            }
            Some(id) => id,
        };

        let target_id = match df_data.get_value_by_index(row_idx, target_id_idx) {
            Some(Value::Null) => {
                skipped_count += 1;
                skipped_null_target += 1;
                continue;
            }
            None => {
                skipped_count += 1;
                skipped_null_target += 1;
                continue;
            }
            Some(id) => id,
        };

        let (source_idx, target_idx) = match (
            lookup.check_source(&source_id),
            lookup.check_target(&target_id),
        ) {
            (Some(src_idx), Some(tgt_idx)) => (src_idx, tgt_idx),
            (None, Some(_)) => {
                // Track missing source node
                missing_source_count += 1;
                skipped_count += 1;
                continue;
            }
            (Some(_), None) => {
                // Track missing target node
                missing_target_count += 1;
                skipped_count += 1;
                continue;
            }
            (None, None) => {
                // Track both missing
                missing_source_count += 1;
                missing_target_count += 1;
                skipped_count += 1;
                continue;
            }
        };

        update_node_titles(
            graph,
            source_idx,
            target_idx,
            row_idx,
            source_title_idx,
            target_title_idx,
            &df_data,
        )?;

        // Use pre-computed property columns (avoids get_column_names() call per row).
        // Skip null values — property access returns Null for missing keys anyway.
        let mut properties = HashMap::with_capacity(property_columns.len());
        for col_name in &property_columns {
            if let Some(value) = df_data.get_value(row_idx, col_name) {
                if !matches!(value, Value::Null) {
                    properties.insert(col_name.clone(), value);
                }
            }
        }

        // This will respect the conflict handling mode we set earlier
        if let Err(e) =
            batch.add_connection(source_idx, target_idx, properties, graph, &connection_type)
        {
            skipped_count += 1;
            errors.push(format!("Failed to add connection: {}", e));
            continue;
        }
    }

    // Report skip reasons
    if skipped_null_source > 0 {
        errors.push(format!(
            "Skipped {} rows: null values in source ID field '{}'",
            skipped_null_source, source_id_field
        ));
    }
    if skipped_null_target > 0 {
        errors.push(format!(
            "Skipped {} rows: null values in target ID field '{}'",
            skipped_null_target, target_id_field
        ));
    }
    if missing_source_count > 0 {
        errors.push(format!(
            "Skipped {} rows: source node not found in type '{}'",
            missing_source_count, source_type
        ));
    }
    if missing_target_count > 0 {
        errors.push(format!(
            "Skipped {} rows: target node not found in type '{}'",
            missing_target_count, target_type
        ));
    }

    update_schema_node(
        graph,
        &connection_type,
        lookup.get_source_type(),
        lookup.get_target_type(),
        batch.get_schema_properties(),
    )?;

    // Execute the batch and get the statistics
    let (stats, metrics) = batch.execute(graph, connection_type)?;

    // Create and return the operation report
    let mut report = ConnectionOperationReport::new(
        "add_connections".to_string(),
        stats.connections_created,
        skipped_count,
        stats.properties_tracked,
        metrics.processing_time * 1000.0, // Convert to milliseconds
    );

    // Add errors if we found any
    if !errors.is_empty() {
        report = report.with_errors(errors);
    }

    Ok(report)
}

fn update_node_titles(
    graph: &mut DirGraph,
    source_idx: NodeIndex,
    target_idx: NodeIndex,
    row_idx: usize,
    source_title_idx: Option<usize>,
    target_title_idx: Option<usize>,
    df_data: &DataFrame,
) -> Result<(), String> {
    if let Some(title_idx) = source_title_idx {
        if let Some(title) = df_data.get_value_by_index(row_idx, title_idx) {
            if let Some(node) = graph.get_node_mut(source_idx) {
                node.title = title;
            }
        }
    }
    if let Some(title_idx) = target_title_idx {
        if let Some(title) = df_data.get_value_by_index(row_idx, title_idx) {
            if let Some(node) = graph.get_node_mut(target_idx) {
                node.title = title;
            }
        }
    }
    Ok(())
}

fn update_schema_node(
    graph: &mut DirGraph,
    connection_type: &str,
    source_type: &str,
    target_type: &str,
    properties: &HashSet<String>,
) -> Result<(), String> {
    if !graph.has_node_type(source_type) {
        return Err(format!(
            "Source type '{}' does not exist in graph",
            source_type
        ));
    }
    if !graph.has_node_type(target_type) {
        return Err(format!(
            "Target type '{}' does not exist in graph",
            target_type
        ));
    }

    // Build property type map — all connection properties default to "Unknown"
    let prop_types: HashMap<String, String> = properties
        .iter()
        .map(|prop| (prop.clone(), "Unknown".to_string()))
        .collect();

    graph.upsert_connection_type_metadata(connection_type, source_type, target_type, prop_types);
    Ok(())
}

pub fn create_connections(
    graph: &mut DirGraph,
    selection: &CurrentSelection,
    connection_type: String,
    conflict_handling: Option<String>,
    copy_properties: Option<HashMap<String, Vec<String>>>, // node_type → prop names to copy onto edge
    source_type_filter: Option<String>,                    // override source level by node type
    target_type_filter: Option<String>,                    // override target level by node type
) -> Result<ConnectionOperationReport, String> {
    let conflict_mode = match conflict_handling.as_deref() {
        Some("replace") => ConflictHandling::Replace,
        Some("skip") => ConflictHandling::Skip,
        Some("preserve") => ConflictHandling::Preserve,
        Some("sum") => ConflictHandling::Sum,
        Some("update") | None => ConflictHandling::Update,
        Some(other) => {
            return Err(format!(
                "Unknown conflict handling mode: '{}'. Valid: 'update' (default), 'replace', 'skip', 'preserve', 'sum'",
                other
            ))
        }
    };

    let level_count = selection.get_level_count();
    if level_count == 0 {
        return Ok(ConnectionOperationReport::new(
            "create_connections".to_string(),
            0,
            0,
            0,
            0.0,
        ));
    }

    // --- Determine which level each node type lives at ---
    let mut type_to_level: HashMap<String, usize> = HashMap::new();
    for lvl_idx in 0..level_count {
        if let Some(level) = selection.get_level(lvl_idx) {
            for node_idx in level.iter_node_indices() {
                if let Some(node) = graph.get_node(node_idx) {
                    type_to_level
                        .entry(node.node_type.clone())
                        .or_insert(lvl_idx);
                }
            }
        }
    }

    // --- Resolve source and target levels ---
    let source_level = if let Some(ref st) = source_type_filter {
        *type_to_level.get(st).ok_or_else(|| {
            format!(
                "source_type '{}' not found in traversal chain. Available: {:?}",
                st,
                type_to_level.keys().collect::<Vec<_>>()
            )
        })?
    } else {
        0
    };

    let target_level = if let Some(ref tt) = target_type_filter {
        *type_to_level.get(tt).ok_or_else(|| {
            format!(
                "target_type '{}' not found in traversal chain. Available: {:?}",
                tt,
                type_to_level.keys().collect::<Vec<_>>()
            )
        })?
    } else {
        level_count - 1
    };

    if source_level >= target_level {
        return Err(format!(
            "source level ({}) must be before target level ({})",
            source_level, target_level
        ));
    }

    // --- Iterate target level groups to create edges ---
    // Each group at the target level has (parent, children). For each target node,
    // walk up through group parents to find the source node at source_level.
    // A child can appear in multiple groups (different parents), producing one edge
    // per distinct (source, target) pair.
    let target_level_data = match selection.get_level(target_level) {
        Some(level) if !level.is_empty() => level,
        _ => {
            return Ok(ConnectionOperationReport::new(
                "create_connections".to_string(),
                0,
                0,
                0,
                0.0,
            ));
        }
    };

    let mut batch = ConnectionBatchProcessor::new(target_level_data.node_count());
    batch.set_conflict_mode(conflict_mode);

    let mut skipped = 0;
    let mut errors = Vec::new();
    let mut detected_source_type = None;
    let mut detected_target_type = None;

    // For the common 2-level case (source_level=0, target_level=1), each group's
    // parent IS the source node, so we don't need parent maps at all.
    // For multi-level cases, build reverse parent maps: child → parents (plural).
    let parent_maps: Vec<HashMap<NodeIndex, Vec<NodeIndex>>> = if target_level - source_level > 1 {
        let mut maps: Vec<HashMap<NodeIndex, Vec<NodeIndex>>> = vec![HashMap::new(); level_count];
        for (lvl_idx, pmap) in maps.iter_mut().enumerate().skip(1) {
            if let Some(level) = selection.get_level(lvl_idx) {
                for (parent_opt, children) in level.iter_groups() {
                    if let Some(parent) = parent_opt {
                        for &child in children {
                            pmap.entry(child).or_default().push(*parent);
                        }
                    }
                }
            }
        }
        maps
    } else {
        Vec::new()
    };

    // Helper: walk from a node at `start_level` up to `source_level`, returning
    // all possible source nodes. For a 1-step walk, this is just the immediate parent.
    let walk_to_sources = |start_node: NodeIndex, start_level: usize| -> Vec<NodeIndex> {
        if start_level == source_level {
            return vec![start_node];
        }
        // BFS walk up through parent maps
        let mut current_nodes = vec![start_node];
        for lvl in (source_level + 1..=start_level).rev() {
            let mut next_nodes = Vec::new();
            for node in &current_nodes {
                if let Some(parents) = parent_maps[lvl].get(node) {
                    next_nodes.extend(parents);
                }
            }
            if next_nodes.is_empty() {
                return Vec::new(); // Orphan — no path to source
            }
            current_nodes = next_nodes;
        }
        current_nodes
    };

    for (parent_opt, targets) in target_level_data.iter_groups() {
        let Some(parent_idx) = parent_opt else {
            // Root-level targets have no parent — skip
            skipped += targets.len();
            continue;
        };

        // Resolve the source node(s) for this group's parent
        let source_nodes = if target_level - source_level == 1 {
            // Direct parent IS the source
            vec![*parent_idx]
        } else {
            walk_to_sources(*parent_idx, target_level - 1)
        };

        if source_nodes.is_empty() {
            skipped += targets.len();
            continue;
        }

        for &target_idx in targets {
            if detected_target_type.is_none() {
                if let Some(node) = graph.get_node(target_idx) {
                    detected_target_type = Some(node.node_type.clone());
                }
            }

            for &source_idx in &source_nodes {
                if detected_source_type.is_none() {
                    if let Some(node) = graph.get_node(source_idx) {
                        detected_source_type = Some(node.node_type.clone());
                    }
                }

                // Collect properties from nodes in the chain (source → ... → target)
                let edge_props = if let Some(ref prop_spec) = copy_properties {
                    let mut props = HashMap::new();
                    // Add source and target node properties
                    for &node_idx in &[source_idx, target_idx] {
                        if let Some(node) = graph.graph.node_weight(node_idx) {
                            if let Some(requested_props) = prop_spec.get(&node.node_type) {
                                if requested_props.is_empty() {
                                    for (k, v) in node.property_iter(&graph.interner) {
                                        props.insert(k.to_string(), v.clone());
                                    }
                                } else {
                                    for prop_name in requested_props {
                                        if let Some(val) = node.get_property(prop_name) {
                                            props.insert(prop_name.clone(), val.into_owned());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    props
                } else {
                    HashMap::new()
                };

                if let Err(e) = batch.add_connection(
                    source_idx,
                    target_idx,
                    edge_props,
                    graph,
                    &connection_type,
                ) {
                    skipped += 1;
                    errors.push(format!("Failed to add connection: {}", e));
                    continue;
                }
            }
        }
    }

    if let (Some(source), Some(target)) = (detected_source_type, detected_target_type) {
        update_schema_node(
            graph,
            &connection_type,
            &source,
            &target,
            batch.get_schema_properties(),
        )?;
    }

    let (stats, metrics) = batch.execute(graph, connection_type)?;

    let mut report = ConnectionOperationReport::new(
        "create_connections".to_string(),
        stats.connections_created,
        skipped,
        stats.properties_tracked,
        metrics.processing_time * 1000.0,
    );

    if !errors.is_empty() {
        report = report.with_errors(errors);
    }

    Ok(report)
}

pub fn update_node_properties(
    graph: &mut DirGraph,
    nodes: &[(Option<NodeIndex>, Value)],
    property: &str,
) -> Result<NodeOperationReport, String> {
    if nodes.is_empty() {
        return Err("No nodes to update".to_string());
    }

    // Track start time for the report
    let start_time = std::time::Instant::now();

    // Create property string once
    let property_string = property.to_string();

    // Track errors
    let mut errors = Vec::new();

    // Step 1: Collect information about node types and check if schema update is needed
    let mut node_types = HashMap::new();
    let mut first_value_type = None;
    let mut skipped_count = 0;

    for (node_idx_opt, value) in nodes {
        if let Some(node_idx) = node_idx_opt {
            if let Some(node) = graph.get_node(*node_idx) {
                // Track node type and count for each node
                *node_types.entry(node.node_type.clone()).or_insert(0) += 1;

                // Capture type of first value for schema
                if first_value_type.is_none() {
                    first_value_type = Some(match value {
                        Value::Int64(_) => "Int64",
                        Value::Float64(_) => "Float64",
                        Value::String(_) => "String",
                        Value::UniqueId(_) => "UniqueId",
                        _ => "Unknown",
                    });
                }
            } else {
                skipped_count += 1;
                errors.push(format!("Node index {:?} not found in graph", node_idx));
            }
        } else {
            skipped_count += 1;
        }
    }

    // Step 2: Update node type metadata for each affected node type
    let type_string = first_value_type
        .map(|t| t.to_string())
        .unwrap_or_else(|| "Calculated".to_string());

    for (node_type, _count) in node_types.iter() {
        // Check for type mismatch with existing metadata
        if let Some(existing_meta) = graph.get_node_type_metadata(node_type) {
            if let Some(existing_type) = existing_meta.get(&property_string) {
                if existing_type != &type_string {
                    errors.push(format!(
                        "Type mismatch for property '{}': existing schema has '{}', but data has '{}'",
                        property_string, existing_type, type_string
                    ));
                }
            }
        }

        let mut new_prop_types = HashMap::new();
        new_prop_types.insert(property_string.clone(), type_string.clone());
        graph.upsert_node_type_metadata(node_type, new_prop_types);
    }

    // Step 3: Prepare batch updates for nodes
    let batch_size = nodes.len();
    let mut batch = BatchProcessor::new(batch_size);

    for (node_idx_opt, value) in nodes {
        if let Some(node_idx) = node_idx_opt {
            // Only add valid nodes to batch
            if graph.graph.node_weight(*node_idx).is_some() {
                let mut properties = HashMap::new();
                properties.insert(property_string.clone(), value.clone());

                // Create update action
                let action = NodeAction::Update {
                    node_idx: *node_idx,
                    title: None, // Don't update title
                    properties,
                    conflict_mode: ConflictHandling::Update,
                };

                if let Err(e) = batch.add_action(action, graph) {
                    errors.push(format!("Failed to update node property: {}", e));
                    skipped_count += 1;
                }
            } else {
                skipped_count += 1;
                errors.push(format!("Node index {:?} is out of bounds", node_idx));
            }
        } else {
            skipped_count += 1;
        }
    }

    // Step 4: Execute batch update
    let (stats, _metrics) = match batch.execute(graph) {
        Ok(result) => result,
        Err(e) => {
            errors.push(format!("Failed to execute batch update: {}", e));
            return Err(format!("Failed to execute batch update: {}", e));
        }
    };

    if stats.updates == 0 && errors.is_empty() {
        errors.push("No nodes were updated".to_string());
    }

    // Calculate elapsed time
    let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    // Create and return the operation report
    let mut report = NodeOperationReport::new(
        "update_node_properties".to_string(),
        0, // We don't create nodes in this function
        stats.updates,
        skipped_count,
        elapsed_ms,
    );

    // Add errors if we found any
    if !errors.is_empty() {
        report = report.with_errors(errors);
    }

    Ok(report)
}

// ── add_properties ──────────────────────────────────────────────────────────

/// Specifies how properties should be copied from a source type.
#[derive(Debug)]
pub enum PropertySpec {
    /// Copy listed properties as-is: `['name', 'status']`
    CopyList(Vec<String>),
    /// Copy all properties: `[]`
    CopyAll,
    /// Rename/aggregate/spatial: `{'new_name': 'source_expr'}`
    RenameMap(HashMap<String, String>),
}

/// Report returned by add_properties().
pub struct AddPropertiesReport {
    pub nodes_updated: usize,
    pub properties_set: usize,
}

/// Enriches the leaf (most recent) level nodes by copying, renaming, aggregating,
/// or computing properties from ancestor nodes in the traversal hierarchy.
pub fn add_properties(
    graph: &mut DirGraph,
    selection: &CurrentSelection,
    property_spec: HashMap<String, PropertySpec>,
) -> Result<AddPropertiesReport, String> {
    let level_count = selection.get_level_count();
    if level_count == 0 {
        return Ok(AddPropertiesReport {
            nodes_updated: 0,
            properties_set: 0,
        });
    }

    let target_level = level_count - 1;

    // Build type → level index map
    let mut type_to_level: HashMap<String, usize> = HashMap::new();
    for lvl_idx in 0..level_count {
        if let Some(level) = selection.get_level(lvl_idx) {
            for node_idx in level.iter_node_indices() {
                if let Some(node) = graph.get_node(node_idx) {
                    type_to_level
                        .entry(node.node_type.clone())
                        .or_insert(lvl_idx);
                }
            }
        }
    }

    // Validate requested types exist in the traversal chain
    for source_type in property_spec.keys() {
        if !type_to_level.contains_key(source_type) {
            return Err(format!(
                "Source type '{}' not found in traversal chain. Available: {:?}",
                source_type,
                type_to_level.keys().collect::<Vec<_>>()
            ));
        }
    }

    // Build reverse parent maps: child → parent for each level
    let mut parent_maps: Vec<HashMap<NodeIndex, NodeIndex>> = vec![HashMap::new(); level_count];
    for (lvl_idx, pmap) in parent_maps.iter_mut().enumerate().skip(1) {
        if let Some(level) = selection.get_level(lvl_idx) {
            for (parent_opt, children) in level.iter_groups() {
                if let Some(parent) = parent_opt {
                    for &child in children {
                        pmap.insert(child, *parent);
                    }
                }
            }
        }
    }

    // Check if any spec requires aggregation
    let has_aggregation = property_spec.values().any(|spec| {
        if let PropertySpec::RenameMap(map) = spec {
            map.values().any(|expr| is_aggregate_expr(expr))
        } else {
            false
        }
    });

    if has_aggregation {
        return add_properties_aggregate(
            graph,
            selection,
            &property_spec,
            &type_to_level,
            &parent_maps,
            target_level,
        );
    }

    // Standard mode: copy/rename from ancestor onto each leaf node
    let target_level_data = match selection.get_level(target_level) {
        Some(level) if !level.is_empty() => level,
        _ => {
            return Ok(AddPropertiesReport {
                nodes_updated: 0,
                properties_set: 0,
            });
        }
    };

    // Collect updates first (to avoid borrow issues with graph)
    let mut updates: Vec<(NodeIndex, HashMap<String, Value>)> = Vec::new();

    for (_parent_opt, targets) in target_level_data.iter_groups() {
        for &target_idx in targets {
            let mut props_to_set: HashMap<String, Value> = HashMap::new();

            for (source_type, spec) in &property_spec {
                let source_level = match type_to_level.get(source_type) {
                    Some(&lvl) => lvl,
                    None => continue,
                };

                let ancestor_idx =
                    walk_to_ancestor(target_idx, target_level, source_level, &parent_maps);
                let ancestor_idx = match ancestor_idx {
                    Some(idx) => idx,
                    None => continue,
                };

                let ancestor_node = match graph.graph.node_weight(ancestor_idx) {
                    Some(n) => n,
                    None => continue,
                };

                match spec {
                    PropertySpec::CopyAll => {
                        for (k, v) in ancestor_node.property_iter(&graph.interner) {
                            props_to_set.insert(k.to_string(), v.clone());
                        }
                    }
                    PropertySpec::CopyList(prop_names) => {
                        for prop_name in prop_names {
                            if let Some(val) = ancestor_node.get_property(prop_name) {
                                props_to_set.insert(prop_name.clone(), val.into_owned());
                            }
                        }
                    }
                    PropertySpec::RenameMap(map) => {
                        for (target_name, source_expr) in map {
                            if is_spatial_compute(source_expr) {
                                if let Some(val) = compute_spatial_property(
                                    graph,
                                    target_idx,
                                    ancestor_idx,
                                    source_expr,
                                ) {
                                    props_to_set.insert(target_name.clone(), val);
                                }
                            } else if let Some(val) = ancestor_node.get_property(source_expr) {
                                props_to_set.insert(target_name.clone(), val.into_owned());
                            }
                        }
                    }
                }
            }

            if !props_to_set.is_empty() {
                updates.push((target_idx, props_to_set));
            }
        }
    }

    // Apply updates
    let mut nodes_updated = 0;
    let mut properties_set = 0;
    for (node_idx, props) in updates {
        // Pre-intern keys before getting mutable node reference (split borrow)
        let interned_props: Vec<(InternedKey, Value)> = props
            .into_iter()
            .map(|(k, v)| (graph.interner.get_or_intern(&k), v))
            .collect();
        if let Some(node) = graph.graph.node_weight_mut(node_idx) {
            let count = interned_props.len();
            for (ik, v) in interned_props {
                node.properties.insert(ik, v);
            }
            nodes_updated += 1;
            properties_set += count;
        }
    }

    Ok(AddPropertiesReport {
        nodes_updated,
        properties_set,
    })
}

fn walk_to_ancestor(
    start: NodeIndex,
    start_level: usize,
    target_level: usize,
    parent_maps: &[HashMap<NodeIndex, NodeIndex>],
) -> Option<NodeIndex> {
    if start_level == target_level {
        return Some(start);
    }
    if target_level >= start_level {
        return None;
    }
    let mut current = start;
    for lvl in (target_level + 1..=start_level).rev() {
        current = *parent_maps[lvl].get(&current)?;
    }
    Some(current)
}

fn is_aggregate_expr(expr: &str) -> bool {
    let trimmed = expr.trim();
    trimmed == "count(*)"
        || trimmed.starts_with("sum(")
        || trimmed.starts_with("mean(")
        || trimmed.starts_with("avg(")
        || trimmed.starts_with("min(")
        || trimmed.starts_with("max(")
        || trimmed.starts_with("std(")
        || trimmed.starts_with("collect(")
}

fn is_spatial_compute(expr: &str) -> bool {
    matches!(
        expr.trim(),
        "distance" | "area" | "perimeter" | "centroid_lat" | "centroid_lon"
    )
}

fn extract_agg_property(expr: &str) -> Option<&str> {
    let trimmed = expr.trim();
    if trimmed == "count(*)" {
        return None;
    }
    let start = trimmed.find('(')?;
    let end = trimmed.rfind(')')?;
    if start + 1 < end {
        Some(trimmed[start + 1..end].trim())
    } else {
        None
    }
}

fn compute_spatial_property(
    graph: &DirGraph,
    leaf_idx: NodeIndex,
    ancestor_idx: NodeIndex,
    spatial_fn: &str,
) -> Option<Value> {
    let leaf_node = graph.get_node(leaf_idx)?;
    let ancestor_node = graph.get_node(ancestor_idx)?;
    let leaf_spatial = graph.get_spatial_config(&leaf_node.node_type);
    let ancestor_spatial = graph.get_spatial_config(&ancestor_node.node_type);

    match spatial_fn.trim() {
        "distance" => {
            let (lat1, lon1) = resolve_location(leaf_node, leaf_spatial)?;
            let (lat2, lon2) = resolve_location(ancestor_node, ancestor_spatial)?;
            Some(Value::Float64(spatial::geodesic_distance(
                lat1, lon1, lat2, lon2,
            )))
        }
        "area" => {
            let geom = resolve_geometry(ancestor_node, ancestor_spatial)?;
            spatial::geometry_area_m2(&geom).ok().map(Value::Float64)
        }
        "perimeter" => {
            let geom = resolve_geometry(ancestor_node, ancestor_spatial)?;
            spatial::geometry_perimeter_m(&geom)
                .ok()
                .map(Value::Float64)
        }
        "centroid_lat" => {
            let geom = resolve_geometry(ancestor_node, ancestor_spatial)?;
            spatial::geometry_centroid(&geom)
                .ok()
                .map(|(lat, _)| Value::Float64(lat))
        }
        "centroid_lon" => {
            let geom = resolve_geometry(ancestor_node, ancestor_spatial)?;
            spatial::geometry_centroid(&geom)
                .ok()
                .map(|(_, lon)| Value::Float64(lon))
        }
        _ => None,
    }
}

fn resolve_location(
    node: &crate::graph::schema::NodeData,
    spatial_config: Option<&crate::graph::schema::SpatialConfig>,
) -> Option<(f64, f64)> {
    let sc = spatial_config?;
    if let Some((ref lat_f, ref lon_f)) = sc.location {
        let lat = node
            .get_property(lat_f)
            .as_deref()
            .and_then(mg_value_to_f64)?;
        let lon = node
            .get_property(lon_f)
            .as_deref()
            .and_then(mg_value_to_f64)?;
        return Some((lat, lon));
    }
    if let Some(ref geom_f) = sc.geometry {
        if let Some(Value::String(wkt)) = node.get_property(geom_f).as_deref() {
            if let Ok(geom) = spatial::parse_wkt(wkt) {
                return spatial::geometry_centroid(&geom).ok();
            }
        }
    }
    None
}

fn resolve_geometry(
    node: &crate::graph::schema::NodeData,
    spatial_config: Option<&crate::graph::schema::SpatialConfig>,
) -> Option<geo::geometry::Geometry<f64>> {
    let sc = spatial_config?;
    let geom_field = sc.geometry.as_deref()?;
    match node.get_property(geom_field).as_deref() {
        Some(Value::String(wkt)) => spatial::parse_wkt(wkt).ok(),
        _ => None,
    }
}

fn mg_value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Float64(f) => Some(*f),
        Value::Int64(i) => Some(*i as f64),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Aggregation mode: groups leaf nodes by ancestor and computes aggregate values.
#[allow(clippy::too_many_arguments)]
fn add_properties_aggregate(
    graph: &mut DirGraph,
    selection: &CurrentSelection,
    property_spec: &HashMap<String, PropertySpec>,
    type_to_level: &HashMap<String, usize>,
    parent_maps: &[HashMap<NodeIndex, NodeIndex>],
    target_level: usize,
) -> Result<AddPropertiesReport, String> {
    let target_level_data = match selection.get_level(target_level) {
        Some(level) if !level.is_empty() => level,
        _ => {
            return Ok(AddPropertiesReport {
                nodes_updated: 0,
                properties_set: 0,
            });
        }
    };

    let mut updates: HashMap<NodeIndex, HashMap<String, Value>> = HashMap::new();

    for (source_type, spec) in property_spec {
        let source_level = match type_to_level.get(source_type) {
            Some(&lvl) => lvl,
            None => continue,
        };

        match spec {
            PropertySpec::CopyList(props) => {
                for (_parent_opt, targets) in target_level_data.iter_groups() {
                    for &target_idx in targets {
                        if let Some(ancestor_idx) =
                            walk_to_ancestor(target_idx, target_level, source_level, parent_maps)
                        {
                            if let Some(ancestor_node) = graph.get_node(ancestor_idx) {
                                for prop_name in props {
                                    if let Some(val) = ancestor_node.get_property(prop_name) {
                                        updates
                                            .entry(target_idx)
                                            .or_default()
                                            .insert(prop_name.clone(), val.into_owned());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            PropertySpec::CopyAll => {
                for (_parent_opt, targets) in target_level_data.iter_groups() {
                    for &target_idx in targets {
                        if let Some(ancestor_idx) =
                            walk_to_ancestor(target_idx, target_level, source_level, parent_maps)
                        {
                            if let Some(ancestor_node) = graph.graph.node_weight(ancestor_idx) {
                                for (k, v) in ancestor_node.property_iter(&graph.interner) {
                                    updates
                                        .entry(target_idx)
                                        .or_default()
                                        .insert(k.to_string(), v.clone());
                                }
                            }
                        }
                    }
                }
            }
            PropertySpec::RenameMap(rename_map) => {
                for (target_name, source_expr) in rename_map {
                    if is_aggregate_expr(source_expr) {
                        let agg_prop = extract_agg_property(source_expr);

                        // Group leaf nodes by ancestor at source_level
                        let mut groups: HashMap<NodeIndex, Vec<NodeIndex>> = HashMap::new();
                        for (_parent_opt, targets) in target_level_data.iter_groups() {
                            for &target_idx in targets {
                                if let Some(ancestor) = walk_to_ancestor(
                                    target_idx,
                                    target_level,
                                    source_level,
                                    parent_maps,
                                ) {
                                    groups.entry(ancestor).or_default().push(target_idx);
                                }
                            }
                        }

                        for (ancestor_idx, leaf_indices) in &groups {
                            let values: Vec<f64> = if let Some(prop) = agg_prop {
                                leaf_indices
                                    .iter()
                                    .filter_map(|&idx| {
                                        graph.get_node(idx).and_then(|n| {
                                            n.get_property(prop)
                                                .as_deref()
                                                .and_then(mg_value_to_f64)
                                        })
                                    })
                                    .collect()
                            } else {
                                vec![]
                            };

                            let agg_value =
                                compute_aggregate(source_expr, &values, leaf_indices.len());
                            updates
                                .entry(*ancestor_idx)
                                .or_default()
                                .insert(target_name.clone(), agg_value);
                        }
                    } else if is_spatial_compute(source_expr) {
                        for (_parent_opt, targets) in target_level_data.iter_groups() {
                            for &target_idx in targets {
                                if let Some(ancestor_idx) = walk_to_ancestor(
                                    target_idx,
                                    target_level,
                                    source_level,
                                    parent_maps,
                                ) {
                                    if let Some(val) = compute_spatial_property(
                                        graph,
                                        target_idx,
                                        ancestor_idx,
                                        source_expr,
                                    ) {
                                        updates
                                            .entry(target_idx)
                                            .or_default()
                                            .insert(target_name.clone(), val);
                                    }
                                }
                            }
                        }
                    } else {
                        // Simple rename
                        for (_parent_opt, targets) in target_level_data.iter_groups() {
                            for &target_idx in targets {
                                if let Some(ancestor_idx) = walk_to_ancestor(
                                    target_idx,
                                    target_level,
                                    source_level,
                                    parent_maps,
                                ) {
                                    if let Some(ancestor_node) = graph.get_node(ancestor_idx) {
                                        if let Some(val) = ancestor_node.get_property(source_expr) {
                                            updates
                                                .entry(target_idx)
                                                .or_default()
                                                .insert(target_name.clone(), val.into_owned());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut nodes_updated = 0;
    let mut properties_set = 0;

    for (node_idx, props) in updates {
        // Pre-intern keys before getting mutable node reference (split borrow)
        let interned_props: Vec<(InternedKey, Value)> = props
            .into_iter()
            .map(|(k, v)| (graph.interner.get_or_intern(&k), v))
            .collect();
        if let Some(node) = graph.graph.node_weight_mut(node_idx) {
            let count = interned_props.len();
            for (ik, v) in interned_props {
                node.properties.insert(ik, v);
            }
            nodes_updated += 1;
            properties_set += count;
        }
    }

    Ok(AddPropertiesReport {
        nodes_updated,
        properties_set,
    })
}

fn compute_aggregate(expr: &str, values: &[f64], count: usize) -> Value {
    let trimmed = expr.trim();
    if trimmed == "count(*)" {
        return Value::Int64(count as i64);
    }
    if trimmed.starts_with("collect(") {
        let s = values
            .iter()
            .map(|v| format!("{}", v))
            .collect::<Vec<_>>()
            .join(", ");
        return Value::String(s);
    }
    if values.is_empty() {
        return Value::Null;
    }
    if trimmed.starts_with("sum(") {
        Value::Float64(values.iter().sum())
    } else if trimmed.starts_with("mean(") || trimmed.starts_with("avg(") {
        Value::Float64(values.iter().sum::<f64>() / values.len() as f64)
    } else if trimmed.starts_with("min(") {
        Value::Float64(values.iter().copied().fold(f64::INFINITY, f64::min))
    } else if trimmed.starts_with("max(") {
        Value::Float64(values.iter().copied().fold(f64::NEG_INFINITY, f64::max))
    } else if trimmed.starts_with("std(") {
        if values.len() < 2 {
            Value::Float64(0.0)
        } else {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            Value::Float64(variance.sqrt())
        }
    } else {
        Value::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::values::{ColumnData, ColumnType};
    use crate::datatypes::{DataFrame, Value};
    use crate::graph::schema::{CurrentSelection, DirGraph, NodeData, SpatialConfig};
    use petgraph::graph::NodeIndex;
    use std::collections::{HashMap, HashSet};

    // ── Helpers ─────────────────────────────────────────────────────────────

    /// Build a minimal DirGraph with nodes of a given type, returning node indices.
    fn make_graph_with_nodes(
        node_type: &str,
        nodes: Vec<(Value, &str, HashMap<String, Value>)>,
    ) -> (DirGraph, Vec<NodeIndex>) {
        let mut graph = DirGraph::new();
        let mut indices = Vec::new();
        for (id, title, props) in nodes {
            let node = NodeData::new(
                id.clone(),
                Value::String(title.to_string()),
                node_type.to_string(),
                props,
                &mut graph.interner,
            );
            let idx = graph.graph.add_node(node);
            graph
                .type_indices
                .entry(node_type.to_string())
                .or_default()
                .push(idx);
            graph
                .id_indices
                .entry(node_type.to_string())
                .or_default()
                .insert(id, idx);
            indices.push(idx);
        }
        (graph, indices)
    }

    /// Build a simple DataFrame from column definitions and row data.
    fn make_dataframe(columns: Vec<(&str, ColumnType, Vec<Option<Value>>)>) -> DataFrame {
        // Create empty DataFrame (no pre-defined columns)
        let mut df = DataFrame::new(vec![]);
        for (name, col_type, values) in columns {
            let data = match col_type {
                ColumnType::UniqueId => ColumnData::UniqueId(
                    values
                        .into_iter()
                        .map(|v| match v {
                            Some(Value::UniqueId(u)) => Some(u),
                            _ => None,
                        })
                        .collect(),
                ),
                ColumnType::Int64 => ColumnData::Int64(
                    values
                        .into_iter()
                        .map(|v| match v {
                            Some(Value::Int64(i)) => Some(i),
                            _ => None,
                        })
                        .collect(),
                ),
                ColumnType::Float64 => ColumnData::Float64(
                    values
                        .into_iter()
                        .map(|v| match v {
                            Some(Value::Float64(f)) => Some(f),
                            _ => None,
                        })
                        .collect(),
                ),
                ColumnType::String => ColumnData::String(
                    values
                        .into_iter()
                        .map(|v| match v {
                            Some(Value::String(s)) => Some(s),
                            _ => None,
                        })
                        .collect(),
                ),
                _ => panic!("unsupported column type in test helper"),
            };
            df.add_column(name.to_string(), col_type, data).unwrap();
        }
        df
    }

    /// Build a graph with Person and City node types for connection tests.
    fn make_graph_with_two_types() -> DirGraph {
        let mut graph = DirGraph::new();
        let df_persons = make_dataframe(vec![
            (
                "id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(1)), Some(Value::UniqueId(2))],
            ),
            (
                "name",
                ColumnType::String,
                vec![
                    Some(Value::String("Alice".into())),
                    Some(Value::String("Bob".into())),
                ],
            ),
        ]);
        add_nodes(
            &mut graph,
            df_persons,
            "Person".to_string(),
            "id".to_string(),
            Some("name".to_string()),
            None,
        )
        .unwrap();

        let df_cities = make_dataframe(vec![
            (
                "id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(10)), Some(Value::UniqueId(20))],
            ),
            (
                "name",
                ColumnType::String,
                vec![
                    Some(Value::String("NYC".into())),
                    Some(Value::String("LA".into())),
                ],
            ),
        ]);
        add_nodes(
            &mut graph,
            df_cities,
            "City".to_string(),
            "id".to_string(),
            Some("name".to_string()),
            None,
        )
        .unwrap();
        graph
    }

    // ====================================================================
    // check_data_validity
    // ====================================================================

    #[test]
    fn test_check_data_validity_column_exists() {
        let df = make_dataframe(vec![(
            "id",
            ColumnType::UniqueId,
            vec![Some(Value::UniqueId(1))],
        )]);
        assert!(check_data_validity(&df, "id").is_ok());
    }

    #[test]
    fn test_check_data_validity_column_missing() {
        let df = make_dataframe(vec![(
            "name",
            ColumnType::String,
            vec![Some(Value::String("Alice".into()))],
        )]);
        let err = check_data_validity(&df, "id").unwrap_err();
        assert!(err.contains("Column 'id' not found"));
        assert!(err.contains("name"));
    }

    // ====================================================================
    // get_column_types
    // ====================================================================

    #[test]
    fn test_get_column_types() {
        let df = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("age", ColumnType::Int64, vec![Some(Value::Int64(30))]),
            (
                "name",
                ColumnType::String,
                vec![Some(Value::String("A".into()))],
            ),
        ]);
        let types = get_column_types(&df);
        assert_eq!(types.get("id").unwrap(), "UniqueId");
        assert_eq!(types.get("age").unwrap(), "Int64");
        assert_eq!(types.get("name").unwrap(), "String");
    }

    #[test]
    fn test_get_column_types_includes_float() {
        let df = make_dataframe(vec![(
            "score",
            ColumnType::Float64,
            vec![Some(Value::Float64(1.5))],
        )]);
        let types = get_column_types(&df);
        assert_eq!(types.get("score").unwrap(), "Float64");
    }

    // ====================================================================
    // add_nodes
    // ====================================================================

    #[test]
    fn test_add_nodes_basic() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![
            (
                "id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(1)), Some(Value::UniqueId(2))],
            ),
            (
                "name",
                ColumnType::String,
                vec![
                    Some(Value::String("Alice".into())),
                    Some(Value::String("Bob".into())),
                ],
            ),
        ]);
        let report = add_nodes(
            &mut graph,
            df,
            "Person".to_string(),
            "id".to_string(),
            Some("name".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(report.nodes_created, 2);
        assert_eq!(report.nodes_updated, 0);
        assert_eq!(report.nodes_skipped, 0);
        assert_eq!(graph.graph.node_count(), 2);
    }

    #[test]
    fn test_add_nodes_skips_null_ids() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![
            (
                "id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(1)), None],
            ),
            (
                "name",
                ColumnType::String,
                vec![
                    Some(Value::String("Alice".into())),
                    Some(Value::String("Bob".into())),
                ],
            ),
        ]);
        let report = add_nodes(
            &mut graph,
            df,
            "Person".to_string(),
            "id".to_string(),
            Some("name".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(report.nodes_created, 1);
        assert_eq!(report.nodes_skipped, 1);
    }

    #[test]
    fn test_add_nodes_updates_existing() {
        let mut graph = DirGraph::new();
        let df1 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            (
                "name",
                ColumnType::String,
                vec![Some(Value::String("Alice".into()))],
            ),
        ]);
        add_nodes(
            &mut graph,
            df1,
            "Person".to_string(),
            "id".to_string(),
            Some("name".to_string()),
            None,
        )
        .unwrap();

        let df2 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            (
                "name",
                ColumnType::String,
                vec![Some(Value::String("Alice Updated".into()))],
            ),
        ]);
        let report = add_nodes(
            &mut graph,
            df2,
            "Person".to_string(),
            "id".to_string(),
            Some("name".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(report.nodes_created, 0);
        assert_eq!(report.nodes_updated, 1);
        assert_eq!(graph.graph.node_count(), 1);
    }

    #[test]
    fn test_add_nodes_invalid_conflict_handling() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![(
            "id",
            ColumnType::UniqueId,
            vec![Some(Value::UniqueId(1))],
        )]);
        let err = add_nodes(
            &mut graph,
            df,
            "Person".to_string(),
            "id".to_string(),
            None,
            Some("invalid_mode".to_string()),
        )
        .unwrap_err();
        assert!(err.contains("Unknown conflict handling mode"));
    }

    #[test]
    fn test_add_nodes_records_id_field_alias() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![(
            "person_id",
            ColumnType::UniqueId,
            vec![Some(Value::UniqueId(1))],
        )]);
        add_nodes(
            &mut graph,
            df,
            "Person".to_string(),
            "person_id".to_string(),
            None,
            None,
        )
        .unwrap();
        assert_eq!(graph.id_field_aliases.get("Person").unwrap(), "person_id");
    }

    #[test]
    fn test_add_nodes_records_title_field_alias() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            (
                "full_name",
                ColumnType::String,
                vec![Some(Value::String("Alice".into()))],
            ),
        ]);
        add_nodes(
            &mut graph,
            df,
            "Person".to_string(),
            "id".to_string(),
            Some("full_name".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(
            graph.title_field_aliases.get("Person").unwrap(),
            "full_name"
        );
    }

    #[test]
    fn test_add_nodes_no_alias_for_standard_fields() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            (
                "title",
                ColumnType::String,
                vec![Some(Value::String("Alice".into()))],
            ),
        ]);
        add_nodes(
            &mut graph,
            df,
            "Person".to_string(),
            "id".to_string(),
            Some("title".to_string()),
            None,
        )
        .unwrap();
        // "id" and "title" are standard names, so no alias should be recorded
        assert!(!graph.id_field_aliases.contains_key("Person"));
        assert!(!graph.title_field_aliases.contains_key("Person"));
    }

    #[test]
    fn test_add_nodes_with_properties() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            (
                "name",
                ColumnType::String,
                vec![Some(Value::String("Alice".into()))],
            ),
            ("age", ColumnType::Int64, vec![Some(Value::Int64(30))]),
        ]);
        add_nodes(
            &mut graph,
            df,
            "Person".to_string(),
            "id".to_string(),
            Some("name".to_string()),
            None,
        )
        .unwrap();
        let node = graph.graph.node_weight(NodeIndex::new(0)).unwrap();
        assert_eq!(node.get_property("age").as_deref(), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_add_nodes_conflict_skip() {
        let mut graph = DirGraph::new();
        let df1 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("val", ColumnType::Int64, vec![Some(Value::Int64(100))]),
        ]);
        add_nodes(
            &mut graph,
            df1,
            "Item".to_string(),
            "id".to_string(),
            None,
            None,
        )
        .unwrap();

        let df2 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("val", ColumnType::Int64, vec![Some(Value::Int64(200))]),
        ]);
        let report = add_nodes(
            &mut graph,
            df2,
            "Item".to_string(),
            "id".to_string(),
            None,
            Some("skip".to_string()),
        )
        .unwrap();
        assert_eq!(report.nodes_created, 0);
    }

    #[test]
    fn test_add_nodes_conflict_replace() {
        let mut graph = DirGraph::new();
        let df1 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("val", ColumnType::Int64, vec![Some(Value::Int64(100))]),
        ]);
        add_nodes(
            &mut graph,
            df1,
            "Item".to_string(),
            "id".to_string(),
            None,
            None,
        )
        .unwrap();

        let df2 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("val", ColumnType::Int64, vec![Some(Value::Int64(200))]),
        ]);
        let report = add_nodes(
            &mut graph,
            df2,
            "Item".to_string(),
            "id".to_string(),
            None,
            Some("replace".to_string()),
        )
        .unwrap();
        assert_eq!(report.nodes_updated, 1);
        assert_eq!(graph.graph.node_count(), 1);
    }

    #[test]
    fn test_add_nodes_conflict_preserve() {
        let mut graph = DirGraph::new();
        let df1 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("val", ColumnType::Int64, vec![Some(Value::Int64(100))]),
        ]);
        add_nodes(
            &mut graph,
            df1,
            "Item".to_string(),
            "id".to_string(),
            None,
            None,
        )
        .unwrap();

        let df2 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("val", ColumnType::Int64, vec![Some(Value::Int64(200))]),
        ]);
        let report = add_nodes(
            &mut graph,
            df2,
            "Item".to_string(),
            "id".to_string(),
            None,
            Some("preserve".to_string()),
        )
        .unwrap();
        // preserve keeps existing value
        assert_eq!(report.nodes_created, 0);
    }

    #[test]
    fn test_add_nodes_conflict_sum() {
        let mut graph = DirGraph::new();
        let df1 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("val", ColumnType::Int64, vec![Some(Value::Int64(100))]),
        ]);
        add_nodes(
            &mut graph,
            df1,
            "Item".to_string(),
            "id".to_string(),
            None,
            None,
        )
        .unwrap();

        let df2 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("val", ColumnType::Int64, vec![Some(Value::Int64(200))]),
        ]);
        let report = add_nodes(
            &mut graph,
            df2,
            "Item".to_string(),
            "id".to_string(),
            None,
            Some("sum".to_string()),
        )
        .unwrap();
        // sum should update the node
        assert_eq!(report.nodes_created, 0);
    }

    #[test]
    fn test_add_nodes_missing_column() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![(
            "name",
            ColumnType::String,
            vec![Some(Value::String("Alice".into()))],
        )]);
        let err = add_nodes(
            &mut graph,
            df,
            "Person".to_string(),
            "id".to_string(),
            None,
            None,
        )
        .unwrap_err();
        assert!(err.contains("Column 'id' not found"));
    }

    #[test]
    fn test_add_nodes_upserts_metadata() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("age", ColumnType::Int64, vec![Some(Value::Int64(30))]),
        ]);
        add_nodes(
            &mut graph,
            df,
            "Person".to_string(),
            "id".to_string(),
            None,
            None,
        )
        .unwrap();

        let meta = graph.get_node_type_metadata("Person").unwrap();
        assert!(meta.contains_key("id"));
        assert!(meta.contains_key("age"));
    }

    #[test]
    fn test_add_nodes_type_mismatch_error() {
        let mut graph = DirGraph::new();
        // First add with Int64 column
        let df1 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            ("score", ColumnType::Int64, vec![Some(Value::Int64(100))]),
        ]);
        add_nodes(
            &mut graph,
            df1,
            "Item".to_string(),
            "id".to_string(),
            None,
            None,
        )
        .unwrap();

        // Second add with same column but different type (Float64)
        let df2 = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(2))]),
            (
                "score",
                ColumnType::Float64,
                vec![Some(Value::Float64(1.5))],
            ),
        ]);
        let report = add_nodes(
            &mut graph,
            df2,
            "Item".to_string(),
            "id".to_string(),
            None,
            None,
        )
        .unwrap();
        // Should produce a type mismatch error in report
        assert!(report.errors.iter().any(|e| e.contains("Type mismatch")));
    }

    #[test]
    fn test_add_nodes_without_title_field() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![(
            "id",
            ColumnType::UniqueId,
            vec![Some(Value::UniqueId(1))],
        )]);
        let report = add_nodes(
            &mut graph,
            df,
            "Item".to_string(),
            "id".to_string(),
            None, // no title field
            None,
        )
        .unwrap();
        assert_eq!(report.nodes_created, 1);
    }

    #[test]
    fn test_add_nodes_multiple_properties() {
        let mut graph = DirGraph::new();
        let df = make_dataframe(vec![
            ("id", ColumnType::UniqueId, vec![Some(Value::UniqueId(1))]),
            (
                "name",
                ColumnType::String,
                vec![Some(Value::String("Alice".into()))],
            ),
            ("age", ColumnType::Int64, vec![Some(Value::Int64(30))]),
            (
                "score",
                ColumnType::Float64,
                vec![Some(Value::Float64(9.5))],
            ),
        ]);
        add_nodes(
            &mut graph,
            df,
            "Person".to_string(),
            "id".to_string(),
            Some("name".to_string()),
            None,
        )
        .unwrap();
        let node = graph.graph.node_weight(NodeIndex::new(0)).unwrap();
        assert_eq!(node.get_property("age").as_deref(), Some(&Value::Int64(30)));
        assert_eq!(
            node.get_property("score").as_deref(),
            Some(&Value::Float64(9.5))
        );
    }

    // ====================================================================
    // add_connections
    // ====================================================================

    #[test]
    fn test_add_connections_basic() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![
            (
                "person_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(1)), Some(Value::UniqueId(2))],
            ),
            (
                "city_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(10)), Some(Value::UniqueId(20))],
            ),
        ]);
        let report = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 2);
        assert_eq!(report.connections_skipped, 0);
        assert_eq!(graph.graph.edge_count(), 2);
    }

    #[test]
    fn test_add_connections_missing_source_column() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![(
            "city_id",
            ColumnType::UniqueId,
            vec![Some(Value::UniqueId(10))],
        )]);
        let err = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.contains("Source ID column 'person_id' not found"));
    }

    #[test]
    fn test_add_connections_missing_target_column() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![(
            "person_id",
            ColumnType::UniqueId,
            vec![Some(Value::UniqueId(1))],
        )]);
        let err = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.contains("Target ID column 'city_id' not found"));
    }

    #[test]
    fn test_add_connections_null_source_skipped() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![
            (
                "person_id",
                ColumnType::UniqueId,
                vec![None, Some(Value::UniqueId(2))],
            ),
            (
                "city_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(10)), Some(Value::UniqueId(20))],
            ),
        ]);
        let report = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 1);
        assert_eq!(report.connections_skipped, 1);
    }

    #[test]
    fn test_add_connections_null_target_skipped() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![
            (
                "person_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(1)), Some(Value::UniqueId(2))],
            ),
            (
                "city_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(10)), None],
            ),
        ]);
        let report = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 1);
        assert_eq!(report.connections_skipped, 1);
    }

    #[test]
    fn test_add_connections_missing_source_node_skipped() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![
            (
                "person_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(999))], // does not exist
            ),
            (
                "city_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(10))],
            ),
        ]);
        let report = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 0);
        assert_eq!(report.connections_skipped, 1);
    }

    #[test]
    fn test_add_connections_missing_target_node_skipped() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![
            (
                "person_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(1))],
            ),
            (
                "city_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(999))], // does not exist
            ),
        ]);
        let report = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 0);
        assert_eq!(report.connections_skipped, 1);
    }

    #[test]
    fn test_add_connections_both_nodes_missing_skipped() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![
            (
                "person_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(999))],
            ),
            (
                "city_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(888))],
            ),
        ]);
        let report = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 0);
        assert_eq!(report.connections_skipped, 1);
    }

    #[test]
    fn test_add_connections_invalid_conflict_handling() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![
            (
                "person_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(1))],
            ),
            (
                "city_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(10))],
            ),
        ]);
        let err = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            None,
            None,
            Some("bad_mode".to_string()),
        )
        .unwrap_err();
        assert!(err.contains("Unknown conflict handling mode"));
    }

    #[test]
    fn test_add_connections_with_edge_properties() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![
            (
                "person_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(1))],
            ),
            (
                "city_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(10))],
            ),
            ("since", ColumnType::Int64, vec![Some(Value::Int64(2020))]),
        ]);
        let report = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 1);
        assert_eq!(graph.graph.edge_count(), 1);
    }

    #[test]
    fn test_add_connections_with_title_fields() {
        let mut graph = make_graph_with_two_types();
        let df = make_dataframe(vec![
            (
                "person_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(1))],
            ),
            (
                "city_id",
                ColumnType::UniqueId,
                vec![Some(Value::UniqueId(10))],
            ),
            (
                "person_name",
                ColumnType::String,
                vec![Some(Value::String("Alice Updated".into()))],
            ),
            (
                "city_name",
                ColumnType::String,
                vec![Some(Value::String("New York City".into()))],
            ),
        ]);
        let report = add_connections(
            &mut graph,
            df,
            "LIVES_IN".to_string(),
            "Person".to_string(),
            "person_id".to_string(),
            "City".to_string(),
            "city_id".to_string(),
            Some("person_name".to_string()),
            Some("city_name".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 1);
    }

    // ====================================================================
    // update_node_titles
    // ====================================================================

    #[test]
    fn test_update_node_titles_source_only() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![
                (Value::UniqueId(1), "Alice", HashMap::new()),
                (Value::UniqueId(2), "Bob", HashMap::new()),
            ],
        );
        let df = make_dataframe(vec![(
            "new_title",
            ColumnType::String,
            vec![Some(Value::String("Alice Updated".into()))],
        )]);
        let title_idx = df.get_column_index("new_title");
        update_node_titles(&mut graph, indices[0], indices[1], 0, title_idx, None, &df).unwrap();
        assert_eq!(
            graph.get_node(indices[0]).unwrap().title,
            Value::String("Alice Updated".into())
        );
        assert_eq!(
            graph.get_node(indices[1]).unwrap().title,
            Value::String("Bob".into())
        );
    }

    #[test]
    fn test_update_node_titles_both() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![
                (Value::UniqueId(1), "Alice", HashMap::new()),
                (Value::UniqueId(2), "Bob", HashMap::new()),
            ],
        );
        let df = make_dataframe(vec![
            (
                "src_title",
                ColumnType::String,
                vec![Some(Value::String("A".into()))],
            ),
            (
                "tgt_title",
                ColumnType::String,
                vec![Some(Value::String("B".into()))],
            ),
        ]);
        update_node_titles(
            &mut graph,
            indices[0],
            indices[1],
            0,
            df.get_column_index("src_title"),
            df.get_column_index("tgt_title"),
            &df,
        )
        .unwrap();
        assert_eq!(
            graph.get_node(indices[0]).unwrap().title,
            Value::String("A".into())
        );
        assert_eq!(
            graph.get_node(indices[1]).unwrap().title,
            Value::String("B".into())
        );
    }

    #[test]
    fn test_update_node_titles_none() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let df = make_dataframe(vec![("x", ColumnType::Int64, vec![Some(Value::Int64(1))])]);
        update_node_titles(&mut graph, indices[0], indices[0], 0, None, None, &df).unwrap();
        assert_eq!(
            graph.get_node(indices[0]).unwrap().title,
            Value::String("Alice".into())
        );
    }

    // ====================================================================
    // update_schema_node
    // ====================================================================

    #[test]
    fn test_update_schema_node_basic() {
        let (mut graph, _) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let node = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
            "City".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let idx = graph.graph.add_node(node);
        graph
            .type_indices
            .entry("City".to_string())
            .or_default()
            .push(idx);

        let mut props = HashSet::new();
        props.insert("weight".to_string());
        let result = update_schema_node(&mut graph, "LIVES_IN", "Person", "City", &props);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_schema_node_missing_source_type() {
        let (mut graph, _) =
            make_graph_with_nodes("City", vec![(Value::UniqueId(10), "NYC", HashMap::new())]);
        let err = update_schema_node(&mut graph, "LIVES_IN", "Person", "City", &HashSet::new())
            .unwrap_err();
        assert!(err.contains("Source type 'Person' does not exist"));
    }

    #[test]
    fn test_update_schema_node_missing_target_type() {
        let (mut graph, _) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let err = update_schema_node(&mut graph, "LIVES_IN", "Person", "City", &HashSet::new())
            .unwrap_err();
        assert!(err.contains("Target type 'City' does not exist"));
    }

    #[test]
    fn test_update_schema_node_empty_properties() {
        let (mut graph, _) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let node = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
            "City".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let idx = graph.graph.add_node(node);
        graph
            .type_indices
            .entry("City".to_string())
            .or_default()
            .push(idx);

        let result = update_schema_node(&mut graph, "LIVES_IN", "Person", "City", &HashSet::new());
        assert!(result.is_ok());
    }

    // ====================================================================
    // update_node_properties
    // ====================================================================

    #[test]
    fn test_update_node_properties_basic() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![
                (Value::UniqueId(1), "Alice", HashMap::new()),
                (Value::UniqueId(2), "Bob", HashMap::new()),
            ],
        );
        let nodes = vec![
            (Some(indices[0]), Value::Int64(30)),
            (Some(indices[1]), Value::Int64(25)),
        ];
        let report = update_node_properties(&mut graph, &nodes, "age").unwrap();
        assert_eq!(report.nodes_updated, 2);
        assert_eq!(report.nodes_skipped, 0);
        assert_eq!(
            graph
                .get_node(indices[0])
                .unwrap()
                .get_property("age")
                .as_deref(),
            Some(&Value::Int64(30))
        );
    }

    #[test]
    fn test_update_node_properties_empty() {
        let (mut graph, _) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let err = update_node_properties(&mut graph, &[], "age").unwrap_err();
        assert!(err.contains("No nodes to update"));
    }

    #[test]
    fn test_update_node_properties_skips_none_indices() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let nodes = vec![
            (Some(indices[0]), Value::Int64(30)),
            (None, Value::Int64(25)),
        ];
        let report = update_node_properties(&mut graph, &nodes, "age").unwrap();
        // Verify the valid node was updated
        assert_eq!(
            graph
                .get_node(indices[0])
                .unwrap()
                .get_property("age")
                .as_deref(),
            Some(&Value::Int64(30))
        );
        // None index is counted as skipped in both the info-gathering pass and the batch pass
        assert!(report.nodes_skipped >= 1);
    }

    #[test]
    fn test_update_node_properties_with_float() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let nodes = vec![(Some(indices[0]), Value::Float64(3.14))];
        let report = update_node_properties(&mut graph, &nodes, "pi").unwrap();
        assert_eq!(report.nodes_updated, 1);
        assert_eq!(
            graph
                .get_node(indices[0])
                .unwrap()
                .get_property("pi")
                .as_deref(),
            Some(&Value::Float64(3.14))
        );
    }

    #[test]
    fn test_update_node_properties_with_string() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let nodes = vec![(Some(indices[0]), Value::String("active".into()))];
        let report = update_node_properties(&mut graph, &nodes, "status").unwrap();
        assert_eq!(report.nodes_updated, 1);
        assert_eq!(
            graph
                .get_node(indices[0])
                .unwrap()
                .get_property("status")
                .as_deref(),
            Some(&Value::String("active".into()))
        );
    }

    #[test]
    fn test_update_node_properties_updates_metadata() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        // Ensure node type metadata is set up
        graph.upsert_node_type_metadata("Person", HashMap::new());

        let nodes = vec![(Some(indices[0]), Value::Int64(30))];
        update_node_properties(&mut graph, &nodes, "age").unwrap();

        let meta = graph.get_node_type_metadata("Person").unwrap();
        assert!(meta.contains_key("age"));
    }

    // ====================================================================
    // walk_to_ancestor
    // ====================================================================

    #[test]
    fn test_walk_to_ancestor_same_level() {
        let parent_maps = vec![HashMap::new()];
        let node = NodeIndex::new(5);
        assert_eq!(walk_to_ancestor(node, 0, 0, &parent_maps), Some(node));
    }

    #[test]
    fn test_walk_to_ancestor_one_level() {
        let parent = NodeIndex::new(0);
        let child = NodeIndex::new(1);
        let mut parent_maps = vec![HashMap::new(), HashMap::new()];
        parent_maps[1].insert(child, parent);
        assert_eq!(walk_to_ancestor(child, 1, 0, &parent_maps), Some(parent));
    }

    #[test]
    fn test_walk_to_ancestor_two_levels() {
        let grandparent = NodeIndex::new(0);
        let parent = NodeIndex::new(1);
        let child = NodeIndex::new(2);
        let mut parent_maps = vec![HashMap::new(), HashMap::new(), HashMap::new()];
        parent_maps[1].insert(parent, grandparent);
        parent_maps[2].insert(child, parent);
        assert_eq!(
            walk_to_ancestor(child, 2, 0, &parent_maps),
            Some(grandparent)
        );
    }

    #[test]
    fn test_walk_to_ancestor_orphan() {
        let child = NodeIndex::new(2);
        let parent_maps = vec![HashMap::new(), HashMap::new(), HashMap::new()];
        assert_eq!(walk_to_ancestor(child, 2, 0, &parent_maps), None);
    }

    #[test]
    fn test_walk_to_ancestor_target_after_start() {
        let node = NodeIndex::new(0);
        let parent_maps = vec![HashMap::new(), HashMap::new()];
        assert_eq!(walk_to_ancestor(node, 0, 1, &parent_maps), None);
    }

    #[test]
    fn test_walk_to_ancestor_mid_level() {
        // Walk from level 3 to level 1 (skip level 0)
        let level1_node = NodeIndex::new(10);
        let level2_node = NodeIndex::new(20);
        let level3_node = NodeIndex::new(30);
        let mut parent_maps = vec![
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        ];
        parent_maps[2].insert(level2_node, level1_node);
        parent_maps[3].insert(level3_node, level2_node);
        assert_eq!(
            walk_to_ancestor(level3_node, 3, 1, &parent_maps),
            Some(level1_node)
        );
    }

    // ====================================================================
    // is_aggregate_expr
    // ====================================================================

    #[test]
    fn test_is_aggregate_expr_count_star() {
        assert!(is_aggregate_expr("count(*)"));
        assert!(is_aggregate_expr("  count(*)  "));
    }

    #[test]
    fn test_is_aggregate_expr_functions() {
        assert!(is_aggregate_expr("sum(value)"));
        assert!(is_aggregate_expr("mean(score)"));
        assert!(is_aggregate_expr("avg(score)"));
        assert!(is_aggregate_expr("min(x)"));
        assert!(is_aggregate_expr("max(x)"));
        assert!(is_aggregate_expr("std(x)"));
        assert!(is_aggregate_expr("collect(x)"));
    }

    #[test]
    fn test_is_aggregate_expr_non_aggregate() {
        assert!(!is_aggregate_expr("name"));
        assert!(!is_aggregate_expr("some_property"));
        assert!(!is_aggregate_expr(""));
    }

    #[test]
    fn test_is_aggregate_expr_partial_match() {
        // These should NOT match: the prefix check requires starts_with
        assert!(!is_aggregate_expr("not_sum(x)"));
        assert!(!is_aggregate_expr("xsum(y)"));
    }

    // ====================================================================
    // is_spatial_compute
    // ====================================================================

    #[test]
    fn test_is_spatial_compute_valid() {
        assert!(is_spatial_compute("distance"));
        assert!(is_spatial_compute("area"));
        assert!(is_spatial_compute("perimeter"));
        assert!(is_spatial_compute("centroid_lat"));
        assert!(is_spatial_compute("centroid_lon"));
        assert!(is_spatial_compute("  distance  "));
    }

    #[test]
    fn test_is_spatial_compute_invalid() {
        assert!(!is_spatial_compute("name"));
        assert!(!is_spatial_compute(""));
        assert!(!is_spatial_compute("distance_km"));
    }

    // ====================================================================
    // extract_agg_property
    // ====================================================================

    #[test]
    fn test_extract_agg_property_count_star() {
        assert_eq!(extract_agg_property("count(*)"), None);
    }

    #[test]
    fn test_extract_agg_property_sum() {
        assert_eq!(extract_agg_property("sum(value)"), Some("value"));
    }

    #[test]
    fn test_extract_agg_property_mean() {
        assert_eq!(extract_agg_property("mean(score)"), Some("score"));
    }

    #[test]
    fn test_extract_agg_property_with_spaces() {
        assert_eq!(extract_agg_property("  sum( total )  "), Some("total"));
    }

    #[test]
    fn test_extract_agg_property_no_parens() {
        assert_eq!(extract_agg_property("no_parens"), None);
    }

    #[test]
    fn test_extract_agg_property_empty_parens() {
        assert_eq!(extract_agg_property("func()"), None);
    }

    #[test]
    fn test_extract_agg_property_nested() {
        // "fn(inner(x))" — start at first '(' and end at last ')'
        assert_eq!(extract_agg_property("fn(inner(x))"), Some("inner(x)"));
    }

    // ====================================================================
    // mg_value_to_f64
    // ====================================================================

    #[test]
    fn test_mg_value_to_f64_float() {
        assert_eq!(mg_value_to_f64(&Value::Float64(3.14)), Some(3.14));
    }

    #[test]
    fn test_mg_value_to_f64_int() {
        assert_eq!(mg_value_to_f64(&Value::Int64(42)), Some(42.0));
    }

    #[test]
    fn test_mg_value_to_f64_string() {
        assert_eq!(
            mg_value_to_f64(&Value::String("2.5".to_string())),
            Some(2.5)
        );
    }

    #[test]
    fn test_mg_value_to_f64_bad_string() {
        assert_eq!(
            mg_value_to_f64(&Value::String("not_a_number".to_string())),
            None
        );
    }

    #[test]
    fn test_mg_value_to_f64_null() {
        assert_eq!(mg_value_to_f64(&Value::Null), None);
    }

    #[test]
    fn test_mg_value_to_f64_unique_id() {
        assert_eq!(mg_value_to_f64(&Value::UniqueId(7)), None);
    }

    #[test]
    fn test_mg_value_to_f64_negative() {
        assert_eq!(mg_value_to_f64(&Value::Int64(-10)), Some(-10.0));
        assert_eq!(mg_value_to_f64(&Value::Float64(-0.5)), Some(-0.5));
    }

    #[test]
    fn test_mg_value_to_f64_zero() {
        assert_eq!(mg_value_to_f64(&Value::Int64(0)), Some(0.0));
        assert_eq!(mg_value_to_f64(&Value::Float64(0.0)), Some(0.0));
        assert_eq!(mg_value_to_f64(&Value::String("0".to_string())), Some(0.0));
    }

    // ====================================================================
    // compute_aggregate
    // ====================================================================

    #[test]
    fn test_compute_aggregate_count() {
        let result = compute_aggregate("count(*)", &[], 5);
        assert_eq!(result, Value::Int64(5));
    }

    #[test]
    fn test_compute_aggregate_count_zero() {
        let result = compute_aggregate("count(*)", &[], 0);
        assert_eq!(result, Value::Int64(0));
    }

    #[test]
    fn test_compute_aggregate_sum() {
        let result = compute_aggregate("sum(x)", &[1.0, 2.0, 3.0], 3);
        assert_eq!(result, Value::Float64(6.0));
    }

    #[test]
    fn test_compute_aggregate_sum_single() {
        let result = compute_aggregate("sum(x)", &[42.0], 1);
        assert_eq!(result, Value::Float64(42.0));
    }

    #[test]
    fn test_compute_aggregate_mean() {
        let result = compute_aggregate("mean(x)", &[2.0, 4.0, 6.0], 3);
        assert_eq!(result, Value::Float64(4.0));
    }

    #[test]
    fn test_compute_aggregate_avg() {
        let result = compute_aggregate("avg(x)", &[10.0, 20.0], 2);
        assert_eq!(result, Value::Float64(15.0));
    }

    #[test]
    fn test_compute_aggregate_min() {
        let result = compute_aggregate("min(x)", &[5.0, 2.0, 8.0], 3);
        assert_eq!(result, Value::Float64(2.0));
    }

    #[test]
    fn test_compute_aggregate_min_negative() {
        let result = compute_aggregate("min(x)", &[-5.0, 2.0, -8.0], 3);
        assert_eq!(result, Value::Float64(-8.0));
    }

    #[test]
    fn test_compute_aggregate_max() {
        let result = compute_aggregate("max(x)", &[5.0, 2.0, 8.0], 3);
        assert_eq!(result, Value::Float64(8.0));
    }

    #[test]
    fn test_compute_aggregate_max_negative() {
        let result = compute_aggregate("max(x)", &[-5.0, -2.0, -8.0], 3);
        assert_eq!(result, Value::Float64(-2.0));
    }

    #[test]
    fn test_compute_aggregate_std_single_value() {
        let result = compute_aggregate("std(x)", &[5.0], 1);
        assert_eq!(result, Value::Float64(0.0));
    }

    #[test]
    fn test_compute_aggregate_std_multiple() {
        let result = compute_aggregate("std(x)", &[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0], 8);
        if let Value::Float64(v) = result {
            // Sample std dev: sqrt(variance) where variance = sum((x-mean)^2) / (n-1)
            // mean = 5.0, variance = 32/7 = 4.571..., std = 2.138...
            assert!((v - 2.138).abs() < 0.01, "Got std = {}", v);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_compute_aggregate_collect() {
        let result = compute_aggregate("collect(x)", &[1.0, 2.0, 3.0], 3);
        if let Value::String(s) = result {
            assert!(s.contains("1"));
            assert!(s.contains("2"));
            assert!(s.contains("3"));
        } else {
            panic!("Expected String");
        }
    }

    #[test]
    fn test_compute_aggregate_empty_values() {
        assert_eq!(compute_aggregate("sum(x)", &[], 0), Value::Null);
        assert_eq!(compute_aggregate("mean(x)", &[], 0), Value::Null);
        assert_eq!(compute_aggregate("min(x)", &[], 0), Value::Null);
        assert_eq!(compute_aggregate("max(x)", &[], 0), Value::Null);
        assert_eq!(compute_aggregate("std(x)", &[], 0), Value::Null);
    }

    #[test]
    fn test_compute_aggregate_unknown_function() {
        let result = compute_aggregate("unknown(x)", &[1.0], 1);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_compute_aggregate_collect_empty() {
        let result = compute_aggregate("collect(x)", &[], 0);
        assert_eq!(result, Value::String("".to_string()));
    }

    // ====================================================================
    // create_connections (via selection)
    // ====================================================================

    #[test]
    fn test_create_connections_single_level_returns_error() {
        // CurrentSelection::new() creates 1 level, so source_level == target_level == 0
        // which triggers "source level must be before target level" error
        let mut graph = DirGraph::new();
        let selection = CurrentSelection::new();
        let result = create_connections(
            &mut graph,
            &selection,
            "CONNECTED_TO".to_string(),
            None,
            None,
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_create_connections_empty_target_level() {
        // Two levels, but target level is empty -- should return 0 connections
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);
        selection.add_level(); // Empty level 1

        let report = create_connections(
            &mut graph,
            &selection,
            "CONNECTED_TO".to_string(),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 0);
    }

    #[test]
    fn test_create_connections_source_after_target_error() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![
                (Value::UniqueId(1), "Alice", HashMap::new()),
                (Value::UniqueId(2), "Bob", HashMap::new()),
            ],
        );
        let city = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
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

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![city_idx]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(city_idx), vec![indices[0], indices[1]]);

        let err = create_connections(
            &mut graph,
            &selection,
            "FROM".to_string(),
            None,
            None,
            Some("Person".to_string()),
            Some("City".to_string()),
        )
        .unwrap_err();
        assert!(err.contains("source level"));
        assert!(err.contains("must be before target level"));
    }

    #[test]
    fn test_create_connections_two_levels() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let city = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
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

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(indices[0]), vec![city_idx]);

        let report = create_connections(
            &mut graph,
            &selection,
            "HAS_CITY".to_string(),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 1);
        assert_eq!(graph.graph.edge_count(), 1);
    }

    #[test]
    fn test_create_connections_invalid_conflict() {
        let mut graph = DirGraph::new();
        let selection = CurrentSelection::new();
        let err = create_connections(
            &mut graph,
            &selection,
            "X".to_string(),
            Some("bad_mode".to_string()),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.contains("Unknown conflict handling mode"));
    }

    #[test]
    fn test_create_connections_nonexistent_source_type_filter() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);

        let err = create_connections(
            &mut graph,
            &selection,
            "X".to_string(),
            None,
            None,
            Some("NonExistent".to_string()),
            None,
        )
        .unwrap_err();
        assert!(err.contains("source_type 'NonExistent' not found"));
    }

    #[test]
    fn test_create_connections_nonexistent_target_type_filter() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let city = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
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

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(indices[0]), vec![city_idx]);

        let err = create_connections(
            &mut graph,
            &selection,
            "X".to_string(),
            None,
            None,
            None,
            Some("NonExistent".to_string()),
        )
        .unwrap_err();
        assert!(err.contains("target_type 'NonExistent' not found"));
    }

    #[test]
    fn test_create_connections_three_levels() {
        // Country -> Person -> City: 3 levels, connect Country to City
        let (mut graph, person_indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );

        let country = NodeData::new(
            Value::UniqueId(100),
            Value::String("USA".into()),
            "Country".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let country_idx = graph.graph.add_node(country);
        graph
            .type_indices
            .entry("Country".to_string())
            .or_default()
            .push(country_idx);
        graph
            .id_indices
            .entry("Country".to_string())
            .or_default()
            .insert(Value::UniqueId(100), country_idx);

        let city = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
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

        // Level 0: Country, Level 1: Person, Level 2: City
        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![country_idx]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(country_idx), vec![person_indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(2)
            .unwrap()
            .add_selection(Some(person_indices[0]), vec![city_idx]);

        let report = create_connections(
            &mut graph,
            &selection,
            "IN_COUNTRY".to_string(),
            None,
            None,
            Some("Country".to_string()),
            Some("City".to_string()),
        )
        .unwrap();
        assert_eq!(report.connections_created, 1);
    }

    #[test]
    fn test_create_connections_with_copy_properties() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(
                Value::UniqueId(1),
                "Alice",
                HashMap::from([("status".to_string(), Value::String("active".into()))]),
            )],
        );
        let city = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
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

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(indices[0]), vec![city_idx]);

        // Copy "status" from Person onto the edge
        let copy_props = HashMap::from([("Person".to_string(), vec!["status".to_string()])]);

        let report = create_connections(
            &mut graph,
            &selection,
            "HAS_CITY".to_string(),
            None,
            Some(copy_props),
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 1);
    }

    #[test]
    fn test_create_connections_copy_all_properties() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(
                Value::UniqueId(1),
                "Alice",
                HashMap::from([
                    ("a".to_string(), Value::Int64(1)),
                    ("b".to_string(), Value::Int64(2)),
                ]),
            )],
        );
        let city = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
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

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(indices[0]), vec![city_idx]);

        // Empty vec means copy all properties
        let copy_props = HashMap::from([("Person".to_string(), vec![])]);

        let report = create_connections(
            &mut graph,
            &selection,
            "HAS_CITY".to_string(),
            None,
            Some(copy_props),
            None,
            None,
        )
        .unwrap();
        assert_eq!(report.connections_created, 1);
    }

    // ====================================================================
    // PropertySpec enum
    // ====================================================================

    #[test]
    fn test_property_spec_debug() {
        let spec = PropertySpec::CopyAll;
        let _ = format!("{:?}", spec);
        let spec2 = PropertySpec::CopyList(vec!["a".into()]);
        let _ = format!("{:?}", spec2);
        let spec3 = PropertySpec::RenameMap(HashMap::from([("new".into(), "old".into())]));
        let _ = format!("{:?}", spec3);
    }

    // ====================================================================
    // add_properties
    // ====================================================================

    #[test]
    fn test_add_properties_empty_selection() {
        let mut graph = DirGraph::new();
        let selection = CurrentSelection::new();
        let report = add_properties(&mut graph, &selection, HashMap::new()).unwrap();
        assert_eq!(report.nodes_updated, 0);
        assert_eq!(report.properties_set, 0);
    }

    #[test]
    fn test_add_properties_copy_list() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(
                Value::UniqueId(1),
                "Alice",
                HashMap::from([("status".to_string(), Value::String("active".into()))]),
            )],
        );
        let city = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
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

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(indices[0]), vec![city_idx]);

        let spec = HashMap::from([(
            "Person".to_string(),
            PropertySpec::CopyList(vec!["status".to_string()]),
        )]);
        let report = add_properties(&mut graph, &selection, spec).unwrap();
        assert_eq!(report.nodes_updated, 1);
        assert_eq!(report.properties_set, 1);
        assert_eq!(
            graph
                .get_node(city_idx)
                .unwrap()
                .get_property("status")
                .as_deref(),
            Some(&Value::String("active".into()))
        );
    }

    #[test]
    fn test_add_properties_copy_all() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(
                Value::UniqueId(1),
                "Alice",
                HashMap::from([
                    ("a".to_string(), Value::Int64(1)),
                    ("b".to_string(), Value::Int64(2)),
                ]),
            )],
        );
        let city = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
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

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(indices[0]), vec![city_idx]);

        let spec = HashMap::from([("Person".to_string(), PropertySpec::CopyAll)]);
        let report = add_properties(&mut graph, &selection, spec).unwrap();
        assert_eq!(report.nodes_updated, 1);
        assert!(report.properties_set >= 2);
    }

    #[test]
    fn test_add_properties_rename_map() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(
                Value::UniqueId(1),
                "Alice",
                HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
            )],
        );
        let city = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
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

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(indices[0]), vec![city_idx]);

        let spec = HashMap::from([(
            "Person".to_string(),
            PropertySpec::RenameMap(HashMap::from([(
                "person_name".to_string(),
                "name".to_string(),
            )])),
        )]);
        let report = add_properties(&mut graph, &selection, spec).unwrap();
        assert_eq!(report.nodes_updated, 1);
        assert_eq!(
            graph
                .get_node(city_idx)
                .unwrap()
                .get_property("person_name")
                .as_deref(),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn test_add_properties_invalid_source_type() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);

        let spec = HashMap::from([("NonExistentType".to_string(), PropertySpec::CopyAll)]);
        let result = add_properties(&mut graph, &selection, spec);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.contains("Source type 'NonExistentType' not found"));
    }

    #[test]
    fn test_add_properties_copy_nonexistent_property() {
        let (mut graph, indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let city = NodeData::new(
            Value::UniqueId(10),
            Value::String("NYC".into()),
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

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(indices[0]), vec![city_idx]);

        let spec = HashMap::from([(
            "Person".to_string(),
            PropertySpec::CopyList(vec!["nonexistent".to_string()]),
        )]);
        let report = add_properties(&mut graph, &selection, spec).unwrap();
        // No properties copied, so nodes_updated should be 0
        assert_eq!(report.nodes_updated, 0);
    }

    // ====================================================================
    // add_properties with aggregation
    // ====================================================================

    #[test]
    fn test_add_properties_aggregate_count() {
        // Aggregation groups leaf nodes (Task at level 1) by their ancestor
        // at source_level. Using source_type="Person" (level 0) means we group
        // tasks by person ancestor and write the aggregate onto Person.
        let (mut graph, person_indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let task1 = NodeData::new(
            Value::UniqueId(100),
            Value::String("T1".into()),
            "Task".to_string(),
            HashMap::from([("score".to_string(), Value::Float64(10.0))]),
            &mut graph.interner,
        );
        let task2 = NodeData::new(
            Value::UniqueId(101),
            Value::String("T2".into()),
            "Task".to_string(),
            HashMap::from([("score".to_string(), Value::Float64(20.0))]),
            &mut graph.interner,
        );
        let t1_idx = graph.graph.add_node(task1);
        let t2_idx = graph.graph.add_node(task2);
        graph
            .type_indices
            .entry("Task".to_string())
            .or_default()
            .extend([t1_idx, t2_idx]);

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![person_indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(person_indices[0]), vec![t1_idx, t2_idx]);

        // source_type = "Person" (level 0): group tasks by person, write to person
        let spec = HashMap::from([(
            "Person".to_string(),
            PropertySpec::RenameMap(HashMap::from([
                ("task_count".to_string(), "count(*)".to_string()),
                ("total_score".to_string(), "sum(score)".to_string()),
            ])),
        )]);
        let report = add_properties(&mut graph, &selection, spec).unwrap();
        assert!(report.nodes_updated > 0);

        let person = graph.get_node(person_indices[0]).unwrap();
        assert_eq!(
            person.get_property("task_count").as_deref(),
            Some(&Value::Int64(2))
        );
        assert_eq!(
            person.get_property("total_score").as_deref(),
            Some(&Value::Float64(30.0))
        );
    }

    #[test]
    fn test_add_properties_aggregate_mean() {
        let (mut graph, person_indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        let task1 = NodeData::new(
            Value::UniqueId(100),
            Value::String("T1".into()),
            "Task".to_string(),
            HashMap::from([("score".to_string(), Value::Float64(10.0))]),
            &mut graph.interner,
        );
        let task2 = NodeData::new(
            Value::UniqueId(101),
            Value::String("T2".into()),
            "Task".to_string(),
            HashMap::from([("score".to_string(), Value::Float64(30.0))]),
            &mut graph.interner,
        );
        let t1_idx = graph.graph.add_node(task1);
        let t2_idx = graph.graph.add_node(task2);
        graph
            .type_indices
            .entry("Task".to_string())
            .or_default()
            .extend([t1_idx, t2_idx]);

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![person_indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(person_indices[0]), vec![t1_idx, t2_idx]);

        // source_type = "Person" (level 0): group tasks by person, write mean to person
        let spec = HashMap::from([(
            "Person".to_string(),
            PropertySpec::RenameMap(HashMap::from([(
                "avg_score".to_string(),
                "mean(score)".to_string(),
            )])),
        )]);
        let report = add_properties(&mut graph, &selection, spec).unwrap();
        assert!(report.nodes_updated > 0);

        let person = graph.get_node(person_indices[0]).unwrap();
        assert_eq!(
            person.get_property("avg_score").as_deref(),
            Some(&Value::Float64(20.0))
        );
    }

    #[test]
    fn test_add_properties_aggregate_with_copy_list() {
        // Aggregation mode also handles CopyList from ancestors
        let (mut graph, person_indices) = make_graph_with_nodes(
            "Person",
            vec![(
                Value::UniqueId(1),
                "Alice",
                HashMap::from([("role".to_string(), Value::String("admin".into()))]),
            )],
        );
        let task1 = NodeData::new(
            Value::UniqueId(100),
            Value::String("T1".into()),
            "Task".to_string(),
            HashMap::from([("score".to_string(), Value::Float64(10.0))]),
            &mut graph.interner,
        );
        let t1_idx = graph.graph.add_node(task1);
        graph
            .type_indices
            .entry("Task".to_string())
            .or_default()
            .push(t1_idx);

        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![person_indices[0]]);
        selection.add_level();
        selection
            .get_level_mut(1)
            .unwrap()
            .add_selection(Some(person_indices[0]), vec![t1_idx]);

        // Mix: aggregate (source=Person groups tasks by person) + copy from Person
        let spec = HashMap::from([(
            "Person".to_string(),
            PropertySpec::RenameMap(HashMap::from([(
                "task_count".to_string(),
                "count(*)".to_string(),
            )])),
        )]);
        let report = add_properties(&mut graph, &selection, spec).unwrap();
        assert!(report.nodes_updated > 0);
    }

    #[test]
    fn test_add_properties_aggregate_empty_target_level() {
        let (mut graph, person_indices) = make_graph_with_nodes(
            "Person",
            vec![(Value::UniqueId(1), "Alice", HashMap::new())],
        );
        // Create a selection with Person at level 0, but empty level 1
        let mut selection = CurrentSelection::new();
        selection
            .get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![person_indices[0]]);
        selection.add_level(); // Empty level 1

        // Even though spec references Task aggregation, the empty target level
        // means nothing should be updated
        let spec = HashMap::from([(
            "Person".to_string(),
            PropertySpec::RenameMap(HashMap::from([("x".to_string(), "count(*)".to_string())])),
        )]);
        let report = add_properties(&mut graph, &selection, spec).unwrap();
        assert_eq!(report.nodes_updated, 0);
    }

    // ====================================================================
    // resolve_location / compute_spatial_property
    // ====================================================================

    #[test]
    fn test_resolve_location_no_spatial_config() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::UniqueId(1),
            Value::String("Test".into()),
            "Place".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        assert!(resolve_location(&node, None).is_none());
    }

    #[test]
    fn test_resolve_location_with_lat_lon() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::UniqueId(1),
            Value::String("Test".into()),
            "Place".to_string(),
            HashMap::from([
                ("lat".to_string(), Value::Float64(40.7128)),
                ("lon".to_string(), Value::Float64(-74.0060)),
            ]),
            &mut graph.interner,
        );
        let config = SpatialConfig {
            location: Some(("lat".to_string(), "lon".to_string())),
            geometry: None,
            points: HashMap::new(),
            shapes: HashMap::new(),
        };
        let result = resolve_location(&node, Some(&config));
        assert!(result.is_some());
        let (lat, lon) = result.unwrap();
        assert!((lat - 40.7128).abs() < 0.0001);
        assert!((lon - (-74.0060)).abs() < 0.0001);
    }

    #[test]
    fn test_resolve_location_missing_lat_field() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::UniqueId(1),
            Value::String("Test".into()),
            "Place".to_string(),
            HashMap::from([("lon".to_string(), Value::Float64(-74.0060))]),
            &mut graph.interner,
        );
        let config = SpatialConfig {
            location: Some(("lat".to_string(), "lon".to_string())),
            geometry: None,
            points: HashMap::new(),
            shapes: HashMap::new(),
        };
        assert!(resolve_location(&node, Some(&config)).is_none());
    }

    #[test]
    fn test_resolve_geometry_no_config() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::UniqueId(1),
            Value::String("Test".into()),
            "Place".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        assert!(resolve_geometry(&node, None).is_none());
    }

    #[test]
    fn test_resolve_geometry_no_geometry_field_in_config() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::UniqueId(1),
            Value::String("Test".into()),
            "Place".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let config = SpatialConfig {
            location: None,
            geometry: None,
            points: HashMap::new(),
            shapes: HashMap::new(),
        };
        assert!(resolve_geometry(&node, Some(&config)).is_none());
    }

    #[test]
    fn test_resolve_geometry_with_wkt() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::UniqueId(1),
            Value::String("Test".into()),
            "Place".to_string(),
            HashMap::from([("geom".to_string(), Value::String("POINT(0 0)".into()))]),
            &mut graph.interner,
        );
        let config = SpatialConfig {
            location: None,
            geometry: Some("geom".to_string()),
            points: HashMap::new(),
            shapes: HashMap::new(),
        };
        let result = resolve_geometry(&node, Some(&config));
        assert!(result.is_some());
    }

    #[test]
    fn test_resolve_geometry_invalid_wkt() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::UniqueId(1),
            Value::String("Test".into()),
            "Place".to_string(),
            HashMap::from([("geom".to_string(), Value::String("NOT_WKT".into()))]),
            &mut graph.interner,
        );
        let config = SpatialConfig {
            location: None,
            geometry: Some("geom".to_string()),
            points: HashMap::new(),
            shapes: HashMap::new(),
        };
        assert!(resolve_geometry(&node, Some(&config)).is_none());
    }

    #[test]
    fn test_compute_spatial_property_distance() {
        let mut graph = DirGraph::new();
        let node1 = NodeData::new(
            Value::UniqueId(1),
            Value::String("A".into()),
            "Place".to_string(),
            HashMap::from([
                ("lat".to_string(), Value::Float64(0.0)),
                ("lon".to_string(), Value::Float64(0.0)),
            ]),
            &mut graph.interner,
        );
        let node2 = NodeData::new(
            Value::UniqueId(2),
            Value::String("B".into()),
            "Place".to_string(),
            HashMap::from([
                ("lat".to_string(), Value::Float64(1.0)),
                ("lon".to_string(), Value::Float64(1.0)),
            ]),
            &mut graph.interner,
        );
        let idx1 = graph.graph.add_node(node1);
        let idx2 = graph.graph.add_node(node2);
        graph
            .type_indices
            .entry("Place".to_string())
            .or_default()
            .extend([idx1, idx2]);
        graph.spatial_configs.insert(
            "Place".to_string(),
            SpatialConfig {
                location: Some(("lat".to_string(), "lon".to_string())),
                geometry: None,
                points: HashMap::new(),
                shapes: HashMap::new(),
            },
        );

        let result = compute_spatial_property(&graph, idx1, idx2, "distance");
        assert!(result.is_some());
        if let Some(Value::Float64(d)) = result {
            assert!(d > 0.0);
        } else {
            panic!("Expected Float64 distance");
        }
    }

    #[test]
    fn test_compute_spatial_property_unknown_fn() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::UniqueId(1),
            Value::String("A".into()),
            "Place".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let idx = graph.graph.add_node(node);
        assert!(compute_spatial_property(&graph, idx, idx, "unknown_fn").is_none());
    }

    #[test]
    fn test_compute_spatial_property_no_spatial_config() {
        let mut graph = DirGraph::new();
        let node = NodeData::new(
            Value::UniqueId(1),
            Value::String("A".into()),
            "NoSpatial".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let idx = graph.graph.add_node(node);
        // No spatial config registered for "NoSpatial" type
        assert!(compute_spatial_property(&graph, idx, idx, "distance").is_none());
    }

    #[test]
    fn test_check_data_validity_invalid_column() {
        let result = check_data_validity(&create_test_dataframe(), "nonexistent_column");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Column 'nonexistent_column' not found"));
    }

    #[test]
    fn test_check_data_validity_valid_column() {
        let result = check_data_validity(&create_test_dataframe(), "id");
        assert!(result.is_ok());
    }

    #[test]
    fn test_compute_aggregate_all_functions() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let sum_result = compute_aggregate("sum(v)", &values, 5);
        assert!(matches!(sum_result, Value::Float64(_)));
        let mean_result = compute_aggregate("mean(v)", &values, 5);
        assert!(matches!(mean_result, Value::Float64(_)));
        let min_result = compute_aggregate("min(v)", &values, 5);
        assert!(matches!(min_result, Value::Float64(_)));
        let max_result = compute_aggregate("max(v)", &values, 5);
        assert!(matches!(max_result, Value::Float64(_)));
        let count_result = compute_aggregate("count(*)", &values, 5);
        assert_eq!(count_result, Value::Int64(5));
        let collect_result = compute_aggregate("collect(v)", &values, 5);
        assert!(matches!(collect_result, Value::String(_)));
    }

    #[test]
    fn test_compute_aggregate_collect_single_value() {
        let values = vec![42.0];
        let result = compute_aggregate("collect(value)", &values, 1);
        match result {
            Value::String(s) => assert_eq!(s, "42"),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_compute_aggregate_collect_with_decimals() {
        let values = vec![1.5, 2.7, 3.14];
        let result = compute_aggregate("collect(value)", &values, 3);
        match result {
            Value::String(s) => {
                assert!(s.contains("1.5"));
                assert!(s.contains("2.7"));
                assert!(s.contains("3.14"));
            }
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_compute_aggregate_count_star() {
        let result = compute_aggregate("count(*)", &[], 5);
        assert_eq!(result, Value::Int64(5));
    }

    #[test]
    fn test_compute_aggregate_empty_values_with_property() {
        let result = compute_aggregate("sum(value)", &[], 0);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_compute_aggregate_max_all_same() {
        let values = vec![5.0, 5.0, 5.0];
        let result = compute_aggregate("max(value)", &values, 3);
        match result {
            Value::Float64(f) => assert!((f - 5.0).abs() < 0.0001),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_compute_aggregate_mean_with_single_value() {
        let values = vec![5.0];
        let result = compute_aggregate("mean(value)", &values, 1);
        match result {
            Value::Float64(f) => assert!((f - 5.0).abs() < 0.0001),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_compute_aggregate_min_with_negatives() {
        let values = vec![5.0, -3.0, 2.0];
        let result = compute_aggregate("min(value)", &values, 3);
        match result {
            Value::Float64(f) => assert!((f - (-3.0)).abs() < 0.0001),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_compute_aggregate_std() {
        let values = vec![1.0, 2.0, 3.0];
        let result = compute_aggregate("std(value)", &values, 3);
        match result {
            Value::Float64(f) => assert!(f > 0.0),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_compute_aggregate_std_two_values() {
        let values = vec![1.0, 3.0];
        let result = compute_aggregate("std(value)", &values, 2);
        match result {
            Value::Float64(f) => {
                assert!((f - std::f64::consts::SQRT_2).abs() < 0.001);
            }
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_compute_aggregate_sum_negative_values() {
        let values = vec![-1.0, -2.0, 3.0];
        let result = compute_aggregate("sum(value)", &values, 3);
        match result {
            Value::Float64(f) => assert!((f - 0.0).abs() < 0.0001),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_compute_aggregate_with_whitespace() {
        let values = vec![1.0, 2.0, 3.0];
        let result = compute_aggregate("  sum(value)  ", &values, 3);
        match result {
            Value::Float64(f) => assert!((f - 6.0).abs() < 0.0001),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_conflict_handling_parse_default() {
        let mode = match None::<&str> {
            Some("replace") => ConflictHandling::Replace,
            Some("skip") => ConflictHandling::Skip,
            Some("preserve") => ConflictHandling::Preserve,
            Some("sum") => ConflictHandling::Sum,
            Some("update") | None => ConflictHandling::Update,
            Some(_) => panic!("Unknown"),
        };
        assert!(matches!(mode, ConflictHandling::Update));
    }

    #[test]
    fn test_conflict_handling_parse_preserve() {
        let mode = match Some("preserve") {
            Some("replace") => ConflictHandling::Replace,
            Some("skip") => ConflictHandling::Skip,
            Some("preserve") => ConflictHandling::Preserve,
            Some("sum") => ConflictHandling::Sum,
            Some("update") | None => ConflictHandling::Update,
            Some(_) => panic!("Unknown"),
        };
        assert!(matches!(mode, ConflictHandling::Preserve));
    }

    #[test]
    fn test_conflict_handling_parse_replace() {
        let mode = match Some("replace") {
            Some("replace") => ConflictHandling::Replace,
            Some("skip") => ConflictHandling::Skip,
            Some("preserve") => ConflictHandling::Preserve,
            Some("sum") => ConflictHandling::Sum,
            Some("update") | None => ConflictHandling::Update,
            Some(_) => panic!("Unknown"),
        };
        assert!(matches!(mode, ConflictHandling::Replace));
    }

    #[test]
    fn test_conflict_handling_parse_skip() {
        let mode = match Some("skip") {
            Some("replace") => ConflictHandling::Replace,
            Some("skip") => ConflictHandling::Skip,
            Some("preserve") => ConflictHandling::Preserve,
            Some("sum") => ConflictHandling::Sum,
            Some("update") | None => ConflictHandling::Update,
            Some(_) => panic!("Unknown"),
        };
        assert!(matches!(mode, ConflictHandling::Skip));
    }

    #[test]
    fn test_conflict_handling_parse_sum() {
        let mode = match Some("sum") {
            Some("replace") => ConflictHandling::Replace,
            Some("skip") => ConflictHandling::Skip,
            Some("preserve") => ConflictHandling::Preserve,
            Some("sum") => ConflictHandling::Sum,
            Some("update") | None => ConflictHandling::Update,
            Some(_) => panic!("Unknown"),
        };
        assert!(matches!(mode, ConflictHandling::Sum));
    }

    #[test]
    fn test_extract_agg_property_complex_names() {
        let result = extract_agg_property("sum(field_with_underscores)");
        assert_eq!(result, Some("field_with_underscores"));
    }

    #[test]
    fn test_extract_agg_property_unbalanced_parens() {
        let result = extract_agg_property("sum(value");
        assert_eq!(result, None);
        let result = extract_agg_property("sum value)");
        assert_eq!(result, None);
    }

    #[test]
    fn test_extract_agg_property_with_special_chars() {
        let result = extract_agg_property("sum(field_name_123)");
        assert_eq!(result, Some("field_name_123"));
    }

    #[test]
    fn test_get_column_types_maps_names_to_types() {
        let df = create_test_dataframe();
        let types = get_column_types(&df);
        for (col_name, col_type) in types {
            assert!(!col_name.is_empty());
            assert!(!col_type.is_empty());
        }
    }

    #[test]
    fn test_get_column_types_returns_map() {
        let df = create_test_dataframe();
        let types = get_column_types(&df);
        assert!(!types.is_empty());
        assert!(types.contains_key("id") || types.contains_key("name"));
    }

    #[test]
    fn test_is_aggregate_expr_all_valid() {
        let agg_fns = vec!["count(*)", "sum(x)", "mean(x)", "avg(x)", "min(x)", "max(x)", "std(x)", "collect(x)"];
        for fn_name in agg_fns {
            assert!(is_aggregate_expr(fn_name));
        }
    }

    #[test]
    fn test_is_aggregate_expr_collect() {
        assert!(is_aggregate_expr("collect(value)"));
    }

    #[test]
    fn test_is_aggregate_expr_count() {
        assert!(is_aggregate_expr("count(*)"));
        assert!(is_aggregate_expr("  count(*)  "));
    }

    #[test]
    fn test_is_aggregate_expr_mean() {
        assert!(is_aggregate_expr("mean(value)"));
        assert!(is_aggregate_expr("avg(value)"));
    }

    #[test]
    fn test_is_aggregate_expr_min_max() {
        assert!(is_aggregate_expr("min(value)"));
        assert!(is_aggregate_expr("max(value)"));
    }

    #[test]
    fn test_is_aggregate_expr_not_aggregate() {
        assert!(!is_aggregate_expr("value"));
        assert!(!is_aggregate_expr("some_field"));
        assert!(!is_aggregate_expr(""));
    }

    #[test]
    fn test_is_aggregate_expr_std() {
        assert!(is_aggregate_expr("std(value)"));
    }

    #[test]
    fn test_is_aggregate_expr_sum() {
        assert!(is_aggregate_expr("sum(value)"));
        assert!(is_aggregate_expr("  sum(field)  "));
    }

    #[test]
    fn test_is_aggregate_expr_uppercase() {
        assert!(!is_aggregate_expr("SUM(value)"));
        assert!(!is_aggregate_expr("COUNT(*)"));
    }

    #[test]
    fn test_is_spatial_compute_all_valid() {
        let spatial_fns = vec!["distance", "area", "perimeter", "centroid_lat", "centroid_lon"];
        for fn_name in spatial_fns {
            assert!(is_spatial_compute(fn_name));
        }
    }

    #[test]
    fn test_is_spatial_compute_area() {
        assert!(is_spatial_compute("area"));
    }

    #[test]
    fn test_is_spatial_compute_centroid() {
        assert!(is_spatial_compute("centroid_lat"));
        assert!(is_spatial_compute("centroid_lon"));
    }

    #[test]
    fn test_is_spatial_compute_distance() {
        assert!(is_spatial_compute("distance"));
        assert!(is_spatial_compute("  distance  "));
    }

    #[test]
    fn test_is_spatial_compute_not_spatial() {
        assert!(!is_spatial_compute("latitude"));
        assert!(!is_spatial_compute("location"));
        assert!(!is_spatial_compute(""));
    }

    #[test]
    fn test_is_spatial_compute_perimeter() {
        assert!(is_spatial_compute("perimeter"));
    }

    #[test]
    fn test_is_spatial_compute_uppercase() {
        assert!(!is_spatial_compute("DISTANCE"));
        assert!(!is_spatial_compute("Area"));
    }

    #[test]
    fn test_mg_value_to_f64_float64() {
        let val = Value::Float64(3.14);
        assert_eq!(mg_value_to_f64(&val), Some(3.14));
    }

    #[test]
    fn test_mg_value_to_f64_int64() {
        let val = Value::Int64(42);
        assert_eq!(mg_value_to_f64(&val), Some(42.0));
    }

    #[test]
    fn test_mg_value_to_f64_large_numbers() {
        let val = Value::Int64(i64::MAX / 2);
        let result = mg_value_to_f64(&val);
        assert!(result.is_some());
        assert!(result.unwrap() > 0.0);
    }

    #[test]
    fn test_mg_value_to_f64_negative_numbers() {
        let val = Value::Float64(-3.14);
        assert_eq!(mg_value_to_f64(&val), Some(-3.14));
        let val = Value::Int64(-42);
        assert_eq!(mg_value_to_f64(&val), Some(-42.0));
        let val = Value::String("-99.99".to_string());
        let result = mg_value_to_f64(&val);
        assert!(result.is_some());
        assert!((result.unwrap() - (-99.99)).abs() < 0.0001);
    }

    #[test]
    fn test_mg_value_to_f64_scientific_notation() {
        let val = Value::String("1e-5".to_string());
        let result = mg_value_to_f64(&val);
        assert!(result.is_some());
        assert!((result.unwrap() - 0.00001).abs() < 0.000001);
    }

    #[test]
    fn test_mg_value_to_f64_string_integer() {
        let val = Value::String("42".to_string());
        assert_eq!(mg_value_to_f64(&val), Some(42.0));
    }

    #[test]
    fn test_mg_value_to_f64_string_invalid() {
        let val = Value::String("not_a_number".to_string());
        assert_eq!(mg_value_to_f64(&val), None);
    }

    #[test]
    fn test_mg_value_to_f64_string_valid() {
        let val = Value::String("3.14".to_string());
        let result = mg_value_to_f64(&val);
        assert!(result.is_some());
        assert!((result.unwrap() - 3.14).abs() < 0.0001);
    }

    #[test]
    fn test_mg_value_to_f64_string_whitespace() {
        let val = Value::String("  42.5  ".to_string());
        let result = mg_value_to_f64(&val);
        // Rust's parse() does not trim whitespace, so this returns None
        assert!(result.is_none());
    }

    #[test]
    fn test_mg_value_to_f64_zero_values() {
        let val = Value::Float64(0.0);
        assert_eq!(mg_value_to_f64(&val), Some(0.0));
        let val = Value::Int64(0);
        assert_eq!(mg_value_to_f64(&val), Some(0.0));
        let val = Value::String("0".to_string());
        assert_eq!(mg_value_to_f64(&val), Some(0.0));
    }

    #[test]
    fn test_property_spec_copy_all() {
        let spec = PropertySpec::CopyAll;
        match spec {
            PropertySpec::CopyAll => {},
            _ => panic!("Expected CopyAll"),
        }
    }

    #[test]
    fn test_property_spec_copy_list() {
        let spec = PropertySpec::CopyList(vec!["name".to_string(), "value".to_string()]);
        match spec {
            PropertySpec::CopyList(props) => assert_eq!(props.len(), 2),
            _ => panic!("Expected CopyList"),
        }
    }

    #[test]
    fn test_property_spec_enum_variants() {
        let spec_copy = PropertySpec::CopyAll;
        let spec_list = PropertySpec::CopyList(vec!["a".to_string()]);
        let spec_rename = PropertySpec::RenameMap(HashMap::new());
        assert!(matches!(spec_copy, PropertySpec::CopyAll));
        assert!(matches!(spec_list, PropertySpec::CopyList(_)));
        assert!(matches!(spec_rename, PropertySpec::RenameMap(_)));
    }

    #[test]
    fn test_property_spec_rename_map() {
        let mut map = HashMap::new();
        map.insert("target".to_string(), "source".to_string());
        let spec = PropertySpec::RenameMap(map);
        match spec {
            PropertySpec::RenameMap(m) => assert_eq!(m.len(), 1),
            _ => panic!("Expected RenameMap"),
        }
    }

    #[test]
    fn test_walk_to_ancestor_missing_parent() {
        use petgraph::graph::NodeIndex;
        let node_idx = NodeIndex::new(0);
        let parent_maps = vec![HashMap::new(); 3];
        let result = walk_to_ancestor(node_idx, 1, 0, &parent_maps);
        assert_eq!(result, None);
    }

    #[test]
    fn test_walk_to_ancestor_multi_level() {
        use petgraph::graph::NodeIndex;
        let leaf_idx = NodeIndex::new(0);
        let mid_idx = NodeIndex::new(1);
        let root_idx = NodeIndex::new(2);
        let mut parent_maps = vec![HashMap::new(); 4];
        // Proper setup: leaf at level 2 has parent mid at level 2 map
        // mid at level 1 has parent root at level 1 map (but we only need up to level 1 for a 2-level walk)
        parent_maps[2].insert(leaf_idx, mid_idx);
        parent_maps[1].insert(mid_idx, root_idx);
        // Walk from leaf (level 2) to level 0: should go through level 2 to get to mid, then level 1 to get to root
        let result = walk_to_ancestor(leaf_idx, 2, 0, &parent_maps);
        assert_eq!(result, Some(root_idx));
    }

    #[test]
    fn test_walk_to_ancestor_target_level_greater_than_start() {
        use petgraph::graph::NodeIndex;
        let node_idx = NodeIndex::new(0);
        let parent_maps = vec![HashMap::new(); 3];
        let result = walk_to_ancestor(node_idx, 1, 2, &parent_maps);
        assert_eq!(result, None);
    }

    #[test]
    fn test_walk_to_ancestor_with_parent_maps() {
        use petgraph::graph::NodeIndex;
        let child_idx = NodeIndex::new(0);
        let parent_idx = NodeIndex::new(1);
        let mut parent_maps = vec![HashMap::new(); 3];
        parent_maps[1].insert(child_idx, parent_idx);
        let result = walk_to_ancestor(child_idx, 1, 0, &parent_maps);
        assert_eq!(result, Some(parent_idx));
    }

    #[test]
    fn test_walk_to_ancestor_zero_level() {
        use petgraph::graph::NodeIndex;
        let node_idx = NodeIndex::new(0);
        let parent_maps = vec![HashMap::new(); 3];
        let result = walk_to_ancestor(node_idx, 0, 0, &parent_maps);
        assert_eq!(result, Some(node_idx));
    }

    fn create_test_dataframe() -> DataFrame {
        use crate::datatypes::values::ColumnType;
        DataFrame::new(vec![
            ("id".to_string(), ColumnType::UniqueId),
            ("name".to_string(), ColumnType::String),
        ])
    }
}
