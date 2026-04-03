// src/graph/calculations.rs
use super::equation_parser::{AggregateType, Evaluator, Expr, Parser};
use super::lookups::TypeLookup;
use super::maintain_graph;
use super::statistics_methods::{get_parent_child_pairs, ParentChildPair};
use crate::datatypes::values::Value;
use crate::graph::reporting::CalculationOperationReport; // Remove unused OperationReport import
use crate::graph::schema::{CurrentSelection, DirGraph, NodeData, StringInterner};
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use std::time::Instant; // For timing operations

#[derive(Debug)]
pub enum EvaluationResult {
    Stored(CalculationOperationReport),
    Computed(Vec<StatResult>),
}

#[derive(Debug)]
pub struct StatResult {
    pub node_idx: Option<NodeIndex>,
    pub parent_idx: Option<NodeIndex>,
    pub parent_title: Option<String>,
    pub value: Value,
    pub error_msg: Option<String>,
}

/// Cache parent titles from pairs to avoid redundant graph lookups
fn cache_parent_titles(
    pairs: &[ParentChildPair],
    graph: &DirGraph,
) -> HashMap<NodeIndex, Option<String>> {
    pairs
        .iter()
        .filter_map(|pair| {
            pair.parent.map(|idx| {
                (
                    idx,
                    graph
                        .get_node(idx)
                        .and_then(|node| node.get_field_ref("title"))
                        .and_then(|v| v.as_string()),
                )
            })
        })
        .collect()
}

pub fn process_equation(
    graph: &mut DirGraph,
    selection: &CurrentSelection,
    expression: &str,
    level_index: Option<usize>,
    store_as: Option<&str>,
    aggregate_connections: Option<bool>,
) -> Result<EvaluationResult, String> {
    // Start tracking time for reporting
    let start_time = Instant::now();

    // Track non-fatal errors that occur during processing
    let mut errors = Vec::new();

    // Check for unknown aggregate function names
    if let Some(unknown_func) = extract_unknown_aggregate_function(expression) {
        let supported = AggregateType::get_supported_names().join(", ");
        return Err(format!(
            "Unknown aggregate function '{}'. Supported functions are: {}",
            unknown_func, supported
        ));
    }

    // Parse the expression first
    let parsed_expr = match Parser::parse_expression(expression) {
        Ok(expr) => expr,
        Err(err) => {
            // Try to provide more context for why the parsing failed
            return Err(if expression.is_empty() {
                "Expression cannot be empty.".to_string()
            } else if expression.contains("(") && !expression.contains(")") {
                "Missing closing parenthesis in expression.".to_string()
            } else if !expression.contains("(") && expression.contains(")") {
                "Unexpected closing parenthesis in expression.".to_string()
            } else if !expression.contains("(") && is_likely_aggregate_name(expression) {
                format!(
                    "Function '{}' requires parentheses. Try '{}(property)' instead.",
                    expression, expression
                )
            } else {
                format!("Failed to parse expression: {}. Check for syntax errors or case sensitivity in function names (use 'sum', not 'SUM').", err)
            });
        }
    };

    // Extract variables from the expression
    let variables = parsed_expr.extract_variables();

    // Check if selection is valid or empty
    if selection.get_level_count() == 0 {
        return Err(
            "No nodes selected. Please apply filters or traversals before calculating.".to_string(),
        );
    }

    // Additional check to see if the current level has any nodes
    let effective_level_index =
        level_index.unwrap_or_else(|| selection.get_level_count().saturating_sub(1));
    let nodes_processed;
    if let Some(level) = selection.get_level(effective_level_index) {
        if level.node_count() == 0 {
            return Err(format!(
                "No nodes found at level {}. Make sure your filters and traversals return data.",
                effective_level_index
            ));
        }

        // Define nodes_processed at first usage - use node_count() to avoid allocation
        nodes_processed = level.node_count();
    } else {
        return Err(format!(
            "Invalid level index: {}. Selection only has {} levels.",
            effective_level_index,
            selection.get_level_count()
        ));
    }
    // If we have a selection, validate variables against schema
    // Skip validation for connection aggregation since properties are on edges, not nodes
    if !aggregate_connections.unwrap_or(false) {
        if let Some(level) = selection.get_level(effective_level_index) {
            if !level.is_empty() {
                // Get a sample node to determine node type
                if let Some(sample_node_idx) = level.iter_node_indices().next() {
                    if let Some(sample_node) = graph.get_node(sample_node_idx) {
                        let node_type = &sample_node.node_type;

                        // Check if schema node exists for this type
                        let schema_lookup =
                            match TypeLookup::new(&graph.graph, "SchemaNode".to_string()) {
                                Ok(lookup) => lookup,
                                Err(_) => {
                                    return Err("Could not access schema information".to_string())
                                }
                            };

                        let schema_title = Value::String(node_type.clone());

                        if let Some(schema_idx) = schema_lookup.check_title(&schema_title) {
                            if let Some(schema_node) = graph.get_node(schema_idx) {
                                // Validate each variable against schema properties
                                // Don't check reserved field names like 'id', 'title', 'type'
                                for var in &variables {
                                    if var != "id"
                                        && var != "title"
                                        && var != "type"
                                        && !schema_node.has_property(var)
                                    {
                                        let available = schema_node
                                            .property_keys(&graph.interner)
                                            .map(|k| k.to_string())
                                            .collect::<Vec<String>>()
                                            .join(", ");
                                        return Err(format!(
                                            "Property '{}' does not exist on '{}' nodes. Available properties: {}",
                                            var, node_type, available
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let is_aggregation = has_aggregation(&parsed_expr);

    // When performing evaluation, we can use an immutable reference to graph
    let results = if aggregate_connections.unwrap_or(false) {
        evaluate_connection_equation(graph, selection, &parsed_expr, level_index)
    } else {
        evaluate_equation(graph, selection, &parsed_expr, level_index)
    };

    // Count nodes with errors for reporting
    let nodes_with_errors = results.iter().filter(|r| r.error_msg.is_some()).count();

    // Collect evaluation errors
    for result in &results {
        if let Some(error_msg) = &result.error_msg {
            let node_info = if let Some(title) = &result.parent_title {
                format!("Node '{}': ", title)
            } else {
                "".to_string()
            };
            errors.push(format!("{}Evaluation error: {}", node_info, error_msg));
        }
    }

    // If we don't need to store results, just return them directly
    if store_as.is_none() {
        if results.is_empty() {
            return Err(
                "No results from calculation. Check that your selection contains data.".to_string(),
            );
        }

        return Ok(EvaluationResult::Computed(results));
    }

    // Only proceed with node updating logic if we need to store results
    let target_property = store_as.unwrap();

    // Determine where to store results based on whether there's aggregation
    let effective_level_index =
        level_index.unwrap_or_else(|| selection.get_level_count().saturating_sub(1));

    // Prepare a Vec to hold valid nodes for update
    let mut nodes_to_update: Vec<(Option<NodeIndex>, Value)> = Vec::new();

    if is_aggregation {
        // For aggregation - get actual parent nodes from the selection
        for result in &results {
            if let Some(parent_idx) = result.parent_idx {
                // Verify the parent node exists in the graph
                if graph.get_node(parent_idx).is_some() {
                    nodes_to_update.push((Some(parent_idx), result.value.clone()));
                }
            }
        }
    } else {
        // For non-aggregation - get actual child nodes from the selection
        if let Some(level) = selection.get_level(effective_level_index) {
            // Create HashMap from node indices to results
            let result_map: HashMap<NodeIndex, &StatResult> = results
                .iter()
                .filter_map(|r| r.node_idx.map(|idx| (idx, r)))
                .collect();

            // Get all node indices directly from the current level
            for node_idx in level.iter_node_indices() {
                // Direct HashMap lookup instead of linear search
                if let Some(&result) = result_map.get(&node_idx) {
                    // Verify node exists in the graph - IMPORTANT: Must check here
                    if graph.get_node(node_idx).is_some() {
                        nodes_to_update.push((Some(node_idx), result.value.clone()));
                    }
                }
            }
        }
    }

    // Check if we found any valid nodes to update
    if nodes_to_update.is_empty() {
        return Err(format!(
            "No valid nodes found to store '{}'. Selection level: {}, Aggregation: {}",
            target_property, effective_level_index, is_aggregation
        ));
    }

    // Update the node properties with verified node indices
    let update_result =
        maintain_graph::update_node_properties(graph, &nodes_to_update, target_property)?;

    // Update nodes_updated from the result (now used)
    let nodes_updated = update_result.nodes_updated;

    // Calculate elapsed time for report
    let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    // Create the report - using all counters to avoid unused assignment warnings
    let mut report = CalculationOperationReport::new(
        "process_equation".to_string(),
        expression.to_string(),
        nodes_processed,
        nodes_updated,
        nodes_with_errors,
        elapsed_ms,
        is_aggregation,
    );

    // Add errors if we found any
    if !errors.is_empty() {
        report = report.with_errors(errors);
    }

    Ok(EvaluationResult::Stored(report))
}

// Helper function to extract potentially unknown aggregate function name from expression
fn extract_unknown_aggregate_function(expression: &str) -> Option<String> {
    // Simple heuristic: if expression contains word(property) pattern but word is not a known aggregate
    let lowercase_expr = expression.to_lowercase();

    // Check for common patterns like "func(arg)" where func is not recognized
    let parts: Vec<&str> = lowercase_expr.split('(').collect();
    if parts.len() > 1 {
        let func_part = parts[0].trim();

        // Skip known functions
        if !is_known_aggregate(func_part) {
            // Check that it looks like a function name (alphanumeric)
            if func_part.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Some(func_part.to_string());
            }
        }
    }

    None
}

// Check if a name is a supported aggregate function
fn is_known_aggregate(name: &str) -> bool {
    AggregateType::from_string(name).is_some()
}

// Check if a string looks like it might be intended as an aggregate function name
fn is_likely_aggregate_name(name: &str) -> bool {
    let name = name.trim().to_lowercase();

    // Common aggregate function names people might try to use
    let common_aggregates = [
        "sum", "avg", "average", "mean", "median", "min", "max", "count", "std", "stdev", "stddev",
        "var", "variance",
    ];

    common_aggregates.contains(&name.as_str())
}

// Modified evaluate_equation to take a parsed expression directly
// Now takes an immutable reference to graph since it only needs to read
pub fn evaluate_equation(
    graph: &DirGraph,
    selection: &CurrentSelection,
    parsed_expr: &Expr,
    level_index: Option<usize>,
) -> Vec<StatResult> {
    let is_aggregation = has_aggregation(parsed_expr);

    if is_aggregation {
        let pairs = get_parent_child_pairs(selection, level_index);
        let parent_titles = cache_parent_titles(&pairs, graph);

        pairs
            .iter()
            .map(|pair| {
                // Collect property objects directly - no need to clone NodeData
                let objects: Vec<HashMap<String, Value>> = pair
                    .children
                    .iter()
                    .filter_map(|&node_idx| {
                        graph
                            .get_node(node_idx)
                            .map(|n| convert_node_to_object(n, &graph.interner))
                    })
                    .collect();

                if objects.is_empty() {
                    return StatResult {
                        node_idx: None,
                        parent_idx: pair.parent,
                        // Use cached parent title instead of looking it up again
                        parent_title: pair
                            .parent
                            .and_then(|idx| parent_titles.get(&idx).cloned().flatten()),
                        value: Value::Null,
                        error_msg: Some("No valid nodes found".to_string()),
                    };
                }

                match Evaluator::evaluate(parsed_expr, &objects) {
                    Ok(value) => StatResult {
                        node_idx: None,
                        parent_idx: pair.parent,
                        // Use cached parent title
                        parent_title: pair
                            .parent
                            .and_then(|idx| parent_titles.get(&idx).cloned().flatten()),
                        value,
                        error_msg: None,
                    },
                    Err(err) => StatResult {
                        node_idx: None,
                        parent_idx: pair.parent,
                        // Use cached parent title
                        parent_title: pair
                            .parent
                            .and_then(|idx| parent_titles.get(&idx).cloned().flatten()),
                        value: Value::Null,
                        error_msg: Some(err),
                    },
                }
            })
            .collect()
    } else {
        let effective_index =
            level_index.unwrap_or_else(|| selection.get_level_count().saturating_sub(1));
        let level = match selection.get_level(effective_index) {
            Some(l) => l,
            None => return vec![],
        };

        let nodes = level.get_all_nodes();

        nodes
            .iter()
            .map(|&node_idx| match graph.get_node(node_idx) {
                Some(node) => {
                    let title = node.get_field_ref("title").and_then(|v| v.as_string());
                    let obj = convert_node_to_object(node, &graph.interner);

                    match Evaluator::evaluate(parsed_expr, &[obj]) {
                        Ok(value) => StatResult {
                            node_idx: Some(node_idx),
                            parent_idx: None,
                            parent_title: title,
                            value,
                            error_msg: None,
                        },
                        Err(err) => StatResult {
                            node_idx: Some(node_idx),
                            parent_idx: None,
                            parent_title: title,
                            value: Value::Null,
                            error_msg: Some(err),
                        },
                    }
                }
                None => StatResult {
                    node_idx: Some(node_idx),
                    parent_idx: None,
                    parent_title: None,
                    value: Value::Null,
                    error_msg: Some("Node not found".to_string()),
                },
            })
            .collect()
    }
}

/// Evaluate an expression on connection (edge) properties instead of node properties.
/// This is useful for aggregating properties stored on edges/connections.
pub fn evaluate_connection_equation(
    graph: &DirGraph,
    selection: &CurrentSelection,
    parsed_expr: &Expr,
    level_index: Option<usize>,
) -> Vec<StatResult> {
    // Connection aggregation requires at least 2 levels (parent -> child relationship)
    if selection.get_level_count() < 2 {
        return vec![StatResult {
            node_idx: None,
            parent_idx: None,
            parent_title: None,
            value: Value::Null,
            error_msg: Some(
                "Connection aggregation requires a traversal (at least 2 selection levels)"
                    .to_string(),
            ),
        }];
    }

    let pairs = get_parent_child_pairs(selection, level_index);
    let parent_titles = cache_parent_titles(&pairs, graph);

    pairs
        .iter()
        .map(|pair| {
            let parent_idx = match pair.parent {
                Some(idx) => idx,
                None => {
                    return StatResult {
                        node_idx: None,
                        parent_idx: None,
                        parent_title: None,
                        value: Value::Null,
                        error_msg: Some("No parent node for connection aggregation".to_string()),
                    };
                }
            };

            // Collect edge properties from edges connecting parent to children
            // Use find_edge for O(1) lookup instead of O(n) linear search
            let edge_objects: Vec<HashMap<String, Value>> = pair
                .children
                .iter()
                .filter_map(|&child_idx| {
                    // Try parent->child direction first, then child->parent
                    let edge_idx = graph
                        .graph
                        .find_edge(parent_idx, child_idx)
                        .or_else(|| graph.graph.find_edge(child_idx, parent_idx));

                    edge_idx
                        .and_then(|idx| graph.graph.edge_weight(idx))
                        .map(|edge_data| {
                            let mut props = edge_data.properties_cloned(&graph.interner);
                            // Also include the connection_type as a property
                            props.insert(
                                "connection_type".to_string(),
                                Value::String(
                                    edge_data.connection_type_str(&graph.interner).to_string(),
                                ),
                            );
                            props
                        })
                })
                .collect();

            if edge_objects.is_empty() {
                return StatResult {
                    node_idx: None,
                    parent_idx: Some(parent_idx),
                    parent_title: parent_titles.get(&parent_idx).cloned().flatten(),
                    value: Value::Null,
                    error_msg: Some("No connections found between parent and children".to_string()),
                };
            }

            match Evaluator::evaluate(parsed_expr, &edge_objects) {
                Ok(value) => StatResult {
                    node_idx: None,
                    parent_idx: Some(parent_idx),
                    parent_title: parent_titles.get(&parent_idx).cloned().flatten(),
                    value,
                    error_msg: None,
                },
                Err(err) => StatResult {
                    node_idx: None,
                    parent_idx: Some(parent_idx),
                    parent_title: parent_titles.get(&parent_idx).cloned().flatten(),
                    value: Value::Null,
                    error_msg: Some(err),
                },
            }
        })
        .collect()
}

fn has_aggregation(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(_, _) => true,
        Expr::Add(left, right) => has_aggregation(left) || has_aggregation(right),
        Expr::Subtract(left, right) => has_aggregation(left) || has_aggregation(right),
        Expr::Multiply(left, right) => has_aggregation(left) || has_aggregation(right),
        Expr::Divide(left, right) => has_aggregation(left) || has_aggregation(right),
        _ => false,
    }
}

fn convert_node_to_object(node: &NodeData, interner: &StringInterner) -> HashMap<String, Value> {
    // Pre-allocate HashMap with exact capacity to avoid reallocations
    let mut object = HashMap::with_capacity(node.property_count());

    // Process all properties - avoid clone for simple Copy-like types
    for (key, value) in node.property_iter(interner) {
        let new_value = match value {
            // Directly construct new values for simple numeric types (avoids Clone overhead)
            Value::Int64(n) => Value::Int64(*n),
            Value::Float64(n) => Value::Float64(*n),
            Value::UniqueId(n) => Value::UniqueId(*n),
            Value::Boolean(b) => Value::Boolean(*b),
            Value::Null => Value::Null,
            Value::String(s) => {
                // Try to parse as number for calculations
                if let Ok(num) = s.parse::<f64>() {
                    Value::Float64(num)
                } else {
                    Value::String(s.clone())
                }
            }
            // DateTime and any future types need clone
            _ => value.clone(),
        };
        object.insert(key.to_string(), new_value);
    }

    object
}

pub fn count_nodes_in_level(selection: &CurrentSelection, level_index: Option<usize>) -> usize {
    let effective_index = match level_index {
        Some(idx) => idx,
        None => selection.get_level_count().saturating_sub(1),
    };

    let level = selection
        .get_level(effective_index)
        .expect("Level should exist");

    level.node_count()
}

pub fn count_nodes_by_parent(
    graph: &DirGraph,
    selection: &CurrentSelection,
    level_index: Option<usize>,
) -> Vec<StatResult> {
    let pairs = get_parent_child_pairs(selection, level_index);

    pairs
        .iter()
        .map(|pair| StatResult {
            node_idx: None,
            parent_idx: pair.parent,
            parent_title: pair.parent.and_then(|idx| {
                graph
                    .get_node(idx)
                    .and_then(|node| node.get_field_ref("title"))
                    .and_then(|v| v.as_string())
            }),
            value: Value::Int64(pair.children.len() as i64),
            error_msg: None,
        })
        .collect()
}

pub fn store_count_results(
    graph: &mut DirGraph,
    selection: &CurrentSelection,
    level_index: Option<usize>,
    group_by_parent: bool,
    target_property: &str,
) -> Result<CalculationOperationReport, String> {
    // Track start time for reporting
    let start_time = std::time::Instant::now();

    // Track errors
    let mut errors = Vec::new();

    let mut nodes_to_update: Vec<(Option<NodeIndex>, Value)> = Vec::new();

    let nodes_processed;
    if group_by_parent {
        // For grouped counting, store count for each parent
        let counts = count_nodes_by_parent(graph, selection, level_index);
        nodes_processed = counts.len();

        for result in &counts {
            if let Some(parent_idx) = result.parent_idx {
                // Verify the parent node exists in the graph
                if graph.get_node(parent_idx).is_some() {
                    nodes_to_update.push((Some(parent_idx), result.value.clone()));
                } else {
                    errors.push(format!(
                        "Parent node index {:?} not found in graph",
                        parent_idx
                    ));
                }
            }
        }
    } else {
        // For flat counting, store same count for all nodes in level
        let count = count_nodes_in_level(selection, level_index);
        let effective_index =
            level_index.unwrap_or_else(|| selection.get_level_count().saturating_sub(1));

        if let Some(level) = selection.get_level(effective_index) {
            nodes_processed = level.node_count();

            // Apply the count to each node in the level
            for node_idx in level.iter_node_indices() {
                if graph.get_node(node_idx).is_some() {
                    nodes_to_update.push((Some(node_idx), Value::Int64(count as i64)));
                } else {
                    errors.push(format!("Node index {:?} not found in graph", node_idx));
                }
            }
        } else {
            let error_msg = format!("No valid level found at index {}", effective_index);
            errors.push(error_msg.clone());
            return Err(error_msg);
        }
    }

    // Check if we found any valid nodes to update
    if nodes_to_update.is_empty() {
        let error_msg = format!(
            "No valid nodes found to store '{}' count values.",
            target_property
        );
        errors.push(error_msg.clone());
        return Err(error_msg);
    }

    // Use the optimized batch update (which now returns a NodeOperationReport)
    let update_result =
        match maintain_graph::update_node_properties(graph, &nodes_to_update, target_property) {
            Ok(result) => result,
            Err(e) => {
                errors.push(format!("Failed to update node properties: {}", e));
                return Err(format!("Failed to update node properties: {}", e));
            }
        };

    // Add any errors from the update operation
    for error in &update_result.errors {
        errors.push(error.clone());
    }

    // Calculate elapsed time
    let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;

    // Create the calculation report
    let mut report = CalculationOperationReport::new(
        "count".to_string(),
        format!(
            "count({})",
            if let Some(idx) = level_index {
                format!("level {}", idx)
            } else {
                "current level".to_string()
            }
        ),
        nodes_processed,
        update_result.nodes_updated,
        update_result.nodes_skipped,
        elapsed_ms,
        group_by_parent,
    );

    // Add errors if we found any
    if !errors.is_empty() {
        report = report.with_errors(errors);
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::values::Value;
    use crate::graph::equation_parser::{AggregateType, Expr, Parser};
    use crate::graph::schema::{CurrentSelection, DirGraph, EdgeData, NodeData, StringInterner};
    use petgraph::graph::NodeIndex;
    use std::collections::HashMap;

    // ========================================================================
    // Helper: build a DirGraph with nodes and optional edges
    // ========================================================================

    /// Create a simple DirGraph with some numbered nodes that have a "score" property.
    fn make_graph_with_nodes(scores: &[f64]) -> (DirGraph, Vec<NodeIndex>) {
        let mut g = DirGraph::new();
        let mut indices = Vec::new();
        for (i, &score) in scores.iter().enumerate() {
            let mut props = HashMap::new();
            props.insert("score".to_string(), Value::Float64(score));
            let node = NodeData::new(
                Value::Int64(i as i64),
                Value::String(format!("node_{}", i)),
                "TestNode".to_string(),
                props,
                &mut g.interner,
            );
            let idx = g.graph.add_node(node);
            indices.push(idx);
        }
        (g, indices)
    }

    /// Build a two-level selection: level 0 has a parent, level 1 has children grouped under that parent.
    fn make_parent_child_selection(parent: NodeIndex, children: &[NodeIndex]) -> CurrentSelection {
        let mut sel = CurrentSelection::new();
        // Level 0: the parent node (grouped under None since it has no parent)
        sel.get_level_mut(0)
            .unwrap()
            .add_selection(None, vec![parent]);

        // Level 1: children grouped under the parent
        sel.add_level();
        sel.get_level_mut(1)
            .unwrap()
            .add_selection(Some(parent), children.to_vec());

        sel
    }

    /// Build a flat (single-level) selection with nodes grouped under None.
    fn make_flat_selection(nodes: &[NodeIndex]) -> CurrentSelection {
        let mut sel = CurrentSelection::new();
        sel.get_level_mut(0)
            .unwrap()
            .add_selection(None, nodes.to_vec());
        sel
    }

    // ========================================================================
    // extract_unknown_aggregate_function
    // ========================================================================

    #[test]
    fn test_extract_unknown_aggregate_known_functions() {
        // Known aggregate functions should return None
        assert!(extract_unknown_aggregate_function("sum(score)").is_none());
        assert!(extract_unknown_aggregate_function("mean(score)").is_none());
        assert!(extract_unknown_aggregate_function("avg(score)").is_none());
        assert!(extract_unknown_aggregate_function("min(score)").is_none());
        assert!(extract_unknown_aggregate_function("max(score)").is_none());
        assert!(extract_unknown_aggregate_function("count(score)").is_none());
        assert!(extract_unknown_aggregate_function("std(score)").is_none());
    }

    #[test]
    fn test_extract_unknown_aggregate_unknown_function() {
        let result = extract_unknown_aggregate_function("median(score)");
        assert_eq!(result, Some("median".to_string()));

        let result = extract_unknown_aggregate_function("foobar(x)");
        assert_eq!(result, Some("foobar".to_string()));
    }

    #[test]
    fn test_extract_unknown_aggregate_no_parens() {
        // No parentheses at all - should return None
        assert!(extract_unknown_aggregate_function("score + 1").is_none());
        assert!(extract_unknown_aggregate_function("score").is_none());
    }

    #[test]
    fn test_extract_unknown_aggregate_expression_with_operator_before_paren() {
        // "score + 1" split by '(' yields ["score + 1"] with len 1, so None
        assert!(extract_unknown_aggregate_function("score + 1").is_none());
        // "a * b(c)" -> parts[0] = "a * b" which has non-alphanumeric chars -> None
        assert!(extract_unknown_aggregate_function("a * b(c)").is_none());
    }

    // ========================================================================
    // is_known_aggregate
    // ========================================================================

    #[test]
    fn test_is_known_aggregate() {
        assert!(is_known_aggregate("sum"));
        assert!(is_known_aggregate("mean"));
        assert!(is_known_aggregate("avg"));
        assert!(is_known_aggregate("std"));
        assert!(is_known_aggregate("min"));
        assert!(is_known_aggregate("max"));
        assert!(is_known_aggregate("count"));
        assert!(!is_known_aggregate("median"));
        assert!(!is_known_aggregate("variance"));
        assert!(!is_known_aggregate("foobar"));
        assert!(!is_known_aggregate(""));
    }

    // ========================================================================
    // is_likely_aggregate_name
    // ========================================================================

    #[test]
    fn test_is_likely_aggregate_name_known_names() {
        for name in &[
            "sum", "avg", "average", "mean", "median", "min", "max", "count", "std", "stdev",
            "stddev", "var", "variance",
        ] {
            assert!(
                is_likely_aggregate_name(name),
                "'{}' should be considered a likely aggregate name",
                name
            );
        }
    }

    #[test]
    fn test_is_likely_aggregate_name_case_insensitive() {
        assert!(is_likely_aggregate_name("SUM"));
        assert!(is_likely_aggregate_name("Avg"));
        assert!(is_likely_aggregate_name("  Mean  "));
    }

    #[test]
    fn test_is_likely_aggregate_name_unknown() {
        assert!(!is_likely_aggregate_name("foobar"));
        assert!(!is_likely_aggregate_name("product"));
        assert!(!is_likely_aggregate_name("score"));
    }

    // ========================================================================
    // has_aggregation
    // ========================================================================

    #[test]
    fn test_has_aggregation_simple_variable() {
        let expr = Expr::Variable("score".to_string());
        assert!(!has_aggregation(&expr));
    }

    #[test]
    fn test_has_aggregation_number() {
        let expr = Expr::Number(42.0);
        assert!(!has_aggregation(&expr));
    }

    #[test]
    fn test_has_aggregation_aggregate_expr() {
        let expr = Expr::Aggregate(
            AggregateType::Sum,
            Box::new(Expr::Variable("score".to_string())),
        );
        assert!(has_aggregation(&expr));
    }

    #[test]
    fn test_has_aggregation_nested_in_add() {
        // sum(score) + 1
        let expr = Expr::Add(
            Box::new(Expr::Aggregate(
                AggregateType::Sum,
                Box::new(Expr::Variable("score".to_string())),
            )),
            Box::new(Expr::Number(1.0)),
        );
        assert!(has_aggregation(&expr));
    }

    #[test]
    fn test_has_aggregation_nested_in_subtract() {
        let expr = Expr::Subtract(
            Box::new(Expr::Number(100.0)),
            Box::new(Expr::Aggregate(
                AggregateType::Min,
                Box::new(Expr::Variable("score".to_string())),
            )),
        );
        assert!(has_aggregation(&expr));
    }

    #[test]
    fn test_has_aggregation_nested_in_multiply() {
        let expr = Expr::Multiply(
            Box::new(Expr::Aggregate(
                AggregateType::Mean,
                Box::new(Expr::Variable("x".to_string())),
            )),
            Box::new(Expr::Number(2.0)),
        );
        assert!(has_aggregation(&expr));
    }

    #[test]
    fn test_has_aggregation_nested_in_divide() {
        let expr = Expr::Divide(
            Box::new(Expr::Number(1.0)),
            Box::new(Expr::Aggregate(
                AggregateType::Count,
                Box::new(Expr::Variable("x".to_string())),
            )),
        );
        assert!(has_aggregation(&expr));
    }

    #[test]
    fn test_has_aggregation_arithmetic_no_aggregate() {
        // score + 1
        let expr = Expr::Add(
            Box::new(Expr::Variable("score".to_string())),
            Box::new(Expr::Number(1.0)),
        );
        assert!(!has_aggregation(&expr));
    }

    // ========================================================================
    // convert_node_to_object
    // ========================================================================

    #[test]
    fn test_convert_node_to_object_numeric_properties() {
        let mut interner = StringInterner::new();
        let mut props = HashMap::new();
        props.insert("score".to_string(), Value::Float64(3.14));
        props.insert("count".to_string(), Value::Int64(42));
        let node = NodeData::new(
            Value::Int64(1),
            Value::String("test".to_string()),
            "TestNode".to_string(),
            props,
            &mut interner,
        );

        let obj = convert_node_to_object(&node, &interner);
        assert_eq!(obj.get("score"), Some(&Value::Float64(3.14)));
        assert_eq!(obj.get("count"), Some(&Value::Int64(42)));
    }

    #[test]
    fn test_convert_node_to_object_string_parsed_as_number() {
        let mut interner = StringInterner::new();
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::String("123.45".to_string()));
        let node = NodeData::new(
            Value::Int64(1),
            Value::String("test".to_string()),
            "TestNode".to_string(),
            props,
            &mut interner,
        );

        let obj = convert_node_to_object(&node, &interner);
        // Numeric strings should be parsed to Float64
        assert_eq!(obj.get("value"), Some(&Value::Float64(123.45)));
    }

    #[test]
    fn test_convert_node_to_object_non_numeric_string_kept() {
        let mut interner = StringInterner::new();
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("hello".to_string()));
        let node = NodeData::new(
            Value::Int64(1),
            Value::String("test".to_string()),
            "TestNode".to_string(),
            props,
            &mut interner,
        );

        let obj = convert_node_to_object(&node, &interner);
        assert_eq!(obj.get("name"), Some(&Value::String("hello".to_string())));
    }

    #[test]
    fn test_convert_node_to_object_null_and_bool() {
        let mut interner = StringInterner::new();
        let mut props = HashMap::new();
        props.insert("empty".to_string(), Value::Null);
        props.insert("flag".to_string(), Value::Boolean(true));
        let node = NodeData::new(
            Value::Int64(1),
            Value::String("test".to_string()),
            "TestNode".to_string(),
            props,
            &mut interner,
        );

        let obj = convert_node_to_object(&node, &interner);
        assert_eq!(obj.get("empty"), Some(&Value::Null));
        assert_eq!(obj.get("flag"), Some(&Value::Boolean(true)));
    }

    // ========================================================================
    // cache_parent_titles
    // ========================================================================

    #[test]
    fn test_cache_parent_titles_with_parents() {
        let (graph, indices) = make_graph_with_nodes(&[10.0, 20.0, 30.0]);
        let pairs = vec![ParentChildPair {
            parent: Some(indices[0]),
            children: vec![indices[1], indices[2]],
        }];

        let titles = cache_parent_titles(&pairs, &graph);
        assert_eq!(titles.len(), 1);
        assert_eq!(
            titles.get(&indices[0]).cloned().flatten(),
            Some("node_0".to_string())
        );
    }

    #[test]
    fn test_cache_parent_titles_no_parent() {
        let (graph, indices) = make_graph_with_nodes(&[10.0]);
        let pairs = vec![ParentChildPair {
            parent: None,
            children: vec![indices[0]],
        }];

        let titles = cache_parent_titles(&pairs, &graph);
        assert!(titles.is_empty());
    }

    // ========================================================================
    // count_nodes_in_level
    // ========================================================================

    #[test]
    fn test_count_nodes_in_level_default() {
        let (_graph, indices) = make_graph_with_nodes(&[1.0, 2.0, 3.0]);
        let sel = make_flat_selection(&indices);

        let count = count_nodes_in_level(&sel, None);
        assert_eq!(count, 3);
    }

    #[test]
    fn test_count_nodes_in_level_explicit_index() {
        let (_graph, indices) = make_graph_with_nodes(&[1.0, 2.0]);
        let sel = make_flat_selection(&indices);

        let count = count_nodes_in_level(&sel, Some(0));
        assert_eq!(count, 2);
    }

    #[test]
    fn test_count_nodes_in_level_empty() {
        let sel = make_flat_selection(&[]);
        let count = count_nodes_in_level(&sel, None);
        assert_eq!(count, 0);
    }

    // ========================================================================
    // count_nodes_by_parent
    // ========================================================================

    #[test]
    fn test_count_nodes_by_parent_single_parent() {
        let (graph, indices) = make_graph_with_nodes(&[10.0, 20.0, 30.0]);
        let sel = make_parent_child_selection(indices[0], &[indices[1], indices[2]]);

        let results = count_nodes_by_parent(&graph, &sel, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, Value::Int64(2));
        assert_eq!(results[0].parent_idx, Some(indices[0]));
        assert_eq!(results[0].parent_title, Some("node_0".to_string()));
        assert!(results[0].error_msg.is_none());
    }

    #[test]
    fn test_count_nodes_by_parent_no_parent() {
        let (graph, indices) = make_graph_with_nodes(&[10.0, 20.0]);
        let sel = make_flat_selection(&indices);

        let results = count_nodes_by_parent(&graph, &sel, None);
        // Flat selection has one group with parent=None
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, Value::Int64(2));
        assert_eq!(results[0].parent_idx, None);
        assert_eq!(results[0].parent_title, None);
    }

    // ========================================================================
    // evaluate_equation — non-aggregation (per-node evaluation)
    // ========================================================================

    #[test]
    fn test_evaluate_equation_simple_variable() {
        let (graph, indices) = make_graph_with_nodes(&[10.0, 20.0, 30.0]);
        let sel = make_flat_selection(&indices);
        let expr = Parser::parse_expression("score").unwrap();

        let results = evaluate_equation(&graph, &sel, &expr, None);
        assert_eq!(results.len(), 3);

        // Collect values (order may vary due to HashMap iteration in SelectionLevel)
        let mut values: Vec<f64> = results
            .iter()
            .filter_map(|r| match &r.value {
                Value::Float64(f) => Some(*f),
                _ => None,
            })
            .collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(values, vec![10.0, 20.0, 30.0]);
    }

    #[test]
    fn test_evaluate_equation_arithmetic() {
        let (graph, indices) = make_graph_with_nodes(&[10.0]);
        let sel = make_flat_selection(&indices);
        let expr = Parser::parse_expression("score + 5").unwrap();

        let results = evaluate_equation(&graph, &sel, &expr, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, Value::Float64(15.0));
        assert!(results[0].error_msg.is_none());
    }

    #[test]
    fn test_evaluate_equation_multiply() {
        let (graph, indices) = make_graph_with_nodes(&[4.0]);
        let sel = make_flat_selection(&indices);
        let expr = Parser::parse_expression("score * 2").unwrap();

        let results = evaluate_equation(&graph, &sel, &expr, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, Value::Float64(8.0));
    }

    #[test]
    fn test_evaluate_equation_empty_level() {
        let (_graph, _indices) = make_graph_with_nodes(&[]);
        let graph = DirGraph::new();
        let sel = make_flat_selection(&[]);
        let expr = Parser::parse_expression("score").unwrap();

        let results = evaluate_equation(&graph, &sel, &expr, None);
        assert!(results.is_empty());
    }

    #[test]
    fn test_evaluate_equation_invalid_level_index() {
        let (graph, indices) = make_graph_with_nodes(&[10.0]);
        let sel = make_flat_selection(&indices);
        let expr = Parser::parse_expression("score").unwrap();

        // Level 5 doesn't exist
        let results = evaluate_equation(&graph, &sel, &expr, Some(5));
        assert!(results.is_empty());
    }

    #[test]
    fn test_evaluate_equation_node_not_in_graph() {
        // Create a selection with a node index that doesn't exist in the graph
        let graph = DirGraph::new();
        let fake_idx = NodeIndex::new(999);
        let sel = make_flat_selection(&[fake_idx]);
        let expr = Parser::parse_expression("score").unwrap();

        let results = evaluate_equation(&graph, &sel, &expr, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, Value::Null);
        assert_eq!(results[0].error_msg, Some("Node not found".to_string()));
    }

    // ========================================================================
    // evaluate_equation — aggregation (sum, mean, etc.)
    // ========================================================================

    #[test]
    fn test_evaluate_equation_sum_aggregation() {
        let (graph, indices) = make_graph_with_nodes(&[10.0, 20.0, 30.0]);
        let sel = make_parent_child_selection(indices[0], &[indices[1], indices[2]]);
        let expr = Parser::parse_expression("sum(score)").unwrap();

        let results = evaluate_equation(&graph, &sel, &expr, None);
        assert_eq!(results.len(), 1);
        // sum of children scores: 20 + 30 = 50
        assert_eq!(results[0].value, Value::Float64(50.0));
        assert!(results[0].error_msg.is_none());
        assert_eq!(results[0].parent_idx, Some(indices[0]));
    }

    #[test]
    fn test_evaluate_equation_mean_aggregation() {
        let (graph, indices) = make_graph_with_nodes(&[0.0, 10.0, 20.0]);
        let sel = make_parent_child_selection(indices[0], &[indices[1], indices[2]]);
        let expr = Parser::parse_expression("mean(score)").unwrap();

        let results = evaluate_equation(&graph, &sel, &expr, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, Value::Float64(15.0));
    }

    #[test]
    fn test_evaluate_equation_aggregation_no_valid_children() {
        // Parent exists but children point to non-existent nodes
        let (graph, indices) = make_graph_with_nodes(&[10.0]);
        let fake_child = NodeIndex::new(999);
        let sel = make_parent_child_selection(indices[0], &[fake_child]);
        let expr = Parser::parse_expression("sum(score)").unwrap();

        let results = evaluate_equation(&graph, &sel, &expr, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, Value::Null);
        assert!(results[0].error_msg.is_some());
    }

    // ========================================================================
    // evaluate_connection_equation
    // ========================================================================

    #[test]
    fn test_evaluate_connection_equation_requires_two_levels() {
        let (graph, indices) = make_graph_with_nodes(&[10.0]);
        let sel = make_flat_selection(&indices);
        let expr = Parser::parse_expression("sum(weight)").unwrap();

        let results = evaluate_connection_equation(&graph, &sel, &expr, None);
        assert_eq!(results.len(), 1);
        assert!(results[0].error_msg.is_some());
        assert!(results[0]
            .error_msg
            .as_ref()
            .unwrap()
            .contains("at least 2 selection levels"));
    }

    #[test]
    fn test_evaluate_connection_equation_with_edge_properties() {
        let (mut graph, indices) = make_graph_with_nodes(&[0.0, 1.0, 2.0]);

        // Add edges with "weight" property from parent to children
        let mut edge_props1 = HashMap::new();
        edge_props1.insert("weight".to_string(), Value::Float64(5.0));
        let edge1 = EdgeData::new("has_child".to_string(), edge_props1, &mut graph.interner);
        graph.graph.add_edge(indices[0], indices[1], edge1);

        let mut edge_props2 = HashMap::new();
        edge_props2.insert("weight".to_string(), Value::Float64(3.0));
        let edge2 = EdgeData::new("has_child".to_string(), edge_props2, &mut graph.interner);
        graph.graph.add_edge(indices[0], indices[2], edge2);

        let sel = make_parent_child_selection(indices[0], &[indices[1], indices[2]]);
        let expr = Parser::parse_expression("sum(weight)").unwrap();

        let results = evaluate_connection_equation(&graph, &sel, &expr, None);
        assert_eq!(results.len(), 1);
        // sum of edge weights: 5.0 + 3.0 = 8.0
        assert_eq!(results[0].value, Value::Float64(8.0));
        assert!(results[0].error_msg.is_none());
        assert_eq!(results[0].parent_idx, Some(indices[0]));
    }

    #[test]
    fn test_evaluate_connection_equation_no_edges() {
        let (graph, indices) = make_graph_with_nodes(&[0.0, 1.0, 2.0]);
        // No edges between parent and children
        let sel = make_parent_child_selection(indices[0], &[indices[1], indices[2]]);
        let expr = Parser::parse_expression("sum(weight)").unwrap();

        let results = evaluate_connection_equation(&graph, &sel, &expr, None);
        assert_eq!(results.len(), 1);
        assert!(results[0].error_msg.is_some());
        assert!(results[0]
            .error_msg
            .as_ref()
            .unwrap()
            .contains("No connections found"));
    }

    #[test]
    fn test_evaluate_connection_equation_no_parent() {
        let (graph, indices) = make_graph_with_nodes(&[10.0, 20.0]);
        // Create a two-level selection but with None as parent
        let mut sel = CurrentSelection::new();
        sel.get_level_mut(0).unwrap().add_selection(None, vec![]);
        sel.add_level();
        sel.get_level_mut(1)
            .unwrap()
            .add_selection(None, vec![indices[0], indices[1]]);

        let expr = Parser::parse_expression("sum(weight)").unwrap();
        let results = evaluate_connection_equation(&graph, &sel, &expr, None);

        assert_eq!(results.len(), 1);
        assert!(results[0].error_msg.is_some());
        assert!(results[0]
            .error_msg
            .as_ref()
            .unwrap()
            .contains("No parent node"));
    }

    // ========================================================================
    // process_equation — error paths (these don't need PyO3)
    // ========================================================================

    #[test]
    fn test_process_equation_empty_expression() {
        let (mut graph, indices) = make_graph_with_nodes(&[10.0]);
        let sel = make_flat_selection(&indices);

        let result = process_equation(&mut graph, &sel, "", None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty"));
    }

    #[test]
    fn test_process_equation_unknown_aggregate() {
        let (mut graph, indices) = make_graph_with_nodes(&[10.0]);
        let sel = make_flat_selection(&indices);

        let result = process_equation(&mut graph, &sel, "median(score)", None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown aggregate function"));
    }

    #[test]
    fn test_process_equation_missing_closing_paren() {
        let (mut graph, indices) = make_graph_with_nodes(&[10.0]);
        let sel = make_flat_selection(&indices);

        let result = process_equation(&mut graph, &sel, "sum(score", None, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("parenthesis"));
    }

    #[test]
    fn test_process_equation_empty_selection() {
        let mut graph = DirGraph::new();
        let sel = CurrentSelection::new();
        // New selection has 1 level but 0 nodes

        let result = process_equation(&mut graph, &sel, "score", None, None, None);
        assert!(result.is_err());
        // Should mention no nodes
        let err = result.unwrap_err();
        assert!(
            err.contains("No nodes") || err.contains("no nodes"),
            "Expected error about no nodes, got: {}",
            err
        );
    }

    #[test]
    fn test_process_equation_computed_result() {
        let (mut graph, indices) = make_graph_with_nodes(&[10.0, 20.0]);
        let sel = make_flat_selection(&indices);

        // No store_as -> should return Computed variant
        let result = process_equation(&mut graph, &sel, "score + 1", None, None, None);
        assert!(result.is_ok());
        match result.unwrap() {
            EvaluationResult::Computed(results) => {
                assert_eq!(results.len(), 2);
                let mut values: Vec<f64> = results
                    .iter()
                    .filter_map(|r| match &r.value {
                        Value::Float64(f) => Some(*f),
                        _ => None,
                    })
                    .collect();
                values.sort_by(|a, b| a.partial_cmp(b).unwrap());
                assert_eq!(values, vec![11.0, 21.0]);
            }
            EvaluationResult::Stored(_) => panic!("Expected Computed variant"),
        }
    }

    #[test]
    fn test_process_equation_stored_result() {
        let (mut graph, indices) = make_graph_with_nodes(&[10.0, 20.0]);
        let sel = make_flat_selection(&indices);

        // With store_as -> should return Stored variant
        let result = process_equation(
            &mut graph,
            &sel,
            "score * 2",
            None,
            Some("doubled_score"),
            None,
        );
        assert!(result.is_ok());
        match result.unwrap() {
            EvaluationResult::Stored(report) => {
                assert_eq!(report.operation_type, "process_equation");
            }
            EvaluationResult::Computed(_) => panic!("Expected Stored variant"),
        }

        // Verify the property was actually stored on the nodes
        for &idx in &indices {
            let node = graph.get_node(idx).unwrap();
            assert!(node.has_property("doubled_score"));
        }
    }

    #[test]
    fn test_process_equation_likely_aggregate_without_parens() {
        let (mut graph, indices) = make_graph_with_nodes(&[10.0]);
        let sel = make_flat_selection(&indices);

        let result = process_equation(&mut graph, &sel, "sum", None, None, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("requires parentheses"), "Got: {}", err);
    }

    #[test]
    fn test_process_equation_invalid_level_index() {
        let (mut graph, indices) = make_graph_with_nodes(&[10.0]);
        let sel = make_flat_selection(&indices);

        let result = process_equation(&mut graph, &sel, "score", Some(99), None, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("Invalid level index") || err.contains("No nodes found"),
            "Got: {}",
            err
        );
    }

    // ========================================================================
    // store_count_results
    // ========================================================================

    #[test]
    fn test_store_count_results_flat() {
        let (mut graph, indices) = make_graph_with_nodes(&[10.0, 20.0, 30.0]);
        let sel = make_flat_selection(&indices);

        let result = store_count_results(&mut graph, &sel, None, false, "node_count");
        assert!(result.is_ok());

        let report = result.unwrap();
        assert_eq!(report.operation_type, "count");

        // Each node should now have node_count = 3
        for &idx in &indices {
            let node = graph.get_node(idx).unwrap();
            assert!(node.has_property("node_count"));
        }
    }

    #[test]
    fn test_store_count_results_grouped() {
        let (mut graph, indices) = make_graph_with_nodes(&[0.0, 1.0, 2.0]);
        let sel = make_parent_child_selection(indices[0], &[indices[1], indices[2]]);

        let result = store_count_results(&mut graph, &sel, None, true, "child_count");
        assert!(result.is_ok());

        // The parent node should have child_count = 2
        let parent = graph.get_node(indices[0]).unwrap();
        assert!(parent.has_property("child_count"));
    }

    #[test]
    fn test_store_count_results_empty_selection() {
        let mut graph = DirGraph::new();
        let sel = make_flat_selection(&[]);

        let result = store_count_results(&mut graph, &sel, None, false, "count");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No valid nodes"));
    }
}
