// src/graph/export.rs
//! Export graph data to various visualization formats

use crate::datatypes::values::Value;
use crate::graph::schema::{CurrentSelection, DirGraph};
use petgraph::visit::EdgeRef;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

/// Export the graph (or selection) to GraphML format.
///
/// GraphML is an XML-based format supported by many graph visualization tools
/// including Gephi, yEd, and Cytoscape.
pub fn to_graphml(
    graph: &DirGraph,
    selection: Option<&CurrentSelection>,
) -> Result<String, String> {
    let mut xml = String::with_capacity(64 * 1024); // Pre-allocate 64KB

    // XML header
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n");
    xml.push_str("         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n");
    xml.push_str("         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns\n");
    xml.push_str("         http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n");

    // Define attribute keys for nodes
    xml.push_str(
        "  <key id=\"node_type\" for=\"node\" attr.name=\"type\" attr.type=\"string\"/>\n",
    );
    xml.push_str(
        "  <key id=\"node_title\" for=\"node\" attr.name=\"title\" attr.type=\"string\"/>\n",
    );
    xml.push_str("  <key id=\"node_id\" for=\"node\" attr.name=\"id\" attr.type=\"string\"/>\n");
    xml.push_str("  <key id=\"node_properties\" for=\"node\" attr.name=\"properties\" attr.type=\"string\"/>\n");

    // Define attribute keys for edges
    xml.push_str("  <key id=\"edge_type\" for=\"edge\" attr.name=\"connection_type\" attr.type=\"string\"/>\n");
    xml.push_str("  <key id=\"edge_properties\" for=\"edge\" attr.name=\"properties\" attr.type=\"string\"/>\n");

    xml.push_str("  <graph id=\"G\" edgedefault=\"directed\">\n");

    // Determine which nodes to export
    let node_indices: Vec<_> = if let Some(sel) = selection {
        let level_idx = sel.get_level_count().saturating_sub(1);
        if let Some(level) = sel.get_level(level_idx) {
            level.get_all_nodes()
        } else {
            graph.graph.node_indices().collect()
        }
    } else {
        graph.graph.node_indices().collect()
    };

    let node_set: std::collections::HashSet<_> = node_indices.iter().copied().collect();

    // Export nodes
    for &idx in &node_indices {
        if let Some(node) = graph.graph.node_weight(idx) {
            xml.push_str(&format!("    <node id=\"n{}\">\n", idx.index()));
            xml.push_str(&format!(
                "      <data key=\"node_type\">{}</data>\n",
                escape_xml(&node.node_type)
            ));
            xml.push_str(&format!(
                "      <data key=\"node_title\">{}</data>\n",
                escape_xml(&value_to_string(&node.title))
            ));
            xml.push_str(&format!(
                "      <data key=\"node_id\">{}</data>\n",
                escape_xml(&value_to_string(&node.id))
            ));

            // Serialize properties as JSON
            if node.property_count() > 0 {
                let props_json = properties_to_json(node.property_iter(&graph.interner));
                xml.push_str(&format!(
                    "      <data key=\"node_properties\">{}</data>\n",
                    escape_xml(&props_json)
                ));
            }

            xml.push_str("    </node>\n");
        }
    }

    // Export edges (only between selected nodes)
    let mut edge_id = 0;
    for &source_idx in &node_indices {
        for edge in graph.graph.edges(source_idx) {
            let target_idx = edge.target();

            // Only include edge if target is in selection
            if node_set.contains(&target_idx) {
                xml.push_str(&format!(
                    "    <edge id=\"e{}\" source=\"n{}\" target=\"n{}\">\n",
                    edge_id,
                    source_idx.index(),
                    target_idx.index()
                ));
                xml.push_str(&format!(
                    "      <data key=\"edge_type\">{}</data>\n",
                    escape_xml(edge.weight().connection_type_str(&graph.interner))
                ));

                if edge.weight().property_count() > 0 {
                    let props_json =
                        properties_to_json(edge.weight().property_iter(&graph.interner));
                    xml.push_str(&format!(
                        "      <data key=\"edge_properties\">{}</data>\n",
                        escape_xml(&props_json)
                    ));
                }

                xml.push_str("    </edge>\n");
                edge_id += 1;
            }
        }
    }

    xml.push_str("  </graph>\n");
    xml.push_str("</graphml>\n");

    Ok(xml)
}

/// Export the graph (or selection) to D3.js compatible JSON format.
///
/// This format is designed for use with D3.js force-directed graph visualizations.
/// The output is a JSON object with "nodes" and "links" arrays.
pub fn to_d3_json(
    graph: &DirGraph,
    selection: Option<&CurrentSelection>,
) -> Result<String, String> {
    // Determine which nodes to export
    let node_indices: Vec<_> = if let Some(sel) = selection {
        let level_idx = sel.get_level_count().saturating_sub(1);
        if let Some(level) = sel.get_level(level_idx) {
            level.get_all_nodes()
        } else {
            graph.graph.node_indices().collect()
        }
    } else {
        graph.graph.node_indices().collect()
    };

    let node_set: std::collections::HashSet<_> = node_indices.iter().copied().collect();

    // Build index mapping (old index -> array position)
    let mut index_map: HashMap<usize, usize> = HashMap::with_capacity(node_indices.len());
    for (pos, &idx) in node_indices.iter().enumerate() {
        index_map.insert(idx.index(), pos);
    }

    // Build nodes array
    let mut nodes_json = Vec::with_capacity(node_indices.len());
    for &idx in &node_indices {
        if let Some(node) = graph.graph.node_weight(idx) {
            let mut obj = String::from("{");
            obj.push_str(&format!("\"id\":{},", json_value(&node.id)));
            obj.push_str(&format!("\"type\":{},", json_string(&node.node_type)));
            obj.push_str(&format!("\"title\":{}", json_value(&node.title)));

            // Add select properties (not all to keep output clean)
            for (key, value) in node.property_iter(&graph.interner) {
                if key != "id" && key != "title" && key != "type" {
                    obj.push_str(&format!(",{}:{}", json_string(key), json_value(value)));
                }
            }

            obj.push('}');
            nodes_json.push(obj);
        }
    }

    // Build links array
    let mut links_json = Vec::new();
    for &source_idx in &node_indices {
        for edge in graph.graph.edges(source_idx) {
            let target_idx = edge.target();

            if node_set.contains(&target_idx) {
                if let (Some(&source_pos), Some(&target_pos)) = (
                    index_map.get(&source_idx.index()),
                    index_map.get(&target_idx.index()),
                ) {
                    let mut link = String::from("{");
                    link.push_str(&format!("\"source\":{},", source_pos));
                    link.push_str(&format!("\"target\":{},", target_pos));
                    link.push_str(&format!(
                        "\"type\":{}",
                        json_string(edge.weight().connection_type_str(&graph.interner))
                    ));

                    // Add edge properties
                    for (key, value) in edge.weight().property_iter(&graph.interner) {
                        link.push_str(&format!(",{}:{}", json_string(key), json_value(value)));
                    }

                    link.push('}');
                    links_json.push(link);
                }
            }
        }
    }

    // Build final JSON
    let mut result = String::with_capacity(32 * 1024);
    result.push_str("{\n  \"nodes\": [\n    ");
    result.push_str(&nodes_json.join(",\n    "));
    result.push_str("\n  ],\n  \"links\": [\n    ");
    result.push_str(&links_json.join(",\n    "));
    result.push_str("\n  ]\n}");

    Ok(result)
}

/// Export to GEXF format (Gephi native format).
///
/// GEXF is the native format for Gephi and supports dynamic graphs,
/// hierarchies, and rich attribute types.
pub fn to_gexf(graph: &DirGraph, selection: Option<&CurrentSelection>) -> Result<String, String> {
    let mut xml = String::with_capacity(64 * 1024);

    // XML header
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<gexf xmlns=\"http://www.gexf.net/1.2draft\"\n");
    xml.push_str("      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n");
    xml.push_str("      xsi:schemaLocation=\"http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd\"\n");
    xml.push_str("      version=\"1.2\">\n");
    xml.push_str("  <meta>\n");
    xml.push_str("    <creator>kglite</creator>\n");
    xml.push_str("    <description>Exported from KnowledgeGraph</description>\n");
    xml.push_str("  </meta>\n");
    xml.push_str("  <graph mode=\"static\" defaultedgetype=\"directed\">\n");

    // Define node attributes
    xml.push_str("    <attributes class=\"node\">\n");
    xml.push_str("      <attribute id=\"0\" title=\"type\" type=\"string\"/>\n");
    xml.push_str("      <attribute id=\"1\" title=\"title\" type=\"string\"/>\n");
    xml.push_str("    </attributes>\n");

    // Define edge attributes
    xml.push_str("    <attributes class=\"edge\">\n");
    xml.push_str("      <attribute id=\"0\" title=\"connection_type\" type=\"string\"/>\n");
    xml.push_str("    </attributes>\n");

    // Determine which nodes to export
    let node_indices: Vec<_> = if let Some(sel) = selection {
        let level_idx = sel.get_level_count().saturating_sub(1);
        if let Some(level) = sel.get_level(level_idx) {
            level.get_all_nodes()
        } else {
            graph.graph.node_indices().collect()
        }
    } else {
        graph.graph.node_indices().collect()
    };

    let node_set: std::collections::HashSet<_> = node_indices.iter().copied().collect();

    // Export nodes
    xml.push_str("    <nodes>\n");
    for &idx in &node_indices {
        if let Some(node) = graph.graph.node_weight(idx) {
            let title_str = value_to_string(&node.title);
            xml.push_str(&format!(
                "      <node id=\"{}\" label=\"{}\">\n",
                idx.index(),
                escape_xml(&title_str)
            ));
            xml.push_str("        <attvalues>\n");
            xml.push_str(&format!(
                "          <attvalue for=\"0\" value=\"{}\"/>\n",
                escape_xml(&node.node_type)
            ));
            xml.push_str(&format!(
                "          <attvalue for=\"1\" value=\"{}\"/>\n",
                escape_xml(&title_str)
            ));
            xml.push_str("        </attvalues>\n");
            xml.push_str("      </node>\n");
        }
    }
    xml.push_str("    </nodes>\n");

    // Export edges
    xml.push_str("    <edges>\n");
    let mut edge_id = 0;
    for &source_idx in &node_indices {
        for edge in graph.graph.edges(source_idx) {
            let target_idx = edge.target();

            if node_set.contains(&target_idx) {
                xml.push_str(&format!(
                    "      <edge id=\"{}\" source=\"{}\" target=\"{}\">\n",
                    edge_id,
                    source_idx.index(),
                    target_idx.index()
                ));
                xml.push_str("        <attvalues>\n");
                xml.push_str(&format!(
                    "          <attvalue for=\"0\" value=\"{}\"/>\n",
                    escape_xml(edge.weight().connection_type_str(&graph.interner))
                ));
                xml.push_str("        </attvalues>\n");
                xml.push_str("      </edge>\n");
                edge_id += 1;
            }
        }
    }
    xml.push_str("    </edges>\n");

    xml.push_str("  </graph>\n");
    xml.push_str("</gexf>\n");

    Ok(xml)
}

/// Export to CSV format (nodes and edges as separate content).
///
/// Returns a tuple of (nodes_csv, edges_csv).
pub fn to_csv(
    graph: &DirGraph,
    selection: Option<&CurrentSelection>,
) -> Result<(String, String), String> {
    // Determine which nodes to export
    let node_indices: Vec<_> = if let Some(sel) = selection {
        let level_idx = sel.get_level_count().saturating_sub(1);
        if let Some(level) = sel.get_level(level_idx) {
            level.get_all_nodes()
        } else {
            graph.graph.node_indices().collect()
        }
    } else {
        graph.graph.node_indices().collect()
    };

    let node_set: std::collections::HashSet<_> = node_indices.iter().copied().collect();

    // Build nodes CSV
    let mut nodes_csv = String::from("id,type,title\n");
    for &idx in &node_indices {
        if let Some(node) = graph.graph.node_weight(idx) {
            nodes_csv.push_str(&format!(
                "{},{},{}\n",
                idx.index(),
                escape_csv(&node.node_type),
                escape_csv(&value_to_string(&node.title))
            ));
        }
    }

    // Build edges CSV
    let mut edges_csv = String::from("source,target,type\n");
    for &source_idx in &node_indices {
        for edge in graph.graph.edges(source_idx) {
            let target_idx = edge.target();

            if node_set.contains(&target_idx) {
                edges_csv.push_str(&format!(
                    "{},{},{}\n",
                    source_idx.index(),
                    target_idx.index(),
                    escape_csv(edge.weight().connection_type_str(&graph.interner))
                ));
            }
        }
    }

    Ok((nodes_csv, edges_csv))
}

/// Metadata for a single connection type in the blueprint.
/// (source_type, target_type, property_columns, property_type_map)
type ConnMeta = (String, String, Vec<String>, BTreeMap<String, String>);

/// Summary of a CSV directory export.
pub struct ExportSummary {
    /// Output directory path.
    pub output_dir: String,
    /// Node counts per type: type_name → row count.
    pub nodes: BTreeMap<String, usize>,
    /// Connection counts per type: connection_type → row count.
    pub connections: BTreeMap<String, usize>,
    /// Total files written (CSVs + blueprint.json).
    pub files_written: usize,
    /// Log lines for verbose output.
    pub log_lines: Vec<String>,
}

/// Export the graph (or selection) to an organized CSV directory tree.
///
/// Creates:
/// - `nodes/<Type>.csv` for each node type (sub-nodes nested under parent)
/// - `connections/<Type>.csv` for each connection type
/// - `blueprint.json` for round-trip re-import via `from_blueprint()`
pub fn to_csv_dir(
    graph: &DirGraph,
    output_dir: &str,
    selection: Option<&CurrentSelection>,
    parent_types: &HashMap<String, String>,
) -> Result<ExportSummary, String> {
    let output = Path::new(output_dir);
    let mut log_lines = Vec::new();
    log_lines.push(format!("Exporting to {}...", output_dir));

    // ── 1. Collect selected node indices ─────────────────────────
    let node_indices = selected_node_indices(graph, selection);
    let node_set: HashSet<_> = node_indices.iter().copied().collect();

    // ── 2. Group nodes by type ───────────────────────────────────
    // type_name → Vec<NodeIndex>
    let mut nodes_by_type: BTreeMap<String, Vec<petgraph::graph::NodeIndex>> = BTreeMap::new();
    for &idx in &node_indices {
        if let Some(node) = graph.graph.node_weight(idx) {
            nodes_by_type
                .entry(node.node_type.clone())
                .or_default()
                .push(idx);
        }
    }

    // ── 3. Collect edges between selected nodes, grouped by type ─
    // conn_type → Vec<(source_idx, target_idx, edge_ref)>
    struct EdgeInfo {
        source_idx: petgraph::graph::NodeIndex,
        target_idx: petgraph::graph::NodeIndex,
        properties: HashMap<String, Value>,
    }
    let mut edges_by_type: BTreeMap<String, Vec<EdgeInfo>> = BTreeMap::new();
    for &source_idx in &node_indices {
        for edge in graph.graph.edges(source_idx) {
            let target_idx = edge.target();
            if node_set.contains(&target_idx) {
                let w = edge.weight();
                edges_by_type
                    .entry(w.connection_type_str(&graph.interner).to_string())
                    .or_default()
                    .push(EdgeInfo {
                        source_idx,
                        target_idx,
                        properties: w.properties_cloned(&graph.interner),
                    });
            }
        }
    }

    // ── 4. Create directories ────────────────────────────────────
    let nodes_dir = output.join("nodes");
    let connections_dir = output.join("connections");
    std::fs::create_dir_all(&nodes_dir)
        .map_err(|e| format!("Failed to create nodes directory: {}", e))?;
    if !edges_by_type.is_empty() {
        std::fs::create_dir_all(&connections_dir)
            .map_err(|e| format!("Failed to create connections directory: {}", e))?;
    }

    // Create sub-node directories
    for parent in parent_types.values() {
        if nodes_by_type.contains_key(parent) {
            let sub_dir = nodes_dir.join(parent);
            std::fs::create_dir_all(&sub_dir)
                .map_err(|e| format!("Failed to create sub-node directory: {}", e))?;
        }
    }

    let mut summary = ExportSummary {
        output_dir: output_dir.to_string(),
        nodes: BTreeMap::new(),
        connections: BTreeMap::new(),
        files_written: 0,
        log_lines: Vec::new(),
    };

    // ── 5. Write node CSVs ───────────────────────────────────────
    // Track property columns and types per node type for the blueprint
    let mut node_type_columns: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut node_type_prop_types: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();

    for (node_type, indices) in &nodes_by_type {
        // First pass: collect the union of all property names
        let mut prop_names: BTreeSet<String> = BTreeSet::new();
        for &idx in indices {
            if let Some(node) = graph.graph.node_weight(idx) {
                for key in node.property_keys(&graph.interner) {
                    prop_names.insert(key.to_string());
                }
            }
        }
        let prop_cols: Vec<String> = prop_names.into_iter().collect();

        // Infer property types from first non-null value of each property
        let mut prop_types: BTreeMap<String, String> = BTreeMap::new();
        for col in &prop_cols {
            for &idx in indices {
                if let Some(node) = graph.graph.node_weight(idx) {
                    if let Some(val) = node.get_property(col) {
                        if !matches!(*val, Value::Null) {
                            prop_types.insert(col.clone(), value_type_name(&val));
                            break;
                        }
                    }
                }
            }
        }

        // Build CSV
        let mut csv = String::with_capacity(4096);
        // Header: id, title, then properties
        csv.push_str("id,title");
        for col in &prop_cols {
            csv.push(',');
            csv.push_str(&escape_csv(col));
        }
        csv.push('\n');

        // Rows
        for &idx in indices {
            if let Some(node) = graph.graph.node_weight(idx) {
                csv.push_str(&escape_csv(&value_to_string(&node.id)));
                csv.push(',');
                csv.push_str(&escape_csv(&value_to_string(&node.title)));
                for col in &prop_cols {
                    csv.push(',');
                    if let Some(val) = node.get_property(col) {
                        csv.push_str(&escape_csv(&value_to_string(&val)));
                    }
                }
                csv.push('\n');
            }
        }

        // Determine file path (nested under parent if sub-node)
        let csv_path = if let Some(parent) = parent_types.get(node_type) {
            nodes_dir.join(parent).join(format!("{}.csv", node_type))
        } else {
            nodes_dir.join(format!("{}.csv", node_type))
        };

        let relative_path = csv_path
            .strip_prefix(output)
            .unwrap_or(&csv_path)
            .to_string_lossy()
            .to_string();

        std::fs::write(&csv_path, &csv)
            .map_err(|e| format!("Failed to write {}: {}", relative_path, e))?;

        log_lines.push(format!(
            "  {}: {} nodes, {} properties",
            relative_path,
            indices.len(),
            prop_cols.len()
        ));

        summary.nodes.insert(node_type.clone(), indices.len());
        summary.files_written += 1;
        node_type_columns.insert(node_type.clone(), prop_cols);
        node_type_prop_types.insert(node_type.clone(), prop_types);
    }

    // ── 6. Write connection CSVs ─────────────────────────────────
    // Track connection metadata for blueprint
    let mut conn_meta: BTreeMap<String, ConnMeta> = BTreeMap::new(); // conn_type -> (source_type, target_type, prop_cols, prop_types)

    for (conn_type, edges) in &edges_by_type {
        // Collect property names across all edges of this type
        let mut prop_names: BTreeSet<String> = BTreeSet::new();
        for edge in edges {
            for key in edge.properties.keys() {
                prop_names.insert(key.clone());
            }
        }
        let prop_cols: Vec<String> = prop_names.into_iter().collect();

        // Infer property types
        let mut prop_types: BTreeMap<String, String> = BTreeMap::new();
        for col in &prop_cols {
            for edge in edges {
                if let Some(val) = edge.properties.get(col) {
                    if !matches!(val, Value::Null) {
                        prop_types.insert(col.clone(), value_type_name(val));
                        break;
                    }
                }
            }
        }

        // Detect source and target types (use first edge)
        let source_type = edges
            .first()
            .and_then(|e| graph.graph.node_weight(e.source_idx))
            .map(|n| n.node_type.clone())
            .unwrap_or_default();
        let target_type = edges
            .first()
            .and_then(|e| graph.graph.node_weight(e.target_idx))
            .map(|n| n.node_type.clone())
            .unwrap_or_default();

        // Build CSV
        let mut csv = String::with_capacity(4096);
        csv.push_str("source_id,source_type,target_id,target_type");
        for col in &prop_cols {
            csv.push(',');
            csv.push_str(&escape_csv(col));
        }
        csv.push('\n');

        for edge in edges {
            let source_id = graph
                .graph
                .node_weight(edge.source_idx)
                .map(|n| value_to_string(&n.id))
                .unwrap_or_default();
            let src_type = graph
                .graph
                .node_weight(edge.source_idx)
                .map(|n| n.node_type.clone())
                .unwrap_or_default();
            let target_id = graph
                .graph
                .node_weight(edge.target_idx)
                .map(|n| value_to_string(&n.id))
                .unwrap_or_default();
            let tgt_type = graph
                .graph
                .node_weight(edge.target_idx)
                .map(|n| n.node_type.clone())
                .unwrap_or_default();

            csv.push_str(&escape_csv(&source_id));
            csv.push(',');
            csv.push_str(&escape_csv(&src_type));
            csv.push(',');
            csv.push_str(&escape_csv(&target_id));
            csv.push(',');
            csv.push_str(&escape_csv(&tgt_type));
            for col in &prop_cols {
                csv.push(',');
                if let Some(val) = edge.properties.get(col) {
                    csv.push_str(&escape_csv(&value_to_string(val)));
                }
            }
            csv.push('\n');
        }

        let csv_path = connections_dir.join(format!("{}.csv", conn_type));
        let relative_path = csv_path
            .strip_prefix(output)
            .unwrap_or(&csv_path)
            .to_string_lossy()
            .to_string();

        std::fs::write(&csv_path, &csv)
            .map_err(|e| format!("Failed to write {}: {}", relative_path, e))?;

        log_lines.push(format!("  {}: {} edges", relative_path, edges.len()));

        summary.connections.insert(conn_type.clone(), edges.len());
        summary.files_written += 1;
        conn_meta.insert(
            conn_type.clone(),
            (source_type, target_type, prop_cols, prop_types),
        );
    }

    // ── 7. Generate blueprint.json ───────────────────────────────
    let blueprint = build_blueprint(
        &nodes_by_type,
        &node_type_columns,
        &node_type_prop_types,
        parent_types,
        &conn_meta,
        output,
    );
    let blueprint_path = output.join("blueprint.json");
    std::fs::write(&blueprint_path, &blueprint)
        .map_err(|e| format!("Failed to write blueprint.json: {}", e))?;
    log_lines.push("  blueprint.json".to_string());
    summary.files_written += 1;

    // Summary line
    let total_nodes: usize = summary.nodes.values().sum();
    let total_edges: usize = summary.connections.values().sum();
    log_lines.push(format!(
        "Done: {} nodes, {} edges, {} files written",
        total_nodes, total_edges, summary.files_written
    ));

    summary.log_lines = log_lines;
    Ok(summary)
}

/// Extract the selected node indices (or all nodes if no selection).
fn selected_node_indices(
    graph: &DirGraph,
    selection: Option<&CurrentSelection>,
) -> Vec<petgraph::graph::NodeIndex> {
    if let Some(sel) = selection {
        let level_idx = sel.get_level_count().saturating_sub(1);
        if let Some(level) = sel.get_level(level_idx) {
            level.get_all_nodes()
        } else {
            graph.graph.node_indices().collect()
        }
    } else {
        graph.graph.node_indices().collect()
    }
}

/// Map a Value variant to a blueprint property type string.
fn value_type_name(value: &Value) -> String {
    match value {
        Value::String(_) => "string".to_string(),
        Value::Int64(_) => "int".to_string(),
        Value::Float64(_) => "float".to_string(),
        Value::Boolean(_) => "bool".to_string(),
        Value::DateTime(_) => "date".to_string(),
        Value::UniqueId(_) => "int".to_string(),
        Value::Point { .. } => "string".to_string(), // serialized as string in CSV
        Value::Null => "string".to_string(),
        Value::NodeRef(_) | Value::EdgeRef { .. } => "int".to_string(),
    }
}

/// Build a blueprint.json string for round-trip re-import.
fn build_blueprint(
    nodes_by_type: &BTreeMap<String, Vec<petgraph::graph::NodeIndex>>,
    _node_type_columns: &BTreeMap<String, Vec<String>>,
    node_type_prop_types: &BTreeMap<String, BTreeMap<String, String>>,
    parent_types: &HashMap<String, String>,
    conn_meta: &BTreeMap<String, ConnMeta>,
    _output_dir: &Path,
) -> String {
    let mut json = String::with_capacity(4096);
    json.push_str("{\n  \"settings\": {\n    \"root\": \".\"\n  },\n  \"nodes\": {");

    let mut first_node = true;
    for node_type in nodes_by_type.keys() {
        if !first_node {
            json.push(',');
        }
        first_node = false;

        // Determine CSV path relative to output_dir
        let csv_rel = if let Some(parent) = parent_types.get(node_type) {
            format!("nodes/{}/{}.csv", parent, node_type)
        } else {
            format!("nodes/{}.csv", node_type)
        };

        json.push_str(&format!("\n    {}: {{\n", json_string(node_type)));
        json.push_str(&format!("      \"csv\": {},\n", json_string(&csv_rel)));
        json.push_str("      \"pk\": \"id\",\n");
        json.push_str("      \"title\": \"title\"");

        // Properties
        if let Some(prop_types) = node_type_prop_types.get(node_type) {
            if !prop_types.is_empty() {
                json.push_str(",\n      \"properties\": {");
                let mut first_prop = true;
                for (col, typ) in prop_types {
                    if !first_prop {
                        json.push(',');
                    }
                    first_prop = false;
                    json.push_str(&format!(
                        "\n        {}: {}",
                        json_string(col),
                        json_string(typ)
                    ));
                }
                json.push_str("\n      }");
            }
        }

        // Sub-node parent reference
        if let Some(parent) = parent_types.get(node_type) {
            json.push_str(&format!(",\n      \"parent\": {}", json_string(parent)));
        }

        // Connection definitions for this node type
        let node_conns: Vec<_> = conn_meta
            .iter()
            .filter(|(_, (src, _, _, _))| src == node_type)
            .collect();

        if !node_conns.is_empty() {
            json.push_str(",\n      \"connections\": {\n        \"junction_edges\": {");
            let mut first_conn = true;
            for (conn_type, (_, target_type, prop_cols, _prop_types)) in &node_conns {
                if !first_conn {
                    json.push(',');
                }
                first_conn = false;
                let conn_csv = format!("connections/{}.csv", conn_type);
                json.push_str(&format!(
                    "\n          {}: {{\n            \"csv\": {},\n            \"source_fk\": \"source_id\",\n            \"target\": {},\n            \"target_fk\": \"target_id\"",
                    json_string(conn_type),
                    json_string(&conn_csv),
                    json_string(target_type)
                ));
                // Edge properties (exclude the 4 standard columns)
                if !prop_cols.is_empty() {
                    json.push_str(",\n            \"properties\": [");
                    let mut first_p = true;
                    for p in prop_cols.iter() {
                        if !first_p {
                            json.push_str(", ");
                        }
                        first_p = false;
                        json.push_str(&json_string(p));
                    }
                    json.push(']');
                }
                json.push_str("\n          }");
            }
            json.push_str("\n        }\n      }");
        }

        json.push_str("\n    }");
    }

    json.push_str("\n  }\n}\n");

    // Pretty-format via serde_json for consistency (the string is already valid JSON)
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&json) {
        if let Ok(pretty) = serde_json::to_string_pretty(&parsed) {
            return pretty;
        }
    }
    json
}

// Helper functions

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::UniqueId(id) => id.to_string(),
        Value::Point { lat, lon } => format!("point({}, {})", lat, lon),
        Value::Null => String::new(),
        Value::NodeRef(idx) => format!("node#{}", idx),
        Value::EdgeRef { edge_idx, .. } => format!("edge#{}", edge_idx),
    }
}

fn json_string(s: &str) -> String {
    format!(
        "\"{}\"",
        s.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    )
}

fn json_value(value: &Value) -> String {
    match value {
        Value::String(s) => json_string(s),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => {
            if f.is_nan() || f.is_infinite() {
                "null".to_string()
            } else {
                f.to_string()
            }
        }
        Value::Boolean(b) => b.to_string(),
        Value::DateTime(dt) => json_string(&dt.to_string()),
        Value::UniqueId(id) => id.to_string(),
        Value::Point { lat, lon } => format!("{{\"lat\":{},\"lon\":{}}}", lat, lon),
        Value::Null => "null".to_string(),
        Value::NodeRef(idx) => idx.to_string(),
        Value::EdgeRef { edge_idx, .. } => edge_idx.to_string(),
    }
}

fn properties_to_json<'a>(properties: impl Iterator<Item = (&'a str, &'a Value)>) -> String {
    let pairs: Vec<String> = properties
        .map(|(k, v)| format!("{}:{}", json_string(k), json_value(v)))
        .collect();
    format!("{{{}}}", pairs.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::values::Value;
    use crate::graph::schema::{DirGraph, EdgeData, NodeData};

    use std::collections::HashMap;

    // ========================================================================
    // Test helpers
    // ========================================================================

    /// Build an empty graph.
    fn empty_graph() -> DirGraph {
        DirGraph::new()
    }

    /// Build a simple graph with two Person nodes and a KNOWS edge.
    fn simple_graph() -> DirGraph {
        let mut g = DirGraph::new();
        let n1 = NodeData::new(
            Value::String("alice".to_string()),
            Value::String("Alice".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let n2 = NodeData::new(
            Value::String("bob".to_string()),
            Value::String("Bob".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let idx1 = g.graph.add_node(n1);
        let idx2 = g.graph.add_node(n2);
        g.type_indices
            .entry("Person".to_string())
            .or_default()
            .extend([idx1, idx2]);
        g.graph.add_edge(
            idx1,
            idx2,
            EdgeData::new("KNOWS".to_string(), HashMap::new(), &mut g.interner),
        );
        g
    }

    /// Build a graph with properties on nodes and edges.
    fn graph_with_properties() -> DirGraph {
        let mut g = DirGraph::new();
        let mut props1 = HashMap::new();
        props1.insert("age".to_string(), Value::Int64(30));
        props1.insert("active".to_string(), Value::Boolean(true));
        let n1 = NodeData::new(
            Value::Int64(1),
            Value::String("Alice".to_string()),
            "Person".to_string(),
            props1,
            &mut g.interner,
        );

        let mut props2 = HashMap::new();
        props2.insert("age".to_string(), Value::Int64(25));
        let n2 = NodeData::new(
            Value::Int64(2),
            Value::String("Bob".to_string()),
            "Person".to_string(),
            props2,
            &mut g.interner,
        );

        let idx1 = g.graph.add_node(n1);
        let idx2 = g.graph.add_node(n2);
        g.type_indices
            .entry("Person".to_string())
            .or_default()
            .extend([idx1, idx2]);

        let mut edge_props = HashMap::new();
        edge_props.insert("since".to_string(), Value::Int64(2020));
        g.graph.add_edge(
            idx1,
            idx2,
            EdgeData::new("KNOWS".to_string(), edge_props, &mut g.interner),
        );
        g
    }

    /// Build a graph with multiple node types for CSV dir export tests.
    fn multi_type_graph() -> DirGraph {
        let mut g = DirGraph::new();
        let n1 = NodeData::new(
            Value::String("alice".to_string()),
            Value::String("Alice".to_string()),
            "Person".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let n2 = NodeData::new(
            Value::String("acme".to_string()),
            Value::String("Acme Corp".to_string()),
            "Company".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let idx1 = g.graph.add_node(n1);
        let idx2 = g.graph.add_node(n2);
        g.type_indices
            .entry("Person".to_string())
            .or_default()
            .push(idx1);
        g.type_indices
            .entry("Company".to_string())
            .or_default()
            .push(idx2);
        g.graph.add_edge(
            idx1,
            idx2,
            EdgeData::new("WORKS_AT".to_string(), HashMap::new(), &mut g.interner),
        );
        g
    }

    // ========================================================================
    // escape_xml
    // ========================================================================

    #[test]
    fn test_escape_xml_no_special_chars() {
        assert_eq!(escape_xml("hello world"), "hello world");
    }

    #[test]
    fn test_escape_xml_ampersand() {
        assert_eq!(escape_xml("A&B"), "A&amp;B");
    }

    #[test]
    fn test_escape_xml_all_special() {
        assert_eq!(
            escape_xml("<tag attr=\"val\" & 'q'>"),
            "&lt;tag attr=&quot;val&quot; &amp; &apos;q&apos;&gt;"
        );
    }

    #[test]
    fn test_escape_xml_empty() {
        assert_eq!(escape_xml(""), "");
    }

    // ========================================================================
    // escape_csv
    // ========================================================================

    #[test]
    fn test_escape_csv_plain() {
        assert_eq!(escape_csv("hello"), "hello");
    }

    #[test]
    fn test_escape_csv_with_comma() {
        assert_eq!(escape_csv("a,b"), "\"a,b\"");
    }

    #[test]
    fn test_escape_csv_with_quotes() {
        assert_eq!(escape_csv("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn test_escape_csv_with_newline() {
        assert_eq!(escape_csv("line1\nline2"), "\"line1\nline2\"");
    }

    #[test]
    fn test_escape_csv_empty() {
        assert_eq!(escape_csv(""), "");
    }

    // ========================================================================
    // value_to_string
    // ========================================================================

    #[test]
    fn test_value_to_string_string() {
        assert_eq!(
            value_to_string(&Value::String("hello".to_string())),
            "hello"
        );
    }

    #[test]
    fn test_value_to_string_int() {
        assert_eq!(value_to_string(&Value::Int64(42)), "42");
    }

    #[test]
    fn test_value_to_string_float() {
        assert_eq!(value_to_string(&Value::Float64(3.14)), "3.14");
    }

    #[test]
    fn test_value_to_string_bool() {
        assert_eq!(value_to_string(&Value::Boolean(true)), "true");
        assert_eq!(value_to_string(&Value::Boolean(false)), "false");
    }

    #[test]
    fn test_value_to_string_null() {
        assert_eq!(value_to_string(&Value::Null), "");
    }

    #[test]
    fn test_value_to_string_unique_id() {
        assert_eq!(value_to_string(&Value::UniqueId(99)), "99");
    }

    #[test]
    fn test_value_to_string_point() {
        assert_eq!(
            value_to_string(&Value::Point { lat: 1.5, lon: 2.5 }),
            "point(1.5, 2.5)"
        );
    }

    #[test]
    fn test_value_to_string_noderef() {
        assert_eq!(value_to_string(&Value::NodeRef(7)), "node#7");
    }

    #[test]
    fn test_value_to_string_edgeref() {
        assert_eq!(
            value_to_string(&Value::EdgeRef {
                edge_idx: 3,
                src_idx: 1,
                dst_idx: 2
            }),
            "edge#3"
        );
    }

    // ========================================================================
    // json_string
    // ========================================================================

    #[test]
    fn test_json_string_plain() {
        assert_eq!(json_string("hello"), "\"hello\"");
    }

    #[test]
    fn test_json_string_with_quotes() {
        assert_eq!(json_string("say \"hi\""), "\"say \\\"hi\\\"\"");
    }

    #[test]
    fn test_json_string_with_backslash() {
        assert_eq!(json_string("a\\b"), "\"a\\\\b\"");
    }

    #[test]
    fn test_json_string_with_newline() {
        assert_eq!(json_string("a\nb"), "\"a\\nb\"");
    }

    #[test]
    fn test_json_string_empty() {
        assert_eq!(json_string(""), "\"\"");
    }

    // ========================================================================
    // json_value
    // ========================================================================

    #[test]
    fn test_json_value_string() {
        assert_eq!(json_value(&Value::String("hi".to_string())), "\"hi\"");
    }

    #[test]
    fn test_json_value_int() {
        assert_eq!(json_value(&Value::Int64(42)), "42");
    }

    #[test]
    fn test_json_value_float() {
        assert_eq!(json_value(&Value::Float64(2.5)), "2.5");
    }

    #[test]
    fn test_json_value_float_nan() {
        assert_eq!(json_value(&Value::Float64(f64::NAN)), "null");
    }

    #[test]
    fn test_json_value_float_infinity() {
        assert_eq!(json_value(&Value::Float64(f64::INFINITY)), "null");
    }

    #[test]
    fn test_json_value_bool() {
        assert_eq!(json_value(&Value::Boolean(true)), "true");
    }

    #[test]
    fn test_json_value_null() {
        assert_eq!(json_value(&Value::Null), "null");
    }

    #[test]
    fn test_json_value_unique_id() {
        assert_eq!(json_value(&Value::UniqueId(5)), "5");
    }

    #[test]
    fn test_json_value_point() {
        assert_eq!(
            json_value(&Value::Point {
                lat: 10.0,
                lon: 20.0
            }),
            "{\"lat\":10,\"lon\":20}"
        );
    }

    #[test]
    fn test_json_value_noderef() {
        assert_eq!(json_value(&Value::NodeRef(3)), "3");
    }

    #[test]
    fn test_json_value_edgeref() {
        assert_eq!(
            json_value(&Value::EdgeRef {
                edge_idx: 5,
                src_idx: 1,
                dst_idx: 2
            }),
            "5"
        );
    }

    // ========================================================================
    // properties_to_json
    // ========================================================================

    #[test]
    fn test_properties_to_json_empty() {
        let props: Vec<(&str, &Value)> = vec![];
        assert_eq!(properties_to_json(props.into_iter()), "{}");
    }

    #[test]
    fn test_properties_to_json_single() {
        let val = Value::Int64(42);
        let props = vec![("age", &val)];
        assert_eq!(properties_to_json(props.into_iter()), "{\"age\":42}");
    }

    #[test]
    fn test_properties_to_json_multiple() {
        let v1 = Value::String("hello".to_string());
        let v2 = Value::Boolean(true);
        let props = vec![("name", &v1), ("active", &v2)];
        let result = properties_to_json(props.into_iter());
        assert!(result.starts_with('{'));
        assert!(result.ends_with('}'));
        assert!(result.contains("\"name\":\"hello\""));
        assert!(result.contains("\"active\":true"));
    }

    // ========================================================================
    // value_type_name
    // ========================================================================

    #[test]
    fn test_value_type_name_string() {
        assert_eq!(value_type_name(&Value::String("x".into())), "string");
    }

    #[test]
    fn test_value_type_name_int() {
        assert_eq!(value_type_name(&Value::Int64(1)), "int");
    }

    #[test]
    fn test_value_type_name_float() {
        assert_eq!(value_type_name(&Value::Float64(1.0)), "float");
    }

    #[test]
    fn test_value_type_name_bool() {
        assert_eq!(value_type_name(&Value::Boolean(true)), "bool");
    }

    #[test]
    fn test_value_type_name_null() {
        assert_eq!(value_type_name(&Value::Null), "string");
    }

    #[test]
    fn test_value_type_name_unique_id() {
        assert_eq!(value_type_name(&Value::UniqueId(1)), "int");
    }

    #[test]
    fn test_value_type_name_point() {
        assert_eq!(
            value_type_name(&Value::Point { lat: 0.0, lon: 0.0 }),
            "string"
        );
    }

    #[test]
    fn test_value_type_name_noderef() {
        assert_eq!(value_type_name(&Value::NodeRef(0)), "int");
    }

    #[test]
    fn test_value_type_name_edgeref() {
        assert_eq!(
            value_type_name(&Value::EdgeRef {
                edge_idx: 0,
                src_idx: 0,
                dst_idx: 0
            }),
            "int"
        );
    }

    // ========================================================================
    // to_graphml — empty graph
    // ========================================================================

    #[test]
    fn test_graphml_empty_graph() {
        let g = empty_graph();
        let result = to_graphml(&g, None).unwrap();
        assert!(result.contains("<?xml version=\"1.0\""));
        assert!(result.contains("<graphml"));
        assert!(result.contains("<graph id=\"G\" edgedefault=\"directed\">"));
        assert!(result.contains("</graphml>"));
        // No node/edge elements
        assert!(!result.contains("<node"));
        assert!(!result.contains("<edge"));
    }

    // ========================================================================
    // to_graphml — simple graph
    // ========================================================================

    #[test]
    fn test_graphml_simple_graph() {
        let g = simple_graph();
        let result = to_graphml(&g, None).unwrap();
        // Should have two nodes
        assert!(result.contains("<node id=\"n0\">"));
        assert!(result.contains("<node id=\"n1\">"));
        // Node types
        assert!(result.contains("<data key=\"node_type\">Person</data>"));
        // Titles
        assert!(result.contains("<data key=\"node_title\">Alice</data>"));
        assert!(result.contains("<data key=\"node_title\">Bob</data>"));
        // Edge
        assert!(result.contains("<edge id=\"e0\""));
        assert!(result.contains("<data key=\"edge_type\">KNOWS</data>"));
    }

    // ========================================================================
    // to_graphml — xml escaping
    // ========================================================================

    #[test]
    fn test_graphml_xml_escaping() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::String("a&b".to_string()),
            Value::String("Title <with> \"special\" chars".to_string()),
            "Type&Co".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);
        let result = to_graphml(&g, None).unwrap();
        assert!(result.contains("Type&amp;Co"));
        assert!(result.contains("a&amp;b"));
        assert!(result.contains("Title &lt;with&gt; &quot;special&quot; chars"));
    }

    // ========================================================================
    // to_graphml — with properties
    // ========================================================================

    #[test]
    fn test_graphml_with_properties() {
        let g = graph_with_properties();
        let result = to_graphml(&g, None).unwrap();
        assert!(result.contains("<data key=\"node_properties\">"));
        assert!(result.contains("<data key=\"edge_properties\">"));
        // Edge property "since"
        assert!(result.contains("&quot;since&quot;"));
    }

    // ========================================================================
    // to_d3_json — empty graph
    // ========================================================================

    #[test]
    fn test_d3_json_empty_graph() {
        let g = empty_graph();
        let result = to_d3_json(&g, None).unwrap();
        // Should parse as valid-ish JSON structure
        assert!(result.contains("\"nodes\""));
        assert!(result.contains("\"links\""));
        // No actual node objects
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["nodes"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["links"].as_array().unwrap().len(), 0);
    }

    // ========================================================================
    // to_d3_json — simple graph
    // ========================================================================

    #[test]
    fn test_d3_json_simple_graph() {
        let g = simple_graph();
        let result = to_d3_json(&g, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let nodes = parsed["nodes"].as_array().unwrap();
        let links = parsed["links"].as_array().unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(links.len(), 1);
        // Check node fields
        assert_eq!(nodes[0]["id"], "alice");
        assert_eq!(nodes[0]["type"], "Person");
        assert_eq!(nodes[0]["title"], "Alice");
        // Check link fields
        assert_eq!(links[0]["source"], 0);
        assert_eq!(links[0]["target"], 1);
        assert_eq!(links[0]["type"], "KNOWS");
    }

    // ========================================================================
    // to_d3_json — with properties on nodes and edges
    // ========================================================================

    #[test]
    fn test_d3_json_with_properties() {
        let g = graph_with_properties();
        let result = to_d3_json(&g, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let nodes = parsed["nodes"].as_array().unwrap();
        let links = parsed["links"].as_array().unwrap();
        // Node 0 should have age property
        assert!(nodes[0].get("age").is_some() || nodes[1].get("age").is_some());
        // Link should have "since" property
        assert_eq!(links[0]["since"], 2020);
    }

    // ========================================================================
    // to_gexf — empty graph
    // ========================================================================

    #[test]
    fn test_gexf_empty_graph() {
        let g = empty_graph();
        let result = to_gexf(&g, None).unwrap();
        assert!(result.contains("<?xml version=\"1.0\""));
        assert!(result.contains("<gexf"));
        assert!(result.contains("<creator>kglite</creator>"));
        assert!(result.contains("<nodes>"));
        assert!(result.contains("</nodes>"));
        assert!(result.contains("<edges>"));
        assert!(result.contains("</edges>"));
        // No actual node elements between tags
        assert!(!result.contains("<node id="));
    }

    // ========================================================================
    // to_gexf — simple graph
    // ========================================================================

    #[test]
    fn test_gexf_simple_graph() {
        let g = simple_graph();
        let result = to_gexf(&g, None).unwrap();
        // Two nodes
        assert!(result.contains("<node id=\"0\" label=\"Alice\">"));
        assert!(result.contains("<node id=\"1\" label=\"Bob\">"));
        // Node attributes
        assert!(result.contains("<attvalue for=\"0\" value=\"Person\"/>"));
        // Edge
        assert!(result.contains("<edge id=\"0\" source=\"0\" target=\"1\">"));
        assert!(result.contains("<attvalue for=\"0\" value=\"KNOWS\"/>"));
    }

    // ========================================================================
    // to_gexf — xml escaping in labels
    // ========================================================================

    #[test]
    fn test_gexf_xml_escaping() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::String("id1".to_string()),
            Value::String("Title <b>bold</b>".to_string()),
            "Type&Co".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);
        let result = to_gexf(&g, None).unwrap();
        assert!(result.contains("label=\"Title &lt;b&gt;bold&lt;/b&gt;\""));
        assert!(result.contains("value=\"Type&amp;Co\""));
    }

    // ========================================================================
    // to_csv — empty graph
    // ========================================================================

    #[test]
    fn test_csv_empty_graph() {
        let g = empty_graph();
        let (nodes_csv, edges_csv) = to_csv(&g, None).unwrap();
        assert_eq!(nodes_csv, "id,type,title\n");
        assert_eq!(edges_csv, "source,target,type\n");
    }

    // ========================================================================
    // to_csv — simple graph
    // ========================================================================

    #[test]
    fn test_csv_simple_graph() {
        let g = simple_graph();
        let (nodes_csv, edges_csv) = to_csv(&g, None).unwrap();
        // Header + 2 data rows
        let node_lines: Vec<&str> = nodes_csv.lines().collect();
        assert_eq!(node_lines.len(), 3);
        assert_eq!(node_lines[0], "id,type,title");
        assert!(node_lines[1].contains("Person"));
        assert!(node_lines[1].contains("Alice"));

        // Edge CSV
        let edge_lines: Vec<&str> = edges_csv.lines().collect();
        assert_eq!(edge_lines.len(), 2);
        assert_eq!(edge_lines[0], "source,target,type");
        assert!(edge_lines[1].contains("KNOWS"));
    }

    // ========================================================================
    // to_csv — CSV escaping of commas in values
    // ========================================================================

    #[test]
    fn test_csv_escaping_commas() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::String("id,1".to_string()),
            Value::String("Title, with comma".to_string()),
            "Type".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);
        let (nodes_csv, _) = to_csv(&g, None).unwrap();
        // Values with commas should be quoted
        assert!(nodes_csv.contains("\"id,1\""));
        assert!(nodes_csv.contains("\"Title, with comma\""));
    }

    // ========================================================================
    // to_csv_dir — basic filesystem export
    // ========================================================================

    #[test]
    fn test_csv_dir_basic() {
        let g = multi_type_graph();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        // Check summary counts
        assert_eq!(summary.nodes.get("Person"), Some(&1));
        assert_eq!(summary.nodes.get("Company"), Some(&1));
        assert_eq!(summary.connections.get("WORKS_AT"), Some(&1));
        assert!(summary.files_written >= 3); // 2 node CSVs + 1 edge CSV + blueprint

        // Check files exist
        assert!(Path::new(dir).join("nodes/Person.csv").exists());
        assert!(Path::new(dir).join("nodes/Company.csv").exists());
        assert!(Path::new(dir).join("connections/WORKS_AT.csv").exists());
        assert!(Path::new(dir).join("blueprint.json").exists());
    }

    // ========================================================================
    // to_csv_dir — empty graph
    // ========================================================================

    #[test]
    fn test_csv_dir_empty_graph() {
        let g = empty_graph();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        assert!(summary.nodes.is_empty());
        assert!(summary.connections.is_empty());
        // Should still write blueprint.json
        assert!(Path::new(dir).join("blueprint.json").exists());
    }

    // ========================================================================
    // to_csv_dir — with parent types (sub-node nesting)
    // ========================================================================

    #[test]
    fn test_csv_dir_parent_types() {
        let mut g = DirGraph::new();
        let n1 = NodeData::new(
            Value::String("dept1".to_string()),
            Value::String("Engineering".to_string()),
            "Department".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let n2 = NodeData::new(
            Value::String("team1".to_string()),
            Value::String("Backend".to_string()),
            "Team".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let idx1 = g.graph.add_node(n1);
        let idx2 = g.graph.add_node(n2);
        g.type_indices
            .entry("Department".to_string())
            .or_default()
            .push(idx1);
        g.type_indices
            .entry("Team".to_string())
            .or_default()
            .push(idx2);

        let mut parent_types = HashMap::new();
        parent_types.insert("Team".to_string(), "Department".to_string());

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let _summary = to_csv_dir(&g, dir, None, &parent_types).unwrap();

        // Team CSV should be nested under Department
        assert!(Path::new(dir).join("nodes/Department/Team.csv").exists());
        assert!(Path::new(dir).join("nodes/Department.csv").exists());
    }

    // ========================================================================
    // to_csv_dir — node properties in CSV
    // ========================================================================

    #[test]
    fn test_csv_dir_node_properties() {
        let g = graph_with_properties();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let _summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        let person_csv = std::fs::read_to_string(Path::new(dir).join("nodes/Person.csv")).unwrap();
        let lines: Vec<&str> = person_csv.lines().collect();
        // Header should include property columns
        assert!(lines[0].contains("age"));
    }

    // ========================================================================
    // to_csv_dir — edge properties in connection CSV
    // ========================================================================

    #[test]
    fn test_csv_dir_edge_properties() {
        let g = graph_with_properties();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let _summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        let conn_csv =
            std::fs::read_to_string(Path::new(dir).join("connections/KNOWS.csv")).unwrap();
        let lines: Vec<&str> = conn_csv.lines().collect();
        // Header should include "since" property
        assert!(lines[0].contains("since"));
        // Data row should include the value
        assert!(lines[1].contains("2020"));
    }

    // ========================================================================
    // to_csv_dir — blueprint.json is valid JSON
    // ========================================================================

    #[test]
    fn test_csv_dir_blueprint_valid_json() {
        let g = multi_type_graph();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let _summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        let bp = std::fs::read_to_string(Path::new(dir).join("blueprint.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&bp).unwrap();
        // Should have settings and nodes
        assert!(parsed.get("settings").is_some());
        assert!(parsed.get("nodes").is_some());
        // Should have Person and Company node types
        assert!(parsed["nodes"].get("Person").is_some());
        assert!(parsed["nodes"].get("Company").is_some());
    }

    // ========================================================================
    // to_csv_dir — blueprint contains connection definitions
    // ========================================================================

    #[test]
    fn test_csv_dir_blueprint_connections() {
        let g = multi_type_graph();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let _summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        let bp = std::fs::read_to_string(Path::new(dir).join("blueprint.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&bp).unwrap();

        // Person node should have connections
        let person = &parsed["nodes"]["Person"];
        assert!(person.get("connections").is_some());
        let junctions = &person["connections"]["junction_edges"];
        assert!(junctions.get("WORKS_AT").is_some());
        assert_eq!(junctions["WORKS_AT"]["target"], "Company");
    }

    // ========================================================================
    // selected_node_indices — no selection returns all
    // ========================================================================

    #[test]
    fn test_selected_node_indices_no_selection() {
        let g = simple_graph();
        let indices = selected_node_indices(&g, None);
        assert_eq!(indices.len(), 2);
    }

    #[test]
    fn test_selected_node_indices_empty_graph() {
        let g = empty_graph();
        let indices = selected_node_indices(&g, None);
        assert!(indices.is_empty());
    }

    // ========================================================================
    // Integration: round-trip content checks
    // ========================================================================

    #[test]
    fn test_graphml_d3_gexf_csv_same_graph() {
        // Ensure all four export formats succeed on the same graph
        let g = graph_with_properties();
        assert!(to_graphml(&g, None).is_ok());
        assert!(to_d3_json(&g, None).is_ok());
        assert!(to_gexf(&g, None).is_ok());
        assert!(to_csv(&g, None).is_ok());
    }

    // ========================================================================
    // Single-node graph (no edges)
    // ========================================================================

    #[test]
    fn test_exports_single_node_no_edges() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::String("solo".to_string()),
            Value::String("Solo Node".to_string()),
            "Singleton".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);

        // GraphML
        let gml = to_graphml(&g, None).unwrap();
        assert!(gml.contains("<node id=\"n0\">"));
        assert!(!gml.contains("<edge"));

        // D3 JSON
        let d3 = to_d3_json(&g, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&d3).unwrap();
        assert_eq!(parsed["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(parsed["links"].as_array().unwrap().len(), 0);

        // GEXF
        let gexf = to_gexf(&g, None).unwrap();
        assert!(gexf.contains("<node id=\"0\""));
        assert!(!gexf.contains("<edge id="));

        // CSV
        let (nc, ec) = to_csv(&g, None).unwrap();
        assert_eq!(nc.lines().count(), 2); // header + 1 row
        assert_eq!(ec.lines().count(), 1); // header only
    }

    // ========================================================================
    // D3 JSON special characters in string values
    // ========================================================================

    #[test]
    fn test_d3_json_special_chars_in_values() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::String("id\"with\\quotes".to_string()),
            Value::String("Title\nwith\nnewlines".to_string()),
            "Type".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);
        let result = to_d3_json(&g, None).unwrap();
        // Should be valid JSON despite special chars
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let nodes = parsed["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0]["id"], "id\"with\\quotes");
    }

    // ========================================================================
    // to_csv_dir — summary log_lines populated
    // ========================================================================

    #[test]
    fn test_csv_dir_summary_log_lines() {
        let g = simple_graph();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();
        assert!(!summary.log_lines.is_empty());
        // Should contain a "Done:" summary line
        assert!(summary.log_lines.last().unwrap().starts_with("Done:"));
        assert_eq!(summary.output_dir, dir);
    }

    // ========================================================================
    // DateTime variant coverage
    // ========================================================================

    #[test]
    fn test_value_to_string_datetime() {
        use chrono::NaiveDate;
        let dt = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let result = value_to_string(&Value::DateTime(dt));
        assert_eq!(result, "2024-06-15");
    }

    #[test]
    fn test_json_value_datetime() {
        use chrono::NaiveDate;
        let dt = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let result = json_value(&Value::DateTime(dt));
        assert_eq!(result, "\"2024-06-15\"");
    }

    #[test]
    fn test_value_type_name_datetime() {
        use chrono::NaiveDate;
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(value_type_name(&Value::DateTime(dt)), "date");
    }

    // ========================================================================
    // Float edge cases in json_value
    // ========================================================================

    #[test]
    fn test_json_value_float_neg_infinity() {
        assert_eq!(json_value(&Value::Float64(f64::NEG_INFINITY)), "null");
    }

    #[test]
    fn test_json_value_float_zero() {
        assert_eq!(json_value(&Value::Float64(0.0)), "0");
    }

    #[test]
    fn test_json_value_float_negative() {
        assert_eq!(json_value(&Value::Float64(-1.5)), "-1.5");
    }

    // ========================================================================
    // Graphs with numeric (Int64) IDs
    // ========================================================================

    #[test]
    fn test_d3_json_numeric_ids() {
        let g = graph_with_properties(); // uses Int64 IDs
        let result = to_d3_json(&g, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let nodes = parsed["nodes"].as_array().unwrap();
        // Int64 IDs should appear as numbers
        assert_eq!(nodes[0]["id"], 1);
        assert_eq!(nodes[1]["id"], 2);
    }

    #[test]
    fn test_graphml_numeric_ids() {
        let g = graph_with_properties();
        let result = to_graphml(&g, None).unwrap();
        assert!(result.contains("<data key=\"node_id\">1</data>"));
        assert!(result.contains("<data key=\"node_id\">2</data>"));
    }

    #[test]
    fn test_csv_numeric_ids() {
        let g = graph_with_properties();
        let (nodes_csv, _) = to_csv(&g, None).unwrap();
        let lines: Vec<&str> = nodes_csv.lines().collect();
        // Rows should contain the integer IDs
        assert!(lines[1].starts_with("0,") || lines[1].starts_with("1,"));
    }

    // ========================================================================
    // Self-loop edges
    // ========================================================================

    #[test]
    fn test_exports_self_loop() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::String("self".to_string()),
            Value::String("Self Node".to_string()),
            "Thing".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let idx = g.graph.add_node(n);
        g.graph.add_edge(
            idx,
            idx,
            EdgeData::new("SELF_REF".to_string(), HashMap::new(), &mut g.interner),
        );

        // GraphML should have edge from n0 to n0
        let gml = to_graphml(&g, None).unwrap();
        assert!(gml.contains("source=\"n0\" target=\"n0\""));
        assert!(gml.contains("SELF_REF"));

        // D3 JSON should have a link with source==target
        let d3 = to_d3_json(&g, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&d3).unwrap();
        let links = parsed["links"].as_array().unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0]["source"], links[0]["target"]);

        // GEXF
        let gexf = to_gexf(&g, None).unwrap();
        assert!(gexf.contains("source=\"0\" target=\"0\""));

        // CSV
        let (_, edges_csv) = to_csv(&g, None).unwrap();
        let lines: Vec<&str> = edges_csv.lines().collect();
        assert_eq!(lines.len(), 2); // header + 1 edge
    }

    // ========================================================================
    // Multiple edges between same pair
    // ========================================================================

    #[test]
    fn test_exports_multiple_edges_same_pair() {
        let mut g = DirGraph::new();
        let n1 = NodeData::new(
            Value::String("a".to_string()),
            Value::String("A".to_string()),
            "Node".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let n2 = NodeData::new(
            Value::String("b".to_string()),
            Value::String("B".to_string()),
            "Node".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let idx1 = g.graph.add_node(n1);
        let idx2 = g.graph.add_node(n2);
        g.graph.add_edge(
            idx1,
            idx2,
            EdgeData::new("LIKES".to_string(), HashMap::new(), &mut g.interner),
        );
        g.graph.add_edge(
            idx1,
            idx2,
            EdgeData::new("KNOWS".to_string(), HashMap::new(), &mut g.interner),
        );

        // D3 JSON should have 2 links
        let d3 = to_d3_json(&g, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&d3).unwrap();
        assert_eq!(parsed["links"].as_array().unwrap().len(), 2);

        // GraphML should have e0 and e1
        let gml = to_graphml(&g, None).unwrap();
        assert!(gml.contains("id=\"e0\""));
        assert!(gml.contains("id=\"e1\""));

        // CSV edges
        let (_, edges_csv) = to_csv(&g, None).unwrap();
        assert_eq!(edges_csv.lines().count(), 3); // header + 2 edges
    }

    // ========================================================================
    // Sparse node properties (some nodes have props, others don't)
    // ========================================================================

    #[test]
    fn test_csv_dir_sparse_properties() {
        let mut g = DirGraph::new();
        let mut props1 = HashMap::new();
        props1.insert("color".to_string(), Value::String("red".to_string()));
        props1.insert("size".to_string(), Value::Int64(10));
        let n1 = NodeData::new(
            Value::String("a".to_string()),
            Value::String("A".to_string()),
            "Item".to_string(),
            props1,
            &mut g.interner,
        );
        // Second node of same type but only one property
        let mut props2 = HashMap::new();
        props2.insert("color".to_string(), Value::String("blue".to_string()));
        let n2 = NodeData::new(
            Value::String("b".to_string()),
            Value::String("B".to_string()),
            "Item".to_string(),
            props2,
            &mut g.interner,
        );
        let idx1 = g.graph.add_node(n1);
        let idx2 = g.graph.add_node(n2);
        g.type_indices
            .entry("Item".to_string())
            .or_default()
            .extend([idx1, idx2]);

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let _summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        let csv = std::fs::read_to_string(Path::new(dir).join("nodes/Item.csv")).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        // Header should have both color and size columns
        assert!(lines[0].contains("color"));
        assert!(lines[0].contains("size"));
        // 3 lines total: header + 2 data rows
        assert_eq!(lines.len(), 3);
    }

    // ========================================================================
    // build_blueprint with parent types and edge properties
    // ========================================================================

    #[test]
    fn test_csv_dir_blueprint_with_parent_and_edge_props() {
        let mut g = DirGraph::new();
        let n1 = NodeData::new(
            Value::String("dept1".to_string()),
            Value::String("Engineering".to_string()),
            "Department".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let n2 = NodeData::new(
            Value::String("team1".to_string()),
            Value::String("Backend".to_string()),
            "Team".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let idx1 = g.graph.add_node(n1);
        let idx2 = g.graph.add_node(n2);
        g.type_indices
            .entry("Department".to_string())
            .or_default()
            .push(idx1);
        g.type_indices
            .entry("Team".to_string())
            .or_default()
            .push(idx2);

        let mut edge_props = HashMap::new();
        edge_props.insert("weight".to_string(), Value::Float64(0.75));
        g.graph.add_edge(
            idx1,
            idx2,
            EdgeData::new("HAS_TEAM".to_string(), edge_props, &mut g.interner),
        );

        let mut parent_types = HashMap::new();
        parent_types.insert("Team".to_string(), "Department".to_string());

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let _summary = to_csv_dir(&g, dir, None, &parent_types).unwrap();

        let bp = std::fs::read_to_string(Path::new(dir).join("blueprint.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&bp).unwrap();

        // Team should have parent reference
        assert_eq!(parsed["nodes"]["Team"]["parent"], "Department");

        // Department should have HAS_TEAM connection with properties
        let junctions = &parsed["nodes"]["Department"]["connections"]["junction_edges"];
        assert!(junctions.get("HAS_TEAM").is_some());
        let has_team = &junctions["HAS_TEAM"];
        assert_eq!(has_team["target"], "Team");
        // Edge properties should be listed
        let props = has_team["properties"].as_array().unwrap();
        assert!(props.iter().any(|p| p == "weight"));
    }

    // ========================================================================
    // GraphML with node properties but no edge properties
    // ========================================================================

    #[test]
    fn test_graphml_node_props_no_edge_props() {
        let mut g = DirGraph::new();
        let mut props = HashMap::new();
        props.insert("role".to_string(), Value::String("admin".to_string()));
        let n1 = NodeData::new(
            Value::String("u1".to_string()),
            Value::String("User 1".to_string()),
            "User".to_string(),
            props,
            &mut g.interner,
        );
        let n2 = NodeData::new(
            Value::String("u2".to_string()),
            Value::String("User 2".to_string()),
            "User".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let idx1 = g.graph.add_node(n1);
        let idx2 = g.graph.add_node(n2);
        g.graph.add_edge(
            idx1,
            idx2,
            EdgeData::new("FOLLOWS".to_string(), HashMap::new(), &mut g.interner),
        );
        let result = to_graphml(&g, None).unwrap();
        // Node should have properties
        assert!(result.contains("<data key=\"node_properties\">"));
        // Edge should NOT have properties data element
        // (the edge element exists but no edge_properties data)
        assert!(result.contains("<data key=\"edge_type\">FOLLOWS</data>"));
        // Count edge_properties occurrences - should be 0 in this graph
        assert!(!result.contains("<data key=\"edge_properties\">"));
    }

    // ========================================================================
    // GEXF with special characters in node titles
    // ========================================================================

    #[test]
    fn test_gexf_node_with_ampersand_in_type() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::String("id1".to_string()),
            Value::String("R&D".to_string()),
            "Dept".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);
        let result = to_gexf(&g, None).unwrap();
        assert!(result.contains("label=\"R&amp;D\""));
    }

    // ========================================================================
    // CSV with quotes in values
    // ========================================================================

    #[test]
    fn test_csv_quotes_in_node_values() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::String("id1".to_string()),
            Value::String("She said \"hello\"".to_string()),
            "Note".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);
        let (nodes_csv, _) = to_csv(&g, None).unwrap();
        // The title with quotes should be properly escaped
        assert!(nodes_csv.contains("\"She said \"\"hello\"\"\""));
    }

    // ========================================================================
    // D3 JSON with all Value types in properties
    // ========================================================================

    #[test]
    fn test_d3_json_varied_property_types() {
        use chrono::NaiveDate;
        let mut g = DirGraph::new();
        let mut props = HashMap::new();
        props.insert("count".to_string(), Value::Int64(42));
        props.insert("score".to_string(), Value::Float64(9.5));
        props.insert("active".to_string(), Value::Boolean(true));
        props.insert("note".to_string(), Value::Null);
        props.insert(
            "created".to_string(),
            Value::DateTime(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
        );
        props.insert(
            "location".to_string(),
            Value::Point {
                lat: 40.7,
                lon: -74.0,
            },
        );
        let n = NodeData::new(
            Value::String("item1".to_string()),
            Value::String("Item".to_string()),
            "Thing".to_string(),
            props,
            &mut g.interner,
        );
        g.graph.add_node(n);

        let result = to_d3_json(&g, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let node = &parsed["nodes"][0];
        assert_eq!(node["count"], 42);
        assert_eq!(node["score"], 9.5);
        assert_eq!(node["active"], true);
        assert_eq!(node["note"], serde_json::Value::Null);
    }

    // ========================================================================
    // properties_to_json with special characters in keys
    // ========================================================================

    #[test]
    fn test_properties_to_json_special_key() {
        let val = Value::String("value".to_string());
        let props = vec![("key\"with\"quotes", &val)];
        let result = properties_to_json(props.into_iter());
        assert!(result.contains("\"key\\\"with\\\"quotes\""));
    }

    // ========================================================================
    // to_csv_dir — connection CSV content format
    // ========================================================================

    #[test]
    fn test_csv_dir_connection_csv_format() {
        let g = multi_type_graph();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let _summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        let csv = std::fs::read_to_string(Path::new(dir).join("connections/WORKS_AT.csv")).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        // Header should have standard columns
        assert_eq!(lines[0], "source_id,source_type,target_id,target_type");
        // Data row
        assert!(lines[1].contains("alice"));
        assert!(lines[1].contains("Person"));
        assert!(lines[1].contains("acme"));
        assert!(lines[1].contains("Company"));
    }

    // ========================================================================
    // Multiple node types in same export (coverage for type grouping)
    // ========================================================================

    #[test]
    fn test_csv_dir_node_csv_per_type() {
        let g = multi_type_graph();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        // Verify separate CSVs per type
        let person_csv = std::fs::read_to_string(Path::new(dir).join("nodes/Person.csv")).unwrap();
        let company_csv =
            std::fs::read_to_string(Path::new(dir).join("nodes/Company.csv")).unwrap();

        // Person CSV should have Alice but not Acme
        assert!(person_csv.contains("alice"));
        assert!(!person_csv.contains("acme"));

        // Company CSV should have Acme but not Alice
        assert!(company_csv.contains("acme"));
        assert!(!company_csv.contains("alice"));

        // Summary should account for all files
        assert!(summary.files_written >= 4); // 2 node + 1 edge + blueprint
    }

    // ========================================================================
    // json_string with combined special characters
    // ========================================================================

    #[test]
    fn test_json_string_all_specials_combined() {
        let result = json_string("line1\nhas \"quotes\" and \\backslash");
        assert_eq!(result, "\"line1\\nhas \\\"quotes\\\" and \\\\backslash\"");
    }

    // ========================================================================
    // escape_csv combined specials
    // ========================================================================

    #[test]
    fn test_escape_csv_comma_and_quotes() {
        // Value with both comma and quote should be quoted with escaped quotes
        assert_eq!(escape_csv("a,\"b\""), "\"a,\"\"b\"\"\"");
    }

    #[test]
    fn test_escape_csv_newline_and_comma() {
        assert_eq!(escape_csv("a\nb,c"), "\"a\nb,c\"");
    }

    // ========================================================================
    // escape_xml with multiple occurrences
    // ========================================================================

    #[test]
    fn test_escape_xml_multiple_ampersands() {
        assert_eq!(escape_xml("a&b&c"), "a&amp;b&amp;c");
    }

    // ========================================================================
    // Large graph (coverage for iteration paths)
    // ========================================================================

    #[test]
    fn test_exports_many_nodes() {
        let mut g = DirGraph::new();
        for i in 0..50 {
            let n = NodeData::new(
                Value::Int64(i),
                Value::String(format!("Node {}", i)),
                "Bulk".to_string(),
                HashMap::new(),
                &mut g.interner,
            );
            g.graph.add_node(n);
        }
        // Add some edges
        let indices: Vec<_> = g.graph.node_indices().collect();
        for i in 0..49 {
            g.graph.add_edge(
                indices[i],
                indices[i + 1],
                EdgeData::new("NEXT".to_string(), HashMap::new(), &mut g.interner),
            );
        }

        let gml = to_graphml(&g, None).unwrap();
        assert!(gml.contains("n49")); // last node present
        assert!(gml.contains("e48")); // last edge present

        let d3 = to_d3_json(&g, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&d3).unwrap();
        assert_eq!(parsed["nodes"].as_array().unwrap().len(), 50);
        assert_eq!(parsed["links"].as_array().unwrap().len(), 49);

        let (nc, ec) = to_csv(&g, None).unwrap();
        assert_eq!(nc.lines().count(), 51); // header + 50
        assert_eq!(ec.lines().count(), 50); // header + 49
    }

    // ========================================================================
    // to_csv_dir on graph with no edges (no connections dir needed)
    // ========================================================================

    #[test]
    fn test_csv_dir_no_edges() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::String("x".to_string()),
            Value::String("X".to_string()),
            "Solo".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);
        g.type_indices
            .entry("Solo".to_string())
            .or_default()
            .push(petgraph::graph::NodeIndex::new(0));

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        assert!(summary.connections.is_empty());
        assert_eq!(summary.nodes.get("Solo"), Some(&1));
        // nodes dir exists, connections dir should NOT exist
        assert!(Path::new(dir).join("nodes/Solo.csv").exists());
        assert!(!Path::new(dir).join("connections").exists());
    }

    // ========================================================================
    // build_blueprint — node property types in blueprint
    // ========================================================================

    #[test]
    fn test_csv_dir_blueprint_property_types() {
        let g = graph_with_properties();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let _summary = to_csv_dir(&g, dir, None, &HashMap::new()).unwrap();

        let bp = std::fs::read_to_string(Path::new(dir).join("blueprint.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&bp).unwrap();
        let person = &parsed["nodes"]["Person"];
        let props = &person["properties"];
        assert_eq!(props["age"], "int");
        assert_eq!(props["active"], "bool");
    }

    // ========================================================================
    // Bidirectional edges
    // ========================================================================

    #[test]
    fn test_exports_bidirectional_edges() {
        let mut g = DirGraph::new();
        let n1 = NodeData::new(
            Value::String("a".to_string()),
            Value::String("A".to_string()),
            "Node".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let n2 = NodeData::new(
            Value::String("b".to_string()),
            Value::String("B".to_string()),
            "Node".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        let idx1 = g.graph.add_node(n1);
        let idx2 = g.graph.add_node(n2);
        g.graph.add_edge(
            idx1,
            idx2,
            EdgeData::new("LINK".to_string(), HashMap::new(), &mut g.interner),
        );
        g.graph.add_edge(
            idx2,
            idx1,
            EdgeData::new("LINK".to_string(), HashMap::new(), &mut g.interner),
        );

        let d3 = to_d3_json(&g, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&d3).unwrap();
        let links = parsed["links"].as_array().unwrap();
        assert_eq!(links.len(), 2);
        // One goes 0->1, the other 1->0
        let sources: Vec<i64> = links
            .iter()
            .map(|l| l["source"].as_i64().unwrap())
            .collect();
        assert!(sources.contains(&0));
        assert!(sources.contains(&1));
    }

    // ========================================================================
    // Null property values in properties_to_json
    // ========================================================================

    #[test]
    fn test_properties_to_json_with_null() {
        let v1 = Value::Null;
        let v2 = Value::Int64(1);
        let props = vec![("missing", &v1), ("present", &v2)];
        let result = properties_to_json(props.into_iter());
        assert!(result.contains("\"missing\":null"));
        assert!(result.contains("\"present\":1"));
    }

    // ========================================================================
    // UniqueId values in exports
    // ========================================================================

    #[test]
    fn test_d3_json_unique_id_node() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::UniqueId(12345),
            Value::String("UID Node".to_string()),
            "Entity".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);
        let result = to_d3_json(&g, None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["nodes"][0]["id"], 12345);
    }

    // ========================================================================
    // Boolean ID value
    // ========================================================================

    #[test]
    fn test_graphml_boolean_id() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::Boolean(true),
            Value::String("Bool Node".to_string()),
            "Test".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);
        let result = to_graphml(&g, None).unwrap();
        assert!(result.contains("<data key=\"node_id\">true</data>"));
    }

    // ========================================================================
    // Null ID value
    // ========================================================================

    #[test]
    fn test_csv_null_id() {
        let mut g = DirGraph::new();
        let n = NodeData::new(
            Value::Null,
            Value::String("No ID".to_string()),
            "Test".to_string(),
            HashMap::new(),
            &mut g.interner,
        );
        g.graph.add_node(n);
        let (nodes_csv, _) = to_csv(&g, None).unwrap();
        let lines: Vec<&str> = nodes_csv.lines().collect();
        // Null id becomes empty string
        assert!(lines[1].starts_with("0,"));
    }

    // ========================================================================
    // Point value in GraphML properties
    // ========================================================================

    #[test]
    fn test_graphml_point_property() {
        let mut g = DirGraph::new();
        let mut props = HashMap::new();
        props.insert(
            "loc".to_string(),
            Value::Point {
                lat: 51.5,
                lon: -0.1,
            },
        );
        let n = NodeData::new(
            Value::String("london".to_string()),
            Value::String("London".to_string()),
            "City".to_string(),
            props,
            &mut g.interner,
        );
        g.graph.add_node(n);
        let result = to_graphml(&g, None).unwrap();
        assert!(result.contains("node_properties"));
        // The point value should appear as "point(51.5, -0.1)" inside the JSON
        assert!(result.contains("point(51.5, -0.1)"));
    }
}
