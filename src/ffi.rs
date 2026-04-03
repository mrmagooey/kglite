// src/ffi.rs
// C-compatible FFI layer for kglite.
//
// Build (no Python required):
//   cargo build --release --no-default-features --features ffi
//   → target/release/libkglite.a  (link with: -lkglite -lm)
//
// Go CGO usage:
//   #cgo LDFLAGS: -L${SRCDIR}/../../../../kglite-ffi/target/release -lkglite -lm
//   #include "kglite.h"

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::sync::Arc;

use petgraph::graph::NodeIndex;

use crate::datatypes::values::Value;
use crate::graph::batch_operations::ConnectionBatchProcessor;
use crate::graph::cypher::ast::CypherQuery;
use crate::graph::cypher::{
    execute_mutable, is_mutation_query, optimize, parse_cypher, CypherExecutor, CypherResult,
};
use crate::graph::io_operations::{load_file, prepare_save, write_graph_v3};
use crate::graph::schema::{DirGraph, EdgeData};

// ─── Cypher parse cache ─────────────────────────────────────────────────
// Caches parsed ASTs keyed by query string to avoid re-parsing identical queries.

use std::sync::RwLock;

struct ParseCache {
    entries: RwLock<HashMap<String, Arc<CypherQuery>>>,
}

impl ParseCache {
    fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::with_capacity(1024)),
        }
    }

    fn get_or_parse(&self, query: &str) -> Result<Arc<CypherQuery>, String> {
        // Fast path: read lock
        if let Ok(map) = self.entries.read() {
            if let Some(cached) = map.get(query) {
                return Ok(Arc::clone(cached));
            }
        }
        // Slow path: parse and insert
        let parsed = parse_cypher(query)?;
        let arc = Arc::new(parsed);
        if let Ok(mut map) = self.entries.write() {
            // Evict if cache is too large
            if map.len() > 4096 {
                map.clear();
            }
            map.insert(query.to_string(), Arc::clone(&arc));
        }
        Ok(arc)
    }
}

static PARSE_CACHE: std::sync::LazyLock<ParseCache> = std::sync::LazyLock::new(ParseCache::new);

// ─── Cypher plan cache ──────────────────────────────────────────────────
// Caches fully-optimized ASTs for parameter-free queries.  The optimizer is
// deterministic and its output depends only on the query text and schema
// (label / relationship-type metadata).  Schema is immutable at runtime in
// kglite, so a simple `query_string → Arc<CypherQuery>` cache is safe.
//
// For queries **with** parameters we always re-optimize because the planner's
// `push_where_into_match` resolves `$param` values and the resulting plan may
// differ across invocations.

struct PlanCache {
    entries: RwLock<HashMap<String, Arc<CypherQuery>>>,
}

impl PlanCache {
    fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::with_capacity(1024)),
        }
    }

    fn get(&self, query: &str) -> Option<Arc<CypherQuery>> {
        self.entries
            .read()
            .ok()
            .and_then(|map| map.get(query).map(Arc::clone))
    }

    fn insert(&self, query: String, plan: Arc<CypherQuery>) {
        if let Ok(mut map) = self.entries.write() {
            if map.len() > 4096 {
                map.clear();
            }
            map.insert(query, plan);
        }
    }
}

static PLAN_CACHE: std::sync::LazyLock<PlanCache> = std::sync::LazyLock::new(PlanCache::new);

/// Parse, then optimize a query—using the plan cache for parameter-free queries.
///
/// Returns an `Arc<CypherQuery>` that is ready for execution.
fn get_optimized_plan(
    query_str: &str,
    params: &HashMap<String, Value>,
    graph: &DirGraph,
) -> Result<Arc<CypherQuery>, String> {
    // For parameter-free queries, try the plan cache first.
    let no_params = params.is_empty();
    if no_params {
        if let Some(cached) = PLAN_CACHE.get(query_str) {
            return Ok(cached);
        }
    }

    // Parse (via parse cache)
    let parsed_arc = PARSE_CACHE.get_or_parse(query_str)?;

    // Clone out of the Arc so we can mutate during optimization
    let mut query = (*parsed_arc).clone();
    optimize(&mut query, graph, params);
    let plan = Arc::new(query);

    // Cache the optimized plan for parameter-free queries
    if no_params {
        PLAN_CACHE.insert(query_str.to_string(), Arc::clone(&plan));
    }

    Ok(plan)
}

// ─── Thread-local error storage ───────────────────────────────────────────

thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<CString>> = std::cell::RefCell::new(None);
}

fn set_error(msg: impl Into<Vec<u8>>) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).ok();
    });
}

fn clear_error() {
    LAST_ERROR.with(|e| *e.borrow_mut() = None);
}

// ─── Opaque handle ────────────────────────────────────────────────────────

/// Opaque graph handle. Owns an `Arc<DirGraph>` protected by an `RwLock`.
/// Read-only queries acquire a shared read lock, allowing concurrent execution.
/// Mutation queries acquire an exclusive write lock.
pub struct KgHandle {
    inner: RwLock<Arc<DirGraph>>,
}

// ─── Lifecycle ────────────────────────────────────────────────────────────

/// Create a new empty graph. Returns NULL on allocation failure.
#[no_mangle]
pub extern "C" fn kg_new() -> *mut KgHandle {
    clear_error();
    let handle = KgHandle {
        inner: RwLock::new(Arc::new(DirGraph::new())),
    };
    Box::into_raw(Box::new(handle))
}

/// Free a graph handle. Passing NULL is a no-op.
#[no_mangle]
pub extern "C" fn kg_free(handle: *mut KgHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

// ─── Persistence ──────────────────────────────────────────────────────────

/// Load a graph from a `.kgl` file. Returns NULL on error; call `kg_last_error()` for details.
#[no_mangle]
pub extern "C" fn kg_load(path: *const c_char) -> *mut KgHandle {
    clear_error();
    if path.is_null() {
        set_error("path is null");
        return std::ptr::null_mut();
    }
    let path_str = unsafe { CStr::from_ptr(path) }
        .to_string_lossy()
        .into_owned();
    match load_file(&path_str) {
        Ok(kg) => {
            let arc = kg.inner.clone();
            Box::into_raw(Box::new(KgHandle {
                inner: RwLock::new(arc),
            }))
        }
        Err(e) => {
            set_error(e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Save a graph to a `.kgl` file. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn kg_save(handle: *const KgHandle, path: *const c_char) -> c_int {
    clear_error();
    if handle.is_null() || path.is_null() {
        set_error("null argument");
        return -1;
    }
    let path_str = unsafe { CStr::from_ptr(path) }
        .to_string_lossy()
        .into_owned();
    let handle_ref = unsafe { &*handle };
    let mut arc = match handle_ref.inner.write() {
        Ok(g) => g,
        Err(e) => {
            set_error(format!("rwlock poisoned: {e}"));
            return -1;
        }
    };
    prepare_save(&mut arc);
    match write_graph_v3(&arc, &path_str) {
        Ok(_) => 0,
        Err(e) => {
            set_error(e.to_string());
            -1
        }
    }
}

// ─── Query execution ──────────────────────────────────────────────────────

/// Execute a Cypher query.
///
/// - `handle`: graph handle (must not be NULL)
/// - `query`:  NUL-terminated Cypher query string
/// - `params_json`: NUL-terminated JSON object of query parameters, or NULL for none
/// - `out`: on success, written with a pointer to a NUL-terminated JSON string;
///          the caller must free this with `kg_free_string()`
///
/// Returns 0 on success, -1 on error. Call `kg_last_error()` on failure.
///
/// Result JSON format:
/// ```json
/// {"columns": ["col1", ...], "rows": [[val, ...], ...]}
/// ```
#[no_mangle]
pub extern "C" fn kg_cypher(
    handle: *mut KgHandle,
    query: *const c_char,
    params_json: *const c_char,
    out: *mut *mut c_char,
) -> c_int {
    clear_error();
    if handle.is_null() || query.is_null() || out.is_null() {
        set_error("null argument");
        return -1;
    }

    let query_str = unsafe { CStr::from_ptr(query) }
        .to_string_lossy()
        .into_owned();
    let params = if params_json.is_null() {
        HashMap::new()
    } else {
        let s = unsafe { CStr::from_ptr(params_json) }.to_string_lossy();
        match parse_params_json(&s) {
            Ok(p) => p,
            Err(e) => {
                set_error(format!("invalid params: {e}"));
                return -1;
            }
        }
    };

    // Parse first (via parse cache) to determine mutation vs read-only
    let parsed = match PARSE_CACHE.get_or_parse(&query_str) {
        Ok(q) => q,
        Err(e) => {
            set_error(format!("parse error: {e}"));
            return -1;
        }
    };

    let handle_ref = unsafe { &mut *handle };

    if is_mutation_query(&parsed) {
        // Write path: exclusive lock for mutations
        let mut arc = match handle_ref.inner.write() {
            Ok(g) => g,
            Err(e) => {
                set_error(format!("rwlock poisoned: {e}"));
                return -1;
            }
        };
        // Optimize with graph context (mutations always re-optimize; params may vary)
        let optimized = match get_optimized_plan(&query_str, &params, &*arc) {
            Ok(p) => p,
            Err(e) => {
                set_error(format!("optimize error: {e}"));
                return -1;
            }
        };
        let graph = Arc::make_mut(&mut *arc);
        let result = execute_mutable(graph, &optimized, params, None);
        emit_result(result, &*arc, out)
    } else {
        // Read path: shared lock allows concurrent read queries
        let arc = match handle_ref.inner.read() {
            Ok(g) => g,
            Err(e) => {
                set_error(format!("rwlock poisoned: {e}"));
                return -1;
            }
        };
        // Optimize with graph context — plan cache handles no-param queries
        let optimized = match get_optimized_plan(&query_str, &params, &*arc) {
            Ok(p) => p,
            Err(e) => {
                set_error(format!("optimize error: {e}"));
                return -1;
            }
        };
        let executor = CypherExecutor::with_params(&*arc, &params, None);
        let result = executor.execute(&optimized);
        emit_result(result, &*arc, out)
    }
}

/// Memory statistics reported by the tracking allocator.
/// All fields are zero when the tracking allocator is not active
/// (i.e. non-FFI builds or Python extension builds).
#[repr(C)]
pub struct KgMemStats {
    /// Current live Rust heap bytes.
    pub current_bytes: u64,
    /// Peak live Rust heap bytes since process start.
    pub peak_bytes: u64,
    /// Total number of allocations since process start.
    pub total_allocs: u64,
}

/// Return current Rust heap statistics from the tracking allocator.
#[no_mangle]
pub extern "C" fn kg_memory_stats() -> KgMemStats {
    #[cfg(all(feature = "ffi", not(feature = "python")))]
    {
        use crate::tracking_alloc::{CURRENT_BYTES, PEAK_BYTES, TOTAL_ALLOCS};
        use std::sync::atomic::Ordering;
        KgMemStats {
            current_bytes: CURRENT_BYTES.load(Ordering::Acquire).max(0) as u64,
            peak_bytes: PEAK_BYTES.load(Ordering::Acquire),
            total_allocs: TOTAL_ALLOCS.load(Ordering::Acquire),
        }
    }
    #[cfg(not(all(feature = "ffi", not(feature = "python"))))]
    KgMemStats {
        current_bytes: 0,
        peak_bytes: 0,
        total_allocs: 0,
    }
}

/// Execute multiple Cypher queries in a single lock acquisition.
///
/// - `queries_json`: JSON array of objects `[{"query": "...", "params": {...}}, ...]`
/// - `out`: receives JSON string with array of results (one per query)
///
/// Returns 0 on success, -1 on error. On error the first failing query's message
/// is available via `kg_last_error()` and earlier successful mutations are NOT rolled back.
#[no_mangle]
pub extern "C" fn kg_cypher_batch(
    handle: *mut KgHandle,
    queries_json: *const c_char,
    out: *mut *mut c_char,
) -> c_int {
    clear_error();
    if handle.is_null() || queries_json.is_null() || out.is_null() {
        set_error("null argument");
        return -1;
    }

    let json_str = unsafe { CStr::from_ptr(queries_json) }.to_string_lossy();
    let batch: Vec<serde_json::Value> = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("invalid batch JSON: {e}"));
            return -1;
        }
    };

    // Pre-parse all queries and determine if any are mutations
    let handle_ref = unsafe { &mut *handle };

    struct BatchEntry {
        query_str: String,
        params: HashMap<String, Value>,
        is_mutation: bool,
    }

    let mut entries = Vec::with_capacity(batch.len());
    let mut has_mutations = false;

    for (i, entry) in batch.iter().enumerate() {
        let query_str = match entry.get("query").and_then(|v| v.as_str()) {
            Some(q) => q.to_string(),
            None => {
                set_error(format!("batch[{i}]: missing 'query' field"));
                return -1;
            }
        };

        let params = if let Some(p) = entry.get("params") {
            if let Some(obj) = p.as_object() {
                match obj
                    .iter()
                    .map(|(k, v)| json_to_value(v.clone()).map(|val| (k.clone(), val)))
                    .collect::<Result<HashMap<String, Value>, String>>()
                {
                    Ok(m) => m,
                    Err(e) => {
                        set_error(format!("batch[{i}]: invalid params: {e}"));
                        return -1;
                    }
                }
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };

        let parsed = match PARSE_CACHE.get_or_parse(&query_str) {
            Ok(q) => q,
            Err(e) => {
                set_error(format!("batch[{i}]: parse error: {e}"));
                return -1;
            }
        };

        let is_mutation = is_mutation_query(&parsed);
        if is_mutation {
            has_mutations = true;
        }
        entries.push(BatchEntry {
            query_str,
            params,
            is_mutation,
        });
    }

    let mut results = Vec::with_capacity(entries.len());

    if has_mutations {
        // Mixed batch: acquire write lock, execute sequentially
        let mut arc = match handle_ref.inner.write() {
            Ok(g) => g,
            Err(e) => {
                set_error(format!("rwlock poisoned: {e}"));
                return -1;
            }
        };

        for (i, entry) in entries.into_iter().enumerate() {
            // Optimize with graph context (plan cache handles no-param queries)
            let optimized = match get_optimized_plan(&entry.query_str, &entry.params, &*arc) {
                Ok(p) => p,
                Err(e) => {
                    set_error(format!("batch[{i}]: optimize error: {e}"));
                    return -1;
                }
            };
            let result = if entry.is_mutation {
                let graph = Arc::make_mut(&mut *arc);
                execute_mutable(graph, &optimized, entry.params, None)
            } else {
                let executor = CypherExecutor::with_params(&*arc, &entry.params, None);
                executor.execute(&optimized)
            };

            match result {
                Ok(r) => {
                    let json = result_to_json(&r.columns, &r.rows, Some(&*arc));
                    results.push(json);
                }
                Err(e) => {
                    set_error(format!("batch[{i}]: {e}"));
                    return -1;
                }
            }
        }
    } else {
        // Read-only batch: acquire shared read lock, execute in parallel
        let arc = match handle_ref.inner.read() {
            Ok(g) => g,
            Err(e) => {
                set_error(format!("rwlock poisoned: {e}"));
                return -1;
            }
        };

        use rayon::prelude::*;
        let parallel_results: Vec<Result<String, (usize, String)>> = entries
            .into_par_iter()
            .enumerate()
            .map(|(i, entry)| {
                // Optimize with graph context (plan cache handles no-param queries)
                let optimized = get_optimized_plan(&entry.query_str, &entry.params, &*arc)
                    .map_err(|e| (i, format!("optimize error: {e}")))?;
                let executor = CypherExecutor::with_params(&*arc, &entry.params, None);
                match executor.execute(&optimized) {
                    Ok(r) => Ok(result_to_json(&r.columns, &r.rows, Some(&*arc))),
                    Err(e) => Err((i, e)),
                }
            })
            .collect();

        for pr in parallel_results {
            match pr {
                Ok(json) => results.push(json),
                Err((i, e)) => {
                    set_error(format!("batch[{i}]: {e}"));
                    return -1;
                }
            }
        }
    }

    // Combine results into a JSON array string
    let combined = format!("[{}]", results.join(","));
    match CString::new(combined) {
        Ok(s) => {
            unsafe { *out = s.into_raw() };
            0
        }
        Err(e) => {
            set_error(format!("result contains null bytes: {e}"));
            -1
        }
    }
}

/// Bulk-create edges by node index, bypassing Cypher for maximum throughput.
///
/// - `edges_json`: JSON array of edge specs:
///   `[{"src": <node_index>, "dst": <node_index>, "type": "EdgeType", "props": {...}}, ...]`
///   `src` and `dst` are petgraph NodeIndex values (the `__node_idx` from query results).
/// - `skip_existing`: if non-zero, skip duplicate-edge checks (faster when edges are known to be new)
/// - `out`: receives a JSON string `{"created": <count>}`.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn kg_create_edges_batch(
    handle: *mut KgHandle,
    edges_json: *const c_char,
    skip_existing: c_int,
    out: *mut *mut c_char,
) -> c_int {
    clear_error();
    if handle.is_null() || edges_json.is_null() || out.is_null() {
        set_error("null argument");
        return -1;
    }

    let json_str = unsafe { CStr::from_ptr(edges_json) }.to_string_lossy();
    let entries: Vec<serde_json::Value> = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("invalid edges JSON: {e}"));
            return -1;
        }
    };

    if entries.is_empty() {
        let result = CString::new(r#"{"created":0}"#).unwrap();
        unsafe { *out = result.into_raw() };
        return 0;
    }

    // Parse all edge specs and group by type
    let mut grouped: HashMap<String, Vec<(NodeIndex, NodeIndex, HashMap<String, Value>)>> =
        HashMap::new();

    for (i, entry) in entries.iter().enumerate() {
        let src = match entry.get("src").and_then(|v| v.as_u64()) {
            Some(v) => NodeIndex::new(v as usize),
            None => {
                set_error(format!("edges[{i}]: missing or invalid 'src'"));
                return -1;
            }
        };
        let dst = match entry.get("dst").and_then(|v| v.as_u64()) {
            Some(v) => NodeIndex::new(v as usize),
            None => {
                set_error(format!("edges[{i}]: missing or invalid 'dst'"));
                return -1;
            }
        };
        let edge_type = match entry.get("type").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => {
                set_error(format!("edges[{i}]: missing 'type'"));
                return -1;
            }
        };

        let props = if let Some(obj) = entry.get("props").and_then(|v| v.as_object()) {
            match obj
                .iter()
                .map(|(k, v)| json_to_value(v.clone()).map(|val| (k.clone(), val)))
                .collect::<Result<HashMap<String, Value>, String>>()
            {
                Ok(m) => m,
                Err(e) => {
                    set_error(format!("edges[{i}]: invalid props: {e}"));
                    return -1;
                }
            }
        } else {
            HashMap::new()
        };

        grouped
            .entry(edge_type)
            .or_default()
            .push((src, dst, props));
    }

    // Acquire write lock ONCE — edge creation is always a mutation
    let handle_ref = unsafe { &mut *handle };
    let mut arc = match handle_ref.inner.write() {
        Ok(g) => g,
        Err(e) => {
            set_error(format!("rwlock poisoned: {e}"));
            return -1;
        }
    };
    let graph = Arc::make_mut(&mut *arc);

    let mut total_created: usize = 0;

    // Process each edge type group with ConnectionBatchProcessor
    for (edge_type, edges) in grouped {
        let mut processor = ConnectionBatchProcessor::new(edges.len());
        processor.set_skip_existence_check(skip_existing != 0);

        for (src, dst, props) in edges {
            if let Err(e) = processor.add_connection(src, dst, props, graph, &edge_type) {
                set_error(format!("add_connection failed: {e}"));
                return -1;
            }
        }

        match processor.execute(graph, edge_type) {
            Ok((stats, _metrics)) => {
                total_created += stats.connections_created;
            }
            Err(e) => {
                set_error(format!("edge batch execute failed: {e}"));
                return -1;
            }
        }
    }

    let result_json = format!(r#"{{"created":{total_created}}}"#);
    match CString::new(result_json) {
        Ok(s) => {
            unsafe { *out = s.into_raw() };
            0
        }
        Err(e) => {
            set_error(format!("result contains null bytes: {e}"));
            -1
        }
    }
}

/// Free a string allocated by `kg_cypher`.
#[no_mangle]
pub extern "C" fn kg_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

/// Return the last error message for this thread, or NULL if there was no error.
/// The pointer remains valid until the next FFI call on this thread.
#[no_mangle]
pub extern "C" fn kg_last_error() -> *const c_char {
    LAST_ERROR.with(|e| match e.borrow().as_ref() {
        None => std::ptr::null(),
        Some(s) => s.as_ptr(),
    })
}

// ─── Helpers ──────────────────────────────────────────────────────────────

fn parse_params_json(s: &str) -> Result<HashMap<String, Value>, String> {
    let map: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(s).map_err(|e| e.to_string())?;
    map.into_iter()
        .map(|(k, v)| json_to_value(v).map(|val| (k, val)))
        .collect()
}

fn json_to_value(v: serde_json::Value) -> Result<Value, String> {
    match v {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Boolean(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int64(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float64(f))
            } else {
                Err(format!("unrepresentable number: {n}"))
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s)),
        // Arrays: try to convert elements to a homogeneous Value list.
        // Falls back to JSON-encoded string for the Cypher executor's parse_list_value().
        serde_json::Value::Array(arr) => {
            // If all elements are strings, pass as JSON-encoded string for IN $param support
            let s =
                serde_json::to_string(&serde_json::Value::Array(arr)).map_err(|e| e.to_string())?;
            Ok(Value::String(s))
        }
        other => Err(format!("unsupported param type: {other}")),
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    value_to_json_with_graph(v, None)
}

fn value_to_json_with_graph(
    v: &Value,
    graph: Option<&crate::graph::schema::DirGraph>,
) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Int64(i) => serde_json::json!(i),
        Value::Float64(f) => serde_json::json!(f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::UniqueId(id) => serde_json::json!(id),
        Value::DateTime(d) => serde_json::Value::String(d.to_string()),
        Value::Point { lat, lon } => serde_json::json!({"lat": lat, "lon": lon}),
        Value::NodeRef(idx) => {
            if let Some(g) = graph {
                let node_idx = petgraph::graph::NodeIndex::new(*idx as usize);
                if let Some(node) = g.get_node(node_idx) {
                    let mut props = serde_json::Map::new();
                    // Built-in fields
                    props.insert("__node_idx".to_string(), serde_json::json!(idx));
                    props.insert("__labels".to_string(), serde_json::json!([node.node_type]));
                    props.insert("id".to_string(), value_to_json(&node.id));
                    props.insert("title".to_string(), value_to_json(&node.title));
                    // All other properties
                    for (k, val) in node.properties_cloned(&g.interner) {
                        props.insert(k, value_to_json(&val));
                    }
                    return serde_json::Value::Object(props);
                }
            }
            serde_json::json!({"__node_ref": idx})
        }
        Value::EdgeRef {
            edge_idx,
            src_idx,
            dst_idx,
        } => {
            if let Some(g) = graph {
                let ei = petgraph::graph::EdgeIndex::new(*edge_idx as usize);
                if let Some(edge) = g.graph.edge_weight(ei) {
                    let kind = edge.connection_type_str(&g.interner).to_string();
                    let mut props = serde_json::Map::new();
                    props.insert("__edge_idx".to_string(), serde_json::json!(edge_idx));
                    props.insert("__src_idx".to_string(), serde_json::json!(src_idx));
                    props.insert("__dst_idx".to_string(), serde_json::json!(dst_idx));
                    props.insert("__type".to_string(), serde_json::Value::String(kind));
                    for (k, val) in edge.properties_cloned(&g.interner) {
                        props.insert(k, value_to_json(&val));
                    }
                    return serde_json::Value::Object(props);
                }
            }
            serde_json::json!({"__edge_ref": edge_idx, "__src": src_idx, "__dst": dst_idx})
        }
    }
}

/// Serialize a query result to JSON, convert to CString, and write to `out`.
/// On success returns 0; on error sets the thread-local error and returns -1.
fn emit_result(
    result: Result<CypherResult, String>,
    graph: &DirGraph,
    out: *mut *mut c_char,
) -> c_int {
    match result {
        Ok(r) => {
            let json = result_to_json(&r.columns, &r.rows, Some(graph));
            match CString::new(json) {
                Ok(s) => {
                    unsafe { *out = s.into_raw() };
                    0
                }
                Err(e) => {
                    set_error(format!("result contains null bytes: {e}"));
                    -1
                }
            }
        }
        Err(e) => {
            set_error(e);
            -1
        }
    }
}

fn result_to_json(
    columns: &[String],
    rows: &[Vec<Value>],
    graph: Option<&crate::graph::schema::DirGraph>,
) -> String {
    let json_rows: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| value_to_json_with_graph(v, graph))
                .collect()
        })
        .collect();
    let obj = serde_json::json!({
        "columns": columns,
        "rows": json_rows,
    });
    obj.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::schema::{DirGraph, NodeData};

    /// Helper: build a small graph with typed nodes for testing.
    fn build_test_graph() -> DirGraph {
        let mut graph = DirGraph::new();
        let n1 = NodeData::new(
            Value::UniqueId(1),
            Value::String("Alice".to_string()),
            "User".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let n2 = NodeData::new(
            Value::UniqueId(2),
            Value::String("Bob".to_string()),
            "User".to_string(),
            HashMap::new(),
            &mut graph.interner,
        );
        let idx1 = graph.graph.add_node(n1);
        let idx2 = graph.graph.add_node(n2);
        graph
            .type_indices
            .entry("User".to_string())
            .or_default()
            .push(idx1);
        graph
            .type_indices
            .entry("User".to_string())
            .or_default()
            .push(idx2);
        graph
    }

    #[test]
    fn test_plan_cache_hit_for_no_param_query() {
        let graph = build_test_graph();
        let params: HashMap<String, Value> = HashMap::new();
        let query = "MATCH (n:User) RETURN count(n)";

        // First call: should miss plan cache, optimize, and insert
        let plan1 = get_optimized_plan(query, &params, &graph).unwrap();

        // Second call: should hit plan cache (same Arc pointer)
        let plan2 = get_optimized_plan(query, &params, &graph).unwrap();

        // Both should return the same Arc (pointer equality proves cache hit)
        assert!(
            Arc::ptr_eq(&plan1, &plan2),
            "second call should return cached plan (same Arc)"
        );
    }

    #[test]
    fn test_plan_cache_miss_for_param_query() {
        let graph = build_test_graph();

        let query = "MATCH (n) WHERE n.name = $name RETURN n";

        let mut params1 = HashMap::new();
        params1.insert("name".to_string(), Value::String("Alice".to_string()));

        let mut params2 = HashMap::new();
        params2.insert("name".to_string(), Value::String("Bob".to_string()));

        // With params: should NOT cache, so different calls produce different Arcs
        let plan_a = get_optimized_plan(query, &params1, &graph).unwrap();
        let plan_b = get_optimized_plan(query, &params2, &graph).unwrap();

        assert!(
            !Arc::ptr_eq(&plan_a, &plan_b),
            "parameterized queries should not share cached plans"
        );
    }

    #[test]
    fn test_plan_cache_produces_optimized_query() {
        use crate::graph::cypher::ast::Clause;

        let graph = build_test_graph();
        let params: HashMap<String, Value> = HashMap::new();

        // This query should be fused into FusedCountTypedNode by the optimizer
        let query = "MATCH (n:User) RETURN count(n)";
        let plan = get_optimized_plan(query, &params, &graph).unwrap();

        // Verify the optimizer actually ran: the plan should contain a fused clause
        let has_fused = plan
            .clauses
            .iter()
            .any(|c| matches!(c, Clause::FusedCountTypedNode { .. }));
        assert!(
            has_fused,
            "optimized plan should contain FusedCountTypedNode, got: {:?}",
            plan.clauses
                .iter()
                .map(|c| std::mem::discriminant(c))
                .collect::<Vec<_>>()
        );
    }
}
