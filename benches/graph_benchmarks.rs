// benches/graph_benchmarks.rs
//
// Criterion benchmarks for kglite's pure-Rust internals.
// No PyO3 / Python types are used — benchmarks link against the lib but
// cannot initialise the Python interpreter.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use std::sync::Arc;

use kglite::graph::cypher::{execute_mutable, parse_cypher, CypherExecutor};
use kglite::graph::graph_algorithms::shortest_path_cost;
use kglite::graph::io_operations::{load_file, prepare_save, write_graph_v3};
use kglite::graph::schema::DirGraph;

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Build a linear chain of `n` nodes via Cypher CREATE, returning the graph
/// and the last node's petgraph index (which is `n - 1` after sequential inserts).
fn build_chain_via_cypher(n: usize) -> DirGraph {
    let mut graph = DirGraph::new();
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();

    // Create all nodes
    for i in 0..n {
        let q = format!("CREATE (n:Node {{id: {id}, name: 'Node_{id}'}})", id = i);
        let parsed = parse_cypher(&q).expect("node create should parse");
        execute_mutable(&mut graph, &parsed, params.clone(), None)
            .expect("node create should succeed");
    }

    // Create chain edges: Node_0 -> Node_1 -> ... -> Node_{n-1}
    // After sequential CREATE, NodeIndex values are 0..n-1.
    for i in 0..n.saturating_sub(1) {
        let q = format!(
            "MATCH (a:Node {{id: {a}}}) MATCH (b:Node {{id: {b}}}) CREATE (a)-[:NEXT]->(b)",
            a = i,
            b = i + 1
        );
        let parsed = parse_cypher(&q).expect("edge create should parse");
        execute_mutable(&mut graph, &parsed, params.clone(), None)
            .expect("edge create should succeed");
    }

    graph
}

/// Build a small graph for use in multiple benchmarks.
/// Returns a pre-built graph with 50 nodes in a chain.
fn make_chain_50() -> DirGraph {
    build_chain_via_cypher(50)
}

// ─── benchmark functions ──────────────────────────────────────────────────────

/// Benchmark: build a DirGraph with 100 nodes and 99 chain edges via Cypher.
fn bench_build_graph(c: &mut Criterion) {
    c.bench_function("build_graph_100_nodes_cypher", |b| {
        b.iter(|| {
            let graph = build_chain_via_cypher(black_box(100));
            black_box(graph);
        });
    });
}

/// Benchmark: parse a non-trivial Cypher query string.
fn bench_cypher_parse(c: &mut Criterion) {
    let query = "MATCH (a:Person)-[:KNOWS]->(b:Person) \
                 WHERE a.age > 30 AND b.name STARTS WITH 'A' \
                 RETURN a.name, b.name, a.age \
                 ORDER BY a.age DESC \
                 LIMIT 25";

    c.bench_function("cypher_parse_match_where_return", |b| {
        b.iter(|| {
            let result = parse_cypher(black_box(query));
            black_box(result)
        });
    });
}

/// Benchmark: BFS shortest-path cost on a 50-node chain.
/// Source = node 0, target = node 49 — the longest path in the chain.
fn bench_shortest_path(c: &mut Criterion) {
    let graph = make_chain_50();
    let source = NodeIndex::new(0);
    let target = NodeIndex::new(49);

    c.bench_function("shortest_path_cost_chain_50", |b| {
        b.iter(|| {
            let cost = shortest_path_cost(black_box(&graph), black_box(source), black_box(target));
            black_box(cost)
        });
    });
}

/// Benchmark: Cypher read-only MATCH on a pre-built 50-node graph.
fn bench_cypher_match(c: &mut Criterion) {
    let graph = make_chain_50();
    let query_str = "MATCH (n:Node) RETURN n.id LIMIT 20";
    let parsed = parse_cypher(query_str).expect("query should parse");
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();

    c.bench_function("cypher_match_node_scan_50", |b| {
        b.iter(|| {
            let executor = CypherExecutor::with_params(black_box(&graph), &params, None);
            let result = executor.execute(black_box(&parsed));
            black_box(result)
        });
    });
}

/// Benchmark: Cypher CREATE — insert 5 nodes into a fresh graph.
fn bench_cypher_create(c: &mut Criterion) {
    let query_str = "CREATE (a:Person {id: 1, name: 'Alice'}) \
                     CREATE (b:Person {id: 2, name: 'Bob'}) \
                     CREATE (c:Person {id: 3, name: 'Carol'}) \
                     CREATE (d:Person {id: 4, name: 'Dave'}) \
                     CREATE (e:Person {id: 5, name: 'Eve'})";
    let parsed = parse_cypher(query_str).expect("query should parse");
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();

    c.bench_function("cypher_create_5_nodes", |b| {
        b.iter(|| {
            let mut graph = DirGraph::new();
            let result = execute_mutable(
                black_box(&mut graph),
                black_box(&parsed),
                params.clone(),
                None,
            );
            black_box(result)
        });
    });
}

/// Benchmark: function dispatch (toUpper, toLower, toString) on every row of a 200-node scan.
/// Targets the parse-time lowercase normalisation of function names (optimization 1).
fn bench_function_dispatch(c: &mut Criterion) {
    let graph = build_chain_via_cypher(200);
    let query_str = "MATCH (n:Node) RETURN toUpper(n.name), toLower(n.name), toString(n.id)";
    let parsed = parse_cypher(query_str).expect("query should parse");
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();

    c.bench_function("bench_function_dispatch", |b| {
        b.iter(|| {
            let executor = CypherExecutor::with_params(black_box(&graph), &params, None);
            let result = executor.execute(black_box(&parsed));
            black_box(result)
        });
    });
}

/// Benchmark: COUNT(DISTINCT n) on a 200-node graph.
/// Targets the HashSet<usize> key optimisation instead of format! string keys (optimization 2).
fn bench_count_distinct(c: &mut Criterion) {
    let graph = build_chain_via_cypher(200);
    let query_str = "MATCH (n:Node) RETURN count(DISTINCT n)";
    let parsed = parse_cypher(query_str).expect("query should parse");
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();

    c.bench_function("bench_count_distinct", |b| {
        b.iter(|| {
            let executor = CypherExecutor::with_params(black_box(&graph), &params, None);
            let result = executor.execute(black_box(&parsed));
            black_box(result)
        });
    });
}

/// Benchmark: edge type access via a GROUP BY query on a chain graph.
/// Targets Arc<HashMap> return from get_edge_type_counts() instead of clone (optimization 3).
fn bench_edge_type_counts(c: &mut Criterion) {
    let graph = build_chain_via_cypher(200);
    let query_str = "MATCH (a)-[r]->(b) RETURN type(r), count(*)";
    let parsed = parse_cypher(query_str).expect("query should parse");
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();

    c.bench_function("bench_edge_type_counts", |b| {
        b.iter(|| {
            let executor = CypherExecutor::with_params(black_box(&graph), &params, None);
            let result = executor.execute(black_box(&parsed));
            black_box(result)
        });
    });
}

/// Benchmark: rand() called once per row on a 50-node graph.
/// Targets thread-local RNG vs re-seeding from SystemTime per call (optimization 4).
fn bench_rand_function(c: &mut Criterion) {
    let graph = make_chain_50();
    let query_str = "MATCH (n:Node) RETURN rand()";
    let parsed = parse_cypher(query_str).expect("query should parse");
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();

    c.bench_function("bench_rand_function", |b| {
        b.iter(|| {
            let executor = CypherExecutor::with_params(black_box(&graph), &params, None);
            let result = executor.execute(black_box(&parsed));
            black_box(result)
        });
    });
}

/// Benchmark: save/load roundtrip for a 20-node graph.
fn bench_save_load_roundtrip(c: &mut Criterion) {
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let path = tmp_dir.path().join("bench.kgl");
    let path_str = path.to_str().unwrap().to_string();

    c.bench_function("save_load_roundtrip_20_nodes", |b| {
        b.iter(|| {
            let mut graph = build_chain_via_cypher(20);
            graph.rebuild_type_indices_and_compact();
            graph.build_connection_types_cache();
            graph.enable_columnar();

            let mut arc = Arc::new(graph);
            prepare_save(&mut arc);
            let graph = Arc::try_unwrap(arc).unwrap_or_else(|_| panic!("sole owner"));

            write_graph_v3(black_box(&graph), black_box(&path_str)).expect("write failed");

            let kg = load_file(black_box(&path_str)).expect("load failed");
            black_box(kg);
        });
    });
}

// ─── registration ─────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_build_graph,
    bench_cypher_parse,
    bench_shortest_path,
    bench_cypher_match,
    bench_cypher_create,
    bench_save_load_roundtrip,
    bench_function_dispatch,
    bench_count_distinct,
    bench_edge_type_counts,
    bench_rand_function,
);
criterion_main!(benches);
