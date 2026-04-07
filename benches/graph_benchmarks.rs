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

/// Benchmark: PropertyStorage iterator — property access on every node during WHERE + RETURN.
/// 200 nodes × 5 properties each. Exercises `keys()` / `iter()` via PropertyKeyIter / PropertyIter
/// enum dispatch instead of heap-allocated Box<dyn Iterator> (optimization 1).
fn bench_property_iter(c: &mut Criterion) {
    // Build a 200-node graph where each node has 5 properties.
    let mut graph = DirGraph::new();
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();
    for i in 0..200usize {
        let q = format!(
            "CREATE (n:Node {{id: {id}, name: 'Node_{id:03}', score: {score}, tag: 'T{tag}', active: true}})",
            id = i,
            score = i * 10,
            tag = i % 10,
        );
        let parsed = parse_cypher(&q).expect("create should parse");
        execute_mutable(&mut graph, &parsed, params.clone(), None).expect("create should succeed");
    }

    // WHERE n.id > 50 triggers property access on every node; RETURN n.name, n.id adds more.
    let query_str = "MATCH (n:Node) WHERE n.id > 50 RETURN n.name, n.id";
    let parsed = parse_cypher(query_str).expect("query should parse");

    c.bench_function("bench_property_iter", |b| {
        b.iter(|| {
            let executor = CypherExecutor::with_params(black_box(&graph), &params, None);
            let result = executor.execute(black_box(&parsed));
            black_box(result)
        });
    });
}

/// Benchmark: `substring()` Cypher function — eliminates intermediate Vec<char> allocation.
/// 200 nodes × 2 substring() calls per row = 400 calls per iteration (optimization 2).
fn bench_substring(c: &mut Criterion) {
    let mut graph = DirGraph::new();
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();
    for i in 0..200usize {
        let q = format!("CREATE (n:Node {{id: {id}, name: 'Node_{id:03}'}})", id = i,);
        let parsed = parse_cypher(&q).expect("create should parse");
        execute_mutable(&mut graph, &parsed, params.clone(), None).expect("create should succeed");
    }

    // substring(n.name, 0, 4) and substring(n.name, 5) — two calls per row × 200 rows
    let query_str = "MATCH (n:Node) RETURN substring(n.name, 0, 4), substring(n.name, 5)";
    let parsed = parse_cypher(query_str).expect("query should parse");

    c.bench_function("bench_substring", |b| {
        b.iter(|| {
            let executor = CypherExecutor::with_params(black_box(&graph), &params, None);
            let result = executor.execute(black_box(&parsed));
            black_box(result)
        });
    });
}

/// Benchmark: `STARTS WITH` string scan — exercises `Value::as_str()` borrowed accessor
/// and `DataFrame::column_name_iter()` zero-alloc iterator (optimizations 3 & 4).
/// 200 nodes scanned, string comparison on each row.
fn bench_property_scan(c: &mut Criterion) {
    let mut graph = DirGraph::new();
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();
    for i in 0..200usize {
        let q = format!("CREATE (n:Node {{id: {id}, name: 'Node_{id:03}'}})", id = i,);
        let parsed = parse_cypher(&q).expect("create should parse");
        execute_mutable(&mut graph, &parsed, params.clone(), None).expect("create should succeed");
    }

    // STARTS WITH triggers as_str() on every row's name value; RETURN n.name exercises column iter
    let query_str = "MATCH (n:Node) WHERE n.name STARTS WITH 'Node' RETURN n.name";
    let parsed = parse_cypher(query_str).expect("query should parse");

    c.bench_function("bench_property_scan", |b| {
        b.iter(|| {
            let executor = CypherExecutor::with_params(black_box(&graph), &params, None);
            let result = executor.execute(black_box(&parsed));
            black_box(result)
        });
    });
}

/// Benchmark: variable-length path expansion across a dense graph.
/// 100 nodes each connected to 3 others via NEXT edges, then queried for
/// all 2-hop paths. Exercises the VLP expansion inner loop where the
/// ANON_VLP_KEYS optimisation eliminates format! allocations.
fn bench_vlp_expansion(c: &mut Criterion) {
    // Build 100 nodes
    let mut graph = DirGraph::new();
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();
    for i in 0..100usize {
        let q = format!("CREATE (n:Node {{id: {id}}})", id = i);
        let parsed = parse_cypher(&q).expect("node create should parse");
        execute_mutable(&mut graph, &parsed, params.clone(), None).expect("node create failed");
    }
    // Each node i connects to (i+1)%100, (i+2)%100, (i+3)%100 — 3 outgoing NEXT edges
    for i in 0..100usize {
        for offset in 1usize..=3 {
            let target = (i + offset) % 100;
            let q = format!(
                "MATCH (a:Node {{id: {a}}}) MATCH (b:Node {{id: {b}}}) CREATE (a)-[:NEXT]->(b)",
                a = i,
                b = target
            );
            let parsed = parse_cypher(&q).expect("edge create should parse");
            execute_mutable(&mut graph, &parsed, params.clone(), None).expect("edge create failed");
        }
    }

    let query_str = "MATCH (a:Node)-[:NEXT*2]->(b:Node) RETURN count(*)";
    let parsed = parse_cypher(query_str).expect("query should parse");

    c.bench_function("bench_vlp_expansion", |b| {
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

/// Benchmark: RETURN n.city, count(n) aggregation over 300 nodes with 5 city groups.
/// This exercises the single-key fast path in execute_return_with_aggregation.
fn bench_group_by_single_key(c: &mut Criterion) {
    // Build graph once outside the timed loop
    let mut graph = DirGraph::new();
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();
    let cities = ["NYC", "LA", "SF", "CHI", "HOU"];
    for i in 0..300usize {
        let city = cities[i % 5];
        let q = format!("CREATE (n:Node {{city: \'{city}\', id: {i}}})");
        let parsed = parse_cypher(&q).expect("create should parse");
        execute_mutable(&mut graph, &parsed, params.clone(), None).expect("create should succeed");
    }

    let query = parse_cypher("MATCH (n:Node) RETURN n.city, count(n)").expect("query should parse");

    c.bench_function("bench_group_by_single_key", |b| {
        b.iter(|| {
            let executor = CypherExecutor::with_params(black_box(&graph), &params, None);
            let result = executor.execute(black_box(&query));
            black_box(result)
        });
    });
}

/// Benchmark: wide aggregation with 6 RETURN items over 10 groups.
/// 300 nodes, category = i % 10 giving 10 groups of 30 nodes each.
/// Query: MATCH (n:Node) RETURN n.category, count(n), sum(n.id), avg(n.id), min(n.id), max(n.id)
/// = 6 RETURN items × 10 groups = 60 column-name computations per query.
/// Exercises the column-name pre-computation optimization (Finding 3).
fn bench_group_aggregate_wide(c: &mut Criterion) {
    let mut graph = DirGraph::new();
    let params: HashMap<String, kglite::datatypes::values::Value> = HashMap::new();
    for i in 0..300usize {
        let category = i % 10;
        let q = format!(
            "CREATE (n:Node {{id: {i}, category: {category}}})",
            i = i,
            category = category,
        );
        let parsed = parse_cypher(&q).expect("create should parse");
        execute_mutable(&mut graph, &parsed, params.clone(), None).expect("create should succeed");
    }

    let query = parse_cypher(
        "MATCH (n:Node) RETURN n.category, count(n), sum(n.id), avg(n.id), min(n.id), max(n.id)",
    )
    .expect("query should parse");

    c.bench_function("bench_group_aggregate_wide", |b| {
        b.iter(|| {
            let executor = CypherExecutor::with_params(black_box(&graph), &params, None);
            let result = executor.execute(black_box(&query));
            black_box(result)
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
    bench_property_iter,
    bench_substring,
    bench_property_scan,
    bench_vlp_expansion,
    bench_group_by_single_key,
    bench_group_aggregate_wide,
);
criterion_main!(benches);
