window.BENCHMARK_DATA = {
  "lastUpdate": 1775445997094,
  "repoUrl": "https://github.com/mrmagooey/kglite",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "640316+mrmagooey@users.noreply.github.com.com",
            "name": "mrmagooey"
          },
          "committer": {
            "email": "640316+mrmagooey@users.noreply.github.com.com",
            "name": "mrmagooey"
          },
          "distinct": true,
          "id": "b67cade34df8af5056bf6d1a8dd85c07bc697c60",
          "message": "fix: resolve clippy errors and doc-test failures from pub module exposure\n\nClippy (14 errors fixed):\n- Make TemporalContext, MethodConfig, SpatialResolve pub to satisfy private\n  type in public interface lint (mod.rs, traversal_methods.rs)\n- Add is_empty() to TypedColumn, TypeSchema, EmbeddingStore (column_store.rs,\n  schema.rs)\n- Add Default impls for ResultRow, ResultSet, TypeSchema, SelectionLevel,\n  DirGraph (result.rs, schema.rs)\n- Allow result_unit_err on TypedColumn::push/set (column_store.rs)\n- Allow should_implement_trait on InternedKey::from_str (schema.rs)\n- Replace or_insert_with(TypeSchema::new) with or_default() (schema.rs)\n- Replace len() > 0 with !is_empty() (mod.rs)\n\nDoc-tests (16 failures fixed):\n- Convert RST-style indented code blocks to fenced ```python blocks in\n  result_view.rs and mod.rs\n- Remove extra 4-space indent from fenced code blocks in pymethods_export.rs,\n  pymethods_indexes.rs, mod.rs (caused Rust to misparse as indented blocks)\n\ncargo fmt: apply formatting to benches/graph_benchmarks.rs\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-04-04T16:43:26+11:00",
          "tree_id": "b3dafd313b4e6716b5eb475ea7c5a8678b108aa1",
          "url": "https://github.com/mrmagooey/kglite/commit/b67cade34df8af5056bf6d1a8dd85c07bc697c60"
        },
        "date": 1775282666644,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 946300,
            "range": "± 20391",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6233,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 401,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15837,
            "range": "± 102",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8269,
            "range": "± 202",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 501961,
            "range": "± 21314",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "640316+mrmagooey@users.noreply.github.com.com",
            "name": "mrmagooey"
          },
          "committer": {
            "email": "640316+mrmagooey@users.noreply.github.com.com",
            "name": "mrmagooey"
          },
          "distinct": true,
          "id": "576b4f4e283667552660365d685ca4fbce4ed2ac",
          "message": "fix: allow dot-property access in pattern_matching tokenizer\n\nThe '.' tokenizer arm rejected any '.' not followed by '.' or a digit,\ncausing parse errors on valid Cypher property access syntax (.propName).\nAdd Token::Dot and emit it when '.' is followed by a letter or '_'.\nThe hard error is preserved as the fallback for genuinely unexpected '.'.\n\nFixes: \"Unexpected single '.', expected '..' or a digit\" on queries\nusing map projection or property access patterns like {.property}.\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-04-06T13:15:07+10:00",
          "tree_id": "4b24e1fe6dafe8c9e13115b709f97ef0e2973e3d",
          "url": "https://github.com/mrmagooey/kglite/commit/576b4f4e283667552660365d685ca4fbce4ed2ac"
        },
        "date": 1775445996247,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 941456,
            "range": "± 51545",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6109,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 386,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15923,
            "range": "± 120",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8219,
            "range": "± 129",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 511101,
            "range": "± 20913",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 217490,
            "range": "± 1421",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 42049,
            "range": "± 157",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 125203,
            "range": "± 1222",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 16918,
            "range": "± 110",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 88724,
            "range": "± 1680",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 207232,
            "range": "± 629",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 83269,
            "range": "± 364",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}