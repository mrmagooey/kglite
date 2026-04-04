window.BENCHMARK_DATA = {
  "lastUpdate": 1775282667108,
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
      }
    ]
  }
}