window.BENCHMARK_DATA = {
  "lastUpdate": 1775945695474,
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
          "id": "0880e2358304efac71dd633388163a95a6adb3fd",
          "message": "fix: HAVING clause now correctly evaluates aggregate expressions post-aggregation\n\nHAVING count(n) > N and similar predicates previously raised \"Aggregate\nfunction cannot be used outside of RETURN/WITH\" because evaluate_expression\ndispatched aggregate names to evaluate_scalar_function. In the fused path\nthis was swallowed by unwrap_or(false), silently dropping all rows; in the\ngeneral path it propagated as an error.\n\nFix: before scalar dispatch, check if the expression is an aggregate and\nlook up the pre-computed value from row.projected using expression_to_string\nas the key — consistent with standard HAVING semantics.\n\nAlso adds regression tests for 13 other confirmed-working behaviours\n(shortestPath multi-type, WHERE pushdown + aggregation, multi-hop path\nvariables) so future regressions are caught immediately.\n\nUpdates CYPHER.md: labels() return type, label model description,\nfunction name case-insensitivity, rand() per-row distinctness,\nsubstring() Unicode correctness, SET/REMOVE label support, XOR/!=\noperators, and architectural differences table.\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-04-06T14:23:49+10:00",
          "tree_id": "f61348f6c7a1b60df6aaf7a8e02f75f67f21265f",
          "url": "https://github.com/mrmagooey/kglite/commit/0880e2358304efac71dd633388163a95a6adb3fd"
        },
        "date": 1775449724779,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 968974,
            "range": "± 5750",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6382,
            "range": "± 76",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 387,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15815,
            "range": "± 104",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8813,
            "range": "± 51",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 522924,
            "range": "± 20517",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 224449,
            "range": "± 2297",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 42477,
            "range": "± 300",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 125716,
            "range": "± 693",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 17049,
            "range": "± 37",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 88000,
            "range": "± 3320",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 219945,
            "range": "± 523",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 81464,
            "range": "± 1745",
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
          "id": "4acd4bc6a4aa15a79ed8ebb8f4b209990ff72900",
          "message": "merge: integrate performance optimizations and fixes from fix/path-node-properties\n\n- Complete secondary label index: O(1) lookup via secondary_label_index HashMap,\n  has_secondary_labels fast-skip flag eliminates O(N) scan on single-label graphs\n- Eliminate format! alloc in VLP BFS loop with ANON_VLP_KEYS static table (~6% on path queries)\n- Store EdgeIndex in PathBinding: captures edge.id() at BFS time, eliminates per-hop adjacency scan\n- Fix map-properties save/load: load_v3 now restores PropertyStorage::Map nodes from disk\n- Fix labels(n) to merge __kinds property into returned label list\n- Fix path nodes to emit full properties in path results\n- Fix FFI save/load to call enable_columnar() before writing v3 format\n- Add 14 new tests covering secondary label index, VLP edge cases, PathBinding changes, and save/load backward compat\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-04-07T13:54:25+10:00",
          "tree_id": "06fa5f4a06383a8050460e92283754f0f5c769ec",
          "url": "https://github.com/mrmagooey/kglite/commit/4acd4bc6a4aa15a79ed8ebb8f4b209990ff72900"
        },
        "date": 1775534428747,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 944504,
            "range": "± 5534",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6242,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 399,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15436,
            "range": "± 1052",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8504,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 519131,
            "range": "± 35470",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 221914,
            "range": "± 3223",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 41662,
            "range": "± 396",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 128505,
            "range": "± 463",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 18020,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 92494,
            "range": "± 741",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 220931,
            "range": "± 3006",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 84060,
            "range": "± 335",
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
          "id": "e79413101747fea06464eb12d310b1a4bf119aef",
          "message": "fix: improve __kinds secondary label matching, NULL logic, and path construction\n\n- pattern_matching: use node_matches_label() for edge target node type\n  checks so nodes with the label in __kinds are matched when traversing\n  relationships (fixes Azure AZServicePrincipal, Domain trust queries)\n\n- pattern_matching: always scan for __kinds nodes in find_matching_nodes\n  even when secondary_label_index has entries for the label, so nodes\n  that gained a label via SET __kinds are discoverable alongside nodes\n  with the label as extra_labels (fixes MATCH (n:Group) after analysis)\n\n- executor: implement three-valued NULL propagation for NOT + string\n  predicates (Contains/StartsWith/EndsWith) — NOT (NULL CONTAINS x)\n  now returns false (excluded) instead of true, matching Neo4j semantics\n\n- executor: deduplicate undirected shortestPath pairs to avoid returning\n  both (A,B) and (B,A) paths for the same node pair\n\n- executor: append fixed-length edges after VLP in path assignments so\n  p=(u)-[*0..4]->()-[r:AdminTo]->(c) includes the AdminTo edge in the\n  path, not just the variable-length portion\n\n- executor: set has_secondary_labels flag when __kinds is SET via Cypher\n  so the pattern executor uses the __kinds fallback scan\n\n- executor: resolve n.name from property storage before falling back to\n  node title, matching Neo4j semantics for property access\n\n- executor: preserve __kinds insertion order in labels() output instead\n  of sorting alphabetically, matching Neo4j label ordering\n\n- parser: add test coverage for inline relationship properties and\n  negative pattern predicates\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-04-07T18:31:20+10:00",
          "tree_id": "f73b123fba054849af5050da6bf49f6abc22e38e",
          "url": "https://github.com/mrmagooey/kglite/commit/e79413101747fea06464eb12d310b1a4bf119aef"
        },
        "date": 1775551198199,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 947345,
            "range": "± 6132",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6108,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 382,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15355,
            "range": "± 253",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8611,
            "range": "± 91",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 521156,
            "range": "± 26945",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 247555,
            "range": "± 1572",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 41376,
            "range": "± 721",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 127217,
            "range": "± 737",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 17006,
            "range": "± 34",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 97285,
            "range": "± 523",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 247194,
            "range": "± 2020",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 106696,
            "range": "± 779",
            "unit": "ns/iter"
          },
          {
            "name": "bench_vlp_expansion",
            "value": 452639,
            "range": "± 2508",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_by_single_key",
            "value": 106132,
            "range": "± 669",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_aggregate_wide",
            "value": 144855,
            "range": "± 659",
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
          "id": "0739e494a671a882839cbb336491750f4a6a7ead",
          "message": "chore: apply cargo fmt and fix clippy iter_cloned_collect warning\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-04-07T19:09:33+10:00",
          "tree_id": "497552f9af800c48275567da57d139d0d7290cb8",
          "url": "https://github.com/mrmagooey/kglite/commit/0739e494a671a882839cbb336491750f4a6a7ead"
        },
        "date": 1775553302705,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 953867,
            "range": "± 8459",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6180,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 385,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15855,
            "range": "± 382",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8506,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 531721,
            "range": "± 75062",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 245858,
            "range": "± 4567",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 40843,
            "range": "± 1259",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 125645,
            "range": "± 1745",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 17085,
            "range": "± 98",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 97183,
            "range": "± 1736",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 244620,
            "range": "± 3153",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 106821,
            "range": "± 783",
            "unit": "ns/iter"
          },
          {
            "name": "bench_vlp_expansion",
            "value": 461300,
            "range": "± 10318",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_by_single_key",
            "value": 107793,
            "range": "± 1646",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_aggregate_wide",
            "value": 143386,
            "range": "± 3121",
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
          "id": "af4431af9212c1f19ea226ab16a2ce54564919c4",
          "message": "docs: update documentation to reflect multi-label support and __kinds absorption\n\n- Remove all \"single-label only\" claims (index.md, core-concepts, cypher guide, import-export)\n- Replace \"Why single-label nodes\" design rationale with \"Label model\" section\n- Update CYPHER.md label model description, function table, and architecture comparison\n- Update CHANGELOG [Unreleased] with __kinds absorption and index fix entries\n- Fix cypher guide \"Not supported\" list (SET n:Label and multi-label are now supported)\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-04-08T06:57:51+10:00",
          "tree_id": "e8083e030b124a74a68a641da8e3e50b7c2aad9d",
          "url": "https://github.com/mrmagooey/kglite/commit/af4431af9212c1f19ea226ab16a2ce54564919c4"
        },
        "date": 1775605780695,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 969795,
            "range": "± 4563",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6202,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 406,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15963,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8875,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 502813,
            "range": "± 17105",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 247989,
            "range": "± 1402",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 43764,
            "range": "± 155",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 135866,
            "range": "± 894",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 16797,
            "range": "± 160",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 100331,
            "range": "± 418",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 238255,
            "range": "± 1015",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 111799,
            "range": "± 1016",
            "unit": "ns/iter"
          },
          {
            "name": "bench_vlp_expansion",
            "value": 451564,
            "range": "± 4039",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_by_single_key",
            "value": 108924,
            "range": "± 449",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_aggregate_wide",
            "value": 145434,
            "range": "± 664",
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
          "id": "e489d422bf18c7cad434d3b90b41463b7ee3a526",
          "message": "fix: update test to populate secondary_label_index after direct mutation\n\nThe dirgraph_nodes_matching_label_extra_labels test directly pushed to\nextra_labels without updating secondary_label_index. This worked with\nthe old O(N) scan but fails with the new O(1) index lookup.\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-04-08T09:50:51+10:00",
          "tree_id": "89550246653ebd51d573f69f65e3d23b3582eafb",
          "url": "https://github.com/mrmagooey/kglite/commit/e489d422bf18c7cad434d3b90b41463b7ee3a526"
        },
        "date": 1775606203802,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 948345,
            "range": "± 4910",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6358,
            "range": "± 584",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 392,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15965,
            "range": "± 58",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8642,
            "range": "± 203",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 500018,
            "range": "± 22762",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 253452,
            "range": "± 1929",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 41429,
            "range": "± 103",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 124423,
            "range": "± 1454",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 16775,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 96881,
            "range": "± 405",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 247613,
            "range": "± 602",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 105026,
            "range": "± 1077",
            "unit": "ns/iter"
          },
          {
            "name": "bench_vlp_expansion",
            "value": 452429,
            "range": "± 1586",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_by_single_key",
            "value": 107649,
            "range": "± 624",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_aggregate_wide",
            "value": 143168,
            "range": "± 2714",
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
          "id": "84833d21dda532d5816705057a85302b7e6b1ac9",
          "message": "fix: store type/node_type/label as regular properties instead of rejecting\n\nSET n.type, SET n.node_type, and SET n.label previously raised \"Cannot\nSET node type via property assignment\", blocking callers that pass\nunfiltered JSON properties through to Cypher (e.g. Okta/GitHub ingest).\n\nNow these values are stored as regular properties in PropertyStorage.\nThe virtual read (n.type in RETURN/WHERE) still returns the primary\nlabel, so stored values are shadowed on direct access but preserved in\nkeys(n) and property iteration. REMOVE n.type likewise removes the\nstored property. Deduplication added to keys(n) and AllProperties map\nprojection to prevent \"type\" appearing twice.\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-04-08T14:26:54+10:00",
          "tree_id": "c9ea2057cbd16260b8d413b6216fb4bc0b91d78b",
          "url": "https://github.com/mrmagooey/kglite/commit/84833d21dda532d5816705057a85302b7e6b1ac9"
        },
        "date": 1775622744800,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 967217,
            "range": "± 4013",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6155,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 401,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15628,
            "range": "± 98",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8482,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 506106,
            "range": "± 9284",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 250165,
            "range": "± 1545",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 41032,
            "range": "± 506",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 127931,
            "range": "± 702",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 17138,
            "range": "± 157",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 95335,
            "range": "± 616",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 249355,
            "range": "± 1175",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 104437,
            "range": "± 279",
            "unit": "ns/iter"
          },
          {
            "name": "bench_vlp_expansion",
            "value": 439191,
            "range": "± 2922",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_by_single_key",
            "value": 104714,
            "range": "± 4409",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_aggregate_wide",
            "value": 143154,
            "range": "± 896",
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
          "id": "7dbf3b2b5892512530b7f54aa1e4b6172cc3ab73",
          "message": "refactor: rework __kinds handling and anonymous edge path synthesis\n\n- Keep __kinds in PropertyStorage instead of absorbing into extra_labels\n  at ingestion time. This preserves the original data and simplifies the\n  mutation path (SET __kinds no longer needs special-case expansion).\n- node_matches_label() now checks __kinds property directly via JSON\n  parse, so MATCH queries still find nodes by secondary kind labels.\n- build_labels_string() and node_to_path_json() merge __kinds into the\n  label list at read time (sorted, deduplicated).\n- resolve_node_property(\"type\") now checks stored property first, falling\n  back to virtual node_type. This lets BloodHound datasets that store\n  \"type\" as a domain property (e.g. type=\"Organization\") read it back.\n- node_type/label remain rejected in SET (only \"type\" is allowed as a\n  user property); REMOVE n.type/node_type/label is re-rejected.\n- Add ANON_EDGE_KEYS and synthesize_path_from_anon_edges() for correct\n  path reconstruction when MATCH p=()-[:REL]->() uses anonymous edges.\n- Planner marks all anonymous edges (not just VLP) as needs_path_info=false\n  when no path assignment exists, reducing binding overhead.\n- find_matching_nodes() now includes an O(N) fallback scan for __kinds\n  nodes not covered by secondary_label_index.\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-04-09T10:40:53+10:00",
          "tree_id": "5695f975914d7ea385e23725d9bde2c89dfe941d",
          "url": "https://github.com/mrmagooey/kglite/commit/7dbf3b2b5892512530b7f54aa1e4b6172cc3ab73"
        },
        "date": 1775695729136,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 954730,
            "range": "± 4818",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6260,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 386,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15190,
            "range": "± 58",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8582,
            "range": "± 37",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 513111,
            "range": "± 17084",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 243741,
            "range": "± 2107",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 41164,
            "range": "± 160",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 124292,
            "range": "± 405",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 17049,
            "range": "± 57",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 95614,
            "range": "± 668",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 238759,
            "range": "± 638",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 104981,
            "range": "± 437",
            "unit": "ns/iter"
          },
          {
            "name": "bench_vlp_expansion",
            "value": 446220,
            "range": "± 1627",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_by_single_key",
            "value": 105631,
            "range": "± 671",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_aggregate_wide",
            "value": 142533,
            "range": "± 996",
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
          "id": "05dbc9d574d34ae983a41e57d53bbbf1175a9353",
          "message": "fix: resolve panics and lint issues from __kinds refactor\n\n- Anonymous edge bindings now populate connection_type from actual edge\n  data instead of InternedKey::default(), fixing a panic in\n  pattern_matches_to_pylist when resolving unregistered interned keys.\n- Update test_kinds_expanded_at_ingestion to expect __kinds retained in\n  storage (matching the new read-time label merging approach).\n- Fix clippy explicit_auto_deref warning (*key → key).\n- Apply cargo fmt formatting.\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-04-09T10:51:28+10:00",
          "tree_id": "ba93875b2e6ec5393a1377c18668a5bf9ce5ea30",
          "url": "https://github.com/mrmagooey/kglite/commit/05dbc9d574d34ae983a41e57d53bbbf1175a9353"
        },
        "date": 1775696220350,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 982334,
            "range": "± 18181",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6469,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 407,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15084,
            "range": "± 98",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8727,
            "range": "± 187",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 545642,
            "range": "± 18312",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 241868,
            "range": "± 1096",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 43425,
            "range": "± 85",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 132430,
            "range": "± 266",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 17003,
            "range": "± 33",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 98205,
            "range": "± 786",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 233016,
            "range": "± 531",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 110547,
            "range": "± 486",
            "unit": "ns/iter"
          },
          {
            "name": "bench_vlp_expansion",
            "value": 450531,
            "range": "± 1627",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_by_single_key",
            "value": 107559,
            "range": "± 377",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_aggregate_wide",
            "value": 146567,
            "range": "± 2643",
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
          "id": "b146f06056911769dbde77bdb4bedc81ce3d6501",
          "message": "chore: apply cargo fmt\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-04-09T16:53:15+10:00",
          "tree_id": "96ccdeb32001d131d00e20e464a975829b6a8a33",
          "url": "https://github.com/mrmagooey/kglite/commit/b146f06056911769dbde77bdb4bedc81ce3d6501"
        },
        "date": 1775717911843,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 947506,
            "range": "± 22543",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 5896,
            "range": "± 153",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 378,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15852,
            "range": "± 344",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8598,
            "range": "± 211",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 515428,
            "range": "± 12706",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 245351,
            "range": "± 5178",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 41587,
            "range": "± 871",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 125258,
            "range": "± 2372",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 16697,
            "range": "± 384",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 95973,
            "range": "± 2121",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 244047,
            "range": "± 4089",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 105899,
            "range": "± 2105",
            "unit": "ns/iter"
          },
          {
            "name": "bench_vlp_expansion",
            "value": 444333,
            "range": "± 7904",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_by_single_key",
            "value": 103854,
            "range": "± 2896",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_aggregate_wide",
            "value": 141750,
            "range": "± 3421",
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
          "id": "1faa6c94c377dc61e117c5f2a5405e4ed8d86ea4",
          "message": "fix: add property-only fallback in MERGE to prevent cross-label duplicates\n\nWhen MERGE (n:SCIM {objectid: X}) encounters an existing (m:Base {objectid: X})\nnode with no SCIM label, the label-scoped lookups all fail and a duplicate node\nis created. This adds a property-only fallback (both index-based and linear scan)\nthat finds the existing node by property alone when all label-scoped lookups miss.\nThe caller's SET clause then adds the missing label.\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-04-12T08:09:40+10:00",
          "tree_id": "8496a44439a0c1ba59a48c86efaea79cd8a04b86",
          "url": "https://github.com/mrmagooey/kglite/commit/1faa6c94c377dc61e117c5f2a5405e4ed8d86ea4"
        },
        "date": 1775945695157,
        "tool": "cargo",
        "benches": [
          {
            "name": "build_graph_100_nodes_cypher",
            "value": 954827,
            "range": "± 10691",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_parse_match_where_return",
            "value": 6638,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "shortest_path_cost_chain_50",
            "value": 384,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_match_node_scan_50",
            "value": 15476,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "cypher_create_5_nodes",
            "value": 8379,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "save_load_roundtrip_20_nodes",
            "value": 507793,
            "range": "± 14941",
            "unit": "ns/iter"
          },
          {
            "name": "bench_function_dispatch",
            "value": 243745,
            "range": "± 3425",
            "unit": "ns/iter"
          },
          {
            "name": "bench_count_distinct",
            "value": 40620,
            "range": "± 4020",
            "unit": "ns/iter"
          },
          {
            "name": "bench_edge_type_counts",
            "value": 122415,
            "range": "± 613",
            "unit": "ns/iter"
          },
          {
            "name": "bench_rand_function",
            "value": 16754,
            "range": "± 51",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_iter",
            "value": 94132,
            "range": "± 425",
            "unit": "ns/iter"
          },
          {
            "name": "bench_substring",
            "value": 242313,
            "range": "± 2168",
            "unit": "ns/iter"
          },
          {
            "name": "bench_property_scan",
            "value": 104653,
            "range": "± 3405",
            "unit": "ns/iter"
          },
          {
            "name": "bench_vlp_expansion",
            "value": 439619,
            "range": "± 2140",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_by_single_key",
            "value": 104267,
            "range": "± 393",
            "unit": "ns/iter"
          },
          {
            "name": "bench_group_aggregate_wide",
            "value": 141195,
            "range": "± 565",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}