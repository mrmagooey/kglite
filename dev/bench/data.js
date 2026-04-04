window.BENCHMARK_DATA = {
  "lastUpdate": 1775282550325,
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
          "id": "a50285874702be375f12efde9614701b160b7cd3",
          "message": "fix: restore ClauseStats test import; bump MSRV to 1.82\n\n- window.rs: add ClauseStats import in #[cfg(test)] mod (was incorrectly\n  removed from top-level import by clippy fix; test at line 388 still uses it)\n- Cargo.toml + ci.yml: bump rust-version and MSRV toolchain pin from 1.80\n  to 1.82 (spade transitive dep uses iter::repeat_n, stable since 1.82)\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-04-04T15:24:22+11:00",
          "tree_id": "52cddde6e0d6794439ab13b02bdaafcf9c04d511",
          "url": "https://github.com/mrmagooey/kglite/commit/a50285874702be375f12efde9614701b160b7cd3"
        },
        "date": 1775277197825,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1142.2546916108593,
            "unit": "iter/sec",
            "range": "stddev: 0.000019090443698079046",
            "extra": "mean: 875.4614950101494 usec\nrounds: 501"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 899.6652878407247,
            "unit": "iter/sec",
            "range": "stddev: 0.000024249822457429037",
            "extra": "mean: 1.1115244897356076 msec\nrounds: 682"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 13629.349220181462,
            "unit": "iter/sec",
            "range": "stddev: 0.0000029868203642912795",
            "extra": "mean: 73.37107471861272 usec\nrounds: 6665"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1592.5435817085627,
            "unit": "iter/sec",
            "range": "stddev: 0.0000202232593402205",
            "extra": "mean: 627.9263007214838 usec\nrounds: 971"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 653648.5848151331,
            "unit": "iter/sec",
            "range": "stddev: 2.800623852025606e-7",
            "extra": "mean: 1.5298740381772922 usec\nrounds: 61471"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 130866.2304456929,
            "unit": "iter/sec",
            "range": "stddev: 0.0000010017771489589084",
            "extra": "mean: 7.641390728488827 usec\nrounds: 20989"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2950.950021616578,
            "unit": "iter/sec",
            "range": "stddev: 0.00000680389106965651",
            "extra": "mean: 338.8739194749845 usec\nrounds: 3887"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1571.9256938737067,
            "unit": "iter/sec",
            "range": "stddev: 0.000020097952042417484",
            "extra": "mean: 636.162385981295 usec\nrounds: 1070"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 13654.670222818942,
            "unit": "iter/sec",
            "range": "stddev: 0.0000030920822109050975",
            "extra": "mean: 73.23501656809363 usec\nrounds: 9295"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1557.6916652337,
            "unit": "iter/sec",
            "range": "stddev: 0.000015933465218762888",
            "extra": "mean: 641.9755734200261 usec\nrounds: 1076"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1481.3878842157778,
            "unit": "iter/sec",
            "range": "stddev: 0.00005919149974017313",
            "extra": "mean: 675.0426479486049 usec\nrounds: 1389"
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
          "id": "314227a546206927c6d52fd78841b0205f787e13",
          "message": "update .gitignore",
          "timestamp": "2026-04-04T16:00:06+11:00",
          "tree_id": "3abd151a6da777cf406c2c79f5551f7030ebc30a",
          "url": "https://github.com/mrmagooey/kglite/commit/314227a546206927c6d52fd78841b0205f787e13"
        },
        "date": 1775278942470,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1098.9780563494965,
            "unit": "iter/sec",
            "range": "stddev: 0.00015863933866836757",
            "extra": "mean: 909.9362759996552 usec\nrounds: 500"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 799.8749358147916,
            "unit": "iter/sec",
            "range": "stddev: 0.00002786950165522549",
            "extra": "mean: 1.2501954433430913 msec\nrounds: 706"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 13184.165342886688,
            "unit": "iter/sec",
            "range": "stddev: 0.0000047827690804847195",
            "extra": "mean: 75.84856333279637 usec\nrounds: 6229"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1634.1895925726858,
            "unit": "iter/sec",
            "range": "stddev: 0.0000345096966520024",
            "extra": "mean: 611.9241026530536 usec\nrounds: 867"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 703242.5894620209,
            "unit": "iter/sec",
            "range": "stddev: 4.240931669928013e-7",
            "extra": "mean: 1.4219844119011589 usec\nrounds: 74544"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 135540.50112474227,
            "unit": "iter/sec",
            "range": "stddev: 0.000001131096622539681",
            "extra": "mean: 7.377868546314935 usec\nrounds: 23514"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2898.2442772591726,
            "unit": "iter/sec",
            "range": "stddev: 0.000010716696160845256",
            "extra": "mean: 345.0364787559196 usec\nrounds: 4919"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1585.2249949599961,
            "unit": "iter/sec",
            "range": "stddev: 0.000021935926187605176",
            "extra": "mean: 630.825279174478 usec\nrounds: 1114"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 13666.65134972883,
            "unit": "iter/sec",
            "range": "stddev: 0.000004739329962834124",
            "extra": "mean: 73.17081371361989 usec\nrounds: 10661"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1345.2100664694249,
            "unit": "iter/sec",
            "range": "stddev: 0.00009829187616101994",
            "extra": "mean: 743.3783205507472 usec\nrounds: 1017"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1349.1647828658154,
            "unit": "iter/sec",
            "range": "stddev: 0.000014638563401557373",
            "extra": "mean: 741.1993054516732 usec\nrounds: 1339"
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
          "id": "246b950982aca25f0686b4cfe93c2c5b3c179f0f",
          "message": "fix: apply cargo fmt to io_operations test (props.insert formatting)\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-04-04T16:01:27+11:00",
          "tree_id": "28a846fde4d87d9df749530969eabbefdd7e4dd6",
          "url": "https://github.com/mrmagooey/kglite/commit/246b950982aca25f0686b4cfe93c2c5b3c179f0f"
        },
        "date": 1775279371093,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1086.1998581210019,
            "unit": "iter/sec",
            "range": "stddev: 0.000030764199389990865",
            "extra": "mean: 920.6408862268519 usec\nrounds: 501"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 800.5581927148613,
            "unit": "iter/sec",
            "range": "stddev: 0.000028035431668162852",
            "extra": "mean: 1.249128432011656 msec\nrounds: 706"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 12577.99952775349,
            "unit": "iter/sec",
            "range": "stddev: 0.000005303652749750735",
            "extra": "mean: 79.50389867589747 usec\nrounds: 7175"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1606.2793011507576,
            "unit": "iter/sec",
            "range": "stddev: 0.000021987409407872995",
            "extra": "mean: 622.5567367291529 usec\nrounds: 923"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 692184.3384098589,
            "unit": "iter/sec",
            "range": "stddev: 4.2829622428689965e-7",
            "extra": "mean: 1.4447018583189557 usec\nrounds: 76249"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 132011.28169356854,
            "unit": "iter/sec",
            "range": "stddev: 0.0000012664939793250221",
            "extra": "mean: 7.575110150973703 usec\nrounds: 25919"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2775.758264378207,
            "unit": "iter/sec",
            "range": "stddev: 0.00001112629528040728",
            "extra": "mean: 360.2619193584598 usec\nrounds: 5047"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1568.6251898994365,
            "unit": "iter/sec",
            "range": "stddev: 0.000021372216909371125",
            "extra": "mean: 637.5009189187567 usec\nrounds: 1258"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 12865.664213707023,
            "unit": "iter/sec",
            "range": "stddev: 0.000004383294403034682",
            "extra": "mean: 77.72626297324037 usec\nrounds: 8556"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1257.6359256846417,
            "unit": "iter/sec",
            "range": "stddev: 0.00042794183221068756",
            "extra": "mean: 795.1426796714735 usec\nrounds: 974"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1342.8463452707485,
            "unit": "iter/sec",
            "range": "stddev: 0.000014283953508395782",
            "extra": "mean: 744.6868389088679 usec\nrounds: 1136"
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
          "id": "b67cade34df8af5056bf6d1a8dd85c07bc697c60",
          "message": "fix: resolve clippy errors and doc-test failures from pub module exposure\n\nClippy (14 errors fixed):\n- Make TemporalContext, MethodConfig, SpatialResolve pub to satisfy private\n  type in public interface lint (mod.rs, traversal_methods.rs)\n- Add is_empty() to TypedColumn, TypeSchema, EmbeddingStore (column_store.rs,\n  schema.rs)\n- Add Default impls for ResultRow, ResultSet, TypeSchema, SelectionLevel,\n  DirGraph (result.rs, schema.rs)\n- Allow result_unit_err on TypedColumn::push/set (column_store.rs)\n- Allow should_implement_trait on InternedKey::from_str (schema.rs)\n- Replace or_insert_with(TypeSchema::new) with or_default() (schema.rs)\n- Replace len() > 0 with !is_empty() (mod.rs)\n\nDoc-tests (16 failures fixed):\n- Convert RST-style indented code blocks to fenced ```python blocks in\n  result_view.rs and mod.rs\n- Remove extra 4-space indent from fenced code blocks in pymethods_export.rs,\n  pymethods_indexes.rs, mod.rs (caused Rust to misparse as indented blocks)\n\ncargo fmt: apply formatting to benches/graph_benchmarks.rs\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-04-04T16:43:26+11:00",
          "tree_id": "b3dafd313b4e6716b5eb475ea7c5a8678b108aa1",
          "url": "https://github.com/mrmagooey/kglite/commit/b67cade34df8af5056bf6d1a8dd85c07bc697c60"
        },
        "date": 1775282549474,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1109.966430617837,
            "unit": "iter/sec",
            "range": "stddev: 0.000019680246053186494",
            "extra": "mean: 900.9281473885416 usec\nrounds: 536"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 800.2646217469953,
            "unit": "iter/sec",
            "range": "stddev: 0.000030854000577364795",
            "extra": "mean: 1.249586665242027 msec\nrounds: 702"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 12999.094086295385,
            "unit": "iter/sec",
            "range": "stddev: 0.000008738991025911333",
            "extra": "mean: 76.92843773277053 usec\nrounds: 7516"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1576.6555346826262,
            "unit": "iter/sec",
            "range": "stddev: 0.00002308771747835966",
            "extra": "mean: 634.2539495802395 usec\nrounds: 952"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 695781.5840084944,
            "unit": "iter/sec",
            "range": "stddev: 3.9490233397047637e-7",
            "extra": "mean: 1.4372326359068905 usec\nrounds: 73557"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 136001.8125766214,
            "unit": "iter/sec",
            "range": "stddev: 0.0000011042630869721464",
            "extra": "mean: 7.352843179473178 usec\nrounds: 23766"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2891.4604546732785,
            "unit": "iter/sec",
            "range": "stddev: 0.000012327320062986467",
            "extra": "mean: 345.84598879219163 usec\nrounds: 5175"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1567.5420189444912,
            "unit": "iter/sec",
            "range": "stddev: 0.000023876371918308484",
            "extra": "mean: 637.9414318177912 usec\nrounds: 1232"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 13530.550292137179,
            "unit": "iter/sec",
            "range": "stddev: 0.000005551424554874794",
            "extra": "mean: 73.90682406916709 usec\nrounds: 11550"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1296.9538267634,
            "unit": "iter/sec",
            "range": "stddev: 0.0001126790643153999",
            "extra": "mean: 771.0374720860648 usec\nrounds: 1021"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1321.0607395389861,
            "unit": "iter/sec",
            "range": "stddev: 0.0000210156836462854",
            "extra": "mean: 756.9674656662436 usec\nrounds: 1267"
          }
        ]
      }
    ]
  }
}