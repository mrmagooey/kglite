window.BENCHMARK_DATA = {
  "lastUpdate": 1775695539355,
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
        "date": 1775445835836,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1081.808426499488,
            "unit": "iter/sec",
            "range": "stddev: 0.000019997051318811744",
            "extra": "mean: 924.3780834983849 usec\nrounds: 503"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 791.8233292252258,
            "unit": "iter/sec",
            "range": "stddev: 0.000027036256155598235",
            "extra": "mean: 1.2629079784482589 msec\nrounds: 696"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 12304.64304605584,
            "unit": "iter/sec",
            "range": "stddev: 0.000004501936017008121",
            "extra": "mean: 81.27013487973895 usec\nrounds: 6984"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1589.3936203070136,
            "unit": "iter/sec",
            "range": "stddev: 0.00010720463569933643",
            "extra": "mean: 629.1707650159285 usec\nrounds: 949"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 701300.5013756528,
            "unit": "iter/sec",
            "range": "stddev: 4.878440975662674e-7",
            "extra": "mean: 1.4259222659023143 usec\nrounds: 71706"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 136091.87234493173,
            "unit": "iter/sec",
            "range": "stddev: 0.000001077205839634753",
            "extra": "mean: 7.347977382994992 usec\nrounds: 22461"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2765.229187767326,
            "unit": "iter/sec",
            "range": "stddev: 0.000010498369607950288",
            "extra": "mean: 361.6336773905566 usec\nrounds: 4476"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1580.286940223332,
            "unit": "iter/sec",
            "range": "stddev: 0.00004589782821590314",
            "extra": "mean: 632.7964716703134 usec\nrounds: 1359"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 12440.550758326905,
            "unit": "iter/sec",
            "range": "stddev: 0.0000050127155109673015",
            "extra": "mean: 80.38229331050029 usec\nrounds: 10569"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1375.9415306072415,
            "unit": "iter/sec",
            "range": "stddev: 0.0001874964091018661",
            "extra": "mean: 726.7750683843899 usec\nrounds: 1009"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1376.9824932645831,
            "unit": "iter/sec",
            "range": "stddev: 0.0000661640795453938",
            "extra": "mean: 726.225645490362 usec\nrounds: 1275"
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
        "date": 1775449576666,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1128.601385698063,
            "unit": "iter/sec",
            "range": "stddev: 0.000024436692574012925",
            "extra": "mean: 886.0524297349499 usec\nrounds: 491"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 896.3282704429122,
            "unit": "iter/sec",
            "range": "stddev: 0.000026129745693162888",
            "extra": "mean: 1.1156626795960138 msec\nrounds: 593"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 13720.470349927687,
            "unit": "iter/sec",
            "range": "stddev: 0.0000028858242022309472",
            "extra": "mean: 72.8837987689883 usec\nrounds: 4711"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1459.363527978494,
            "unit": "iter/sec",
            "range": "stddev: 0.0001496050305033366",
            "extra": "mean: 685.230225936369 usec\nrounds: 748"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 630087.1166406268,
            "unit": "iter/sec",
            "range": "stddev: 2.9619925975106933e-7",
            "extra": "mean: 1.5870821249791636 usec\nrounds: 57449"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 133475.48022620342,
            "unit": "iter/sec",
            "range": "stddev: 7.263471608426735e-7",
            "extra": "mean: 7.492012752494174 usec\nrounds: 18898"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2957.1165664107543,
            "unit": "iter/sec",
            "range": "stddev: 0.00004609125018665433",
            "extra": "mean: 338.16725771272695 usec\nrounds: 2658"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1480.0465514233592,
            "unit": "iter/sec",
            "range": "stddev: 0.00005676912688270436",
            "extra": "mean: 675.6544238681555 usec\nrounds: 972"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 14743.350879876272,
            "unit": "iter/sec",
            "range": "stddev: 0.000002724657794592375",
            "extra": "mean: 67.82718583771454 usec\nrounds: 5790"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1507.6649013009517,
            "unit": "iter/sec",
            "range": "stddev: 0.0001062767278357748",
            "extra": "mean: 663.2773629850428 usec\nrounds: 978"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1522.3693921390393,
            "unit": "iter/sec",
            "range": "stddev: 0.000016837153629853622",
            "extra": "mean: 656.8707996650717 usec\nrounds: 1198"
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
        "date": 1775534283884,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1079.3144033599333,
            "unit": "iter/sec",
            "range": "stddev: 0.00001622441742986406",
            "extra": "mean: 926.5140879126365 usec\nrounds: 546"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 792.533788664865,
            "unit": "iter/sec",
            "range": "stddev: 0.00003136196401185314",
            "extra": "mean: 1.2617758565027757 msec\nrounds: 669"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 13777.910825574087,
            "unit": "iter/sec",
            "range": "stddev: 0.000004070858423618511",
            "extra": "mean: 72.57994427891303 usec\nrounds: 7394"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1608.965237920916,
            "unit": "iter/sec",
            "range": "stddev: 0.000018717820028943736",
            "extra": "mean: 621.5174675197999 usec\nrounds: 1016"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 693142.0137965697,
            "unit": "iter/sec",
            "range": "stddev: 4.483492343941982e-7",
            "extra": "mean: 1.4427057949101467 usec\nrounds: 77310"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 132775.61450582708,
            "unit": "iter/sec",
            "range": "stddev: 0.0000011573353597094743",
            "extra": "mean: 7.53150345959132 usec\nrounds: 33675"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2762.254301925441,
            "unit": "iter/sec",
            "range": "stddev: 0.000011580476130178565",
            "extra": "mean: 362.02314873867545 usec\nrounds: 4955"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1573.2158581073022,
            "unit": "iter/sec",
            "range": "stddev: 0.00008212902372410802",
            "extra": "mean: 635.6406813767284 usec\nrounds: 1337"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 14510.230792923327,
            "unit": "iter/sec",
            "range": "stddev: 0.00000971142089225787",
            "extra": "mean: 68.91689141758533 usec\nrounds: 11908"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1357.5982750323922,
            "unit": "iter/sec",
            "range": "stddev: 0.00002946827591121613",
            "extra": "mean: 736.5949253111272 usec\nrounds: 964"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1361.1689735481248,
            "unit": "iter/sec",
            "range": "stddev: 0.000015717936945657997",
            "extra": "mean: 734.6626461763416 usec\nrounds: 1334"
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
        "date": 1775551014195,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1086.684872759302,
            "unit": "iter/sec",
            "range": "stddev: 0.000029217638642963798",
            "extra": "mean: 920.2299811727457 usec\nrounds: 478"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 794.7879973029845,
            "unit": "iter/sec",
            "range": "stddev: 0.00002823449130734252",
            "extra": "mean: 1.258197158730853 msec\nrounds: 693"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 14342.720745706396,
            "unit": "iter/sec",
            "range": "stddev: 0.000004850684045429736",
            "extra": "mean: 69.72177857533465 usec\nrounds: 7244"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1643.0721880430845,
            "unit": "iter/sec",
            "range": "stddev: 0.000020955892782573982",
            "extra": "mean: 608.6159861247545 usec\nrounds: 1009"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 668568.5661646071,
            "unit": "iter/sec",
            "range": "stddev: 4.233672426090956e-7",
            "extra": "mean: 1.4957328995240136 usec\nrounds: 70393"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 132613.7952188068,
            "unit": "iter/sec",
            "range": "stddev: 0.0000013309804921404067",
            "extra": "mean: 7.540693623540786 usec\nrounds: 21219"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2884.103613773035,
            "unit": "iter/sec",
            "range": "stddev: 0.000028077805891653767",
            "extra": "mean: 346.72818106274013 usec\nrounds: 5175"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1594.581973603957,
            "unit": "iter/sec",
            "range": "stddev: 0.00003024587901626869",
            "extra": "mean: 627.1236076624355 usec\nrounds: 1305"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 14610.114142204011,
            "unit": "iter/sec",
            "range": "stddev: 0.000004217013865899588",
            "extra": "mean: 68.44573493860089 usec\nrounds: 9926"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1318.3333054687769,
            "unit": "iter/sec",
            "range": "stddev: 0.0001510233491418825",
            "extra": "mean: 758.5335179288496 usec\nrounds: 1004"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1344.0688974257878,
            "unit": "iter/sec",
            "range": "stddev: 0.000015069461542296302",
            "extra": "mean: 744.0094789152836 usec\nrounds: 1328"
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
        "date": 1775553117640,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1056.7559202491739,
            "unit": "iter/sec",
            "range": "stddev: 0.00002452163626042867",
            "extra": "mean: 946.2923091684299 usec\nrounds: 469"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 778.5141935488413,
            "unit": "iter/sec",
            "range": "stddev: 0.00003763608077426645",
            "extra": "mean: 1.2844980968702964 msec\nrounds: 671"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 13449.133888611279,
            "unit": "iter/sec",
            "range": "stddev: 0.000005597255761394673",
            "extra": "mean: 74.35423041232414 usec\nrounds: 5603"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1515.7414249460417,
            "unit": "iter/sec",
            "range": "stddev: 0.000033614570139769164",
            "extra": "mean: 659.7431353013253 usec\nrounds: 813"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 685846.24207227,
            "unit": "iter/sec",
            "range": "stddev: 5.313582856244817e-7",
            "extra": "mean: 1.4580527509759635 usec\nrounds: 66103"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 134588.24863338826,
            "unit": "iter/sec",
            "range": "stddev: 0.0000015455031317996835",
            "extra": "mean: 7.430069193663042 usec\nrounds: 19337"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2721.242451221137,
            "unit": "iter/sec",
            "range": "stddev: 0.000024451037880831037",
            "extra": "mean: 367.4792003745413 usec\nrounds: 4806"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1503.6413992088706,
            "unit": "iter/sec",
            "range": "stddev: 0.0000878304139452713",
            "extra": "mean: 665.0521863298937 usec\nrounds: 1229"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 14154.045172049267,
            "unit": "iter/sec",
            "range": "stddev: 0.000005227525376648476",
            "extra": "mean: 70.6511804819411 usec\nrounds: 10001"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1284.5751913616105,
            "unit": "iter/sec",
            "range": "stddev: 0.00016352842753475477",
            "extra": "mean: 778.4674705884913 usec\nrounds: 969"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1343.138257559331,
            "unit": "iter/sec",
            "range": "stddev: 0.00001881787202711031",
            "extra": "mean: 744.5249916543506 usec\nrounds: 1318"
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
        "date": 1775605586235,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1069.346048582712,
            "unit": "iter/sec",
            "range": "stddev: 0.000024673229292790462",
            "extra": "mean: 935.1509750518818 usec\nrounds: 481"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 787.6282358097633,
            "unit": "iter/sec",
            "range": "stddev: 0.00003276346664152197",
            "extra": "mean: 1.2696345236682591 msec\nrounds: 676"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 13339.585999280891,
            "unit": "iter/sec",
            "range": "stddev: 0.0000049441629295142436",
            "extra": "mean: 74.96484523986786 usec\nrounds: 6985"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1548.0005730570408,
            "unit": "iter/sec",
            "range": "stddev: 0.000024611649421767897",
            "extra": "mean: 645.9945928993864 usec\nrounds: 845"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 643907.6452739245,
            "unit": "iter/sec",
            "range": "stddev: 4.4454494389443214e-7",
            "extra": "mean: 1.5530177461623251 usec\nrounds: 58886"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 129804.57929601145,
            "unit": "iter/sec",
            "range": "stddev: 0.0000012550444490262301",
            "extra": "mean: 7.703888456196609 usec\nrounds: 20288"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2817.204954852533,
            "unit": "iter/sec",
            "range": "stddev: 0.00003710453911380669",
            "extra": "mean: 354.961749686524 usec\nrounds: 4786"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1592.3612873704485,
            "unit": "iter/sec",
            "range": "stddev: 0.000023525308860807348",
            "extra": "mean: 627.9981860469326 usec\nrounds: 1290"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 14362.904877080038,
            "unit": "iter/sec",
            "range": "stddev: 0.000004296560524545432",
            "extra": "mean: 69.62379884557858 usec\nrounds: 11782"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1315.0483503457492,
            "unit": "iter/sec",
            "range": "stddev: 0.0001691032676406339",
            "extra": "mean: 760.428314089807 usec\nrounds: 1022"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1345.861844019215,
            "unit": "iter/sec",
            "range": "stddev: 0.000014468140061755718",
            "extra": "mean: 743.0183153225072 usec\nrounds: 1240"
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
        "date": 1775606020852,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1069.9599702146475,
            "unit": "iter/sec",
            "range": "stddev: 0.000017364734201106645",
            "extra": "mean: 934.6144041252192 usec\nrounds: 485"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 794.549871379619,
            "unit": "iter/sec",
            "range": "stddev: 0.000030055652007613233",
            "extra": "mean: 1.2585742393534682 msec\nrounds: 681"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 14065.46759052361,
            "unit": "iter/sec",
            "range": "stddev: 0.000019784291787428165",
            "extra": "mean: 71.09610779479057 usec\nrounds: 7069"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1614.4527154359344,
            "unit": "iter/sec",
            "range": "stddev: 0.000022533192153311582",
            "extra": "mean: 619.4049478432572 usec\nrounds: 997"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 725845.4670171929,
            "unit": "iter/sec",
            "range": "stddev: 3.9183965448126544e-7",
            "extra": "mean: 1.3777037199246065 usec\nrounds: 70943"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 130607.39770934581,
            "unit": "iter/sec",
            "range": "stddev: 0.0000013144364747468904",
            "extra": "mean: 7.656534143842324 usec\nrounds: 19579"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2904.5708705749757,
            "unit": "iter/sec",
            "range": "stddev: 0.00001344083802176689",
            "extra": "mean: 344.28493727957976 usec\nrounds: 4815"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1589.9895187488034,
            "unit": "iter/sec",
            "range": "stddev: 0.000026320840438796057",
            "extra": "mean: 628.9349635379491 usec\nrounds: 1289"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 14727.546943884263,
            "unit": "iter/sec",
            "range": "stddev: 0.000004457175805297565",
            "extra": "mean: 67.89997029445955 usec\nrounds: 11917"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1286.9493093055496,
            "unit": "iter/sec",
            "range": "stddev: 0.00028876931118928037",
            "extra": "mean: 777.0313817096725 usec\nrounds: 1006"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1356.7748894842302,
            "unit": "iter/sec",
            "range": "stddev: 0.0000152720778997355",
            "extra": "mean: 737.0419424405355 usec\nrounds: 1303"
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
        "date": 1775622566357,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1064.3727353576426,
            "unit": "iter/sec",
            "range": "stddev: 0.00001898583114210329",
            "extra": "mean: 939.5204957631572 usec\nrounds: 472"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 786.3745144560982,
            "unit": "iter/sec",
            "range": "stddev: 0.00002892945766960148",
            "extra": "mean: 1.2716587091987048 msec\nrounds: 674"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 13894.40699520368,
            "unit": "iter/sec",
            "range": "stddev: 0.00001585979035470252",
            "extra": "mean: 71.97140549756445 usec\nrounds: 7058"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1640.6421246367606,
            "unit": "iter/sec",
            "range": "stddev: 0.000022820966974579864",
            "extra": "mean: 609.5174474575928 usec\nrounds: 885"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 685310.3069192674,
            "unit": "iter/sec",
            "range": "stddev: 4.078770845632134e-7",
            "extra": "mean: 1.4591929960828744 usec\nrounds: 66592"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 132952.46616034902,
            "unit": "iter/sec",
            "range": "stddev: 0.0000012534033288428704",
            "extra": "mean: 7.521485150895488 usec\nrounds: 23840"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2817.6374201334156,
            "unit": "iter/sec",
            "range": "stddev: 0.000014476523551455697",
            "extra": "mean: 354.90726835699456 usec\nrounds: 4930"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1620.6269309712634,
            "unit": "iter/sec",
            "range": "stddev: 0.000020355702226424498",
            "extra": "mean: 617.0451575802746 usec\nrounds: 1339"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 14730.569808391361,
            "unit": "iter/sec",
            "range": "stddev: 0.000008355123905039107",
            "extra": "mean: 67.88603652184206 usec\nrounds: 11856"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1350.0912495075959,
            "unit": "iter/sec",
            "range": "stddev: 0.0003176003370310684",
            "extra": "mean: 740.6906758078161 usec\nrounds: 1021"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1375.0602102901769,
            "unit": "iter/sec",
            "range": "stddev: 0.000012813030421086673",
            "extra": "mean: 727.2408819021615 usec\nrounds: 1304"
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
        "date": 1775695538929,
        "tool": "pytest",
        "benches": [
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_nodes",
            "value": 1209.11185574339,
            "unit": "iter/sec",
            "range": "stddev: 0.000021652245918333913",
            "extra": "mean: 827.0533410535263 usec\nrounds: 475"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_add_connections",
            "value": 827.6326171828647,
            "unit": "iter/sec",
            "range": "stddev: 0.0000663054985539121",
            "extra": "mean: 1.208265574892212 msec\nrounds: 701"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_match",
            "value": 14230.9206113318,
            "unit": "iter/sec",
            "range": "stddev: 0.000003459245178790448",
            "extra": "mean: 70.26952277449428 usec\nrounds: 6257"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_cypher_where",
            "value": 1537.2282070940669,
            "unit": "iter/sec",
            "range": "stddev: 0.00003204552434215096",
            "extra": "mean: 650.5215005717153 usec\nrounds: 875"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_traversal",
            "value": 659963.4925603537,
            "unit": "iter/sec",
            "range": "stddev: 3.969237912879869e-7",
            "extra": "mean: 1.5152353293368725 usec\nrounds: 80007"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_shortest_path",
            "value": 131244.58737470937,
            "unit": "iter/sec",
            "range": "stddev: 0.0000011170055989521521",
            "extra": "mean: 7.619361834290001 usec\nrounds: 18818"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_enable",
            "value": 2872.5575783230092,
            "unit": "iter/sec",
            "range": "stddev: 0.000023253541388483505",
            "extra": "mean: 348.1218296706161 usec\nrounds: 4732"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_where",
            "value": 1505.7193489741403,
            "unit": "iter/sec",
            "range": "stddev: 0.000024611239616784112",
            "extra": "mean: 664.1343891086402 usec\nrounds: 1267"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_cypher_match",
            "value": 14365.18182451855,
            "unit": "iter/sec",
            "range": "stddev: 0.000003911073224284468",
            "extra": "mean: 69.61276315300069 usec\nrounds: 10796"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_columnar_save_kgl",
            "value": 1212.7480351240627,
            "unit": "iter/sec",
            "range": "stddev: 0.000018778982030346655",
            "extra": "mean: 824.5735891031159 usec\nrounds: 881"
          },
          {
            "name": "tests/benchmarks/test_bench_core.py::test_bench_save_v3",
            "value": 1235.6155925009539,
            "unit": "iter/sec",
            "range": "stddev: 0.00003275177449330694",
            "extra": "mean: 809.3131926054325 usec\nrounds: 1163"
          }
        ]
      }
    ]
  }
}