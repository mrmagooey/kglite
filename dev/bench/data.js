window.BENCHMARK_DATA = {
  "lastUpdate": 1775277198188,
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
      }
    ]
  }
}