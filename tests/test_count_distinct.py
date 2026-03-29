"""Tests for COUNT(DISTINCT expr) and DISTINCT on other aggregate functions."""

import pytest

from kglite import KnowledgeGraph


@pytest.fixture
def people():
    """5 people across 2 cities, some sharing ages. One person has no city."""
    g = KnowledgeGraph()
    g.cypher("CREATE (:Person {name: 'Alice',   city: 'NYC', age: 30})")
    g.cypher("CREATE (:Person {name: 'Bob',     city: 'NYC', age: 30})")
    g.cypher("CREATE (:Person {name: 'Charlie', city: 'LA',  age: 25})")
    g.cypher("CREATE (:Person {name: 'Dave',    city: 'LA',  age: 25})")
    g.cypher("CREATE (:Person {name: 'Eve'})")  # no city, no age → nulls
    return g


@pytest.fixture
def scores():
    """Duplicate numeric values for testing distinct aggregation."""
    g = KnowledgeGraph()
    for v in [10, 10, 20, 30, 30]:
        g.cypher(f"CREATE (:Score {{val: {v}}})")
    return g


@pytest.fixture
def fan_in():
    """Two people both connected to one city — tests distinct on graph nodes."""
    g = KnowledgeGraph()
    g.cypher("CREATE (:City   {name: 'NYC'})")
    g.cypher("CREATE (:Person {name: 'Alice'})")
    g.cypher("CREATE (:Person {name: 'Bob'})")
    g.cypher("MATCH (a:Person {name: 'Alice'}), (c:City {name: 'NYC'}) CREATE (a)-[:LIVES_IN]->(c)")
    g.cypher("MATCH (a:Person {name: 'Bob'}),   (c:City {name: 'NYC'}) CREATE (a)-[:LIVES_IN]->(c)")
    return g


# ---------------------------------------------------------------------------
# count(DISTINCT prop) — basic distinct on property values
# ---------------------------------------------------------------------------


class TestCountDistinctProperty:
    def test_deduplicates_repeated_values(self, people):
        """count(DISTINCT n.city) counts unique cities, not rows."""
        r = people.cypher("MATCH (n:Person) RETURN count(DISTINCT n.city) AS cnt")
        assert r[0]["cnt"] == 2  # NYC, LA (null excluded)

    def test_without_distinct_counts_all_non_null(self, people):
        """count(n.city) counts all non-null values."""
        r = people.cypher("MATCH (n:Person) RETURN count(n.city) AS cnt")
        assert r[0]["cnt"] == 4  # Eve has no city → excluded

    def test_distinct_vs_plain_difference(self, people):
        """Distinct count is lower than plain count when duplicates exist."""
        plain = people.cypher("MATCH (n:Person) RETURN count(n.city) AS cnt")[0]["cnt"]
        distinct = people.cypher("MATCH (n:Person) RETURN count(DISTINCT n.city) AS cnt")[0]["cnt"]
        assert distinct < plain

    def test_null_values_excluded(self, people):
        """Nulls are not counted even in count(DISTINCT ...)."""
        r = people.cypher("MATCH (n:Person) RETURN count(DISTINCT n.city) AS cnt")
        assert r[0]["cnt"] == 2  # NYC and LA only, not null

    def test_all_unique_values_unchanged(self, people):
        """count(DISTINCT n.name) == count(n.name) when all values are unique."""
        plain = people.cypher("MATCH (n:Person) RETURN count(n.name) AS cnt")[0]["cnt"]
        distinct = people.cypher("MATCH (n:Person) RETURN count(DISTINCT n.name) AS cnt")[0]["cnt"]
        assert plain == distinct == 5

    def test_single_row(self):
        """count(DISTINCT ...) on a single row returns 1."""
        g = KnowledgeGraph()
        g.cypher("CREATE (:Item {val: 42})")
        r = g.cypher("MATCH (n:Item) RETURN count(DISTINCT n.val) AS cnt")
        assert r[0]["cnt"] == 1

    def test_empty_graph(self):
        """count(DISTINCT ...) on no matching rows returns 0."""
        g = KnowledgeGraph()
        r = g.cypher("MATCH (n:Person) RETURN count(DISTINCT n.city) AS cnt")
        assert r[0]["cnt"] == 0


# ---------------------------------------------------------------------------
# count(DISTINCT n) — distinct on node identity, not value
# ---------------------------------------------------------------------------


class TestCountDistinctNode:
    def test_distinct_node_variable(self, people):
        """count(DISTINCT n) counts each unique node once."""
        r = people.cypher("MATCH (n:Person) RETURN count(DISTINCT n) AS cnt")
        assert r[0]["cnt"] == 5

    def test_fan_in_counts_one_city(self, fan_in):
        """Two edges pointing to same city node → count(DISTINCT c) == 1."""
        r = fan_in.cypher(
            "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN count(DISTINCT c) AS cnt"
        )
        assert r[0]["cnt"] == 1

    def test_fan_in_without_distinct_counts_edges(self, fan_in):
        """count(c) without DISTINCT counts each occurrence (one per edge)."""
        r = fan_in.cypher(
            "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN count(c) AS cnt"
        )
        assert r[0]["cnt"] == 2


# ---------------------------------------------------------------------------
# count(DISTINCT ...) grouped by another property
# ---------------------------------------------------------------------------


class TestCountDistinctGrouped:
    def test_grouped_by_city_distinct_age(self, people):
        """Each city has 2 people with the same age; distinct age count per city == 1."""
        r = people.cypher(
            "MATCH (n:Person) RETURN n.city, count(DISTINCT n.age) AS cnt ORDER BY n.city"
        )
        by_city = {row["n.city"]: row["cnt"] for row in r}
        assert by_city["NYC"] == 1  # both age 30
        assert by_city["LA"] == 1   # both age 25

    def test_grouped_by_city_plain_count(self, people):
        """Plain count(n.age) per city returns 2 for cities with 2 people."""
        r = people.cypher(
            "MATCH (n:Person) RETURN n.city, count(n.age) AS cnt ORDER BY n.city"
        )
        by_city = {row["n.city"]: row["cnt"] for row in r}
        assert by_city["NYC"] == 2
        assert by_city["LA"] == 2

    def test_group_with_all_distinct_values(self, people):
        """count(DISTINCT n.name) per city == count(n.name) since names are unique."""
        r = people.cypher(
            "MATCH (n:Person) RETURN n.city, count(DISTINCT n.name) AS cnt ORDER BY n.city"
        )
        by_city = {row["n.city"]: row["cnt"] for row in r}
        assert by_city["NYC"] == 2
        assert by_city["LA"] == 2


# ---------------------------------------------------------------------------
# count(DISTINCT ...) in WITH clause
# ---------------------------------------------------------------------------


class TestCountDistinctWithClause:
    def test_with_count_distinct(self, people):
        """count(DISTINCT n.city) in WITH propagates correctly."""
        r = people.cypher(
            "MATCH (n:Person) WITH count(DISTINCT n.city) AS cnt RETURN cnt"
        )
        assert r[0]["cnt"] == 2

    def test_with_count_distinct_filtered(self, people):
        """WITH ... WHERE cnt > N filters on the distinct count."""
        r = people.cypher(
            "MATCH (n:Person) WITH count(DISTINCT n.city) AS cnt WHERE cnt > 1 RETURN cnt"
        )
        assert len(r) == 1
        assert r[0]["cnt"] == 2

    def test_with_count_distinct_group_key(self, people):
        """GROUP BY city in WITH with count(DISTINCT n.age)."""
        r = people.cypher(
            "MATCH (n:Person) WITH n.city AS city, count(DISTINCT n.age) AS cnt "
            "RETURN city, cnt ORDER BY city"
        )
        by_city = {row["city"]: row["cnt"] for row in r}
        assert by_city["NYC"] == 1
        assert by_city["LA"] == 1


# ---------------------------------------------------------------------------
# collect(DISTINCT ...) and other aggregates with DISTINCT
# ---------------------------------------------------------------------------


class TestOtherDistinctAggregates:
    def test_collect_distinct_deduplicates(self, people):
        """collect(DISTINCT n.city) should contain each city once."""
        r = people.cypher("MATCH (n:Person) RETURN collect(DISTINCT n.city) AS cities")
        cities = r[0]["cities"]
        assert sorted(cities) == ["LA", "NYC"]
        assert len(cities) == len(set(cities))

    def test_sum_distinct(self, scores):
        """sum(DISTINCT n.val): 10+20+30 = 60 (duplicates excluded)."""
        r = scores.cypher("MATCH (n:Score) RETURN sum(DISTINCT n.val) AS s")
        assert r[0]["s"] == pytest.approx(60.0)

    def test_sum_plain(self, scores):
        """sum(n.val) without DISTINCT: 10+10+20+30+30 = 100."""
        r = scores.cypher("MATCH (n:Score) RETURN sum(n.val) AS s")
        assert r[0]["s"] == pytest.approx(100.0)

    def test_avg_distinct(self, scores):
        """avg(DISTINCT n.val): mean of {10, 20, 30} == 20."""
        r = scores.cypher("MATCH (n:Score) RETURN avg(DISTINCT n.val) AS a")
        assert r[0]["a"] == pytest.approx(20.0)

    def test_avg_plain(self, scores):
        """avg(n.val) without DISTINCT: mean of [10,10,20,30,30] == 20."""
        r = scores.cypher("MATCH (n:Score) RETURN avg(n.val) AS a")
        assert r[0]["a"] == pytest.approx(20.0)

    def test_collect_distinct_vs_plain(self, people):
        """collect(DISTINCT n.city) has fewer items than collect(n.city)."""
        plain = people.cypher("MATCH (n:Person) RETURN collect(n.city) AS c")[0]["c"]
        distinct = people.cypher("MATCH (n:Person) RETURN collect(DISTINCT n.city) AS c")[0]["c"]
        assert len(plain) > len(distinct)


# ---------------------------------------------------------------------------
# count(*) — not affected by DISTINCT keyword
# ---------------------------------------------------------------------------


class TestCountStar:
    def test_count_star_counts_all_rows(self, people):
        """count(*) always counts all rows regardless."""
        r = people.cypher("MATCH (n:Person) RETURN count(*) AS cnt")
        assert r[0]["cnt"] == 5

    def test_count_n_excludes_null_properties(self, people):
        """count(n.city) skips rows where city is null."""
        r = people.cypher("MATCH (n:Person) RETURN count(n.city) AS cnt")
        assert r[0]["cnt"] == 4  # Eve has no city


# ---------------------------------------------------------------------------
# Alias and column naming
# ---------------------------------------------------------------------------


class TestCountDistinctAliases:
    def test_alias_is_used_as_column_name(self, people):
        """AS alias sets the column name in result dict."""
        r = people.cypher("MATCH (n:Person) RETURN count(DISTINCT n.city) AS unique_cities")
        assert "unique_cities" in r[0]
        assert r[0]["unique_cities"] == 2

    def test_unaliased_column_name(self, people):
        """Without alias, column name is the expression string."""
        r = people.cypher("MATCH (n:Person) RETURN count(DISTINCT n.city)")
        key = list(r[0].keys())[0]
        assert "count" in key.lower()
        assert r[0][key] == 2
