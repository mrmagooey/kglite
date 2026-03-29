"""Tests for multi-label node support: CREATE (n:Primary:Extra), SET n:Label, REMOVE n:Label, labels(n)."""

import os
import tempfile

import pytest

import kglite
from kglite import KnowledgeGraph


@pytest.fixture
def g():
    return KnowledgeGraph()


# ---------------------------------------------------------------------------
# CREATE with multiple labels
# ---------------------------------------------------------------------------


def test_create_single_label(g):
    g.cypher("CREATE (n:Person {name: 'Alice'})")
    result = g.cypher("MATCH (n:Person) RETURN n.name")
    assert result[0]["n.name"] == "Alice"


def test_create_multi_label_primary(g):
    g.cypher("CREATE (n:Person:Director {name: 'Alice'})")
    # Node is findable by primary label
    result = g.cypher("MATCH (n:Person) RETURN n.name")
    assert result[0]["n.name"] == "Alice"


def test_create_multi_label_extra_in_labels_fn(g):
    g.cypher("CREATE (n:Person:Director {name: 'Alice'})")
    result = g.cypher("MATCH (n:Person) RETURN labels(n)")
    labels = result[0]["labels(n)"]
    assert "Person" in labels
    assert "Director" in labels


def test_create_multi_label_python_dict_has_labels_key(g):
    g.cypher("CREATE (n:Person:Director {name: 'Alice'})")
    nodes = g.select("Person").collect()
    assert len(nodes) == 1
    node = nodes[0]
    assert "labels" in node
    assert "Person" in node["labels"]
    assert "Director" in node["labels"]


def test_create_three_labels(g):
    g.cypher("CREATE (n:Animal:Pet:Dog {name: 'Rex'})")
    result = g.cypher("MATCH (n:Animal) RETURN labels(n)")
    labels = result[0]["labels(n)"]
    assert "Animal" in labels
    assert "Pet" in labels
    assert "Dog" in labels


def test_create_no_label_defaults_node(g):
    g.cypher("CREATE (n {name: 'Unknown'})")
    result = g.cypher("MATCH (n:Node) RETURN n.name")
    assert result[0]["n.name"] == "Unknown"


# ---------------------------------------------------------------------------
# labels(n) function
# ---------------------------------------------------------------------------


def test_labels_fn_single_label(g):
    g.cypher("CREATE (n:Person {name: 'Bob'})")
    result = g.cypher("MATCH (n:Person) RETURN labels(n)")
    labels = result[0]["labels(n)"]
    assert "Person" in labels


def test_labels_fn_no_duplicates(g):
    g.cypher("CREATE (n:Person {name: 'Bob'})")
    result = g.cypher("MATCH (n:Person) RETURN labels(n)")
    labels = result[0]["labels(n)"]
    assert labels.count("Person") == 1


def test_labels_property_access(g):
    g.cypher("CREATE (n:Person:Manager {name: 'Carol'})")
    result = g.cypher("MATCH (n:Person) RETURN n.labels")
    labels = result[0]["n.labels"]
    assert "Person" in labels
    assert "Manager" in labels


# ---------------------------------------------------------------------------
# SET n:Label
# ---------------------------------------------------------------------------


def test_set_label_adds_extra(g):
    g.cypher("CREATE (n:Person {name: 'Alice'})")
    g.cypher("MATCH (n:Person) SET n:Employee")
    result = g.cypher("MATCH (n:Person) RETURN labels(n)")
    labels = result[0]["labels(n)"]
    assert "Employee" in labels
    assert "Person" in labels


def test_set_label_idempotent(g):
    g.cypher("CREATE (n:Person {name: 'Alice'})")
    g.cypher("MATCH (n:Person) SET n:Employee")
    g.cypher("MATCH (n:Person) SET n:Employee")  # second SET should be no-op
    result = g.cypher("MATCH (n:Person) RETURN labels(n)")
    labels = result[0]["labels(n)"]
    assert labels.count("Employee") == 1


def test_set_primary_label_noop(g):
    """Setting the primary label again should be a no-op, not duplicate it."""
    g.cypher("CREATE (n:Person {name: 'Alice'})")
    g.cypher("MATCH (n:Person) SET n:Person")
    result = g.cypher("MATCH (n:Person) RETURN labels(n)")
    labels = result[0]["labels(n)"]
    assert labels.count("Person") == 1


def test_set_label_node_still_queryable_by_primary(g):
    g.cypher("CREATE (n:Person {name: 'Dave'})")
    g.cypher("MATCH (n:Person) SET n:VIP")
    # Primary label index still works
    result = g.cypher("MATCH (n:Person) RETURN n.name")
    assert result[0]["n.name"] == "Dave"


# ---------------------------------------------------------------------------
# REMOVE n:Label
# ---------------------------------------------------------------------------


def test_remove_extra_label(g):
    g.cypher("CREATE (n:Person:Director {name: 'Eve'})")
    g.cypher("MATCH (n:Person) REMOVE n:Director")
    result = g.cypher("MATCH (n:Person) RETURN labels(n)")
    labels = result[0]["labels(n)"]
    assert "Director" not in labels
    assert "Person" in labels


def test_remove_nonexistent_label_noop(g):
    g.cypher("CREATE (n:Person {name: 'Frank'})")
    # Removing a label the node doesn't have should not error
    g.cypher("MATCH (n:Person) REMOVE n:Ghost")
    result = g.cypher("MATCH (n:Person) RETURN labels(n)")
    assert "Person" in result[0]["labels(n)"]


def test_remove_primary_label_errors(g):
    g.cypher("CREATE (n:Person {name: 'Grace'})")
    with pytest.raises(Exception, match="primary label"):
        g.cypher("MATCH (n:Person) REMOVE n:Person")


# ---------------------------------------------------------------------------
# Python dict "labels" key (via collect())
# ---------------------------------------------------------------------------


def test_python_dict_labels_single(g):
    g.cypher("CREATE (n:Person {name: 'Hank'})")
    nodes = g.select("Person").collect()
    assert nodes[0]["labels"] == ["Person"]


def test_python_dict_labels_multi(g):
    g.cypher("CREATE (n:Person:Engineer {name: 'Ivy'})")
    nodes = g.select("Person").collect()
    node = nodes[0]
    assert set(node["labels"]) == {"Person", "Engineer"}


def test_python_dict_labels_after_set(g):
    g.cypher("CREATE (n:Person {name: 'Jack'})")
    g.cypher("MATCH (n:Person) SET n:Contractor")
    nodes = g.select("Person").collect()
    node = nodes[0]
    assert set(node["labels"]) == {"Person", "Contractor"}


def test_python_dict_labels_after_remove(g):
    g.cypher("CREATE (n:Person:Contractor {name: 'Kim'})")
    g.cypher("MATCH (n:Person) REMOVE n:Contractor")
    nodes = g.select("Person").collect()
    node = nodes[0]
    assert node["labels"] == ["Person"]


# ---------------------------------------------------------------------------
# Save / load roundtrip
# ---------------------------------------------------------------------------


def test_save_load_preserves_extra_labels(g):
    g.cypher("CREATE (n:Person:Director {name: 'Alice'})")
    g.cypher("CREATE (n:Person {name: 'Bob'})")
    g.cypher("MATCH (n:Person {name: 'Bob'}) SET n:Manager")

    with tempfile.NamedTemporaryFile(suffix=".kgl", delete=False) as f:
        path = f.name
    try:
        g.save(path)
        loaded = kglite.load(path)

        result = loaded.cypher("MATCH (n:Person) RETURN n.name, labels(n) ORDER BY n.name")
        by_name = {r["n.name"]: r["labels(n)"] for r in result}

        assert set(by_name["Alice"]) == {"Person", "Director"}
        assert set(by_name["Bob"]) == {"Person", "Manager"}
    finally:
        os.unlink(path)
