"""
Tests for nstore tuple store functions.

Tests NStore operations: add, ask, delete, and query with pattern matching.
"""

import pytest

from bb import (
    storage_db_open,
    storage_nstore_create,
    storage_nstore_add,
    storage_nstore_ask,
    storage_nstore_delete,
    storage_nstore_query,
    storage_nstore_bytes,
    storage_nstore_count,
    Variable,
)


# ============================================================================
# Tests for storage_nstore_create
# ============================================================================


def test_storage_nstore_create_basic():
    """Test creating basic nstore"""
    store = storage_nstore_create((0,), 3)

    assert store.prefix == (0,)
    assert store.n == 3
    assert len(store.indices) > 0


def test_storage_nstore_create_custom_prefix():
    """Test creating nstore with custom prefix"""
    store = storage_nstore_create(("blog",), 3)

    assert store.prefix == ("blog",)
    assert store.n == 3


def test_storage_nstore_create_generates_indices():
    """Test that storage_nstore_create generates correct indices"""
    store = storage_nstore_create((0,), 4)

    # For n=4, should have 6 indices
    assert len(store.indices) == 6

    # Each index should have length 4
    for index in store.indices:
        assert len(index) == 4


# ============================================================================
# Tests for storage_nstore_add
# ============================================================================


def test_storage_nstore_add_basic():
    """Test adding tuple to nstore"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))

    # Should be able to find it
    assert storage_nstore_ask(db, store, ("user123", "name", "Alice"))


def test_storage_nstore_add_multiple():
    """Test adding multiple tuples"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))
    storage_nstore_add(db, store, ("user123", "email", "alice@example.com"))
    storage_nstore_add(db, store, ("user456", "name", "Bob"))

    # All should exist
    assert storage_nstore_ask(db, store, ("user123", "name", "Alice"))
    assert storage_nstore_ask(db, store, ("user123", "email", "alice@example.com"))
    assert storage_nstore_ask(db, store, ("user456", "name", "Bob"))


def test_storage_nstore_add_different_types():
    """Test adding tuples with different value types"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "age", 42))
    storage_nstore_add(db, store, ("user123", "score", 3.14))
    storage_nstore_add(db, store, ("user123", "active", True))

    assert storage_nstore_ask(db, store, ("user123", "age", 42))
    assert storage_nstore_ask(db, store, ("user123", "score", 3.14))
    assert storage_nstore_ask(db, store, ("user123", "active", True))


def test_storage_nstore_add_wrong_size():
    """Test that adding tuple with wrong size raises error"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    with pytest.raises(AssertionError, match="Expected 3 items"):
        storage_nstore_add(db, store, ("too", "few"))

    with pytest.raises(AssertionError, match="Expected 3 items"):
        storage_nstore_add(db, store, ("too", "many", "items", "here"))


# ============================================================================
# Tests for storage_nstore_ask
# ============================================================================


def test_storage_nstore_ask_existing():
    """Test asking for existing tuple"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))

    assert storage_nstore_ask(db, store, ("user123", "name", "Alice")) is True


def test_storage_nstore_ask_nonexistent():
    """Test asking for nonexistent tuple"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    assert storage_nstore_ask(db, store, ("user123", "name", "Alice")) is False


def test_storage_nstore_ask_after_add():
    """Test ask immediately after add"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("blog", "title", "hyper.dev"))

    assert storage_nstore_ask(db, store, ("blog", "title", "hyper.dev"))


def test_storage_nstore_ask_partial_match():
    """Test that ask requires exact match"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))

    # Different values should not match
    assert storage_nstore_ask(db, store, ("user123", "name", "Bob")) is False
    assert storage_nstore_ask(db, store, ("user456", "name", "Alice")) is False
    assert storage_nstore_ask(db, store, ("user123", "email", "Alice")) is False


# ============================================================================
# Tests for storage_nstore_delete
# ============================================================================


def test_storage_nstore_delete_existing():
    """Test deleting existing tuple"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))
    assert storage_nstore_ask(db, store, ("user123", "name", "Alice"))

    storage_nstore_delete(db, store, ("user123", "name", "Alice"))

    assert not storage_nstore_ask(db, store, ("user123", "name", "Alice"))


def test_storage_nstore_delete_nonexistent():
    """Test deleting nonexistent tuple does not error"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    # Should not raise
    storage_nstore_delete(db, store, ("user123", "name", "Alice"))


def test_storage_nstore_delete_one_of_many():
    """Test deleting one tuple doesn't affect others"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))
    storage_nstore_add(db, store, ("user123", "email", "alice@example.com"))
    storage_nstore_add(db, store, ("user456", "name", "Bob"))

    storage_nstore_delete(db, store, ("user123", "email", "alice@example.com"))

    # Others should still exist
    assert storage_nstore_ask(db, store, ("user123", "name", "Alice"))
    assert not storage_nstore_ask(db, store, ("user123", "email", "alice@example.com"))
    assert storage_nstore_ask(db, store, ("user456", "name", "Bob"))


# ============================================================================
# Tests for storage_nstore_query - Simple queries
# ============================================================================


def test_storage_nstore_query_single_variable():
    """Test query with single variable"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("P4X432", "blog/title", "hyper.dev"))

    results = storage_nstore_query(
        db, store, ("P4X432", "blog/title", Variable("title"))
    )

    assert len(results) == 1
    assert results[0] == {"title": "hyper.dev"}


def test_storage_nstore_query_multiple_results():
    """Test query returning multiple results"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "tag", "python"))
    storage_nstore_add(db, store, ("user123", "tag", "rust"))
    storage_nstore_add(db, store, ("user123", "tag", "go"))

    results = storage_nstore_query(db, store, ("user123", "tag", Variable("tag")))

    assert len(results) == 3
    tags = {r["tag"] for r in results}
    assert tags == {"python", "rust", "go"}


def test_storage_nstore_query_no_results():
    """Test query with no matching tuples"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))

    results = storage_nstore_query(db, store, ("user456", "name", Variable("name")))

    assert len(results) == 0


def test_storage_nstore_query_multiple_variables():
    """Test query with multiple variables"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))
    storage_nstore_add(db, store, ("user456", "name", "Bob"))

    results = storage_nstore_query(
        db, store, (Variable("uid"), "name", Variable("name"))
    )

    assert len(results) == 2

    # Check both users are in results
    uids = {r["uid"] for r in results}
    names = {r["name"] for r in results}
    assert uids == {"user123", "user456"}
    assert names == {"Alice", "Bob"}


def test_storage_nstore_query_no_variables():
    """Test query with no variables (exact match)"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))
    storage_nstore_add(db, store, ("user456", "name", "Bob"))

    results = storage_nstore_query(db, store, ("user123", "name", "Alice"))

    assert len(results) == 1
    assert results[0] == {}  # No variables, empty binding


# ============================================================================
# Tests for storage_nstore_query - Multi-pattern joins
# ============================================================================


def test_storage_nstore_query_two_pattern_join():
    """Test query with two patterns (simple join)"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    # Blog data
    storage_nstore_add(db, store, ("P4X432", "blog/title", "hyper.dev"))

    # Post data
    storage_nstore_add(db, store, ("123456", "post/blog", "P4X432"))
    storage_nstore_add(db, store, ("123456", "post/title", "Hello World"))

    results = storage_nstore_query(
        db,
        store,
        (Variable("blog_uid"), "blog/title", "hyper.dev"),
        (Variable("post_uid"), "post/blog", Variable("blog_uid")),
    )

    assert len(results) == 1
    assert results[0]["blog_uid"] == "P4X432"
    assert results[0]["post_uid"] == "123456"


def test_storage_nstore_query_three_pattern_join():
    """Test query with three patterns (multi-hop join)"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    # Blog
    storage_nstore_add(db, store, ("P4X432", "blog/title", "hyper.dev"))

    # Posts
    storage_nstore_add(db, store, ("123456", "post/blog", "P4X432"))
    storage_nstore_add(db, store, ("123456", "post/title", "Hello World"))
    storage_nstore_add(db, store, ("654321", "post/blog", "P4X432"))
    storage_nstore_add(db, store, ("654321", "post/title", "Goodbye World"))

    results = storage_nstore_query(
        db,
        store,
        (Variable("blog_uid"), "blog/title", "hyper.dev"),
        (Variable("post_uid"), "post/blog", Variable("blog_uid")),
        (Variable("post_uid"), "post/title", Variable("post_title")),
    )

    assert len(results) == 2

    titles = {r["post_title"] for r in results}
    assert titles == {"Hello World", "Goodbye World"}


def test_storage_nstore_query_join_filters():
    """Test that join properly filters results"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    # Two blogs
    storage_nstore_add(db, store, ("blog1", "blog/title", "Blog One"))
    storage_nstore_add(db, store, ("blog2", "blog/title", "Blog Two"))

    # Posts for blog1
    storage_nstore_add(db, store, ("post1", "post/blog", "blog1"))
    storage_nstore_add(db, store, ("post1", "post/title", "Post 1"))

    # Posts for blog2
    storage_nstore_add(db, store, ("post2", "post/blog", "blog2"))
    storage_nstore_add(db, store, ("post2", "post/title", "Post 2"))

    # Query only blog1 posts
    results = storage_nstore_query(
        db,
        store,
        (Variable("blog_uid"), "blog/title", "Blog One"),
        (Variable("post_uid"), "post/blog", Variable("blog_uid")),
        (Variable("post_uid"), "post/title", Variable("post_title")),
    )

    assert len(results) == 1
    assert results[0]["post_title"] == "Post 1"


def test_storage_nstore_query_multiple_join_results():
    """Test join that produces multiple results"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    # One author, multiple posts
    storage_nstore_add(db, store, ("alice", "author/name", "Alice"))

    storage_nstore_add(db, store, ("post1", "post/author", "alice"))
    storage_nstore_add(db, store, ("post1", "post/title", "First Post"))
    storage_nstore_add(db, store, ("post2", "post/author", "alice"))
    storage_nstore_add(db, store, ("post2", "post/title", "Second Post"))
    storage_nstore_add(db, store, ("post3", "post/author", "alice"))
    storage_nstore_add(db, store, ("post3", "post/title", "Third Post"))

    results = storage_nstore_query(
        db,
        store,
        (Variable("author_uid"), "author/name", "Alice"),
        (Variable("post_uid"), "post/author", Variable("author_uid")),
        (Variable("post_uid"), "post/title", Variable("title")),
    )

    assert len(results) == 3

    titles = {r["title"] for r in results}
    assert titles == {"First Post", "Second Post", "Third Post"}


# ============================================================================
# Tests for storage_nstore_query - Edge cases
# ============================================================================


def test_storage_nstore_query_empty_store():
    """Test query on empty store"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    results = storage_nstore_query(
        db, store, (Variable("a"), Variable("b"), Variable("c"))
    )

    assert len(results) == 0


def test_storage_nstore_query_with_integers():
    """Test query with integer values"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "age", 25))
    storage_nstore_add(db, store, ("user456", "age", 30))

    results = storage_nstore_query(db, store, (Variable("uid"), "age", Variable("age")))

    assert len(results) == 2

    ages = {r["age"] for r in results}
    assert ages == {25, 30}


def test_storage_nstore_query_with_nested_tuple():
    """Test query with nested tuple values"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("item123", "tags", ("python", "code", "tutorial")))

    results = storage_nstore_query(db, store, ("item123", "tags", Variable("tags")))

    assert len(results) == 1
    assert results[0]["tags"] == ("python", "code", "tutorial")


def test_storage_nstore_query_pattern_wrong_size():
    """Test that pattern with wrong size raises error"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    with pytest.raises(AssertionError, match="Pattern length .* doesn't match"):
        storage_nstore_query(db, store, (Variable("a"), Variable("b")))


def test_storage_nstore_query_result_list_slicing():
    """Test that query results can be sliced for pagination"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    # Add many tuples
    for i in range(10):
        storage_nstore_add(db, store, (f"user{i}", "type", "user"))

    results = storage_nstore_query(db, store, (Variable("uid"), "type", "user"))

    # Should get all 10
    assert len(results) == 10

    # Test slicing
    page1 = results[0:5]
    page2 = results[5:10]

    assert len(page1) == 5
    assert len(page2) == 5


# ============================================================================
# Tests for storage_nstore_count
# ============================================================================


def test_storage_nstore_count_single_pattern():
    """Test count with single pattern"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "tag", "python"))
    storage_nstore_add(db, store, ("user123", "tag", "rust"))
    storage_nstore_add(db, store, ("user123", "tag", "go"))

    count = storage_nstore_count(db, store, ("user123", "tag", Variable("tag")))

    assert count == 3


def test_storage_nstore_count_no_matches():
    """Test count with no matching tuples"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))

    count = storage_nstore_count(db, store, ("user456", "name", Variable("name")))

    assert count == 0


def test_storage_nstore_count_empty_store():
    """Test count on empty store"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    count = storage_nstore_count(
        db, store, (Variable("a"), Variable("b"), Variable("c"))
    )

    assert count == 0


def test_storage_nstore_count_exact_match():
    """Test count with exact match (no variables)"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))
    storage_nstore_add(db, store, ("user456", "name", "Bob"))

    count = storage_nstore_count(db, store, ("user123", "name", "Alice"))

    assert count == 1


def test_storage_nstore_count_multiple_variables():
    """Test count with multiple variables"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))
    storage_nstore_add(db, store, ("user456", "name", "Bob"))
    storage_nstore_add(db, store, ("user789", "email", "carol@example.com"))

    count = storage_nstore_count(db, store, (Variable("uid"), "name", Variable("name")))

    assert count == 2


def test_storage_nstore_count_with_integers():
    """Test count with integer values"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "age", 25))
    storage_nstore_add(db, store, ("user456", "age", 30))
    storage_nstore_add(db, store, ("user789", "age", 35))

    count = storage_nstore_count(db, store, (Variable("uid"), "age", Variable("age")))

    assert count == 3


def test_storage_nstore_count_pattern_wrong_size():
    """Test that pattern with wrong size raises error"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    with pytest.raises(AssertionError, match="Pattern length .* doesn't match"):
        storage_nstore_count(db, store, (Variable("a"), Variable("b")))


# ============================================================================
# Tests for storage_nstore_bytes
# ============================================================================


def test_storage_nstore_bytes_single_pattern():
    """Test bytes with single pattern"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "tag", "python"))
    storage_nstore_add(db, store, ("user123", "tag", "rust"))
    storage_nstore_add(db, store, ("user123", "tag", "go"))

    total = storage_nstore_bytes(db, store, ("user123", "tag", Variable("tag")))

    # Should be non-zero
    assert total > 0


def test_storage_nstore_bytes_no_matches():
    """Test bytes with no matching tuples"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "name", "Alice"))

    total = storage_nstore_bytes(db, store, ("user456", "name", Variable("name")))

    assert total == 0


def test_storage_nstore_bytes_empty_store():
    """Test bytes on empty store"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    total = storage_nstore_bytes(
        db, store, (Variable("a"), Variable("b"), Variable("c"))
    )

    assert total == 0


def test_storage_nstore_bytes_increases_with_more_data():
    """Test that bytes increases with more data"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    storage_nstore_add(db, store, ("user123", "tag", "python"))
    bytes1 = storage_nstore_bytes(db, store, ("user123", "tag", Variable("tag")))

    storage_nstore_add(db, store, ("user123", "tag", "rust"))
    bytes2 = storage_nstore_bytes(db, store, ("user123", "tag", Variable("tag")))

    storage_nstore_add(db, store, ("user123", "tag", "go"))
    bytes3 = storage_nstore_bytes(db, store, ("user123", "tag", Variable("tag")))

    assert bytes1 < bytes2 < bytes3


def test_storage_nstore_bytes_pattern_wrong_size():
    """Test that pattern with wrong size raises error"""
    db = storage_db_open(":memory:")
    store = storage_nstore_create((0,), 3)

    with pytest.raises(AssertionError, match="Pattern length .* doesn't match"):
        storage_nstore_bytes(db, store, (Variable("a"), Variable("b")))
