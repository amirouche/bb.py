"""
Tests for nstore tuple store functions.

Tests NStore operations: add, ask, delete, and query with pattern matching.
"""
import pytest

from bb import (
    db_open,
    nstore_create,
    nstore_add,
    nstore_ask,
    nstore_delete,
    nstore_query,
    nstore_bytes,
    nstore_count,
    Variable
)


# ============================================================================
# Tests for nstore_create
# ============================================================================

def test_nstore_create_basic():
    """Test creating basic nstore"""
    store = nstore_create((0,), 3)

    assert store.prefix == (0,)
    assert store.n == 3
    assert len(store.indices) > 0


def test_nstore_create_custom_prefix():
    """Test creating nstore with custom prefix"""
    store = nstore_create(('blog',), 3)

    assert store.prefix == ('blog',)
    assert store.n == 3


def test_nstore_create_generates_indices():
    """Test that nstore_create generates correct indices"""
    store = nstore_create((0,), 4)

    # For n=4, should have 6 indices
    assert len(store.indices) == 6

    # Each index should have length 4
    for index in store.indices:
        assert len(index) == 4


# ============================================================================
# Tests for nstore_add
# ============================================================================

def test_nstore_add_basic():
    """Test adding tuple to nstore"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))

    # Should be able to find it
    assert nstore_ask(db, store, ('user123', 'name', 'Alice'))


def test_nstore_add_multiple():
    """Test adding multiple tuples"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))
    nstore_add(db, store, ('user123', 'email', 'alice@example.com'))
    nstore_add(db, store, ('user456', 'name', 'Bob'))

    # All should exist
    assert nstore_ask(db, store, ('user123', 'name', 'Alice'))
    assert nstore_ask(db, store, ('user123', 'email', 'alice@example.com'))
    assert nstore_ask(db, store, ('user456', 'name', 'Bob'))


def test_nstore_add_different_types():
    """Test adding tuples with different value types"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'age', 42))
    nstore_add(db, store, ('user123', 'score', 3.14))
    nstore_add(db, store, ('user123', 'active', True))

    assert nstore_ask(db, store, ('user123', 'age', 42))
    assert nstore_ask(db, store, ('user123', 'score', 3.14))
    assert nstore_ask(db, store, ('user123', 'active', True))


def test_nstore_add_wrong_size():
    """Test that adding tuple with wrong size raises error"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    with pytest.raises(AssertionError, match="Expected 3 items"):
        nstore_add(db, store, ('too', 'few'))

    with pytest.raises(AssertionError, match="Expected 3 items"):
        nstore_add(db, store, ('too', 'many', 'items', 'here'))


# ============================================================================
# Tests for nstore_ask
# ============================================================================

def test_nstore_ask_existing():
    """Test asking for existing tuple"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))

    assert nstore_ask(db, store, ('user123', 'name', 'Alice')) is True


def test_nstore_ask_nonexistent():
    """Test asking for nonexistent tuple"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    assert nstore_ask(db, store, ('user123', 'name', 'Alice')) is False


def test_nstore_ask_after_add():
    """Test ask immediately after add"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('blog', 'title', 'hyper.dev'))

    assert nstore_ask(db, store, ('blog', 'title', 'hyper.dev'))


def test_nstore_ask_partial_match():
    """Test that ask requires exact match"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))

    # Different values should not match
    assert nstore_ask(db, store, ('user123', 'name', 'Bob')) is False
    assert nstore_ask(db, store, ('user456', 'name', 'Alice')) is False
    assert nstore_ask(db, store, ('user123', 'email', 'Alice')) is False


# ============================================================================
# Tests for nstore_delete
# ============================================================================

def test_nstore_delete_existing():
    """Test deleting existing tuple"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))
    assert nstore_ask(db, store, ('user123', 'name', 'Alice'))

    nstore_delete(db, store, ('user123', 'name', 'Alice'))

    assert not nstore_ask(db, store, ('user123', 'name', 'Alice'))


def test_nstore_delete_nonexistent():
    """Test deleting nonexistent tuple does not error"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    # Should not raise
    nstore_delete(db, store, ('user123', 'name', 'Alice'))


def test_nstore_delete_one_of_many():
    """Test deleting one tuple doesn't affect others"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))
    nstore_add(db, store, ('user123', 'email', 'alice@example.com'))
    nstore_add(db, store, ('user456', 'name', 'Bob'))

    nstore_delete(db, store, ('user123', 'email', 'alice@example.com'))

    # Others should still exist
    assert nstore_ask(db, store, ('user123', 'name', 'Alice'))
    assert not nstore_ask(db, store, ('user123', 'email', 'alice@example.com'))
    assert nstore_ask(db, store, ('user456', 'name', 'Bob'))


# ============================================================================
# Tests for nstore_query - Simple queries
# ============================================================================

def test_nstore_query_single_variable():
    """Test query with single variable"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('P4X432', 'blog/title', 'hyper.dev'))

    results = nstore_query(db, store, ('P4X432', 'blog/title', Variable('title')))

    assert len(results) == 1
    assert results[0] == {'title': 'hyper.dev'}


def test_nstore_query_multiple_results():
    """Test query returning multiple results"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'tag', 'python'))
    nstore_add(db, store, ('user123', 'tag', 'rust'))
    nstore_add(db, store, ('user123', 'tag', 'go'))

    results = nstore_query(db, store, ('user123', 'tag', Variable('tag')))

    assert len(results) == 3
    tags = {r['tag'] for r in results}
    assert tags == {'python', 'rust', 'go'}


def test_nstore_query_no_results():
    """Test query with no matching tuples"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))

    results = nstore_query(db, store, ('user456', 'name', Variable('name')))

    assert len(results) == 0


def test_nstore_query_multiple_variables():
    """Test query with multiple variables"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))
    nstore_add(db, store, ('user456', 'name', 'Bob'))

    results = nstore_query(db, store, (Variable('uid'), 'name', Variable('name')))

    assert len(results) == 2

    # Check both users are in results
    uids = {r['uid'] for r in results}
    names = {r['name'] for r in results}
    assert uids == {'user123', 'user456'}
    assert names == {'Alice', 'Bob'}


def test_nstore_query_no_variables():
    """Test query with no variables (exact match)"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))
    nstore_add(db, store, ('user456', 'name', 'Bob'))

    results = nstore_query(db, store, ('user123', 'name', 'Alice'))

    assert len(results) == 1
    assert results[0] == {}  # No variables, empty binding


# ============================================================================
# Tests for nstore_query - Multi-pattern joins
# ============================================================================

def test_nstore_query_two_pattern_join():
    """Test query with two patterns (simple join)"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    # Blog data
    nstore_add(db, store, ('P4X432', 'blog/title', 'hyper.dev'))

    # Post data
    nstore_add(db, store, ('123456', 'post/blog', 'P4X432'))
    nstore_add(db, store, ('123456', 'post/title', 'Hello World'))

    results = nstore_query(
        db, store,
        (Variable('blog_uid'), 'blog/title', 'hyper.dev'),
        (Variable('post_uid'), 'post/blog', Variable('blog_uid'))
    )

    assert len(results) == 1
    assert results[0]['blog_uid'] == 'P4X432'
    assert results[0]['post_uid'] == '123456'


def test_nstore_query_three_pattern_join():
    """Test query with three patterns (multi-hop join)"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    # Blog
    nstore_add(db, store, ('P4X432', 'blog/title', 'hyper.dev'))

    # Posts
    nstore_add(db, store, ('123456', 'post/blog', 'P4X432'))
    nstore_add(db, store, ('123456', 'post/title', 'Hello World'))
    nstore_add(db, store, ('654321', 'post/blog', 'P4X432'))
    nstore_add(db, store, ('654321', 'post/title', 'Goodbye World'))

    results = nstore_query(
        db, store,
        (Variable('blog_uid'), 'blog/title', 'hyper.dev'),
        (Variable('post_uid'), 'post/blog', Variable('blog_uid')),
        (Variable('post_uid'), 'post/title', Variable('post_title'))
    )

    assert len(results) == 2

    titles = {r['post_title'] for r in results}
    assert titles == {'Hello World', 'Goodbye World'}


def test_nstore_query_join_filters():
    """Test that join properly filters results"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    # Two blogs
    nstore_add(db, store, ('blog1', 'blog/title', 'Blog One'))
    nstore_add(db, store, ('blog2', 'blog/title', 'Blog Two'))

    # Posts for blog1
    nstore_add(db, store, ('post1', 'post/blog', 'blog1'))
    nstore_add(db, store, ('post1', 'post/title', 'Post 1'))

    # Posts for blog2
    nstore_add(db, store, ('post2', 'post/blog', 'blog2'))
    nstore_add(db, store, ('post2', 'post/title', 'Post 2'))

    # Query only blog1 posts
    results = nstore_query(
        db, store,
        (Variable('blog_uid'), 'blog/title', 'Blog One'),
        (Variable('post_uid'), 'post/blog', Variable('blog_uid')),
        (Variable('post_uid'), 'post/title', Variable('post_title'))
    )

    assert len(results) == 1
    assert results[0]['post_title'] == 'Post 1'


def test_nstore_query_multiple_join_results():
    """Test join that produces multiple results"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    # One author, multiple posts
    nstore_add(db, store, ('alice', 'author/name', 'Alice'))

    nstore_add(db, store, ('post1', 'post/author', 'alice'))
    nstore_add(db, store, ('post1', 'post/title', 'First Post'))
    nstore_add(db, store, ('post2', 'post/author', 'alice'))
    nstore_add(db, store, ('post2', 'post/title', 'Second Post'))
    nstore_add(db, store, ('post3', 'post/author', 'alice'))
    nstore_add(db, store, ('post3', 'post/title', 'Third Post'))

    results = nstore_query(
        db, store,
        (Variable('author_uid'), 'author/name', 'Alice'),
        (Variable('post_uid'), 'post/author', Variable('author_uid')),
        (Variable('post_uid'), 'post/title', Variable('title'))
    )

    assert len(results) == 3

    titles = {r['title'] for r in results}
    assert titles == {'First Post', 'Second Post', 'Third Post'}


# ============================================================================
# Tests for nstore_query - Edge cases
# ============================================================================

def test_nstore_query_empty_store():
    """Test query on empty store"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    results = nstore_query(db, store, (Variable('a'), Variable('b'), Variable('c')))

    assert len(results) == 0


def test_nstore_query_with_integers():
    """Test query with integer values"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'age', 25))
    nstore_add(db, store, ('user456', 'age', 30))

    results = nstore_query(db, store, (Variable('uid'), 'age', Variable('age')))

    assert len(results) == 2

    ages = {r['age'] for r in results}
    assert ages == {25, 30}


def test_nstore_query_with_nested_tuple():
    """Test query with nested tuple values"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('item123', 'tags', ('python', 'code', 'tutorial')))

    results = nstore_query(db, store, ('item123', 'tags', Variable('tags')))

    assert len(results) == 1
    assert results[0]['tags'] == ('python', 'code', 'tutorial')


def test_nstore_query_pattern_wrong_size():
    """Test that pattern with wrong size raises error"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    with pytest.raises(AssertionError, match="Pattern length .* doesn't match"):
        nstore_query(db, store, (Variable('a'), Variable('b')))


def test_nstore_query_result_list_slicing():
    """Test that query results can be sliced for pagination"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    # Add many tuples
    for i in range(10):
        nstore_add(db, store, (f'user{i}', 'type', 'user'))

    results = nstore_query(db, store, (Variable('uid'), 'type', 'user'))

    # Should get all 10
    assert len(results) == 10

    # Test slicing
    page1 = results[0:5]
    page2 = results[5:10]

    assert len(page1) == 5
    assert len(page2) == 5


# ============================================================================
# Tests for nstore_count
# ============================================================================

def test_nstore_count_single_pattern():
    """Test count with single pattern"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'tag', 'python'))
    nstore_add(db, store, ('user123', 'tag', 'rust'))
    nstore_add(db, store, ('user123', 'tag', 'go'))

    count = nstore_count(db, store, ('user123', 'tag', Variable('tag')))

    assert count == 3


def test_nstore_count_no_matches():
    """Test count with no matching tuples"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))

    count = nstore_count(db, store, ('user456', 'name', Variable('name')))

    assert count == 0


def test_nstore_count_empty_store():
    """Test count on empty store"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    count = nstore_count(db, store, (Variable('a'), Variable('b'), Variable('c')))

    assert count == 0


def test_nstore_count_exact_match():
    """Test count with exact match (no variables)"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))
    nstore_add(db, store, ('user456', 'name', 'Bob'))

    count = nstore_count(db, store, ('user123', 'name', 'Alice'))

    assert count == 1


def test_nstore_count_multiple_variables():
    """Test count with multiple variables"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))
    nstore_add(db, store, ('user456', 'name', 'Bob'))
    nstore_add(db, store, ('user789', 'email', 'carol@example.com'))

    count = nstore_count(db, store, (Variable('uid'), 'name', Variable('name')))

    assert count == 2


def test_nstore_count_with_integers():
    """Test count with integer values"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'age', 25))
    nstore_add(db, store, ('user456', 'age', 30))
    nstore_add(db, store, ('user789', 'age', 35))

    count = nstore_count(db, store, (Variable('uid'), 'age', Variable('age')))

    assert count == 3


def test_nstore_count_pattern_wrong_size():
    """Test that pattern with wrong size raises error"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    with pytest.raises(AssertionError, match="Pattern length .* doesn't match"):
        nstore_count(db, store, (Variable('a'), Variable('b')))


# ============================================================================
# Tests for nstore_bytes
# ============================================================================

def test_nstore_bytes_single_pattern():
    """Test bytes with single pattern"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'tag', 'python'))
    nstore_add(db, store, ('user123', 'tag', 'rust'))
    nstore_add(db, store, ('user123', 'tag', 'go'))

    total = nstore_bytes(db, store, ('user123', 'tag', Variable('tag')))

    # Should be non-zero
    assert total > 0


def test_nstore_bytes_no_matches():
    """Test bytes with no matching tuples"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'name', 'Alice'))

    total = nstore_bytes(db, store, ('user456', 'name', Variable('name')))

    assert total == 0


def test_nstore_bytes_empty_store():
    """Test bytes on empty store"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    total = nstore_bytes(db, store, (Variable('a'), Variable('b'), Variable('c')))

    assert total == 0


def test_nstore_bytes_increases_with_more_data():
    """Test that bytes increases with more data"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    nstore_add(db, store, ('user123', 'tag', 'python'))
    bytes1 = nstore_bytes(db, store, ('user123', 'tag', Variable('tag')))

    nstore_add(db, store, ('user123', 'tag', 'rust'))
    bytes2 = nstore_bytes(db, store, ('user123', 'tag', Variable('tag')))

    nstore_add(db, store, ('user123', 'tag', 'go'))
    bytes3 = nstore_bytes(db, store, ('user123', 'tag', Variable('tag')))

    assert bytes1 < bytes2 < bytes3


def test_nstore_bytes_pattern_wrong_size():
    """Test that pattern with wrong size raises error"""
    db = db_open(':memory:')
    store = nstore_create((0,), 3)

    with pytest.raises(AssertionError, match="Pattern length .* doesn't match"):
        nstore_bytes(db, store, (Variable('a'), Variable('b')))
