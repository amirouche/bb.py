"""
Tests for database functions.

Tests SQLite3-based ordered key-value store operations.
"""

import pytest

from bb import (
    db_open,
    db_get,
    db_set,
    db_delete,
    db_query,
    db_transaction,
    db_bytes,
    db_count,
)


# ============================================================================
# Tests for db_open
# ============================================================================


def test_db_open_memory():
    """Test opening in-memory database"""
    db = db_open(":memory:")

    assert db is not None

    # Verify table exists
    cursor = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='kv'")
    assert cursor.fetchone() is not None


def test_db_open_file(tmp_path):
    """Test opening file-based database"""
    db_path = tmp_path / "test.db"
    db = db_open(str(db_path))

    assert db is not None
    assert db_path.exists()


def test_db_open_creates_index():
    """Test that db_open creates index on key column"""
    db = db_open(":memory:")

    # Verify index exists
    cursor = db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_key'")
    assert cursor.fetchone() is not None


# ============================================================================
# Tests for db_set
# ============================================================================


def test_db_set_basic():
    """Test basic set operation"""
    db = db_open(":memory:")
    key = b"test_key"
    value = b"test_value"

    db_set(db, key, value)

    # Verify stored
    cursor = db.execute("SELECT value FROM kv WHERE key = ?", (key,))
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == value


def test_db_set_replace():
    """Test that set replaces existing value"""
    db = db_open(":memory:")
    key = b"test_key"
    value1 = b"value1"
    value2 = b"value2"

    db_set(db, key, value1)
    db_set(db, key, value2)

    # Should have only the new value
    cursor = db.execute("SELECT value FROM kv WHERE key = ?", (key,))
    row = cursor.fetchone()
    assert row[0] == value2


def test_db_set_multiple_keys():
    """Test setting multiple different keys"""
    db = db_open(":memory:")

    db_set(db, b"key1", b"value1")
    db_set(db, b"key2", b"value2")
    db_set(db, b"key3", b"value3")

    # All three should exist
    cursor = db.execute("SELECT COUNT(*) FROM kv")
    assert cursor.fetchone()[0] == 3


def test_db_set_key_size_limit():
    """Test that keys exceeding 1KB are rejected"""
    db = db_open(":memory:")
    oversized_key = b"x" * 1025

    with pytest.raises(AssertionError, match="Key size .* exceeds maximum"):
        db_set(db, oversized_key, b"value")


def test_db_set_value_size_limit():
    """Test that values exceeding 1MB are rejected"""
    db = db_open(":memory:")
    oversized_value = b"x" * (1048576 + 1)

    with pytest.raises(AssertionError, match="Value size .* exceeds maximum"):
        db_set(db, b"key", oversized_value)


def test_db_set_max_key_size():
    """Test that 1KB key is accepted"""
    db = db_open(":memory:")
    max_key = b"x" * 1024

    db_set(db, max_key, b"value")

    # Should succeed
    assert db_get(db, max_key) == b"value"


def test_db_set_max_value_size():
    """Test that 1MB value is accepted"""
    db = db_open(":memory:")
    max_value = b"x" * 1048576

    db_set(db, b"key", max_value)

    # Should succeed
    assert db_get(db, b"key") == max_value


# ============================================================================
# Tests for db_get
# ============================================================================


def test_db_get_existing_key():
    """Test getting existing key"""
    db = db_open(":memory:")
    key = b"test_key"
    value = b"test_value"

    db_set(db, key, value)
    result = db_get(db, key)

    assert result == value


def test_db_get_nonexistent_key():
    """Test getting nonexistent key returns None"""
    db = db_open(":memory:")

    result = db_get(db, b"nonexistent")

    assert result is None


def test_db_get_after_delete():
    """Test getting key after deletion returns None"""
    db = db_open(":memory:")
    key = b"test_key"

    db_set(db, key, b"value")
    db_delete(db, key)
    result = db_get(db, key)

    assert result is None


# ============================================================================
# Tests for db_delete
# ============================================================================


def test_db_delete_existing_key():
    """Test deleting existing key"""
    db = db_open(":memory:")
    key = b"test_key"

    db_set(db, key, b"value")
    db_delete(db, key)

    # Key should no longer exist
    assert db_get(db, key) is None


def test_db_delete_nonexistent_key():
    """Test deleting nonexistent key does not error"""
    db = db_open(":memory:")

    # Should not raise
    db_delete(db, b"nonexistent")


def test_db_delete_multiple_keys():
    """Test deleting one key doesn't affect others"""
    db = db_open(":memory:")

    db_set(db, b"key1", b"value1")
    db_set(db, b"key2", b"value2")
    db_set(db, b"key3", b"value3")

    db_delete(db, b"key2")

    # key1 and key3 should still exist
    assert db_get(db, b"key1") == b"value1"
    assert db_get(db, b"key2") is None
    assert db_get(db, b"key3") == b"value3"


# ============================================================================
# Tests for db_query
# ============================================================================


def test_db_query_forward_scan():
    """Test forward range scan (key <= other)"""
    db = db_open(":memory:")

    # Insert ordered keys
    db_set(db, b"a", b"value_a")
    db_set(db, b"b", b"value_b")
    db_set(db, b"c", b"value_c")
    db_set(db, b"d", b"value_d")

    # Query [b, d) - should get b and c
    results = db_query(db, b"b", b"d")

    assert len(results) == 2
    assert results[0] == (b"b", b"value_b")
    assert results[1] == (b"c", b"value_c")


def test_db_query_reverse_scan():
    """Test reverse range scan (key > other)"""
    db = db_open(":memory:")

    # Insert ordered keys
    db_set(db, b"a", b"value_a")
    db_set(db, b"b", b"value_b")
    db_set(db, b"c", b"value_c")
    db_set(db, b"d", b"value_d")

    # Query reverse [d, b) - should get c and b in descending order
    # Range is [b, d) = {b, c}, returned in descending order
    results = db_query(db, b"d", b"b")

    assert len(results) == 2
    assert results[0] == (b"c", b"value_c")
    assert results[1] == (b"b", b"value_b")


def test_db_query_empty_result():
    """Test query with no matching keys"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"z", b"value_z")

    # Query [m, n) - no keys in this range
    results = db_query(db, b"m", b"n")

    assert len(results) == 0


def test_db_query_offset():
    """Test query with offset parameter"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"b", b"value_b")
    db_set(db, b"c", b"value_c")
    db_set(db, b"d", b"value_d")

    # Query with offset=2 and limit (SQLite requires LIMIT with OFFSET)
    results = db_query(db, b"a", b"e", offset=2, limit=10)

    assert len(results) == 2
    assert results[0] == (b"c", b"value_c")
    assert results[1] == (b"d", b"value_d")


def test_db_query_limit():
    """Test query with limit parameter"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"b", b"value_b")
    db_set(db, b"c", b"value_c")
    db_set(db, b"d", b"value_d")

    # Query with limit=2
    results = db_query(db, b"a", b"e", limit=2)

    assert len(results) == 2
    assert results[0] == (b"a", b"value_a")
    assert results[1] == (b"b", b"value_b")


def test_db_query_offset_and_limit():
    """Test query with both offset and limit"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"b", b"value_b")
    db_set(db, b"c", b"value_c")
    db_set(db, b"d", b"value_d")

    # Query with offset=1, limit=2
    results = db_query(db, b"a", b"e", offset=1, limit=2)

    assert len(results) == 2
    assert results[0] == (b"b", b"value_b")
    assert results[1] == (b"c", b"value_c")


def test_db_query_prefix_scan():
    """Test prefix scan using range query"""
    db = db_open(":memory:")

    db_set(db, b"user:1:name", b"alice")
    db_set(db, b"user:1:email", b"alice@example.com")
    db_set(db, b"user:2:name", b"bob")
    db_set(db, b"post:1:title", b"hello")

    # Query all user:1: keys
    key_start = b"user:1:"
    key_end = b"user:1;"  # Next character after ':' is ';'

    results = db_query(db, key_start, key_end)

    assert len(results) == 2
    assert results[0][0] == b"user:1:email"
    assert results[1][0] == b"user:1:name"


# ============================================================================
# Tests for db_transaction
# ============================================================================


def test_db_transaction_commit():
    """Test that transaction commits on success"""
    db = db_open(":memory:")

    with db_transaction(db):
        db_set(db, b"key1", b"value1")
        db_set(db, b"key2", b"value2")

    # Both should be committed
    assert db_get(db, b"key1") == b"value1"
    assert db_get(db, b"key2") == b"value2"


def test_db_transaction_rollback():
    """Test that transaction rolls back on exception"""
    db = db_open(":memory:")

    # Set initial value
    db_set(db, b"key1", b"initial")
    db.commit()

    try:
        with db_transaction(db):
            db_set(db, b"key1", b"modified")
            db_set(db, b"key2", b"new_value")
            raise ValueError("Test error")
    except ValueError:
        pass

    # Should have rolled back
    assert db_get(db, b"key1") == b"initial"
    assert db_get(db, b"key2") is None


def test_db_transaction_nested_operations():
    """Test multiple operations within transaction"""
    db = db_open(":memory:")

    with db_transaction(db):
        db_set(db, b"key1", b"value1")
        assert db_get(db, b"key1") == b"value1"

        db_set(db, b"key2", b"value2")
        db_delete(db, b"key1")

        assert db_get(db, b"key1") is None
        assert db_get(db, b"key2") == b"value2"

    # Final state should be committed
    assert db_get(db, b"key1") is None
    assert db_get(db, b"key2") == b"value2"


def test_db_transaction_returns_db():
    """Test that transaction yields database connection"""
    db = db_open(":memory:")

    with db_transaction(db) as conn:
        assert conn is db


# ============================================================================
# Tests for db_bytes
# ============================================================================


def test_db_bytes_basic():
    """Test basic bytes calculation"""
    db = db_open(":memory:")

    # Insert keys and values with known sizes
    db_set(db, b"aa", b"value1")  # key: 2, value: 6 = 8
    db_set(db, b"ab", b"value2")  # key: 2, value: 6 = 8
    db_set(db, b"ac", b"val")  # key: 2, value: 3 = 5

    # Query all keys [aa, ad)
    total = db_bytes(db, b"aa", b"ad")

    assert total == 21  # 8 + 8 + 5


def test_db_bytes_forward_scan():
    """Test bytes calculation with forward range scan"""
    db = db_open(":memory:")

    db_set(db, b"a", b"1")
    db_set(db, b"b", b"22")
    db_set(db, b"c", b"333")
    db_set(db, b"d", b"4444")

    # Query [b, d) - should get b and c
    total = db_bytes(db, b"b", b"d")

    # b: 1 + 2 = 3, c: 1 + 3 = 4, total = 7
    assert total == 7


def test_db_bytes_reverse_scan():
    """Test bytes calculation with reverse range scan"""
    db = db_open(":memory:")

    db_set(db, b"a", b"1")
    db_set(db, b"b", b"22")
    db_set(db, b"c", b"333")
    db_set(db, b"d", b"4444")

    # Query reverse [d, b) - should get c and b in descending order
    total = db_bytes(db, b"d", b"b")

    # Same range [b, d) = b + c = 7
    assert total == 7


def test_db_bytes_empty_result():
    """Test bytes calculation with no matching keys"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"z", b"value_z")

    # Query [m, n) - no keys in this range
    total = db_bytes(db, b"m", b"n")

    assert total == 0


def test_db_bytes_with_offset():
    """Test bytes calculation with offset"""
    db = db_open(":memory:")

    db_set(db, b"a", b"11")  # key: 1, value: 2 = 3
    db_set(db, b"b", b"222")  # key: 1, value: 3 = 4
    db_set(db, b"c", b"3333")  # key: 1, value: 4 = 5
    db_set(db, b"d", b"44444")  # key: 1, value: 5 = 6

    # Query with offset=2, limit required for offset
    total = db_bytes(db, b"a", b"e", offset=2, limit=10)

    # Skip a and b, get c and d: 5 + 6 = 11
    assert total == 11


def test_db_bytes_with_limit():
    """Test bytes calculation with limit"""
    db = db_open(":memory:")

    db_set(db, b"a", b"11")  # key: 1, value: 2 = 3
    db_set(db, b"b", b"222")  # key: 1, value: 3 = 4
    db_set(db, b"c", b"3333")  # key: 1, value: 4 = 5
    db_set(db, b"d", b"44444")  # key: 1, value: 5 = 6

    # Query with limit=2
    total = db_bytes(db, b"a", b"e", limit=2)

    # Get only a and b: 3 + 4 = 7
    assert total == 7


def test_db_bytes_with_offset_and_limit():
    """Test bytes calculation with offset and limit"""
    db = db_open(":memory:")

    db_set(db, b"a", b"11")  # key: 1, value: 2 = 3
    db_set(db, b"b", b"222")  # key: 1, value: 3 = 4
    db_set(db, b"c", b"3333")  # key: 1, value: 4 = 5
    db_set(db, b"d", b"44444")  # key: 1, value: 5 = 6

    # Query with offset=1, limit=2
    total = db_bytes(db, b"a", b"e", offset=1, limit=2)

    # Skip a, get b and c: 4 + 5 = 9
    assert total == 9


# ============================================================================
# Tests for db_count
# ============================================================================


def test_db_count_basic():
    """Test basic count"""
    db = db_open(":memory:")

    db_set(db, b"aa", b"value1")
    db_set(db, b"ab", b"value2")
    db_set(db, b"ac", b"value3")

    # Count all keys [aa, ad)
    count = db_count(db, b"aa", b"ad")

    assert count == 3


def test_db_count_forward_scan():
    """Test count with forward range scan"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"b", b"value_b")
    db_set(db, b"c", b"value_c")
    db_set(db, b"d", b"value_d")

    # Count [b, d) - should get b and c
    count = db_count(db, b"b", b"d")

    assert count == 2


def test_db_count_reverse_scan():
    """Test count with reverse range scan"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"b", b"value_b")
    db_set(db, b"c", b"value_c")
    db_set(db, b"d", b"value_d")

    # Count reverse [d, b) - should get b and c
    count = db_count(db, b"d", b"b")

    assert count == 2


def test_db_count_empty_result():
    """Test count with no matching keys"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"z", b"value_z")

    # Count [m, n) - no keys in this range
    count = db_count(db, b"m", b"n")

    assert count == 0


def test_db_count_with_offset():
    """Test count with offset"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"b", b"value_b")
    db_set(db, b"c", b"value_c")
    db_set(db, b"d", b"value_d")

    # Count with offset=2, limit required
    count = db_count(db, b"a", b"e", offset=2, limit=10)

    # Skip a and b, count c and d
    assert count == 2


def test_db_count_with_limit():
    """Test count with limit"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"b", b"value_b")
    db_set(db, b"c", b"value_c")
    db_set(db, b"d", b"value_d")

    # Count with limit=2
    count = db_count(db, b"a", b"e", limit=2)

    assert count == 2


def test_db_count_with_offset_and_limit():
    """Test count with offset and limit"""
    db = db_open(":memory:")

    db_set(db, b"a", b"value_a")
    db_set(db, b"b", b"value_b")
    db_set(db, b"c", b"value_c")
    db_set(db, b"d", b"value_d")

    # Count with offset=1, limit=2
    count = db_count(db, b"a", b"e", offset=1, limit=2)

    # Skip a, count b and c
    assert count == 2


def test_db_count_single_key():
    """Test count with single matching key"""
    db = db_open(":memory:")

    db_set(db, b"key", b"value")

    count = db_count(db, b"key", b"key\x00")

    assert count == 1
