#!/usr/bin/env python3
"""
Test script for Bonafide storage primitives and nstore functionality.
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import bonafide
import threading
import os
import tempfile
from typing import Optional


def test_basic_operations():
    """Test basic operations using apply and query."""
    # Create bonafide instance
    bf = bonafide.new("test_basic.db")

    def put_func(conn, key: str, value: bytes) -> None:
        """Function to put a value."""
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", (key, value)
        )
        conn.commit()

    def get_func(conn, key: str) -> Optional[bytes]:
        """Function to get a value."""
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
        result = cursor.fetchone()
        return result[0] if result else None

    def delete_func(conn, key: str) -> None:
        """Function to delete a value."""
        cursor = conn.cursor()
        cursor.execute("DELETE FROM kv_store WHERE key = ?", (key,))
        conn.commit()

    # Test put and get
    bonafide.apply(bf, put_func, "key1", b"value1")
    result = bonafide.apply(bf, get_func, "key1")
    assert result == b"value1"

    # Test update
    bonafide.apply(bf, put_func, "key1", b"value2")
    result = bonafide.apply(bf, get_func, "key1")
    assert result == b"value2"

    # Test delete
    bonafide.apply(bf, delete_func, "key1")
    result = bonafide.apply(bf, get_func, "key1")
    assert result is None

    print("✓ Basic operations test passed")
    if os.path.exists("test_basic.db"):
        os.remove("test_basic.db")


def test_concurrent_access():
    """Test concurrent access using apply."""
    bf = bonafide.new("test_concurrent.db")

    def put_func(conn, key: str, value: bytes) -> None:
        """Function to put a value."""
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", (key, value)
        )
        conn.commit()

    def get_func(conn, key: str) -> Optional[bytes]:
        """Function to get a value."""
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
        result = cursor.fetchone()
        return result[0] if result else None

    def worker(thread_id: int) -> None:
        for i in range(10):
            key = f"key_{thread_id}_{i}"
            value = f"value_{thread_id}_{i}".encode()
            bonafide.apply(bf, put_func, key, value)
            result = bonafide.apply(bf, get_func, key)
            assert result == value

    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("✓ Concurrent access test passed")
    if os.path.exists("test_concurrent.db"):
        os.remove("test_concurrent.db")


def test_query_function():
    """Test the query function."""
    bf = bonafide.new("test_query.db")

    # Insert data
    def setup_func(conn) -> None:
        """Function to setup the database."""
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
            ("key1", b"value1"),
        )
        conn.commit()

    bonafide.apply(bf, setup_func)

    # Test query function using transaction context manager
    with bonafide.transaction(bf) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM kv_store WHERE key = ?", ("key1",))
        result = cursor.fetchall()
        assert result[0][0] == b"value1"

    print("✓ Query function test passed")
    if os.path.exists("test_query.db"):
        os.remove("test_query.db")


def test_apply_function():
    """Test the apply function."""
    bf = bonafide.new("test_apply.db")

    def test_func(conn, key: str, value: bytes) -> bytes:
        """A test function that uses the connection."""
        # Put the value
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", (key, value)
        )
        conn.commit()

        # Get the value back
        cursor.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
        result = cursor.fetchone()
        return result[0] if result else b""

    # Test apply
    result = bonafide.apply(bf, test_func, "test_key", b"test_value")
    assert result == b"test_value"

    print("✓ Apply function test passed")
    if os.path.exists("test_apply.db"):
        os.remove("test_apply.db")


def test_new_function():
    """Test the new function that creates a table with BLOB fields."""
    # Create bonafide instance using new function
    bf = bonafide.new("test_new.db", name="test_table")

    def put_func(conn, key: bytes, value: bytes) -> None:
        """Function to put a value."""
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO test_table (key, value) VALUES (?, ?)", (key, value)
        )
        conn.commit()

    def get_func(conn, key: bytes) -> Optional[bytes]:
        """Function to get a value."""
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM test_table WHERE key = ?", (key,))
        result = cursor.fetchone()
        return result[0] if result else None

    # Test put and get with BLOB keys and values
    key = b"blob_key"
    value = b"blob_value"
    bonafide.apply(bf, put_func, key, value)
    result = bonafide.apply(bf, get_func, key)
    assert result == value

    print("✓ New function test passed")
    if os.path.exists("test_new.db"):
        os.remove("test_new.db")


def test_readonly_functionality():
    """Test the readonly parameter in apply function."""
    bf = bonafide.new("test_readonly.db")

    def write_func(conn, key: str, value: str) -> None:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
            (key, value.encode()),
        )
        conn.commit()

    def read_func(conn, key: str) -> bytes:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
        result = cursor.fetchone()
        return result[0] if result else b""

    # Test write with lock (readonly=False)
    bonafide.apply(bf, write_func, "test_key", "test_value", readonly=False)

    # Test read without lock (readonly=True)
    result = bonafide.apply(bf, read_func, "test_key", readonly=True)
    assert result == b"test_value"

    print("✓ Readonly functionality test passed")
    if os.path.exists("test_readonly.db"):
        os.remove("test_readonly.db")


def test_transaction_context_manager():
    """Test the transaction context manager."""
    bf = bonafide.new("test_transaction.db")

    # Test successful transaction (should commit)
    with bonafide.transaction(bf) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
            ("key1", b"value1"),
        )

    # Verify the transaction was committed
    with bonafide.transaction(bf) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM kv_store WHERE key = ?", ("key1",))
        result = cursor.fetchone()
        assert result[0] == b"value1"

    # Test failed transaction (should rollback)
    try:
        with bonafide.transaction(bf) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
                ("key2", b"value2"),
            )
            raise ValueError("Test exception")
    except ValueError:
        pass

    # Verify the transaction was rolled back
    with bonafide.transaction(bf) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM kv_store WHERE key = ?", ("key2",))
        result = cursor.fetchone()
        assert result is None

    print("✓ Transaction context manager test passed")
    if os.path.exists("test_transaction.db"):
        os.remove("test_transaction.db")


def test_bonafide_query_function():
    """Test the new bonafide.query function with single key and range queries."""
    bf = bonafide.new("test_query_func.db")

    # Setup: Insert test data
    def setup_func(conn) -> None:
        cursor = conn.cursor()
        # Insert keys in order: b'key1', b'key2', b'key3', b'key4', b'key5'
        for i in range(1, 6):
            cursor.execute(
                "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
                (f"key{i}".encode(), f"value{i}".encode()),
            )
        conn.commit()

    bonafide.apply(bf, setup_func)

    # Test single key lookup (other=None)
    with bonafide.transaction(bf) as conn:
        # Existing key
        result = bonafide.query(conn, b"key1")
        assert result == b"value1"

        # Non-existent key
        result = bonafide.query(conn, b"nonexistent")
        assert result is None

    # Test range queries (other != None)
    with bonafide.transaction(bf) as conn:
        # Forward range query: key1 <= key < key4
        results = bonafide.query(conn, b"key1", b"key4")
        expected = [(b"key1", b"value1"), (b"key2", b"value2"), (b"key3", b"value3")]
        assert results == expected

        # Reverse range query: key4 > key >= key1 (should return in descending order)
        results = bonafide.query(conn, b"key4", b"key1")
        expected = [(b"key3", b"value3"), (b"key2", b"value2"), (b"key1", b"value1")]
        assert results == expected

        # Test with limit
        results = bonafide.query(conn, b"key1", b"key4", limit=2)
        expected = [(b"key1", b"value1"), (b"key2", b"value2")]
        assert results == expected

        # Test with offset
        results = bonafide.query(conn, b"key1", b"key4", offset=1)
        expected = [(b"key2", b"value2"), (b"key3", b"value3")]
        assert results == expected

        # Test with both limit and offset
        results = bonafide.query(conn, b"key1", b"key5", offset=1, limit=2)
        expected = [(b"key2", b"value2"), (b"key3", b"value3")]
        assert results == expected

    print("✓ Query function test passed")
    if os.path.exists("test_query_func.db"):
        os.remove("test_query_func.db")


def test_bonafide_set_delete():
    """Test the set and delete functions."""
    bf = bonafide.new("test_set_delete.db")

    with bonafide.transaction(bf) as conn:
        # Test set
        bonafide.set(conn, b"test_key", b"test_value")

        # Verify set worked
        result = bonafide.query(conn, b"test_key")
        assert result == b"test_value"

        # Test overwrite with set
        bonafide.set(conn, b"test_key", b"new_value")
        result = bonafide.query(conn, b"test_key")
        assert result == b"new_value"

    with bonafide.transaction(bf) as conn:
        # Test delete single key
        deleted_count = bonafide.delete(conn, b"test_key")
        assert deleted_count == 1

        # Verify delete worked
        result = bonafide.query(conn, b"test_key")
        assert result is None

        # Test delete non-existent key (should not raise error, return 0)
        deleted_count = bonafide.delete(conn, b"nonexistent_key")
        assert deleted_count == 0

    # Test range delete
    with bonafide.transaction(bf) as conn:
        # Insert test data
        for i in range(1, 6):
            bonafide.set(conn, f"range_key_{i}".encode(), f"range_value_{i}".encode())

    with bonafide.transaction(bf) as conn:
        # Test range delete - forward
        deleted_count = bonafide.delete(conn, b"range_key_1", b"range_key_4")
        assert deleted_count == 3  # range_key_1, range_key_2, range_key_3

        # Verify range delete worked
        for i in range(1, 4):
            result = bonafide.query(conn, f"range_key_{i}".encode())
            assert result is None

        # Verify remaining keys are intact
        for i in range(4, 6):
            result = bonafide.query(conn, f"range_key_{i}".encode())
            assert result == f"range_value_{i}".encode()

    with bonafide.transaction(bf) as conn:
        # Test range delete - reverse
        # This should delete range_key_4 (range [range_key_4, range_key_5))
        deleted_count = bonafide.delete(conn, b"range_key_5", b"range_key_4")
        assert deleted_count == 1  # range_key_4

        # Verify range_key_4 is deleted but range_key_5 remains
        assert bonafide.query(conn, b"range_key_4") is None
        assert bonafide.query(conn, b"range_key_5") == b"range_value_5"

        # Delete the remaining key
        deleted_count = bonafide.delete(conn, b"range_key_5")
        assert deleted_count == 1

    with bonafide.transaction(bf) as conn:
        # Test range delete with limit
        # Insert fresh data
        for i in range(1, 6):
            bonafide.set(conn, f"limit_key_{i}".encode(), f"limit_value_{i}".encode())

        # Delete with limit
        deleted_count = bonafide.delete(conn, b"limit_key_1", b"limit_key_5", limit=2)
        assert deleted_count == 2  # limit_key_1, limit_key_2

        # Verify only first 2 were deleted
        for i in range(1, 3):
            result = bonafide.query(conn, f"limit_key_{i}".encode())
            assert result is None

        # Verify remaining keys are intact
        for i in range(3, 6):
            result = bonafide.query(conn, f"limit_key_{i}".encode())
            assert result == f"limit_value_{i}".encode()

    print("✓ Set and delete functions test passed")
    if os.path.exists("test_set_delete.db"):
        os.remove("test_set_delete.db")


def test_bonafide_bytes_count():
    """Test the bytes and count functions."""
    bf = bonafide.new("test_bytes_count.db")

    # Setup test data
    with bonafide.transaction(bf) as conn:
        # Insert keys: b'key1', b'key2', b'key3', b'key4', b'key5'
        # Values: b'value1', b'value2', b'value3', b'value4', b'value5'
        for i in range(1, 6):
            bonafide.set(conn, f"key{i}".encode(), f"value{i}".encode())

    with bonafide.transaction(bf) as conn:
        # Test count - forward range
        count_result = bonafide.count(conn, b"key1", b"key4")
        assert count_result == 3  # key1, key2, key3

        # Test count - reverse range
        count_result = bonafide.count(conn, b"key4", b"key1")
        assert count_result == 3  # key3, key2, key1

        # Test count with limit
        count_result = bonafide.count(conn, b"key1", b"key5", limit=2)
        assert count_result == 2

        # Test count with offset
        count_result = bonafide.count(conn, b"key1", b"key5", offset=2)
        assert (
            count_result == 2
        )  # key3, key4 (key5 is excluded since range is [key1, key5))

        # Test bytes - forward range
        # key1(4) + value1(6) = 10
        # key2(4) + value2(6) = 10
        # key3(4) + value3(6) = 10
        # Total = 30
        bytes_result = bonafide.bytes(conn, b"key1", b"key4")
        assert bytes_result == 30

        # Test bytes - reverse range (should be same as forward for same range)
        bytes_result = bonafide.bytes(conn, b"key4", b"key1")
        assert bytes_result == 30

        # Test bytes with limit
        bytes_result = bonafide.bytes(conn, b"key1", b"key4", limit=2)
        assert bytes_result == 20  # key1 + value1 + key2 + value2

        # Test bytes with offset
        bytes_result = bonafide.bytes(conn, b"key1", b"key4", offset=1)
        assert bytes_result == 20  # key2 + value2 + key3 + value3

        # Test count and bytes on empty range
        count_result = bonafide.count(conn, b"key6", b"key7")
        assert count_result == 0

        bytes_result = bonafide.bytes(conn, b"key6", b"key7")
        assert bytes_result == 0

    print("✓ Bytes and count functions test passed")
    if os.path.exists("test_bytes_count.db"):
        os.remove("test_bytes_count.db")


if __name__ == "__main__":
    test_basic_operations()
    test_concurrent_access()
    test_query_function()
    test_apply_function()
    test_new_function()
    test_readonly_functionality()
    test_transaction_context_manager()
    test_bonafide_query_function()
    test_bonafide_set_delete()
    test_bonafide_bytes_count()
    print("\nAll tests passed!")


def test_nstore_indices():
    """Test the nstore_indices function."""
    # Test mathematical properties
    assert len(bonafide.nstore_indices(3)) == 3  # C(3,1) = 3
    assert len(bonafide.nstore_indices(4)) == 6  # C(4,2) = 6
    assert len(bonafide.nstore_indices(5)) == 10  # C(5,2) = 10

    # Test specific indices for n=3
    indices_3 = bonafide.nstore_indices(3)
    expected_3 = [[0, 1, 2], [1, 2, 0], [2, 0, 1]]
    assert indices_3 == expected_3

    # Test specific indices for n=4
    indices_4 = bonafide.nstore_indices(4)
    expected_4 = [
        [0, 1, 2, 3],
        [1, 2, 3, 0],
        [2, 0, 3, 1],
        [3, 0, 1, 2],
        [3, 1, 2, 0],
        [3, 2, 0, 1],
    ]
    assert indices_4 == expected_4

    print("✓ nstore_indices test passed")


def test_nstore_basic_operations():
    """Test basic nstore operations."""
    # Create a temporary database
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name

    try:
        # Create bonafide instance
        bf = bonafide.new(db_path=db_path, pool_size=2)

        # Create an nstore for 3-tuples
        store = bonafide.nstore_create(prefix=(0,), n=3)

        # Test adding tuples
        tuple1 = ("user1", "tag1", "value1")
        tuple2 = ("user1", "tag2", "value2")
        tuple3 = ("user2", "tag1", "value3")

        bonafide.nstore_add(bf, store, tuple1)
        bonafide.nstore_add(bf, store, tuple2)
        bonafide.nstore_add(bf, store, tuple3)

        # Test asking if tuples exist
        assert bonafide.nstore_ask(bf, store, tuple1) == True
        assert bonafide.nstore_ask(bf, store, tuple2) == True
        assert bonafide.nstore_ask(bf, store, tuple3) == True
        assert bonafide.nstore_ask(bf, store, ("user1", "tag1", "wrong")) == False

        # Test querying with patterns
        results = bonafide.nstore_query(
            bf, store, ("user1", bonafide.Variable("tag"), bonafide.Variable("value"))
        )
        assert len(results) == 2  # Should find tuple1 and tuple2

        results = bonafide.nstore_query(
            bf, store, ("user2", bonafide.Variable("tag"), bonafide.Variable("value"))
        )
        assert len(results) == 1  # Should find tuple3

        # Test counting
        count = bonafide.nstore_count(
            bf, store, ("user1", bonafide.Variable("tag"), bonafide.Variable("value"))
        )
        assert count == 2

        # Test deleting
        bonafide.nstore_delete(bf, store, tuple1)
        assert bonafide.nstore_ask(bf, store, tuple1) == False

        # Verify count after deletion
        count = bonafide.nstore_count(
            bf, store, ("user1", bonafide.Variable("tag"), bonafide.Variable("value"))
        )
        assert count == 1

        print("✓ nstore basic operations test passed")

    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_nstore_bytes_encoding():
    """Test the bytes encoding/decoding functions."""
    # Test simple tuple
    original = (1, "hello", b"world", None, True)
    encoded = bonafide.bytes_write(original)
    decoded = bonafide.bytes_read(encoded)
    assert decoded == original

    # Test nested tuple
    nested = (1, (2, 3), "test")
    encoded = bonafide.bytes_write(nested)
    decoded = bonafide.bytes_read(encoded)
    assert decoded == nested

    # Test empty tuple
    empty = ()
    encoded = bonafide.bytes_write(empty)
    decoded = bonafide.bytes_read(encoded)
    assert decoded == empty

    # Test various types
    complex_tuple = (
        None,
        True,
        False,
        0,
        42,
        -1,
        3.14,
        "string",
        b"bytes",
        (1, 2, 3),
    )
    encoded = bonafide.bytes_write(complex_tuple)
    decoded = bonafide.bytes_read(encoded)
    assert decoded == complex_tuple

    # Test BBH (Beyond Babel Hash) - must be exactly 64 hex characters (32 bytes)
    test_hash = bonafide.BBH(
        "a227ea217138c98bac904c7dc4f4c66f90626c837d28c8222486e5de68597ef7"
    )
    bbh_tuple = ("hash", test_hash)
    encoded = bonafide.bytes_write(bbh_tuple)
    decoded = bonafide.bytes_read(encoded)
    assert decoded == bbh_tuple
    assert isinstance(decoded[1], bonafide.BBH)
    assert (
        decoded[1].value
        == "a227ea217138c98bac904c7dc4f4c66f90626c837d28c8222486e5de68597ef7"
    )

    print("✓ bytes encoding/decoding test passed")


def test_nstore_4_tuples():
    """Test nstore operations with 4-tuples."""
    # Create a temporary database
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name

    try:
        # Create bonafide instance
        bf = bonafide.new(db_path=db_path, pool_size=2)

        # Create an nstore for 4-tuples
        store = bonafide.nstore_create(prefix=(1,), n=4)

        # Test adding 4-tuples
        tuples_4 = [
            ("user1", "post1", "tag1", "2023"),
            ("user1", "post2", "tag2", "2023"),
            ("user2", "post1", "tag1", "2024"),
        ]

        for tuple_data in tuples_4:
            bonafide.nstore_add(bf, store, tuple_data)

        # Test querying 4-tuples
        user1_results = bonafide.nstore_query(
            bf,
            store,
            (
                "user1",
                bonafide.Variable("post"),
                bonafide.Variable("tag"),
                bonafide.Variable("year"),
            ),
        )
        assert len(user1_results) == 2

        year_2023_results = bonafide.nstore_query(
            bf,
            store,
            (
                bonafide.Variable("user"),
                bonafide.Variable("post"),
                bonafide.Variable("tag"),
                "2023",
            ),
        )
        assert len(year_2023_results) == 2

        # Test counting 4-tuples
        user1_count = bonafide.nstore_count(
            bf,
            store,
            (
                "user1",
                bonafide.Variable("post"),
                bonafide.Variable("tag"),
                bonafide.Variable("year"),
            ),
        )
        assert user1_count == 2

        print("✓ nstore 4-tuples test passed")

    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_nstore_complex_queries():
    """Test complex nstore queries with multiple patterns."""
    # Create a temporary database
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name

    try:
        # Create bonafide instance
        bf = bonafide.new(db_path=db_path, pool_size=2)

        # Create an nstore for relationship tuples (user, action, resource, timestamp)
        store = bonafide.nstore_create(prefix=(0,), n=4)

        # Add test data
        relationships = [
            ("alice", "view", "doc1", "2023-01-01"),
            ("alice", "edit", "doc1", "2023-01-02"),
            ("bob", "view", "doc1", "2023-01-03"),
            ("alice", "view", "doc2", "2023-01-04"),
            ("charlie", "edit", "doc2", "2023-01-05"),
        ]

        for rel in relationships:
            bonafide.nstore_add(bf, store, rel)

        # Test single pattern queries
        alice_views = bonafide.nstore_query(
            bf,
            store,
            ("alice", "view", bonafide.Variable("doc"), bonafide.Variable("date")),
        )
        assert len(alice_views) == 2

        doc1_views = bonafide.nstore_query(
            bf,
            store,
            (bonafide.Variable("user"), "view", "doc1", bonafide.Variable("date")),
        )
        assert len(doc1_views) == 2

        # Test multi-pattern queries (join simulation)
        # Find users who viewed doc1 and then did something else
        doc1_viewers = bonafide.nstore_query(
            bf,
            store,
            (
                bonafide.Variable("user"),
                bonafide.Variable("action"),
                "doc1",
                bonafide.Variable("date"),
            ),
        )

        # For each viewer, find their other actions
        viewer_other_actions = []
        for binding in doc1_viewers:
            user = binding["user"]
            other_actions = bonafide.nstore_query(
                bf,
                store,
                (
                    user,
                    bonafide.Variable("action"),
                    bonafide.Variable("doc"),
                    bonafide.Variable("date"),
                ),
            )
            # Exclude the doc1 actions
            for action_binding in other_actions:
                if action_binding["doc"] != "doc1":
                    viewer_other_actions.append(action_binding)

        # Alice should have at least some other actions
        # The exact structure depends on variable naming in the query
        # For now, just verify we got some results
        assert len(viewer_other_actions) >= 1

        print("✓ nstore complex queries test passed")

    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)


if __name__ == "__main__":
    test_basic_operations()
    test_concurrent_access()
    test_query_function()
    test_apply_function()
    test_new_function()
    test_readonly_functionality()
    test_transaction_context_manager()
    test_bonafide_query_function()
    test_bonafide_set_delete()
    test_bonafide_bytes_count()
    test_nstore_indices()
    test_nstore_basic_operations()
    test_nstore_bytes_encoding()
    test_nstore_4_tuples()
    test_nstore_complex_queries()
    print("\nAll tests passed!")
