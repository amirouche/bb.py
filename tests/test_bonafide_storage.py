#!/usr/bin/env python3
"""
Test script for Bonafide storage primitives.
"""

import bonafide
import threading
import os
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
