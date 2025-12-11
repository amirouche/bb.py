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

    # Test query function
    result = bonafide.query(bf, "SELECT value FROM kv_store WHERE key = ?", ("key1",))
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


if __name__ == "__main__":
    test_basic_operations()
    test_concurrent_access()
    test_query_function()
    test_apply_function()
    test_new_function()
    test_readonly_functionality()
    print("\nAll tests passed!")
