#!/usr/bin/env python3
"""
Bonafide Storage Primitives

Bonafide is a storage layer for bb.py that implements an ordered key-value store using SQLite. It provides thread-safe access to the database through a connection pool, ensuring efficient concurrency while respecting SQLite's single-writer constraint.
"""

import sqlite3
import threading
import os
import queue
from collections import namedtuple
from contextlib import contextmanager
from typing import Any, Callable, List, Optional, TypeVar

# Type variable for function return types
T = TypeVar("T")

# Store builtin bytes to avoid naming conflicts
_builtin_bytes = bytes

# Default constants
DEFAULT_DB_PATH = "bonafide.db"
DEFAULT_POOL_SIZE = os.cpu_count() * 2 if os.cpu_count() else 4

# Bonafide namedtuple to hold configuration and state
Bonafide = namedtuple(
    "Bonafide",
    ["db_path", "pool_size", "worker_queue", "worker_threads", "worker_lock"],
)


def _bonafide_worker(bonafide: Bonafide) -> None:
    """
    Worker thread that processes database operations.
    """
    while True:
        try:
            # Get task from queue
            task_id, func, args, kwargs, result_queue = bonafide.worker_queue.get()

            # Create a new connection for this operation
            conn = sqlite3.connect(bonafide.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")

            try:
                result = func(conn, *args, **kwargs)
                result_queue.put((task_id, result))
            except Exception as e:
                result_queue.put((task_id, e))
            finally:
                conn.close()
                bonafide.worker_queue.task_done()
        except Exception:
            # If any error occurs, continue processing
            bonafide.worker_queue.task_done()


def _start_worker_threads(bonafide: Bonafide, num_threads: int = None) -> None:
    """
    Start worker threads for processing database operations.
    """
    num_threads = num_threads if num_threads is not None else bonafide.pool_size

    with bonafide.worker_lock:
        # Don't start more threads than needed
        if len(bonafide.worker_threads) >= num_threads:
            return

        # Start new worker threads
        for _ in range(num_threads - len(bonafide.worker_threads)):
            thread = threading.Thread(
                target=_bonafide_worker, args=(bonafide,), daemon=True
            )
            thread.start()
            bonafide.worker_threads.append(thread)


def apply(
    bonafide: Bonafide,
    func: Callable[..., T],
    *args: Any,
    readonly: bool = False,
    **kwargs: Any,
) -> T:
    """
    Execute a function within the bonafide storage context.

    Args:
        bonafide: The Bonafide configuration and state.
        func: The function to execute. It should accept a connection as the first parameter.
        *args: Arguments to pass to the function (after the connection).
        readonly: If False, acquire the worker lock for write operations.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The result of the function execution.
    """
    # Start worker threads if not already running
    _start_worker_threads(bonafide)

    # Acquire lock for write operations
    if not readonly:
        bonafide.worker_lock.acquire()

    try:
        # Create a result queue for this specific call
        result_queue = queue.Queue()

        # Generate a unique task ID
        task_id = id(result_queue)

        # Put the task in the worker queue
        bonafide.worker_queue.put((task_id, func, args, kwargs, result_queue))

        # Wait for the result
        task_id_result, result = result_queue.get()

        # Check if we got the right result
        if task_id_result != task_id:
            raise RuntimeError("Received result for wrong task")

        # Handle exceptions
        if isinstance(result, Exception):
            raise result

        return result
    finally:
        # Release lock for write operations
        if not readonly:
            bonafide.worker_lock.release()


def query(
    conn: sqlite3.Connection,
    key: _builtin_bytes,
    other: Optional[_builtin_bytes] = None,
    offset: int = 0,
    limit: Optional[int] = None,
) -> Optional[_builtin_bytes]:
    """Query key-value pairs from the database.

    Args:
        conn: SQLite connection
        key: Key to query
        other: Optional end key for range queries
        offset: Number of results to skip
        limit: Maximum results to return

    Returns:
        If other is None: value associated with key, or None if not found
        If other is not None: list of (key, value) tuples in range [key, other)
    """
    if other is None:
        # Single key lookup
        cursor = conn.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
        result = cursor.fetchone()
        return result[0] if result else None
    else:
        # Range query inspired by storage_db_query
        if key <= other:
            # Forward scan: key <= k < other
            query = "SELECT key, value FROM kv_store WHERE key >= ? AND key < ? ORDER BY key ASC"
            params = [key, other]
        else:
            # Reverse scan: other <= k < key, descending order
            query = "SELECT key, value FROM kv_store WHERE key >= ? AND key < ? ORDER BY key DESC"
            params = [other, key]

        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
            if offset > 0:
                query += " OFFSET ?"
                params.append(offset)
        elif offset > 0:
            query += " LIMIT -1 OFFSET ?"
            params.append(offset)

        cursor = conn.execute(query, params)
        return [(row[0], row[1]) for row in cursor]


def set(conn: sqlite3.Connection, key: _builtin_bytes, value: _builtin_bytes) -> None:
    """Set a key-value pair in the database.

    Args:
        conn: SQLite connection
        key: Key to set
        value: Value to associate with the key
    """
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", (key, value)
    )


def delete(
    conn: sqlite3.Connection,
    key: _builtin_bytes,
    other: Optional[_builtin_bytes] = None,
    offset: int = 0,
    limit: Optional[int] = None,
) -> int:
    """Delete key-value pairs from the database.

    Args:
        conn: SQLite connection
        key: Start key (inclusive if forward, exclusive if reverse)
        other: Optional end key for range delete
        offset: Number of results to skip
        limit: Maximum results to delete

    Returns:
        Number of rows deleted

    Behavior:
        - If other is None: delete single key
        - If other is not None: delete range [key, other)
        - If key <= other: forward scan [key, other) in ascending order
        - If key > other: reverse scan [other, key) in descending order
    """
    if other is None:
        # Single key delete
        cursor = conn.cursor()
        cursor.execute("DELETE FROM kv_store WHERE key = ?", (key,))
        return cursor.rowcount
    else:
        # Range delete
        if key <= other:
            # Forward scan: key <= k < other
            base_query = "DELETE FROM kv_store WHERE key >= ? AND key < ?"
            params: List[Any] = [key, other]
        else:
            # Reverse scan: other <= k < key
            base_query = "DELETE FROM kv_store WHERE key >= ? AND key < ?"
            params = [other, key]

        if limit is not None:
            base_query += " LIMIT ?"
            params.append(limit)
            if offset > 0:
                base_query += " OFFSET ?"
                params.append(offset)
        elif offset > 0:
            base_query += " LIMIT -1 OFFSET ?"
            params.append(offset)

        cursor = conn.cursor()
        cursor.execute(base_query, params)
        return cursor.rowcount


def bytes(
    conn: sqlite3.Connection,
    key: _builtin_bytes,
    other: _builtin_bytes,
    offset: int = 0,
    limit: Optional[int] = None,
) -> int:
    """Calculate total bytes (key lengths + value lengths) in a key range.

    Args:
        conn: SQLite connection
        key: Start key (inclusive if forward, exclusive if reverse)
        other: End key (exclusive if forward, inclusive if reverse)
        offset: Number of results to skip
        limit: Maximum results to consider

    Returns:
        Total bytes (key lengths + value lengths)

    Behavior:
        - If key <= other: forward scan [key, other) in ascending order
        - If key > other: reverse scan [other, key) in descending order
    """
    if key <= other:
        # Forward scan: key <= k < other
        base_query = "SELECT key, value FROM kv_store WHERE key >= ? AND key < ? ORDER BY key ASC"
        params: List[Any] = [key, other]
    else:
        # Reverse scan: other <= k < key, descending order
        base_query = "SELECT key, value FROM kv_store WHERE key >= ? AND key < ? ORDER BY key DESC"
        params = [other, key]

    if limit is not None:
        base_query += " LIMIT ?"
        params.append(limit)
        if offset > 0:
            base_query += " OFFSET ?"
            params.append(offset)
    elif offset > 0:
        base_query += " LIMIT -1 OFFSET ?"
        params.append(offset)

    # Wrap in SUM query
    query = f"SELECT COALESCE(SUM(LENGTH(key) + LENGTH(value)), 0) FROM ({base_query})"
    cursor = conn.execute(query, params)
    return cursor.fetchone()[0]


def count(
    conn: sqlite3.Connection,
    key: _builtin_bytes,
    other: _builtin_bytes,
    offset: int = 0,
    limit: Optional[int] = None,
) -> int:
    """Count the number of key-value pairs in a key range.

    Args:
        conn: SQLite connection
        key: Start key (inclusive if forward, exclusive if reverse)
        other: End key (exclusive if forward, inclusive if reverse)
        offset: Number of results to skip
        limit: Maximum results to count

    Returns:
        Number of key-value pairs in the range

    Behavior:
        - If key <= other: forward scan [key, other) in ascending order
        - If key > other: reverse scan [other, key) in descending order
    """
    if key <= other:
        # Forward scan: key <= k < other
        base_query = (
            "SELECT key FROM kv_store WHERE key >= ? AND key < ? ORDER BY key ASC"
        )
        params: List[Any] = [key, other]
    else:
        # Reverse scan: other <= k < key
        base_query = (
            "SELECT key FROM kv_store WHERE key >= ? AND key < ? ORDER BY key DESC"
        )
        params = [other, key]

    if limit is not None:
        base_query += " LIMIT ?"
        params.append(limit)
        if offset > 0:
            base_query += " OFFSET ?"
            params.append(offset)
    elif offset > 0:
        base_query += " LIMIT -1 OFFSET ?"
        params.append(offset)

    # Wrap in COUNT query
    query = f"SELECT COUNT(*) FROM ({base_query})"
    cursor = conn.execute(query, params)
    return cursor.fetchone()[0]


@contextmanager
def transaction(bonafide: Bonafide):
    """
    Context manager for database transactions.

    Commits the transaction if no exception is raised, rolls back otherwise.

    Args:
        bonafide: The Bonafide configuration and state.

    Yields:
        A database connection for the transaction.
    """
    # Create a new connection for this transaction
    conn = sqlite3.connect(bonafide.db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        yield conn
        # Commit if no exception occurred
        conn.commit()
    except Exception:
        # Rollback if an exception occurred
        conn.rollback()
        raise
    finally:
        conn.close()


def new(
    db_path: str = DEFAULT_DB_PATH,
    pool_size: int = DEFAULT_POOL_SIZE,
    name: str = "kv_store",
) -> Bonafide:
    """
    Create a new Bonafide instance with an initialized table.

    Args:
        db_path: Path to the SQLite database file.
        pool_size: Maximum number of worker threads.
        name: Name of the table to create with key and value BLOB fields.

    Returns:
        A Bonafide namedtuple with configuration and state.
    """
    # Create the Bonafide instance
    bonafide = Bonafide(
        db_path=db_path,
        pool_size=pool_size,
        worker_queue=queue.Queue(),
        worker_threads=[],
        worker_lock=threading.Lock(),
    )

    # Initialize the table with key and value BLOB fields
    def _initialize_table(conn: sqlite3.Connection, table_name: str) -> None:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                key BLOB PRIMARY KEY,
                value BLOB NOT NULL
            )
            """
        )
        conn.commit()

    # Apply the table initialization
    apply(bonafide, _initialize_table, name)

    return bonafide
