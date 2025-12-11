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
from typing import Any, Callable, List, Tuple, TypeVar

# Type variable for function return types
T = TypeVar("T")

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
    bonafide: Bonafide, query_str: str, params: Tuple[Any, ...] = ()
) -> List[Tuple[Any, ...]]:
    """
    Execute a query within a transaction.

    Args:
        bonafide: The Bonafide configuration and state.
        query_str: The SQL query to execute.
        params: Parameters for the query.

    Returns:
        A list of rows returned by the query.
    """

    def _execute_query_func(
        conn: sqlite3.Connection, query_str: str, params: Tuple[Any, ...]
    ) -> List[Tuple[Any, ...]]:
        cursor = conn.cursor()
        cursor.execute(query_str, params)
        result = cursor.fetchall()
        conn.commit()
        return result

    return apply(bonafide, _execute_query_func, query_str, params)


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


if __name__ == "__main__":
    # Example usage
    bonafide = new()

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
    result = apply(bonafide, test_func, "example_key", b"example_value")
    print(f"Retrieved value: {result}")
