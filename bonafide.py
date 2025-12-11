#!/usr/bin/env python3
"""
Bonafide Storage Primitives

Bonafide is a storage layer for bb.py that implements an ordered key-value store using SQLite. It provides thread-safe access to the database through a connection pool, ensuring efficient concurrency while respecting SQLite's single-writer constraint.
"""

import sqlite3
import threading
import os
import queue
import itertools
import struct
import uuid
from collections import namedtuple
from typing import Any, Callable, List, Optional, TypeVar, Dict, Tuple

# Type variable for function return types
T = TypeVar("T")

# Store builtin bytes to avoid naming conflicts
_builtin_bytes = bytes

# Default constants
DEFAULT_DB_PATH = "bonafide.db"
POOL_SIZE_DEFAULT = 4


def pool_size_default() -> int:
    """Calculate the default pool size based on available CPU cores.

    Returns:
        int: Default pool size (2 * CPU count, or 4 if CPU count is not available)
    """
    return os.cpu_count() * 2 if os.cpu_count() else POOL_SIZE_DEFAULT


# Order-preserving encoding constants
_BONAFIDE_BYTE_NULL = 0x00
_BONAFIDE_BYTE_BYTES = 0x01
_BONAFIDE_BYTE_STRING = 0x02
_BONAFIDE_BYTE_NESTED = 0x03
_BONAFIDE_BYTE_INT_ZERO = 0x04
_BONAFIDE_BYTE_INT_POS = 0x05
_BONAFIDE_BYTE_INT_NEG = 0x06
_BONAFIDE_BYTE_FLOAT = 0x07
_BONAFIDE_BYTE_TRUE = 0x08
_BONAFIDE_BYTE_FALSE = 0x09
_BONAFIDE_BYTE_UUID = 0x0A
_BONAFIDE_BYTE_BBH = 0x0B

# BBH (Beyond Babel Hash) type for content-addressed references
BBH = namedtuple("BBH", ["value"])

# Variable type for pattern matching
Variable = namedtuple("Variable", ["name"])

# NStore type
NStore = namedtuple("NStore", ["prefix", "n", "indices", "name"])


def _bytes_write_one(value: Any, nested: bool = False) -> bytes:
    """Encode a single value to bytes with order preservation."""
    if value is None:
        return _builtin_bytes(
            [_BONAFIDE_BYTE_NULL, 0xFF] if nested else [_BONAFIDE_BYTE_NULL]
        )
    elif isinstance(value, bool):
        return _builtin_bytes([_BONAFIDE_BYTE_TRUE if value else _BONAFIDE_BYTE_FALSE])
    elif isinstance(value, _builtin_bytes):
        return (
            _builtin_bytes([_BONAFIDE_BYTE_BYTES])
            + value.replace(b"\x00", b"\x00\xff")
            + b"\x00"
        )
    elif isinstance(value, str):
        return (
            _builtin_bytes([_BONAFIDE_BYTE_STRING])
            + value.encode("utf-8").replace(b"\x00", b"\x00\xff")
            + b"\x00"
        )
    elif value == 0:
        return _builtin_bytes([_BONAFIDE_BYTE_INT_ZERO])
    elif isinstance(value, int):
        if value > 0:
            return _builtin_bytes([_BONAFIDE_BYTE_INT_POS]) + struct.pack(">Q", value)
        else:
            return _builtin_bytes([_BONAFIDE_BYTE_INT_NEG]) + struct.pack(
                ">Q", (1 << 64) - 1 + value
            )
    elif isinstance(value, float):
        bits = struct.pack(">d", value)
        # Flip sign bit, or flip all bits if negative
        if bits[0] & 0x80:
            bits = _builtin_bytes(b ^ 0xFF for b in bits)
        else:
            bits = _builtin_bytes([bits[0] ^ 0x80]) + bits[1:]
        return _builtin_bytes([_BONAFIDE_BYTE_FLOAT]) + bits
    elif isinstance(value, uuid.UUID):
        # UUIDs are stored as 16 bytes (128 bits)
        # UUID.bytes maintains lexicographic ordering for ULIDs
        return _builtin_bytes([_BONAFIDE_BYTE_UUID]) + value.bytes
    elif isinstance(value, BBH):
        # BBH stores a SHA256 hash (32 bytes)
        # value can be bytes or hex string
        if isinstance(value.value, _builtin_bytes):
            if len(value.value) != 32:
                raise ValueError(
                    f"BBH bytes must be exactly 32 bytes, got {len(value.value)}"
                )
            return _builtin_bytes([_BONAFIDE_BYTE_BBH]) + value.value
        elif isinstance(value.value, str):
            if len(value.value) != 64:
                raise ValueError(
                    f"BBH hex string must be exactly 64 characters, got {len(value.value)}"
                )
            return _builtin_bytes([_BONAFIDE_BYTE_BBH]) + _builtin_bytes.fromhex(
                value.value
            )
        else:
            raise ValueError(
                f"BBH value must be bytes or hex string, got {type(value.value)}"
            )
    elif isinstance(value, (tuple, list)):
        return (
            _builtin_bytes([_BONAFIDE_BYTE_NESTED])
            + b"".join(_bytes_write_one(v, True) for v in value)
            + _builtin_bytes([0x00])
        )
    else:
        raise ValueError(f"Unsupported type for encoding: {type(value)}")


def _bytes_read_one(data: bytes, pos: int = 0) -> Tuple[Any, int]:
    """Decode a single value from bytes."""
    code = data[pos]
    if code == _BONAFIDE_BYTE_NULL:
        return (None, pos + 1)
    elif code == _BONAFIDE_BYTE_BYTES:
        end = pos + 1
        while end < len(data):
            if data[end] == 0x00 and (end + 1 >= len(data) or data[end + 1] != 0xFF):
                break
            end += 1 if data[end] != 0x00 else 2
        return (data[pos + 1 : end].replace(b"\x00\xff", b"\x00"), end + 1)
    elif code == _BONAFIDE_BYTE_STRING:
        end = pos + 1
        while end < len(data):
            if data[end] == 0x00 and (end + 1 >= len(data) or data[end + 1] != 0xFF):
                break
            end += 1 if data[end] != 0x00 else 2
        return (
            data[pos + 1 : end].replace(b"\x00\xff", b"\x00").decode("utf-8"),
            end + 1,
        )
    elif code == _BONAFIDE_BYTE_INT_ZERO:
        return (0, pos + 1)
    elif code == _BONAFIDE_BYTE_INT_POS:
        return (struct.unpack(">Q", data[pos + 1 : pos + 9])[0], pos + 9)
    elif code == _BONAFIDE_BYTE_INT_NEG:
        val = struct.unpack(">Q", data[pos + 1 : pos + 9])[0]
        return (val - ((1 << 64) - 1), pos + 9)
    elif code == _BONAFIDE_BYTE_FLOAT:
        bits = bytearray(data[pos + 1 : pos + 9])
        if bits[0] & 0x80:
            bits[0] ^= 0x80
        else:
            bits = _builtin_bytes(b ^ 0xFF for b in bits)
        return (struct.unpack(">d", _builtin_bytes(bits))[0], pos + 9)
    elif code == _BONAFIDE_BYTE_TRUE:
        return (True, pos + 1)
    elif code == _BONAFIDE_BYTE_FALSE:
        return (False, pos + 1)
    elif code == _BONAFIDE_BYTE_UUID:
        # UUIDs are stored as 16 bytes (128 bits)
        return (uuid.UUID(bytes=data[pos + 1 : pos + 17]), pos + 17)
    elif code == _BONAFIDE_BYTE_BBH:
        # BBH stores a SHA256 hash (32 bytes)
        # Return as hex string for easier use
        hash_bytes = data[pos + 1 : pos + 33]
        return (BBH(hash_bytes.hex()), pos + 33)
    elif code == _BONAFIDE_BYTE_NESTED:
        result = []
        pos += 1
        while pos < len(data):
            if data[pos] == 0x00:
                if pos + 1 < len(data) and data[pos + 1] == 0xFF:
                    result.append(None)
                    pos += 2
                else:
                    break
            else:
                val, pos = _bytes_read_one(data, pos)
                result.append(val)
        return (tuple(result), pos + 1)
    else:
        raise ValueError(f"Unknown encode type code: {code}")


def bytes_write(items: Tuple) -> bytes:
    """Encode a tuple to bytes with order preservation."""
    return b"".join(_bytes_write_one(item) for item in items)


def bytes_read(data: bytes) -> Tuple:
    """Decode bytes back to tuple."""
    result = []
    pos = 0
    while pos < len(data):
        val, pos = _bytes_read_one(data, pos)
        result.append(val)
    return tuple(result)


def bytes_next(data: _builtin_bytes) -> Optional[_builtin_bytes]:
    """Compute next byte sequence for exclusive upper bound in range queries."""
    if not data:
        return _builtin_bytes([0x00])

    # Find rightmost byte that's not 0xFF
    for i in range(len(data) - 1, -1, -1):
        if data[i] != 0xFF:
            # Increment this byte and truncate everything after
            return data[:i] + _builtin_bytes([data[i] + 1])

    # All bytes are 0xFF, no successor exists
    return None


# Bonafide namedtuple to hold configuration and state
Bonafide = namedtuple(
    "Bonafide",
    [
        "db_path",
        "pool_size",
        "worker_queue",
        "worker_threads",
        "worker_lock",
        "subspace",
    ],
)

# Connection and transaction types
BonafideCnx = namedtuple("BonafideCnx", ["bonafide", "sqlite"])
BonafideTxn = namedtuple("BonafideTxn", ["cnx"])


def transactional(func):
    def wrapper(something, *args, **kwargs):
        if isinstance(something, BonafideCnx):
            cnx = something
            txn = BonafideTxn(cnx)
            try:
                out = func(txn, *args, **kwargs)
                # Commit if no exception occurred
                cnx.commit()
            except Exception:
                # Rollback if an exception occurred
                cnx.rollback()
                raise
            else:
                return out
            finally:
                cnx.close()
        elif isinstance(something, BonafideTxn):
            return func(something, *args, **kwargs)
        else:
            msg = "transactional does not support unexpected: {}".format(
                type(something)
            )
            raise NotImplementedError(msg)

    return wrapper


@transactional
def nstore_add(txn, name: str, items: Tuple) -> None:
    """Add a tuple to the nstore."""
    nstore = nstore_get(txn.cnx.bonafide, name)
    assert len(items) == nstore.n, f"Expected {nstore.n} items, got {len(items)}"

    # Add to all permuted indices
    for subspace, index in enumerate(nstore.indices):
        permuted = _nstore_permute(items, index)
        key = bytes_write(nstore.prefix + (subspace,) + permuted)
        set(txn, key, b"\x01")


def _nstore_indices_verify_coverage(indices: List[List[int]], n: int) -> bool:
    """Verify that indices cover all possible query patterns."""
    tab = list(range(n))
    for r in range(1, n + 1):
        for combination in itertools.combinations(tab, r):
            covered = False
            for index in indices:
                for perm in itertools.permutations(combination):
                    if len(perm) <= len(index):
                        if all(a == b for a, b in zip(perm, index)):
                            covered = True
                            break
                if covered:
                    break
            if not covered:
                return False
    return True


def nstore_indices(n: int) -> List[List[int]]:
    """Compute minimal set of permutation indices for n-tuple store.

    This function implements an algorithm based on Dilworth's theorem to determine
    the minimal number of permutation indices needed to enable efficient single-hop
    queries for any query pattern in an n-tuple store.

    Mathematical Foundation:
        - Based on covering the boolean lattice by the minimal number of maximal chains
        - By Dilworth's theorem, this minimal number equals the cardinality of the maximal
          antichain in the boolean lattice, which is the central binomial coefficient C(n, n//2)
        - For n=3: C(3,1) = 3 indices needed
        - For n=4: C(4,2) = 6 indices needed
        - For n=5: C(5,2) = 10 indices needed

    Args:
        n: Number of elements in tuples

    Returns:
        List of exactly C(n, n//2) index permutations in lexicographic order

    Examples:
        >>> nstore_indices(3)  # C(3, 1) = 3 indices
        [[0, 1, 2], [1, 2, 0], [2, 0, 1]]
        >>> nstore_indices(4)  # C(4, 2) = 6 indices
        [[0, 1, 2, 3], [1, 2, 3, 0], [2, 0, 3, 1], [3, 0, 1, 2], [3, 1, 2, 0], [3, 2, 0, 1]]
    """
    tab = list(range(n))
    cx = list(itertools.combinations(tab, n // 2))
    out = []

    for combo in cx:
        L = [(i, i in combo) for i in tab]
        a, b = [], []

        while True:
            # Find swap pair (inline findij logic)
            found = False
            for idx in range(len(L) - 1):
                if L[idx][1] is False and L[idx + 1][1] is True:
                    remaining = L[:idx] + L[idx + 2 :]
                    i, j = L[idx][0], L[idx + 1][0]
                    L = remaining
                    a.append(j)
                    b.append(i)
                    found = True
                    break

            if not found:
                out.append(list(reversed(a)) + [x[0] for x in L] + list(reversed(b)))
                break

    out.sort()

    # Verify coverage
    assert _nstore_indices_verify_coverage(out, n), (
        "Generated indices do not cover all combinations"
    )

    return out


# NStore functions


def nstore_create(prefix: Tuple, n: int, name: str) -> NStore:
    """Create an NStore instance.

    This function maintains backward compatibility with the old API while supporting
    the new subspace-based approach.

    Args:
        prefix: Tuple prefix for this nstore
        n: Number of elements in tuples
        name: Name for this nstore instance

    Returns:
        NStore instance with the specified configuration
    """
    # Semi-private function
    indices = nstore_indices(n)
    return NStore(prefix=prefix, n=n, indices=indices, name=name)


def nstore_new(name: str, prefix: Tuple, n: int) -> NStore:
    """Create a new NStore instance with subspace-based configuration.

    This function creates an NStore that can be looked up by name in the Bonafide
    configuration, allowing for more flexible nstore management.

    Args:
        name: Unique name for this nstore instance
        prefix: Tuple prefix for this nstore
        n: Number of elements in tuples

    Returns:
        NStore instance with the specified configuration
    """
    # Semi-private function
    indices = nstore_indices(n)
    return NStore(prefix=prefix, n=n, indices=indices, name=name)


def nstore(bonafide: Bonafide, name: str, n: int) -> NStore:
    """Create and register a new NStore with inferred prefix (name,).

    This is a convenience function that creates an NStore with a prefix of (name,)
    and automatically registers it in the Bonafide subspace.

    Args:
        bonafide: Bonafide instance
        name: Unique name for this nstore instance (used as prefix)
        n: Number of elements in tuples

    Returns:
        NStore instance with the specified configuration
    """
    prefix = (name,)
    nstore_instance = nstore_new(name, prefix, n)
    nstore_register(bonafide, name, nstore_instance)
    return nstore_instance


def _nstore_permute(items: Tuple, index: List[int]) -> Tuple:
    """Permute tuple elements according to index."""
    return tuple(items[i] for i in index)


def _nstore_unpermute(items: Tuple, index: List[int]) -> Tuple:
    """Reverse a permutation to get original tuple."""
    result = [None] * len(items)
    for i, idx in enumerate(index):
        result[idx] = items[i]
    return tuple(result)


def nstore_register(bonafide: Bonafide, name: str, nstore: NStore) -> None:
    """Register an NStore instance in the Bonafide subspace.

    Args:
        bonafide: Bonafide instance
        name: Name to register the nstore under
        nstore: NStore instance to register
    """
    # Semi-private function
    bonafide.subspace[name] = nstore


def nstore_get(bonafide: Bonafide, name: str) -> NStore:
    """Get an NStore instance by name from the Bonafide subspace.

    Args:
        bonafide: Bonafide instance
        name: Name of the nstore to retrieve

    Returns:
        NStore instance

    Raises:
        KeyError: If no nstore with the given name exists
    """
    return bonafide.subspace[name]


@transactional
def nstore_delete(txn, name: str, items: Tuple) -> None:
    """Delete a tuple from the nstore."""
    nstore = nstore_get(txn.cnx.bonafide, name)
    assert len(items) == nstore.n, f"Expected {nstore.n} items, got {len(items)}"

    # Delete from all permuted indices
    for subspace, index in enumerate(nstore.indices):
        permuted = _nstore_permute(items, index)
        key = bytes_write(nstore.prefix + (subspace,) + permuted)
        delete(txn, key)


@transactional
def nstore_ask(txn, name: str, items: Tuple) -> bool:
    """Check if a tuple exists in the nstore."""
    nstore = nstore_get(txn.cnx.bonafide, name)
    assert len(items) == nstore.n, f"Expected {nstore.n} items, got {len(items)}"

    # Check base index (subspace 0) with the original tuple
    # The key format is: prefix + (subspace,) + permuted_tuple
    # For subspace 0, the permutation is the identity permutation [0, 1, 2, ...]
    permuted = _nstore_permute(items, nstore.indices[0])
    key = bytes_write(nstore.prefix + (0,) + permuted)
    return query(txn, key) is not None


def _nstore_pattern_to_combination(pattern: Tuple) -> List[int]:
    """Extract positions of non-variable elements from pattern."""
    return [i for i, item in enumerate(pattern) if not isinstance(item, Variable)]


def _nstore_pattern_to_index(
    pattern: Tuple, indices: List[List[int]]
) -> Tuple[List[int], int]:
    """Find the index and subspace that matches the pattern."""
    combination = _nstore_pattern_to_combination(pattern)

    for subspace, index in enumerate(indices):
        # Check if any permutation of combination is a prefix of index
        for perm in itertools.permutations(combination):
            if len(perm) <= len(index) and all(a == b for a, b in zip(perm, index)):
                return (index, subspace)

    raise ValueError(f"No matching index found for pattern {pattern}")


def _nstore_pattern_to_prefix(pattern: Tuple, index: List[int]) -> Tuple:
    """Extract the concrete prefix from pattern for range query."""
    result = []
    for idx in index:
        value = pattern[idx]
        if isinstance(value, Variable):
            break
        result.append(value)
    return tuple(result)


def _nstore_bind_pattern(pattern: Tuple, bindings: Dict[str, Any]) -> Tuple:
    """Replace variables in pattern with their bound values."""
    return tuple(
        bindings[item.name]
        if isinstance(item, Variable) and item.name in bindings
        else item
        for item in pattern
    )


def _nstore_bind_tuple(
    pattern: Tuple, tuple_items: Tuple, seed: Dict[str, Any]
) -> Dict[str, Any]:
    """Bind variables in pattern to values from matching tuple."""
    result = dict(seed)
    for pattern_item, tuple_item in zip(pattern, tuple_items):
        if isinstance(pattern_item, Variable):
            result[pattern_item.name] = tuple_item
    return result


@transactional
def nstore_query(
    txn, name: str, pattern: Tuple, *patterns: Tuple
) -> List[Dict[str, Any]]:
    """Query tuples matching pattern and optional additional where patterns."""
    nstore = nstore_get(txn.cnx.bonafide, name)
    patterns = [pattern] + list(patterns)

    # Start with initial empty binding
    bindings = [{}]

    # Process each pattern
    for pat in patterns:
        assert len(pat) == nstore.n, (
            f"Pattern length {len(pat)} doesn't match nstore size {nstore.n}"
        )

        new_bindings = []

        for binding in bindings:
            # Bind variables in pattern with current bindings
            bound_pattern = _nstore_bind_pattern(pat, binding)

            # Find matching index
            index, subspace = _nstore_pattern_to_index(bound_pattern, nstore.indices)

            # Build prefix for range query
            prefix_items = _nstore_pattern_to_prefix(bound_pattern, index)
            key_start = bytes_write(nstore.prefix + (subspace,) + prefix_items)
            key_end = bytes_next(key_start)
            if key_end is None:
                # All bytes are 0xFF, use next longer sequence
                key_end = key_start + b"\x00"

            # Range scan
            results = query(txn, key_start, key_end)
            results = [(row[0], row[1]) for row in results]

            for key, _ in results:
                # Decode key
                unpacked = bytes_read(key)

                # Extract tuple (skip prefix + subspace)
                permuted_tuple = unpacked[len(nstore.prefix) + 1 :]

                # Reverse permutation
                original_tuple = _nstore_unpermute(permuted_tuple, index)

                # Bind variables from pattern
                new_binding = _nstore_bind_tuple(pat, original_tuple, binding)
                new_bindings.append(new_binding)

        bindings = new_bindings

    return bindings


@transactional
def nstore_count(txn, name: str, pattern: Tuple) -> int:
    """Count tuples matching pattern."""
    nstore = nstore_get(txn.cnx.bonafide, name)
    assert len(pattern) == nstore.n, (
        f"Pattern length {len(pattern)} doesn't match nstore size {nstore.n}"
    )

    # Find matching index
    index, subspace = _nstore_pattern_to_index(pattern, nstore.indices)

    # Build prefix for range query
    prefix_items = _nstore_pattern_to_prefix(pattern, index)
    key_start = bytes_write(nstore.prefix + (subspace,) + prefix_items)
    key_end = bytes_next(key_start)
    if key_end is None:
        # All bytes are 0xFF, use next longer sequence
        key_end = key_start + b"\x00"

    return count(txn, key_start, key_end)


@transactional
def nstore_bytes(txn, name: str, pattern: Tuple) -> int:
    """Sum the length of bytes in keys and values for tuples matching pattern."""
    nstore = nstore_get(txn.cnx.bonafide, name)
    assert len(pattern) == nstore.n, (
        f"Pattern length {len(pattern)} doesn't match nstore size {nstore.n}"
    )

    # Find matching index
    index, subspace = _nstore_pattern_to_index(pattern, nstore.indices)

    # Build prefix for range query
    prefix_items = _nstore_pattern_to_prefix(pattern, index)
    key_start = bytes_write(nstore.prefix + (subspace,) + prefix_items)
    key_end = bytes_next(key_start)
    if key_end is None:
        # All bytes are 0xFF, use next longer sequence
        key_end = key_start + b"\x00"

    return bytes(txn, key_start, key_end)


def _bonafide_worker(bonafide: Bonafide) -> None:
    """
    Worker thread that processes database operations.
    """
    # TODO: implement clean exit shutdown

    # Create a new connection for this operation
    cnx = sqlite3.connect(bonafide.db_path, check_same_thread=False)
    cnx.execute("PRAGMA journal_mode=WAL")
    cnx = BonafideCnx(bonafide, cnx)

    while True:
        try:
            # Get task from queue
            task_id, func, args, kwargs, result_queue = bonafide.worker_queue.get()
            try:
                result = func(cnx, *args, **kwargs)
                result_queue.put((task_id, result))
            except Exception as e:
                result_queue.put((task_id, e))
            finally:
                bonafide.worker_queue.task_done()
        except Exception:
            # If any error occurs, continue processing
            bonafide.worker_queue.task_done()
            cnx.sqlite.close()
            cnx = sqlite3.connect(bonafide.db_path, check_same_thread=False)
            cnx.execute("PRAGMA journal_mode=WAL")
            cnx = BonafideCnx(bonafide, cnx)


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
    Execute a function within the bonafide thread.

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


@transactional
def query(
    txn,
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
        cursor = txn.cnx.sqlite.execute(
            "SELECT value FROM kv_store WHERE key = ?", (key,)
        )
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

        cursor = txn.cnx.sqlite.execute(query, params)
        return [(row[0], row[1]) for row in cursor]


@transactional
def set(txn, key: _builtin_bytes, value: _builtin_bytes) -> None:
    """Set a key-value pair in the database.

    Args:
        conn: SQLite connection
        key: Key to set
        value: Value to associate with the key
    """
    cursor = txn.cnx.sqlite.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", (key, value)
    )


@transactional
def delete(
    txn,
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
        cursor = txn.cnx.sqlite.cursor()
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

        cursor = txn.cnx.sqlite.cursor()
        cursor.execute(base_query, params)
        return cursor.rowcount


@transactional
def bytes(
    txn,
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
    cursor = txn.cnx.sqlite.execute(query, params)
    return cursor.fetchone()[0]


@transactional
def count(
    txn,
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
    cursor = txn.cnx.sqlite.execute(query, params)
    return cursor.fetchone()[0]


def new(
    db_path: str = DEFAULT_DB_PATH,
    pool_size: int = None,
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
    # Calculate default pool size if not provided
    if pool_size is None:
        pool_size = pool_size_default()

    # Create the Bonafide instance
    bonafide = Bonafide(
        db_path=db_path,
        pool_size=pool_size,
        worker_queue=queue.Queue(),
        worker_threads=[],
        worker_lock=threading.Lock(),
        subspace={},
    )

    # Initialize the table with key and value BLOB fields
    cnx = sqlite3.connect(bonafide.db_path, check_same_thread=False)
    cnx.execute("PRAGMA journal_mode=WAL")
    cnx.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {name} (
            key BLOB PRIMARY KEY,
            value BLOB NOT NULL
        )
        """
    )
    cnx.commit()
    cnx.close()

    return bonafide
