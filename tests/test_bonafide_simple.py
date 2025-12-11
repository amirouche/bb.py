#!/usr/bin/env python3
"""
Simple tests for core bonafide.py functions
"""

import sys
import os
import uuid

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from bonafide import (
    pool_size_default,
    bytes_write,
    bytes_read,
    bytes_next,
    nstore_indices,
    nstore_create,
    nstore_new,
    nstore_register,
    nstore_get,
    BBH,
    Variable,
    NStore,
    Bonafide,
    BonafideCnx,
    BonafideTxn,
)


def test_pool_size_default_uses_cpu_count(monkeypatch):
    """Test that pool_size_default uses CPU count"""
    monkeypatch.setattr(os, "cpu_count", lambda: 4)
    result = pool_size_default()
    assert result == 8  # 2 * 4


def test_pool_size_default_fallback(monkeypatch):
    """Test fallback when CPU count is not available"""
    monkeypatch.setattr(os, "cpu_count", lambda: None)
    result = pool_size_default()
    assert result == 4  # POOL_SIZE_DEFAULT


def test_bytes_write_read_roundtrip():
    """Test that bytes_write and bytes_read work together"""
    original = (
        None,
        True,
        False,
        0,
        1,
        -1,
        3.14,
        b"hello",
        "world",
        (1, 2, 3),
        (4, 5, 6),
    )
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)
    assert decoded == original


def test_bytes_write_none():
    """Test encoding None"""
    assert bytes_write((None,)) == b"\x00"


def test_bytes_write_boolean():
    """Test encoding booleans"""
    assert bytes_write((True,)) == b"\x08"
    assert bytes_write((False,)) == b"\x09"


def test_bytes_write_integers():
    """Test encoding integers"""
    assert bytes_write((0,)) == b"\x04"
    assert bytes_write((42,)) == b"\x05\x00\x00\x00\x00\x00\x00\x00*"
    assert bytes_write((-42,)) == b"\x06\xff\xff\xff\xff\xff\xff\xff\xd5"


def test_bytes_write_float():
    """Test encoding floats"""
    encoded = bytes_write((3.14,))
    assert len(encoded) == 9  # 1 byte type + 8 bytes float


def test_bytes_write_string():
    """Test encoding strings"""
    encoded = bytes_write(("hello",))
    assert encoded.startswith(b"\x02hello\x00")


def test_bytes_write_bytes():
    """Test encoding bytes"""
    encoded = bytes_write((b"hello",))
    assert encoded.startswith(b"\x01hello\x00")


def test_bytes_write_uuid():
    """Test encoding UUID"""
    test_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    encoded = bytes_write((test_uuid,))
    assert len(encoded) == 17  # 1 byte type + 16 bytes UUID


def test_bytes_write_bbh():
    """Test encoding BBH"""
    test_bbh = BBH("a" * 64)  # 64 char hex string
    encoded = bytes_write((test_bbh,))
    assert len(encoded) == 33  # 1 byte type + 32 bytes hash


def test_bytes_write_nested():
    """Test encoding nested tuples"""
    original = ((1, 2), (3, 4))
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)
    assert decoded == original


def test_bytes_next_simple():
    """Test bytes_next with simple cases"""
    assert bytes_next(b"\x00") == b"\x01"
    assert bytes_next(b"\x01") == b"\x02"
    assert bytes_next(b"\xff") is None


def test_bytes_next_multi_byte():
    """Test bytes_next with multi-byte sequences"""
    assert bytes_next(b"\x00\x00") == b"\x00\x01"
    # Note: bytes_next returns the next byte sequence, which may be shorter
    # when there's trailing 0xFF bytes
    result = bytes_next(b"\x00\xff")
    assert result == b"\x01"  # This is correct behavior
    assert bytes_next(b"\xff\xff") is None


def test_nstore_indices_n_1():
    """Test nstore_indices for n=1"""
    indices = nstore_indices(1)
    assert len(indices) == 1
    assert indices[0] == [0]


def test_nstore_indices_n_2():
    """Test nstore_indices for n=2"""
    indices = nstore_indices(2)
    assert len(indices) == 2
    assert [0, 1] in indices
    assert [1, 0] in indices


def test_nstore_indices_n_3():
    """Test nstore_indices for n=3"""
    indices = nstore_indices(3)
    assert len(indices) == 3  # C(3,1) = 3
    expected = [[0, 1, 2], [1, 2, 0], [2, 0, 1]]
    for idx in indices:
        assert idx in expected


def test_nstore_indices_n_4():
    """Test nstore_indices for n=4"""
    indices = nstore_indices(4)
    assert len(indices) == 6  # C(4,2) = 6


def test_nstore_create():
    """Test nstore_create function"""
    prefix = ("test",)
    n = 3
    name = "test_store"
    nstore = nstore_create(prefix, n, name)

    assert nstore.prefix == prefix
    assert nstore.n == n
    assert nstore.name == name
    assert len(nstore.indices) == 3  # C(3,1) = 3


def test_nstore_new():
    """Test nstore_new function"""
    prefix = ("test",)
    n = 2
    name = "test_store"
    nstore = nstore_new(name, prefix, n)

    assert nstore.prefix == prefix
    assert nstore.n == n
    assert nstore.name == name
    assert len(nstore.indices) == 2  # C(2,1) = 2


def test_nstore_register_and_get():
    """Test nstore_register and nstore_get functions"""
    # Create a mock Bonafide instance
    bonafide = Bonafide(
        db_path="test.db",
        pool_size=4,
        worker_queue=None,
        worker_threads=[],
        worker_lock=None,
        subspace={},
    )

    nstore = nstore_new("test", ("test",), 2)
    nstore_register(bonafide, "test", nstore)

    retrieved = nstore_get(bonafide, "test")
    assert retrieved == nstore


def test_bbh_creation_with_bytes():
    """Test BBH creation with bytes"""
    hash_bytes = b"\x00" * 32  # 32 bytes
    bbh = BBH(hash_bytes)
    assert bbh.value == hash_bytes


def test_bbh_creation_with_hex_string():
    """Test BBH creation with hex string"""
    hash_hex = "0" * 64  # 64 hex characters
    bbh = BBH(hash_hex)
    assert bbh.value == hash_hex


def test_variable_creation():
    """Test Variable creation"""
    var = Variable("test_var")
    assert var.name == "test_var"


def test_variable_equality():
    """Test Variable equality"""
    var1 = Variable("test")
    var2 = Variable("test")
    var3 = Variable("other")

    assert var1 == var2
    assert var1 != var3


def test_nstore_creation():
    """Test NStore creation"""
    prefix = ("test",)
    n = 3
    indices = [[0, 1, 2], [1, 2, 0], [2, 0, 1]]
    name = "test_store"

    nstore = NStore(prefix=prefix, n=n, indices=indices, name=name)

    assert nstore.prefix == prefix
    assert nstore.n == n
    assert nstore.indices == indices
    assert nstore.name == name


def test_bonafide_creation():
    """Test Bonafide namedtuple creation"""
    bonafide = Bonafide(
        db_path="test.db",
        pool_size=4,
        worker_queue=None,
        worker_threads=[],
        worker_lock=None,
        subspace={},
    )

    assert bonafide.db_path == "test.db"
    assert bonafide.pool_size == 4


def test_bonafide_cnx_creation():
    """Test BonafideCnx creation"""
    bonafide = Bonafide(
        db_path="test.db",
        pool_size=4,
        worker_queue=None,
        worker_threads=[],
        worker_lock=None,
        subspace={},
    )

    # Create a mock connection
    mock_conn = object()
    cnx = BonafideCnx(bonafide, mock_conn)

    assert cnx.bonafide == bonafide
    assert cnx.sqlite == mock_conn


def test_bonafide_txn_creation():
    """Test BonafideTxn creation"""
    # Create a mock connection
    mock_cnx = object()
    txn = BonafideTxn(mock_cnx)

    assert txn.cnx == mock_cnx
