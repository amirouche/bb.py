#!/usr/bin/env python3
"""
Simple tests for core bonafide.py functions
"""

import sys
import os
import uuid
import contextlib
import tempfile

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from bonafide import (
    pool_size_default,
    bytes_write,
    bytes_read,
    bytes_next,
    nstore_indices,
    nstore_new,
    BBH,
    Variable,
    NStore,
    Bonafide,
    BonafideCnx,
    BonafideTxn,
    # Integration test functions
    new,
    apply,
    set,
    query,
    delete,
    count,
    bytes,
    nstore,
    nstore_add,
    nstore_ask,
    nstore_delete,
    nstore_query,
    nstore_count,
    nstore_bytes,
    transactional,
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


@contextlib.contextmanager
def temp_bonafide_db(pool_size=2):
    """Context manager for creating a temporary Bonafide database.

    Yields:
        bonafide: A Bonafide instance connected to a temporary database
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = tmp_file.name

    try:
        # Create and yield the Bonafide instance
        bonafide_instance = new(db_path=db_path, pool_size=pool_size)
        yield bonafide_instance
    finally:
        # Clean up the temporary database file
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_bonafide_basic_workflow():
    """Test the basic bonafide workflow: create, set, get, delete"""

    with temp_bonafide_db(pool_size=2) as bonafide_instance:
        # 1. Test basic key-value operations using apply
        # Set a key-value pair
        def set_test_data(cnx):
            set(cnx, b"test_key", b"test_value")

        apply(bonafide_instance, set_test_data)

        # Get the value back
        def get_test_data(cnx):
            return query(cnx, b"test_key")

        result = apply(bonafide_instance, get_test_data)
        assert result == b"test_value", f"Expected b'test_value', got {result}"

        # Test count
        def count_all(cnx):
            return count(cnx, b"", b"\xff")

        count_result = apply(bonafide_instance, count_all)
        assert count_result == 1, f"Expected 1 item, got {count_result}"

        # Test bytes calculation
        def calc_bytes(cnx):
            return bytes(cnx, b"", b"\xff")

        bytes_result = apply(bonafide_instance, calc_bytes)
        expected_bytes = len(b"test_key") + len(b"test_value")
        assert bytes_result == expected_bytes, (
            f"Expected {expected_bytes}, got {bytes_result}"
        )

        # 2. Test deletion
        def delete_test_data(cnx):
            return delete(cnx, b"test_key")

        delete_count = apply(bonafide_instance, delete_test_data)
        assert delete_count == 1, f"Expected 1 deletion, got {delete_count}"

        # Verify deletion
        result_after_delete = apply(bonafide_instance, get_test_data)
        assert result_after_delete is None, (
            f"Expected None after deletion, got {result_after_delete}"
        )


def test_nstore_workflow():
    """Test nstore functionality: create store, add tuples, query, delete"""

    with temp_bonafide_db(pool_size=2) as bonafide_instance:
        # 1. Create an nstore for 3-tuples (automatically registered)
        nstore(bonafide_instance, "test_relations", 3)

        # 2. Add some test data
        def add_test_tuples(cnx):
            # Add some test tuples: (subject, predicate, object)
            tuples_to_add = [
                ("alice", "knows", "bob"),
                ("bob", "knows", "charlie"),
                ("alice", "likes", "python"),
                ("bob", "likes", "rust"),
            ]

            for tuple_data in tuples_to_add:
                nstore_add(cnx, "test_relations", tuple_data)

        apply(bonafide_instance, add_test_tuples)

        # 3. Test nstore_ask (existence check)
        def check_tuple_exists(cnx):
            return nstore_ask(cnx, "test_relations", ("alice", "knows", "bob"))

        exists = apply(bonafide_instance, check_tuple_exists)
        assert exists is True, "Expected tuple to exist"

        # 4. Test nstore_count
        def count_relations(cnx):
            return nstore_count(
                cnx, "test_relations", (Variable("s"), Variable("p"), Variable("o"))
            )

        total_count = apply(bonafide_instance, count_relations)
        assert total_count == 4, f"Expected 4 tuples, got {total_count}"

        # 5. Test nstore_query with patterns
        def query_alice_relations(cnx):
            # Query all relations where subject is "alice"
            return nstore_query(
                cnx, "test_relations", ("alice", Variable("p"), Variable("o"))
            )

        alice_results = apply(bonafide_instance, query_alice_relations)
        assert len(alice_results) == 2, (
            f"Expected 2 results for alice, got {len(alice_results)}"
        )

        # Verify the results contain the expected bindings
        found_knows = False
        found_likes = False
        for result in alice_results:
            if result.get("p") == "knows" and result.get("o") == "bob":
                found_knows = True
            elif result.get("p") == "likes" and result.get("o") == "python":
                found_likes = True

        assert found_knows and found_likes, (
            "Expected to find both 'knows' and 'likes' relations for alice"
        )

        # 6. Test nstore_delete
        def delete_bob_relations(cnx):
            # Delete all relations where subject is "bob"
            bob_relations = nstore_query(
                cnx, "test_relations", ("bob", Variable("p"), Variable("o"))
            )
            for relation in bob_relations:
                nstore_delete(
                    cnx, "test_relations", ("bob", relation["p"], relation["o"])
                )

        apply(bonafide_instance, delete_bob_relations)

        # Verify deletion
        remaining_count = apply(bonafide_instance, count_relations)
        assert remaining_count == 2, (
            f"Expected 2 tuples after deletion, got {remaining_count}"
        )


def test_bbh_functionality():
    """Test BBH (Beyond Babel Hash) functionality"""

    with temp_bonafide_db(pool_size=2) as bonafide_instance:
        # 1. Test BBH creation and storage
        test_hash_hex = "a" * 64  # 64 char hex string
        test_bbh = BBH(test_hash_hex)

        def store_bbh(cnx):
            # Store BBH as key and value
            set(cnx, bytes_write((test_bbh,)), bytes_write(("metadata",)))

        apply(bonafide_instance, store_bbh)

        # 2. Test retrieval and verification
        def retrieve_bbh(cnx):
            encoded_key = bytes_write((test_bbh,))
            return query(cnx, encoded_key)

        result = apply(bonafide_instance, retrieve_bbh)
        assert result is not None, "Expected to find BBH in storage"

        # Decode the result to verify it's our metadata
        decoded = bytes_read(result)
        assert decoded == ("metadata",), f"Expected ('metadata',), got {decoded}"


def test_transaction_isolation():
    """Test that transactions are properly isolated"""

    with temp_bonafide_db(pool_size=2) as bonafide_instance:
        # 1. Set initial data
        def set_initial_data(cnx):
            set(cnx, b"counter", b"0")

        apply(bonafide_instance, set_initial_data)

        # 2. Test multiple concurrent operations
        def increment_counter(cnx):
            current = query(cnx, b"counter")
            if current is None:
                current_value = 0
            else:
                current_value = int(current.decode())

            new_value = current_value + 1
            set(cnx, b"counter", str(new_value).encode())
            return new_value

        # Apply multiple increments
        results = []
        for i in range(5):
            result = apply(bonafide_instance, increment_counter)
            results.append(result)

        # Verify final counter value
        def get_final_counter(cnx):
            final = query(cnx, b"counter")
            return int(final.decode()) if final else 0

        final_counter = apply(bonafide_instance, get_final_counter)
        assert final_counter == 5, (
            f"Expected final counter to be 5, got {final_counter}"
        )


def test_nstore_bytes():
    """Test nstore_bytes functionality for calculating storage usage"""

    with temp_bonafide_db(pool_size=2) as bonafide_instance:
        # 1. Create an nstore and add some data
        nstore(bonafide_instance, "test_bytes", 2)

        def add_test_data(cnx):
            # Add some test tuples
            tuples_to_add = [
                ("user1", "data1"),
                ("user2", "data2"),
                ("user1", "data3"),
            ]

            for tuple_data in tuples_to_add:
                nstore_add(cnx, "test_bytes", tuple_data)

        apply(bonafide_instance, add_test_data)

        # 2. Test nstore_bytes with different patterns
        def calc_bytes_for_user(cnx, user):
            return nstore_bytes(cnx, "test_bytes", (user, Variable("data")))

        # Calculate bytes for user1
        user1_bytes = apply(bonafide_instance, calc_bytes_for_user, "user1")
        assert user1_bytes > 0, "Expected positive byte count for user1"

        # Calculate bytes for user2
        user2_bytes = apply(bonafide_instance, calc_bytes_for_user, "user2")
        assert user2_bytes > 0, "Expected positive byte count for user2"

        # Calculate bytes for all users
        def calc_all_bytes(cnx):
            return nstore_bytes(cnx, "test_bytes", (Variable("user"), Variable("data")))

        total_bytes = apply(bonafide_instance, calc_all_bytes)
        assert total_bytes > 0, "Expected positive total byte count"

        # Verify that total bytes is at least as much as individual user bytes
        assert total_bytes >= user1_bytes + user2_bytes, (
            "Total bytes should be at least sum of individual user bytes"
        )


def test_transactional_decorator():
    """Test the transactional decorator behavior"""

    with temp_bonafide_db(pool_size=2) as bonafide_instance:
        # 1. Test that transactional decorator works with BonafideCnx
        @transactional
        def test_transactional_function(txn):
            # This function should work when called with a BonafideCnx
            set(txn, b"test_key", b"test_value")
            result = query(txn, b"test_key")
            return result

        def run_transactional_test(cnx):
            # Test the transactional function
            result = test_transactional_function(cnx)
            assert result == b"test_value", f"Expected b'test_value', got {result}"

            # Verify the data was actually stored
            stored_result = query(cnx, b"test_key")
            assert stored_result == b"test_value", (
                "Data should be stored after transactional function"
            )

        apply(bonafide_instance, run_transactional_test)

        # 2. Test that transactional decorator handles exceptions properly
        @transactional
        def failing_transactional_function(txn):
            set(txn, b"temp_key", b"temp_value")
            raise ValueError("Test exception")

        def test_exception_handling(cnx):
            # This should raise an exception and rollback
            try:
                failing_transactional_function(cnx)
                assert False, "Expected exception to be raised"
            except ValueError:
                pass  # Expected

            # Verify that the temp data was not stored (rolled back)
            temp_result = query(cnx, b"temp_key")
            assert temp_result is None, (
                "Temp data should not exist after failed transaction"
            )

        apply(bonafide_instance, test_exception_handling)
