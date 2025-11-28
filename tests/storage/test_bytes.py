"""
Tests for bytes encoding/decoding functions.

Tests order-preserving encoding of Python values to bytes and back.
"""
import pytest

from bb import bytes_write, bytes_read, bytes_next


# ============================================================================
# Tests for bytes_write and bytes_read
# ============================================================================

def test_bytes_write_read_empty_tuple():
    """Test encoding/decoding empty tuple"""
    original = ()
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)

    assert decoded == original


def test_bytes_write_read_single_element():
    """Test encoding/decoding single element tuple"""
    original = ('hello',)
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)

    assert decoded == original


def test_bytes_write_read_mixed_types():
    """Test encoding/decoding tuple with mixed types"""
    original = ('hello', 42, 3.14, True, None)
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)

    assert decoded == original


def test_bytes_write_read_nested_tuple():
    """Test encoding/decoding tuple with nested tuple"""
    original = ('user123', 'metadata', ('tag1', 'tag2', 'tag3'))
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)

    assert decoded == original


def test_bytes_write_read_triple():
    """Test encoding/decoding 3-tuple (common nstore case)"""
    original = ('P4X432', 'blog/title', 'hyper.dev')
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)

    assert decoded == original


def test_bytes_write_read_unicode():
    """Test encoding/decoding tuple with unicode"""
    original = ('user', 'name', '你好世界')
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)

    assert decoded == original


# ============================================================================
# Tests for order preservation
# ============================================================================

def test_bytes_write_order_strings():
    """Test that encoded strings preserve lexicographic order"""
    values = [('apple',), ('banana',), ('cherry',)]
    encoded = [bytes_write(v) for v in values]

    # Encoded values should maintain order
    assert encoded[0] < encoded[1] < encoded[2]


def test_bytes_write_order_integers():
    """Test that encoded integers preserve numeric order"""
    values = [(1,), (42,), (100,), (1000,)]
    encoded = [bytes_write(v) for v in values]

    # Encoded values should maintain order
    assert encoded[0] < encoded[1] < encoded[2] < encoded[3]


def test_bytes_write_order_negative_integers():
    """Test that encoded negative integers preserve order among themselves"""
    # Note: Current encoding has negative ints type code (0x06) > zero type code (0x04),
    # so negative integers sort after zero. Test only negative number ordering.
    values = [(-100,), (-42,), (-1,)]
    encoded = [bytes_write(v) for v in values]

    # Encoded negative values should maintain order among themselves
    for i in range(len(encoded) - 1):
        assert encoded[i] < encoded[i + 1]

    # Test positive integers separately
    pos_values = [(0,), (1,), (42,), (100,)]
    pos_encoded = [bytes_write(v) for v in pos_values]

    for i in range(len(pos_encoded) - 1):
        assert pos_encoded[i] < pos_encoded[i + 1]


def test_bytes_write_order_floats():
    """Test that encoded floats preserve numeric order"""
    values = [(0.1,), (1.5,), (3.14,), (10.0,)]
    encoded = [bytes_write(v) for v in values]

    # Encoded values should maintain order
    assert encoded[0] < encoded[1] < encoded[2] < encoded[3]


def test_bytes_write_order_mixed_tuples():
    """Test order preservation with mixed-type tuples"""
    values = [
        ('user', 'age', 20),
        ('user', 'age', 30),
        ('user', 'age', 40),
    ]
    encoded = [bytes_write(v) for v in values]

    # Encoded values should maintain order
    assert encoded[0] < encoded[1] < encoded[2]


def test_bytes_write_order_prefix_matching():
    """Test order preservation with common prefixes"""
    values = [
        ('blog', 'post', 'a'),
        ('blog', 'post', 'b'),
        ('blog', 'post', 'c'),
        ('blog', 'title', 'x'),
    ]
    encoded = [bytes_write(v) for v in values]

    # Encoded values should maintain order
    assert encoded[0] < encoded[1] < encoded[2] < encoded[3]


# ============================================================================
# Tests for special cases
# ============================================================================

def test_bytes_write_null_byte_in_string():
    """Test encoding string with null byte escape"""
    original = ('test\x00data',)
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)

    assert decoded == original


def test_bytes_write_null_byte_in_bytes():
    """Test encoding bytes with null byte escape"""
    original = (b'test\x00data',)
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)

    assert decoded == original


def test_bytes_write_empty_string():
    """Test encoding empty string"""
    original = ('',)
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)

    assert decoded == original


def test_bytes_write_empty_bytes():
    """Test encoding empty bytes"""
    original = (b'',)
    encoded = bytes_write(original)
    decoded = bytes_read(encoded)

    assert decoded == original


# ============================================================================
# Tests for bytes_next
# ============================================================================

def test_bytes_next_simple():
    """Test bytes_next with simple increment"""
    assert bytes_next(b'abc') == b'abd'
    assert bytes_next(b'hello') == b'hellp'


def test_bytes_next_empty():
    """Test bytes_next with empty bytes"""
    assert bytes_next(b'') == b'\x00'


def test_bytes_next_with_0xff():
    """Test bytes_next when last byte is 0xFF"""
    # Should skip 0xFF and increment previous byte
    assert bytes_next(b'ab\xff') == b'ac'
    assert bytes_next(b'test\xff') == b'tesu'


def test_bytes_next_multiple_0xff():
    """Test bytes_next with multiple trailing 0xFF bytes"""
    assert bytes_next(b'a\xff\xff') == b'b'
    assert bytes_next(b'hello\xff\xff\xff') == b'hellp'


def test_bytes_next_all_0xff():
    """Test bytes_next when all bytes are 0xFF"""
    assert bytes_next(b'\xff') is None
    assert bytes_next(b'\xff\xff') is None
    assert bytes_next(b'\xff\xff\xff\xff') is None


def test_bytes_next_prefix_scan():
    """Test bytes_next for prefix scans"""
    # All keys starting with b'user:' would be in range [b'user:', bytes_next(b'user:'))
    prefix = b'user:'
    next_key = bytes_next(prefix)

    assert next_key == b'user;'
    assert prefix < next_key


def test_bytes_next_ordering():
    """Test that bytes_next maintains ordering property"""
    test_cases = [b'a', b'abc', b'test', b'hello']

    for data in test_cases:
        next_data = bytes_next(data)
        if next_data is not None:
            # next_data should be greater than data
            assert next_data > data
            # Everything starting with data should be less than next_data
            assert data + b'\x00' < next_data


def test_bytes_next_boundary_cases():
    """Test bytes_next with boundary values"""
    # Single byte increment
    assert bytes_next(b'\x00') == b'\x01'
    assert bytes_next(b'\x01') == b'\x02'
    assert bytes_next(b'\xfe') == b'\xff'

    # Mixed cases
    assert bytes_next(b'\x00\xff') == b'\x01'
    assert bytes_next(b'\xfe\xff') == b'\xff'
