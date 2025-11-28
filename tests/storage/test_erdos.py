"""
Tests for erdos_indices computation.

Tests the Erdős indices algorithm for generating minimal permutation sets
that cover all possible query patterns.

Mathematical Foundation:
    Based on Dilworth's theorem: covering the boolean lattice by the minimal number
    of maximal chains. The minimal number equals the cardinality of the maximal
    antichain in the boolean lattice, which is the central binomial coefficient C(n, n//2).

IMPORTANT: The length of erdos_indices(n) always equals the central binomial
coefficient C(n, n//2), which is the number of ways to choose n//2 items from n.
"""
import pytest
import math

from bb import erdos_indices


def test_erdos_indices_central_binomial_coefficient():
    """Test that erdos_indices length equals central binomial coefficient C(n, n//2)"""
    # Test for multiple values of n
    test_cases = [
        (3, math.comb(3, 1)),  # C(3, 1) = 3
        (4, math.comb(4, 2)),  # C(4, 2) = 6
        (5, math.comb(5, 2)),  # C(5, 2) = 10
        (6, math.comb(6, 3)),  # C(6, 3) = 20
    ]

    for n, expected_count in test_cases:
        indices = erdos_indices(n)
        assert len(indices) == expected_count, \
            f"For n={n}, expected {expected_count} indices (C({n}, {n//2})), got {len(indices)}"


def test_erdos_indices_n4_count():
    """Test that erdos_indices for n=4 generates correct number of indices"""
    indices = erdos_indices(4)

    # For n=4, we expect C(4, 2) = 6 indices (central binomial coefficient)
    assert len(indices) == 6
    assert len(indices) == math.comb(4, 2)


def test_erdos_indices_n4_length():
    """Test that erdos_indices for n=4 generates indices of correct length"""
    indices = erdos_indices(4)

    # Each index should have length 4
    for index in indices:
        assert len(index) == 4


def test_erdos_indices_n4_contains_all_positions():
    """Test that each index contains all positions 0-3"""
    indices = erdos_indices(4)

    for index in indices:
        assert set(index) == {0, 1, 2, 3}


def test_erdos_indices_n4_sorted():
    """Test that indices are returned in sorted order"""
    indices = erdos_indices(4)

    # Indices should be sorted lexicographically
    assert indices == sorted(indices)


def test_erdos_indices_n4_specific_indices():
    """Test that erdos_indices for n=4 generates expected indices"""
    indices = erdos_indices(4)

    # Based on the algorithm, these are the expected indices for n=4
    expected = [
        [0, 1, 2, 3],
        [1, 2, 3, 0],
        [2, 0, 3, 1],
        [3, 0, 1, 2],
        [3, 1, 2, 0],
        [3, 2, 0, 1]
    ]

    assert indices == expected


def test_erdos_indices_n5_count():
    """Test that erdos_indices for n=5 generates correct number of indices"""
    indices = erdos_indices(5)

    # For n=5, we expect C(5, 2) = 10 indices (central binomial coefficient)
    assert len(indices) == 10
    assert len(indices) == math.comb(5, 2)


def test_erdos_indices_n5_length():
    """Test that erdos_indices for n=5 generates indices of correct length"""
    indices = erdos_indices(5)

    # Each index should have length 5
    for index in indices:
        assert len(index) == 5


def test_erdos_indices_n5_contains_all_positions():
    """Test that each index contains all positions 0-4"""
    indices = erdos_indices(5)

    for index in indices:
        assert set(index) == {0, 1, 2, 3, 4}


def test_erdos_indices_n5_sorted():
    """Test that indices are returned in sorted order"""
    indices = erdos_indices(5)

    # Indices should be sorted lexicographically
    assert indices == sorted(indices)


def test_erdos_indices_n5_coverage():
    """Test that erdos_indices for n=5 covers all query patterns"""
    import itertools

    indices = erdos_indices(5)
    tab = list(range(5))

    # Check all possible combinations
    for r in range(1, 6):
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

            assert covered, f"Combination {combination} not covered by any index"


def test_erdos_indices_n5_specific_first_index():
    """Test that first index for n=5 is identity permutation"""
    indices = erdos_indices(5)

    # First index should be [0, 1, 2, 3, 4]
    assert indices[0] == [0, 1, 2, 3, 4]
