from bb.pool import (
    object_28cdad4124395ef2b6ff6d41b605ee1c6d12fcc5cdb5a61407b1f17a9a8499d4 as twice,
)
from bb.pool import (
    object_d6ecfc908f64e3118d390d922f6dc2354d0a10a1bc2d823994afcccbe2280f02 as add,
)


def triple(number):
    """Triple a number by adding its double to itself."""
    return add(twice(number), number)
