"""Saving refuses a function that reads a name defined nowhere: it would only fail with a NameError when run."""
from tests.test_import_by_name import Pool, hash_of

OK = '''import math
from collections import Counter
import bb
helper = bb.resolve("helper")

COUNTER = None


def ok(items, n):
    """Everything here is defined somewhere."""
    total = 0
    def add(x):
        nonlocal total
        total += x
    for item in items:
        add(helper(item))
    squares = [i * i for i in range(n)]
    pick = lambda k: squares[k]
    try:
        pass
    except ValueError as error:
        print(error)
    global COUNTER
    COUNTER = Counter(items)
    return math.sqrt(total) + pick(0) + len(__name__)
'''


def test_undefined_names_are_refused_with_their_line(tmp_path):
    pool = Pool(tmp_path)
    refused = pool.edit('broken', 'def broken(x):\n    """Calls a name defined nowhere."""\n'
                                  '    return undefined_thing(x) + other\n')
    assert refused.returncode == 1
    assert 'defined nowhere' in refused.stderr
    assert 'line 3: other' in refused.stderr and 'line 3: undefined_thing' in refused.stderr


def test_closures_comprehensions_globals_imports_and_bb_names_are_defined(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('helper', 'def helper(x):\n    """Double."""\n    return 2 * x\n')
    saved = pool.edit('ok', OK.replace('\nCOUNTER = None\n', ''))
    assert saved.returncode == 0, saved.stderr


def test_a_pool_name_whose_binding_is_gone_is_refused(tmp_path):
    # The bug writers hit: the bb.resolve binding was dropped while unused, then the name was used
    pool = Pool(tmp_path)
    pool.edit('helper', 'def helper(x):\n    """Double."""\n    return 2 * x\n')
    refused = pool.edit('page', 'def page(x):\n    """Uses helper with no binding."""\n    return helper(x)\n')
    assert refused.returncode == 1 and 'line 3: helper' in refused.stderr
