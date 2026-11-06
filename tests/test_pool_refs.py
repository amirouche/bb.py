"""Normalization 4: pool functions are stored as bb.ref("<hash>"); older objects keep working."""
import ast

import bb
from tests.conftest import normalize_code_for_test
from tests.test_import_by_name import Pool, hash_of
from tests.test_internals import _pool_add


def _save_v3_caller(leaf):
    """Store a caller the way normalization 3 did: an object_<hash> import and object_<hash>._bb_v_0."""
    code = normalize_code_for_test(f"from bb.pool import object_{leaf}\n\n"
                                   f"def _bb_v_0(_bb_v_1):\n    return object_{leaf}._bb_v_0(_bb_v_1) + 1")
    func = bb.hash_compute(code)
    bb.object_save(func, code, bb.code_create_metadata(), normalization=3)
    bb.mapping_save(func, 'eng', '', {'_bb_v_0': 'caller', '_bb_v_1': 'n'}, {leaf: 'leaf'})
    return func


def test_version_3_objects_still_load_and_refactor(mock_bb_dir):
    leaf = _pool_add("def leaf(n):\n    return 2 * n\n")
    caller = _save_v3_caller(leaf)
    assert bb.code_extract_dependencies(bb.object_load(caller)['normalized_code']) == [leaf]
    assert bb.code_load_normalized(caller)(3) == 7

    other = _pool_add("def leaf(n):\n    return 3 * n\n")
    tree = bb.code_swap_refs(ast.parse(bb.object_load(caller)['normalized_code']), {leaf: other})
    assert f"object_{other}._bb_v_0" in ast.unparse(tree) and leaf not in ast.unparse(tree)


def test_refs_are_loaded_and_swapped(mock_bb_dir):
    leaf = _pool_add("def leaf(n):\n    return 2 * n\n")
    caller = _pool_add(f"import bb\nleaf = bb.ref('{leaf}')\n\ndef caller(n):\n    return leaf(n) + 1\n")
    code = bb.object_load(caller)['normalized_code']
    assert f"bb.ref('{leaf}')" in code and 'bb.pool' not in code
    assert bb.code_load_normalized(caller)(3) == 7
    other = _pool_add("def leaf(n):\n    return 3 * n\n")
    assert f"bb.ref('{other}')" in ast.unparse(bb.code_swap_refs(ast.parse(code), {leaf: other}))


def test_a_function_named_object_something_imports_by_name(tmp_path):
    pool = Pool(tmp_path)
    page = hash_of(pool.edit('object_page', 'def object_page(n):\n    """A page."""\n    return n + 1\n'))
    saved = pool.edit('app', 'import bb\nobject_page = bb.latest("object_page")\n\n\ndef app(n):\n    """The app."""\n'
                      '    return object_page(n)\n')
    assert saved.returncode == 0, saved.stderr
    assert f'Import: object_page -> {page}' in saved.stdout
    assert pool.run(hash_of(saved), 1) == '2'


def test_from_bb_pool_import_is_refused(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n')
    saved = pool.edit('app', 'from bb.pool import double\n\n\ndef app(n):\n    """The app."""\n    return double(n)\n')
    assert saved.returncode == 1 and '`from bb.pool import ...` is gone' in saved.stderr
