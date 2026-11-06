"""
Tests for storage functions.

Tests for saving and loading functions.
"""
import json
from unittest.mock import patch

import pytest

import bb
from tests.conftest import normalize_code_for_test


# ============================================================================
# Tests for Write Path
# ============================================================================

def test_object_save_creates_object_json(mock_bb_dir):
    """Test that object_save creates proper object.json"""
    test_hash = "abcd1234" + "0" * 56
    normalized_code = normalize_code_for_test("def _bb_v_0(): pass")
    metadata = {
        'created': '2025-01-01T00:00:00Z',
        'name': 'testuser',
        'email': 'test@example.com'
    }

    bb.object_save(test_hash, normalized_code, metadata)

    # Check that object.json was created: pool/functions/XX/YYYY.../object.json
    pool_dir = mock_bb_dir / '.bb' / 'pool'
    func_dir = pool_dir / 'functions' / test_hash[:2] / test_hash[2:]
    object_json = func_dir / 'object.json'

    assert object_json.exists()

    # Load and verify structure
    with open(object_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    assert data['schema_version'] == 1
    assert data['hash'] == test_hash
    assert data['normalized_code'] == normalized_code
    assert data['metadata'] == metadata


def test_object_save_no_language_data(mock_bb_dir):
    """Test that object_save does NOT include language-specific data"""
    test_hash = "abcd1234" + "0" * 56
    normalized_code = normalize_code_for_test("def _bb_v_0(): pass")
    metadata = bb.code_create_metadata()

    bb.object_save(test_hash, normalized_code, metadata)

    object_json = mock_bb_dir / '.bb' / 'pool' / 'functions' / test_hash[:2] / test_hash[2:] / 'object.json'

    with open(object_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Should NOT have docstrings, name_mappings, alias_mappings
    assert 'docstrings' not in data
    assert 'name_mappings' not in data
    assert 'alias_mappings' not in data


def test_mapping_save_creates_mapping_json(mock_bb_dir):
    """Test that mapping_save creates proper mapping.json"""
    func_hash = "abcd1234" + "0" * 56
    lang = "eng"
    docstring = "Test function"
    name_mapping = {"_bb_v_0": "test_func"}
    alias_mapping = {}
    comment = "Test variant"

    # First create the function (object.json must exist)
    normalized_code = normalize_code_for_test("def _bb_v_0(): pass")
    metadata = bb.code_create_metadata()
    bb.object_save(func_hash, normalized_code, metadata)

    # Now save the mapping
    mapping_hash = bb.mapping_save(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # Check that the mapping was created: pool/mappings/XX/YYYY.../<lang>/<mapping hash>.json
    pool_dir = mock_bb_dir / '.bb' / 'pool'
    mapping_json = pool_dir / 'mappings' / func_hash[:2] / func_hash[2:] / lang / f'{mapping_hash}.json'

    assert mapping_json.exists()

    # Load and verify structure
    with open(mapping_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    assert data['docstring'] == docstring
    assert data['name_mapping'] == name_mapping
    assert data['alias_mapping'] == alias_mapping
    assert data['comment'] == comment
    # A mapping names what it maps, and no longer has a parent: lineage steps say that
    assert (data['function'], data['lang']) == (func_hash, lang)
    assert 'parent' not in data


def test_mapping_save_returns_hash(mock_bb_dir):
    """Test that mapping_save returns the mapping hash"""
    func_hash = "abcd1234" + "0" * 56
    lang = "eng"
    docstring = "Test"
    name_mapping = {"_bb_v_0": "test"}
    alias_mapping = {}
    comment = ""

    # Create function first
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())

    # Save mapping
    mapping_hash = bb.mapping_save(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # Verify it's a valid hash
    assert len(mapping_hash) == 64
    assert all(c in '0123456789abcdef' for c in mapping_hash)

    # Verify it matches computed hash
    expected_hash = bb.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, comment,
                                                 function=func_hash, lang=lang)
    assert mapping_hash == expected_hash


def test_mapping_save_deduplication(mock_bb_dir):
    """Identical mappings of one function share one file; another function's mapping is its own"""
    func_hash1 = "aaaa" + "0" * 60
    func_hash2 = "bbbb" + "0" * 60
    lang = "eng"
    docstring = "Identical docstring"
    name_mapping = {"_bb_v_0": "identical"}
    alias_mapping = {}
    comment = "Same comment"

    # Create two different functions
    bb.object_save(func_hash1, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())
    bb.object_save(func_hash2, normalize_code_for_test("def _bb_v_0(): return 42"), bb.code_create_metadata())

    # Save identical mappings for both
    mapping_hash1 = bb.mapping_save(func_hash1, lang, docstring, name_mapping, alias_mapping, comment)
    mapping_hash2 = bb.mapping_save(func_hash2, lang, docstring, name_mapping, alias_mapping, comment)

    # The function is part of the mapping: different hashes
    assert mapping_hash1 != mapping_hash2
    # Saving the same mapping of the same function again is the same file
    assert bb.mapping_save(func_hash1, lang, docstring, name_mapping, alias_mapping, comment) == mapping_hash1
    assert len(bb.mappings_list(func_hash1, lang)) == 1


def test_mapping_save_different_comments_different_hashes(mock_bb_dir):
    """Test that different comments produce different mapping hashes"""
    func_hash = "abcd1234" + "0" * 56
    lang = "eng"
    docstring = "Test"
    name_mapping = {"_bb_v_0": "test"}
    alias_mapping = {}

    # Create function
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())

    # Save two mappings with different comments
    hash1 = bb.mapping_save(func_hash, lang, docstring, name_mapping, alias_mapping, "Formal")
    hash2 = bb.mapping_save(func_hash, lang, docstring, name_mapping, alias_mapping, "Informal")

    # Hashes should be different
    assert hash1 != hash2


def test_write_integration_full_structure(mock_bb_dir):
    """Integration test: verify complete directory structure"""
    func_hash = "test1234" + "0" * 56
    normalized_code = normalize_code_for_test("def _bb_v_0(_bb_v_1): return _bb_v_1 * 2")
    metadata = {
        'created': '2025-01-01T00:00:00Z',
        'name': 'testuser',
        'email': 'test@example.com',
        'tags': ['math'],
        'dependencies': []
    }

    # Save function
    bb.object_save(func_hash, normalized_code, metadata)

    # Save mappings in two languages
    eng_hash = bb.mapping_save(
        func_hash, "eng",
        "Double the input",
        {"_bb_v_0": "double", "_bb_v_1": "value"},
        {},
        "Simple English"
    )

    fra_hash = bb.mapping_save(
        func_hash, "fra",
        "Doubler l'entrée",
        {"_bb_v_0": "doubler", "_bb_v_1": "valeur"},
        {},
        "Français simple"
    )

    # Verify directory structure
    pool_dir = mock_bb_dir / '.bb' / 'pool'
    func_dir = pool_dir / 'functions' / func_hash[:2] / func_hash[2:]
    mapping_dir = pool_dir / 'mappings' / func_hash[:2] / func_hash[2:]

    # Check object.json exists
    assert (func_dir / 'object.json').exists()

    # Check language directories exist
    assert (mapping_dir / 'eng').exists()
    assert (mapping_dir / 'fra').exists()

    # Check mapping files exist
    assert (mapping_dir / 'eng' / f'{eng_hash}.json').exists()
    assert (mapping_dir / 'fra' / f'{fra_hash}.json').exists()


# ============================================================================
# Tests for Read Path
# ============================================================================

def test_object_load_loads_object_json(mock_bb_dir):
    """Test that object_load loads object.json correctly"""
    func_hash = "test5678" + "0" * 56
    normalized_code = normalize_code_for_test("def _bb_v_0(_bb_v_1): return _bb_v_1 * 2")
    metadata = {
        'created': '2025-01-01T00:00:00Z',
        'name': 'testuser',
        'email': 'test@example.com',
        'tags': ['test'],
        'dependencies': []
    }

    # Save function first
    bb.object_save(func_hash, normalized_code, metadata)

    # Load it back
    loaded_data = bb.object_load(func_hash)

    # Verify data
    assert loaded_data['schema_version'] == 1
    assert loaded_data['hash'] == func_hash
    assert loaded_data['normalized_code'] == normalized_code
    assert loaded_data['metadata'] == metadata


def test_mappings_list_single_mapping(mock_bb_dir):
    """Test that mappings_list returns single mapping correctly"""
    func_hash = "list1234" + "0" * 56
    lang = "eng"
    docstring = "Test function"
    name_mapping = {"_bb_v_0": "test_func"}
    alias_mapping = {}
    comment = "Test variant"

    # Create function and mapping
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())
    bb.mapping_save(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # List mappings
    mappings = bb.mappings_list(func_hash, lang)

    # Should have exactly one mapping
    assert len(mappings) == 1
    mapping_hash, mapping_comment = mappings[0]
    assert len(mapping_hash) == 64
    assert mapping_comment == comment


def test_mappings_list_multiple_mappings(mock_bb_dir):
    """Test that mappings_list returns multiple mappings"""
    func_hash = "list5678" + "0" * 56
    lang = "eng"

    # Create function
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())

    # Add two mappings with different comments
    bb.mapping_save(func_hash, lang, "Doc 1", {"_bb_v_0": "func1"}, {}, "Formal")
    bb.mapping_save(func_hash, lang, "Doc 2", {"_bb_v_0": "func2"}, {}, "Casual")

    # List mappings
    mappings = bb.mappings_list(func_hash, lang)

    # Should have two mappings
    assert len(mappings) == 2

    # Extract comments
    comments = [comment for _, comment in mappings]
    assert "Formal" in comments
    assert "Casual" in comments


def test_mappings_list_no_mappings(mock_bb_dir):
    """Test that mappings_list returns empty list when no mappings exist"""
    func_hash = "nomaps12" + "0" * 56

    # Create function without any mappings
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())

    # List mappings for a language that doesn't exist
    mappings = bb.mappings_list(func_hash, "fra")

    # Should be empty
    assert len(mappings) == 0


def test_mapping_load_loads_correctly(mock_bb_dir):
    """Test that mapping_load loads a specific mapping"""
    func_hash = "load1234" + "0" * 56
    lang = "eng"
    docstring = "Test docstring"
    name_mapping = {"_bb_v_0": "test_func", "_bb_v_1": "param"}
    alias_mapping = {"abc123": "helper"}
    comment = "Test variant"

    # Create function and mapping
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())
    mapping_hash = bb.mapping_save(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # Load the mapping
    loaded_doc, loaded_name, loaded_alias, loaded_comment = bb.mapping_load(func_hash, lang, mapping_hash)

    # Verify data
    assert loaded_doc == docstring
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_comment == comment


def test_function_load_integration(mock_bb_dir):
    """Integration test: write, read, verify correctness"""
    func_hash = "integ123" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _bb_v_0(_bb_v_1): return _bb_v_1 + 1")
    docstring = "Increment by one"
    name_mapping = {"_bb_v_0": "increment", "_bb_v_1": "value"}
    alias_mapping = {}
    comment = "Simple increment"

    # Write function
    bb.code_save(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping, comment)

    # Read back
    loaded_code, loaded_name, loaded_alias, loaded_doc = bb.code_load(func_hash, lang)

    # Verify correctness
    assert loaded_code == normalized_code
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_doc == docstring


def test_function_load_dispatch_multiple_mappings(mock_bb_dir):
    """Test that dispatch with multiple mappings defaults to first one"""
    func_hash = "multi123" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _bb_v_0(): pass")

    # Create function with two mappings
    bb.object_save(func_hash, normalized_code, bb.code_create_metadata())
    hash1 = bb.mapping_save(func_hash, lang, "Doc 1", {"_bb_v_0": "func1"}, {}, "First")
    hash2 = bb.mapping_save(func_hash, lang, "Doc 2", {"_bb_v_0": "func2"}, {}, "Second")

    # Load without specifying mapping_hash (should return first alphabetically)
    loaded_code, loaded_name, loaded_alias, loaded_doc = bb.code_load(func_hash, lang)

    # Should load one of the mappings (implementation will pick first alphabetically)
    assert loaded_code == normalized_code
    assert loaded_name in [{"_bb_v_0": "func1"}, {"_bb_v_0": "func2"}]
    assert loaded_doc in ["Doc 1", "Doc 2"]


def test_function_load_dispatch_explicit_mapping(mock_bb_dir):
    """Test that dispatch can load specific mapping by hash"""
    func_hash = "explicit1" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _bb_v_0(): pass")

    # Create function with two mappings
    bb.object_save(func_hash, normalized_code, bb.code_create_metadata())
    hash1 = bb.mapping_save(func_hash, lang, "Doc 1", {"_bb_v_0": "func1"}, {}, "First")
    hash2 = bb.mapping_save(func_hash, lang, "Doc 2", {"_bb_v_0": "func2"}, {}, "Second")

    # Load with specific mapping_hash
    loaded_code, loaded_name, loaded_alias, loaded_doc = bb.code_load(func_hash, lang, mapping_hash=hash2)

    # Should load the second mapping
    assert loaded_code == normalized_code
    assert loaded_name == {"_bb_v_0": "func2"}
    assert loaded_doc == "Doc 2"


# ============================================================================
# Tests for the markdown documentation key (bb edit)
# ============================================================================

def test_mapping_hash_unchanged_without_markdown():
    """An empty markdown key must not change the hash of existing mappings"""
    args = ("Doc", {"_bb_v_0": "f"}, {}, "")
    assert bb.code_compute_mapping_hash(*args) == bb.code_compute_mapping_hash(*args, markdown="")


def test_mapping_hash_includes_markdown():
    """Different markdown documentation gives different mapping hashes"""
    args = ("Doc", {"_bb_v_0": "f"}, {}, "")
    assert bb.code_compute_mapping_hash(*args, markdown="Prose") != bb.code_compute_mapping_hash(*args)


def test_mapping_save_markdown_key(mock_bb_dir):
    """mapping_save writes 'markdown' only when there is documentation"""
    func_hash = "abcd" + "0" * 60
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())

    plain = bb.mapping_save(func_hash, "eng", "", {"_bb_v_0": "f"}, {}, "")
    documented = bb.mapping_save(func_hash, "eng", "", {"_bb_v_0": "f"}, {}, "", markdown="Prose")

    with open(bb.storage_mapping_path(func_hash, "eng", plain), encoding='utf-8') as f:
        assert 'markdown' not in json.load(f)
    assert bb.mapping_load_markdown(func_hash, "eng", plain) == ""
    assert bb.mapping_load_markdown(func_hash, "eng", documented) == "Prose"


def test_mapping_select(mock_bb_dir):
    """mapping_select returns None without mappings, else the first alphabetically"""
    func_hash = "abcd" + "0" * 60
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())
    assert bb.mapping_select(func_hash, "eng") is None

    first = bb.mapping_save(func_hash, "eng", "", {"_bb_v_0": "f"}, {}, "one")
    second = bb.mapping_save(func_hash, "eng", "", {"_bb_v_0": "f"}, {}, "two")
    assert bb.mapping_select(func_hash, "eng") == min(first, second)


def _save_version(func_hash, names, parent=None, created='2026-01-01T00:00:00Z'):
    """
    Save a function object with one mapping per language, {lang: name}, and
    a lineage step for each: after parent's step in that language (an edit,
    or a rename when the name differs), else an add.
    """
    metadata = {'created': created, 'name': '', 'email': ''}
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), metadata)
    first = None
    for lang, name in names.items():
        mapping = bb.mapping_save(func_hash, lang, "", {"_bb_v_0": name}, {}, "")
        previous = bb.lineage_step_for(parent, lang) if parent else None
        relation, translates = 'add', None
        if previous:
            relation = 'rename' if bb.lineage_index()['steps'][previous]['name'] != name else 'edit'
        elif first:
            relation, translates = 'translate', first  # the other languages translate the first one
        step = bb.lineage_save(previous, func_hash, lang, mapping, relation, translates=translates, metadata=metadata)
        first = first or step


def test_resolve_name_prefers_fork_over_parent(mock_bb_dir):
    """A fork wins over its parent even when both share the same creation time"""
    child_hash, parent_hash = "aaaa" + "0" * 60, "bbbb" + "0" * 60
    # The fork sorts first, so hash order cannot explain the result
    _save_version(parent_hash, {"eng": "f"})
    _save_version(child_hash, {"eng": "f"}, parent=parent_hash)
    assert bb.helper_resolve_name("f", "eng") == (child_hash, "eng")


def test_resolve_name_refuses_diverged_forks(mock_bb_dir, capsys):
    """Two forks of one function leave the name ambiguous: no guessing by time"""
    root, one, two = "aaaa" + "0" * 60, "bbbb" + "0" * 60, "cccc" + "0" * 60
    _save_version(root, {"eng": "f"})
    _save_version(one, {"eng": "f"}, parent=root, created='2026-01-02T00:00:00Z')
    _save_version(two, {"eng": "f"}, parent=root, created='2026-01-03T00:00:00Z')
    with pytest.raises(SystemExit):
        bb.helper_resolve_name("f", "eng")
    err = capsys.readouterr().err
    assert "ambiguous" in err and one in err and two in err


def test_resolve_name_waits_for_translation_in_other_languages(mock_bb_dir, capsys):
    """An English edit moves the English name only: the French name waits for a translation"""
    root, fork = "aaaa" + "0" * 60, "bbbb" + "0" * 60
    _save_version(root, {"eng": "double", "fra": "doubler"})
    _save_version(fork, {"eng": "double"}, parent=root)

    assert bb.helper_resolve_name("double", "eng") == (fork, "eng")
    assert bb.helper_resolve_name("doubler", "fra") == (root, "fra")
    assert bb.helper_resolve_name("doubler") == (root, "fra")
    [french] = bb.lineage_current_steps("fra", "doubler")
    assert bb.lineage_waiting(french) == ["eng"]
    [english] = bb.lineage_current_steps("eng", "double")
    assert bb.lineage_waiting(english) == []


def test_resolve_name_rename_supersedes(mock_bb_dir, capsys):
    """A rename ends the old name's lineage: only the new name is current"""
    root, renamed = "aaaa" + "0" * 60, "bbbb" + "0" * 60
    _save_version(root, {"eng": "old_name"})
    _save_version(renamed, {"eng": "new_name"}, parent=root)
    assert bb.helper_resolve_name("new_name", "eng") == (renamed, "eng")
    with pytest.raises(SystemExit):
        bb.helper_resolve_name("old_name", "eng")
    assert "No function found with name: old_name@eng" in capsys.readouterr().err


# ============================================================================
# Tests for worklogs
# ============================================================================

def test_uuid7_layout(monkeypatch):
    """The fallback uuid7 sets version 7, the RFC variant, and starts with the time"""
    monkeypatch.delattr(bb.uuid, 'uuid7', raising=False)
    monkeypatch.setattr(bb.time, 'time_ns', lambda: 1_700_000_000_123_000_000)
    value = bb.storage_uuid7()
    assert value.version == 7
    assert value.variant == bb.uuid.RFC_4122
    assert value.int >> 80 == 1_700_000_000_123


def test_uuid7_sorts_by_time(monkeypatch):
    """Later uuid7 values sort after earlier ones"""
    monkeypatch.delattr(bb.uuid, 'uuid7', raising=False)
    times = iter([1_000_000_000, 2_000_000_000])
    monkeypatch.setattr(bb.time, 'time_ns', lambda: next(times))
    assert str(bb.storage_uuid7()) < str(bb.storage_uuid7())


def test_worklog_message_hash_is_canonical():
    """The translation hash covers language and message, in sorted-key compact JSON"""
    expected = bb.hash_compute('{"language": "fra", "message": "Été"}')
    assert bb.worklog_compute_message_hash("fra", "Été") == expected
    assert bb.worklog_compute_message_hash("eng", "Été") != expected


def test_worklog_save_and_translate(mock_bb_dir, monkeypatch):
    """A worklog keeps its original language, references, and every translation"""
    monkeypatch.setattr(bb, 'storage_get_worklog_directory', lambda: mock_bb_dir / '.bb' / 'worklog')
    ref = "a" * 64 + "@eng@" + "b" * 64
    worklog_id, original_hash = bb.worklog_save("eng", "Explored", [ref])
    french_hash = bb.worklog_save_message(worklog_id, "fra", "Exploré")

    worklog = bb.worklog_load(worklog_id)
    assert worklog['uuid'] == worklog_id
    assert worklog['language'] == "eng"
    assert worklog['references'] == [ref]
    assert worklog['messages'] == {"eng": [(original_hash, "Explored")], "fra": [(french_hash, "Exploré")]}
    assert [w['uuid'] for w in bb.worklog_list()] == [worklog_id]


def test_worklog_pick_message_prefers_requested_then_config():
    """The requested language wins, then the user's languages, then the original"""
    worklog = {'language': 'eng', 'messages': {'eng': [('h1', 'Hello')], 'fra': [('h2', 'Bonjour')]}}
    with patch.object(bb, 'storage_read_config', return_value={'user': {'languages': ['kab', 'fra']}}):
        assert bb.worklog_pick_message(worklog, 'eng') == ('eng', 'Hello')
        assert bb.worklog_pick_message(worklog) == ('fra', 'Bonjour')
    with patch.object(bb, 'storage_read_config', return_value={'user': {'languages': ['kab']}}):
        assert bb.worklog_pick_message(worklog) == ('eng', 'Hello')


# ============================================================================
# Tests for mapping parents
# ============================================================================

def test_mapping_hash_includes_function_and_language():
    """A mapping names what it maps: the same names for another function or language hash apart"""
    args = ("Doc", {"_bb_v_0": "f"}, {}, "", "")
    one = bb.code_compute_mapping_hash(*args, function="a" * 64, lang="eng")
    assert one != bb.code_compute_mapping_hash(*args, function="b" * 64, lang="eng")
    assert one != bb.code_compute_mapping_hash(*args, function="a" * 64, lang="fra")


def test_mapping_select_prefers_documented_over_previous(mock_bb_dir):
    """A 'document' step supersedes the mapping it comes after"""
    func_hash = "abcd" + "0" * 60
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())

    # Find an original/child pair where the child sorts after the original,
    # so only lineage (not alphabetical order) can explain the choice
    for i in range(50):
        original = bb.mapping_save(func_hash, "eng", f"v{i}", {"_bb_v_0": "f"}, {}, "")
        child = bb.mapping_save(func_hash, "eng", "documented", {"_bb_v_0": "f"}, {}, "")
        if child > original:
            break
    first = bb.lineage_save(None, func_hash, "eng", original, 'add')
    bb.lineage_save(first, func_hash, "eng", child, 'document')

    assert child in bb.mapping_leaves(func_hash, "eng")
    assert original not in bb.mapping_leaves(func_hash, "eng")
    assert bb.mapping_select(func_hash, "eng") == child


def test_translation_in_other_language_does_not_supersede(mock_bb_dir):
    """A translation is a step in another language, so both stay current"""
    func_hash = "abcd" + "0" * 60
    bb.object_save(func_hash, normalize_code_for_test("def _bb_v_0(): pass"), bb.code_create_metadata())
    english = bb.mapping_save(func_hash, "eng", "", {"_bb_v_0": "f"}, {}, "")
    french = bb.mapping_save(func_hash, "fra", "", {"_bb_v_0": "f"}, {}, "")
    source = bb.lineage_save(None, func_hash, "eng", english, 'add')
    bb.lineage_save(None, func_hash, "fra", french, 'translate', translates=source)
    assert bb.mapping_leaves(func_hash, "eng") == [english]
    assert bb.helper_find_heads_by_name("f", "fra")[0] == [func_hash]


# ============================================================================
# Tests for function identity: code plus checks, written once
# ============================================================================

def test_function_hash_without_checks_is_code_hash():
    """Functions without checks keep the hash they always had"""
    code = normalize_code_for_test("def _bb_v_0(): return 1")
    assert bb.code_compute_function_hash(code) == bb.hash_compute(code)
    assert bb.code_compute_function_hash(code, []) == bb.hash_compute(code)


def test_function_hash_covers_checks_in_any_order():
    """Checks change the hash; their order and duplicates do not"""
    code = normalize_code_for_test("def _bb_v_0(): return 1")
    one, two = "a" * 64, "b" * 64
    with_checks = bb.code_compute_function_hash(code, [one, two])
    assert with_checks != bb.code_compute_function_hash(code)
    assert with_checks == bb.code_compute_function_hash(code, [two, one, two])
    assert with_checks != bb.code_compute_function_hash(code, [one])


def test_object_save_writes_once(mock_bb_dir):
    """Saving an existing object keeps its original metadata and checks"""
    code = normalize_code_for_test("def _bb_v_0(): return 1")
    func_hash = bb.code_compute_function_hash(code, ["a" * 64])
    assert bb.object_save(func_hash, code, {'created': 'first', 'parent': 'p' * 64}, checks=["a" * 64]) is True
    assert bb.object_save(func_hash, code, {'created': 'second'}) is False

    data = bb.object_load(func_hash)
    assert data['metadata'] == {'created': 'first', 'parent': 'p' * 64}
    assert data['checks'] == ["a" * 64]
    assert 'checks' not in data['metadata']


# ============================================================================
# Tests for bb search
# ============================================================================

def _search(capsys, *query, **flags):
    capsys.readouterr()  # drop what saving the pool printed
    bb.command_search(list(query), as_json=True, **flags)
    return json.loads(capsys.readouterr().out)


def test_search_lists_current_versions_unless_all(mock_bb_dir, capsys):
    """A superseded version is hidden by default and marked with --all"""
    root, fork = "aaaa" + "0" * 60, "bbbb" + "0" * 60
    _save_version(root, {"eng": "double"})
    _save_version(fork, {"eng": "double"}, parent=root)
    assert [r['hash'] for r in _search(capsys, "double")] == [fork]
    results = {r['hash']: r for r in _search(capsys, "double", all_versions=True)}
    assert results[root]['current'] is False and results[fork]['current'] is True


def test_search_crosses_languages_and_shows_yours(mock_bb_dir, capsys):
    """A French word finds the function; the result is shown in your language"""
    func = "aaaa" + "0" * 60
    _save_version(func, {"eng": "double", "fra": "doubler"})
    bb.storage_write_config({'user': {'name': '', 'email': '', 'languages': ['eng']}})
    [result] = _search(capsys, "doubler")
    assert result['hash'] == func and result['name'] == 'double' and result['lang'] == 'eng'


def test_search_terms_are_and(mock_bb_dir, capsys):
    """Every term must appear"""
    _save_version("aaaa" + "0" * 60, {"eng": "count_words"})
    _save_version("bbbb" + "0" * 60, {"eng": "tokenize_words"})
    assert [r['name'] for r in _search(capsys, "words", "count")] == ['count_words']


def test_search_diverged_filter(mock_bb_dir, capsys):
    """Two forks of one function make its name diverged"""
    root, one, two = "aaaa" + "0" * 60, "bbbb" + "0" * 60, "cccc" + "0" * 60
    _save_version(root, {"eng": "f"})
    _save_version(one, {"eng": "f"}, parent=root)
    _save_version(two, {"eng": "f"}, parent=root)
    _save_version("dddd" + "0" * 60, {"eng": "g"})
    assert sorted(r['hash'] for r in _search(capsys, diverged=True)) == [one, two]


def test_search_alone_lists_the_pool(mock_bb_dir, capsys):
    """Searching for nothing at all lists every current function (an empty pool: nothing)"""
    bb.command_search([])
    assert 'Functions (0 current versions)' in capsys.readouterr().out
