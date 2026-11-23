"""Storage v2: lineage per language and name, shared code, renames, translations, bb.resolve."""
import json

from tests.test_import_by_name import Pool, hash_of
from tests.test_translate_file import fill

PAGE = 'def {name}():\n    """A page, for now."""\n    return "todo"\n'
USES = 'import bb\n{dep} = bb.resolve("{dep}")\n\n\ndef {name}():\n    """Use {dep}."""\n    return {dep}()\n'


def heads(pool, name):
    """The current versions of name, as bb latest --hash prints them."""
    return pool.bb('latest', name, '--hash').stdout.split()


def test_two_names_share_one_code_and_each_follows_its_own_edits(tmp_path):
    pool = Pool(tmp_path)
    shared = hash_of(pool.edit('login_page', PAGE.format(name='login_page')))
    assert hash_of(pool.edit('register_page', PAGE.format(name='register_page'))) == shared

    # One caller using both names of one code is refused
    both = ('import bb\nlogin_page = bb.resolve("login_page")\nregister_page = bb.resolve("register_page")\n\n\n'
            'def app():\n    """Both pages."""\n    return login_page() + register_page()\n')
    refused = pool.edit('app', both)
    assert refused.returncode == 1
    assert 'login_page and register_page are the same function here (same code' in refused.stderr

    # A caller may use one of them, another caller the other
    uses_login = pool.edit('show_login', USES.format(name='show_login', dep='login_page'))
    assert f'Import: login_page -> {shared}' in uses_login.stdout
    uses_register = pool.edit('show_register', USES.format(name='show_register', dep='register_page'))
    assert f'Import: register_page -> {shared}' in uses_register.stdout

    # Editing one name moves that name only
    login = hash_of(pool.edit('login_page', 'def login_page():\n    """Log in."""\n    return "login"\n'))
    assert heads(pool, 'login_page@eng') == [login]
    assert heads(pool, 'register_page@eng') == [shared]
    register = hash_of(pool.edit('register_page', 'def register_page():\n    """Sign up."""\n    return "register"\n'))
    assert heads(pool, 'register_page@eng') == [register] and heads(pool, 'login_page@eng') == [login]

    # bb.resolve picks each name's own current version, and the outdated callers are told apart
    again = pool.edit('show_login2', USES.format(name='show_login2', dep='login_page'))
    assert f'Import: login_page -> {login}' in again.stdout
    # show_login and show_register have one code too (they used one function), and each
    # name is out of date by its own lineage
    callers = heads(pool, 'show_login@eng')
    assert callers == heads(pool, 'show_register@eng')
    assert 'uses 1 out-of-date version(s)' in pool.bb('search', '--outdated').stdout
    assert f'saving picks {login[:12]}' in pool.bb('show', 'show_login@eng').stdout
    assert f'saving picks {register[:12]}' in pool.bb('show', 'show_register@eng').stdout

    # Updating one name brings that name along, not the other one
    assert pool.bb('update', 'show_register@eng', '-m', 'catch up').returncode == 0
    assert heads(pool, 'show_login@eng') == callers
    assert heads(pool, 'show_register@eng') != callers
    assert f"bb.ref('{register}')" in json.dumps(pool.bb('show', 'show_register@eng').stdout) or \
        "register_page = bb.resolve('register_page')" in pool.bb('show', 'show_register@eng').stdout
    assert 'out of date' not in pool.bb('show', 'show_register@eng').stdout
    assert f'saving picks {login[:12]}' in pool.bb('show', 'show_login@eng').stdout


def test_a_rename_supersedes_the_old_name(tmp_path):
    pool = Pool(tmp_path)
    first = hash_of(pool.edit('login_page', PAGE.format(name='login_page')))
    renamed = pool.edit('login_page', 'def sign_in():\n    """A page, for now."""\n    return "sign in"\n')
    assert renamed.returncode == 0, renamed.stderr
    assert 'Renamed: login_page -> sign_in' in renamed.stdout
    assert f'Parent: {first}' in renamed.stdout
    assert heads(pool, 'sign_in@eng') == [hash_of(renamed)]
    gone = pool.bb('latest', 'login_page@eng')
    assert gone.returncode == 1 and 'No function found with name: login_page@eng' in gone.stderr
    assert 'Rename: sign_in@eng' in pool.bb('log').stdout


def test_update_moves_every_name_of_a_shared_version(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('layout', 'def layout():\n    """Frame."""\n    return "<main>"\n')
    page = 'import bb\nlayout = bb.resolve("layout")\n\n\ndef {name}():\n    """A page."""\n    return layout()\n'
    shared = hash_of(pool.edit('login_page', page.format(name='login_page')))
    assert hash_of(pool.edit('register_page', page.format(name='register_page'))) == shared
    pool.edit('layout', 'def layout():\n    """Frame."""\n    return "<main class=page>"\n')

    updated = pool.bb('update', 'login_page@eng', '-m', 'new layout')
    assert updated.returncode == 0, updated.stderr
    new = hash_of(updated)
    assert new != shared
    # The code both names share moved, so both names moved
    assert heads(pool, 'login_page@eng') == [new] and heads(pool, 'register_page@eng') == [new]
    assert 'nothing outdated' in pool.bb('search', '--outdated').stdout


def test_a_french_name_waits_for_the_translation_of_an_english_edit(tmp_path):
    pool = Pool(tmp_path)
    v1 = hash_of(pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n'))
    draft = tmp_path / 'double.fra.md'
    pool.bb('translate', 'double@eng', 'fra', str(draft))
    fill(draft, {'double': 'doubler', 'n': 'nombre'}, docstring='Le double.')
    assert pool.bb('translate', 'double@eng', 'fra', str(draft)).returncode == 0
    assert heads(pool, 'doubler@fra') == [v1]

    v2 = hash_of(pool.edit('double', 'def double(n):\n    """Twice n."""\n    return n + n\n'))
    assert heads(pool, 'double@eng') == [v2]
    assert heads(pool, 'doubler@fra') == [v1]  # no French step: it waits
    assert 'Note: newer version in eng, not yet translated to fra' in pool.bb('latest', 'doubler@fra').stdout
    assert 'newer version in eng, not yet translated to fra' in pool.bb('search', '--outdated').stdout
    assert 'not yet translated to fra (newer in eng)' in pool.bb('tree', 'doubler@fra').stdout
    assert 'not yet translated' not in pool.bb('tree', 'double@eng').stdout

    # Translating the new version brings the French name along
    draft.unlink()
    pool.bb('translate', 'double@eng', 'fra', str(draft))
    fill(draft, {'double': 'doubler', 'n': 'nombre'}, docstring='Le double.')
    assert pool.bb('translate', 'double@eng', 'fra', str(draft)).returncode == 0
    assert heads(pool, 'doubler@fra') == [v2]
    assert 'Note:' not in pool.bb('latest', 'doubler@fra').stdout
    assert 'nothing outdated' in pool.bb('search', '--outdated').stdout


def test_resolve_falls_back_through_your_languages(tmp_path):
    pool = Pool(tmp_path)
    english = hash_of(pool.add('helper', 'def helper(n):\n    """Plus one."""\n    return n + 1\n'))
    kabyle_source = tmp_path / 'helper_kab.py'
    kabyle_source.write_text('def helper(n):\n    """Times ten."""\n    return n * 10\n')
    kabyle = hash_of(pool.bb('add', f'{kabyle_source}@kab'))
    user = tmp_path / 'user_fra.py'
    user.write_text('import bb\nhelper = bb.resolve("helper")\n\n\ndef utiliser(n):\n    """Utilise helper."""\n'
                    '    return helper(n)\n')

    # Nothing is called helper in French: your languages decide, in order
    pool.bb('whoami', 'language', 'fra', 'eng', 'kab')
    saved = pool.bb('add', f'{user}@fra')
    assert saved.returncode == 0, saved.stderr
    assert f'Import: helper -> {english}' in saved.stdout
    pool.bb('whoami', 'language', 'fra', 'kab', 'eng')
    assert f'Import: helper -> {kabyle}' in pool.bb('add', f'{user}@fra').stdout
    # bb.latest is the same as bb.resolve
    user.write_text(user.read_text().replace('bb.resolve', 'bb.latest'))
    assert f'Import: helper -> {kabyle}' in pool.bb('add', f'{user}@fra').stdout


def test_outdated_names_each_dependency_by_the_callers_own_name(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('login_page', PAGE.format(name='login_page'))
    pool.edit('register_page', PAGE.format(name='register_page'))
    pool.edit('show_register', USES.format(name='show_register', dep='register_page'))
    pool.edit('register_page', 'def register_page():\n    """Sign up."""\n    return "register"\n')
    status = [line for line in pool.bb('search', '--outdated').stdout.splitlines()
              if 'out-of-date version' in line]
    # The old code also carries login_page: the caller's own name is shown
    assert status and status[0].endswith('out-of-date version(s): register_page')


def test_caller_by_name_lists_only_the_callers_that_use_that_name(tmp_path):
    pool = Pool(tmp_path)
    shared = hash_of(pool.edit('login_page', PAGE.format(name='login_page')))
    assert hash_of(pool.edit('register_page', PAGE.format(name='register_page'))) == shared
    uses_login = hash_of(pool.edit('show_login', USES.format(name='show_login', dep='login_page')))
    by_login = pool.bb('caller', 'login_page@eng').stdout
    by_register = pool.bb('caller', 'register_page@eng').stdout
    assert uses_login in by_login and uses_login not in by_register
    assert uses_login in pool.bb('caller', shared).stdout  # by hash: every caller


def test_caller_lists_current_callers_and_all_shows_old_ones(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('helper', PAGE.format(name='helper'))
    old = hash_of(pool.edit('page', USES.format(name='page', dep='helper')))
    new = hash_of(pool.edit('page', 'def page():\n    """No helper any more."""\n    return "page"\n'))
    assert new != old
    # page's current version no longer uses helper: nothing does
    assert pool.bb('caller', 'helper@eng').stdout.strip() == ''
    every = pool.bb('caller', 'helper@eng', '--all').stdout
    assert f'bb show {old}  # not current' in every


def test_caller_follows_a_switch_between_two_names_of_one_code(tmp_path):
    pool = Pool(tmp_path)
    shared = hash_of(pool.edit('stats_card', PAGE.format(name='stats_card')))
    assert hash_of(pool.edit('stat_card', PAGE.format(name='stat_card'))) == shared
    first = hash_of(pool.edit('page', USES.format(name='page', dep='stats_card')))
    # Same code, now through the other name: the hash stays, the current mapping changes
    assert hash_of(pool.edit('page', USES.format(name='page', dep='stat_card'))) == first
    assert first not in pool.bb('caller', 'stats_card@eng').stdout
    assert first in pool.bb('caller', 'stat_card@eng').stdout
    assert first not in pool.bb('search', '--uses', 'stats_card@eng').stdout
    assert first in pool.bb('search', '--uses', 'stat_card@eng').stdout


def test_search_similar_lists_one_code_saved_under_two_names(tmp_path):
    pool = Pool(tmp_path)
    # Small and identical: too small to score, but the same code under two names is a duplicate
    pool.edit('stats_card', PAGE.format(name='stats_card'))
    pool.edit('stat_card', PAGE.format(name='stat_card'))
    pool.edit('other', 'def other():\n    """Something else."""\n    return 42\n')
    found = pool.bb('search', '--similar').stdout
    assert 'same code under the names stat_card, stats_card' in found
    assert 'Name: other' not in found
