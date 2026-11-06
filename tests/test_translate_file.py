"""bb translate NAME@lang TARGET FILE: write a translation file, fill it in, save it."""
from tests.test_import_by_name import Pool, hash_of


def fill(path, names, docstring='', documentation=''):
    text = path.read_text()
    for source, target in names.items():
        text = text.replace(f"- {source} -> \n", f"- {source} -> {target}\n")
    text = text.replace("## Docstring (fra)\n\n", f"## Docstring (fra)\n\n{docstring}\n")
    text = text.replace("## Documentation (fra)\n\n", f"## Documentation (fra)\n\n{documentation}\n")
    path.write_text(text)


def test_a_function_is_translated_through_a_file(tmp_path):
    pool = Pool(tmp_path)
    func = hash_of(pool.edit('total_price', 'def total_price(items, rate):\n    """Sum the prices, with tax."""\n'
                             '    total = 0\n    for item in items:\n        total += item\n    return total * rate\n'))
    draft = tmp_path / 'total.fra.md'
    wrote = pool.bb('translate', 'total_price@eng', 'fra', str(draft))
    assert wrote.returncode == 0 and 'Wrote' in wrote.stdout, wrote.stderr
    assert '- total_price -> \n' in draft.read_text() and 'def total_price(items, rate):' in draft.read_text()

    # Incomplete: refused, nothing saved
    fill(draft, {'total_price': 'prix_total'})
    refused = pool.bb('translate', 'total_price@eng', 'fra', str(draft))
    assert refused.returncode == 1 and "'items' has no fra name" in refused.stderr, refused.stderr
    assert 'prix_total' not in pool.bb('search', 'prix_total').stdout.split('Search Results')[1].split('\n', 2)[2]
    fill(draft, {'items': 'articles', 'rate': 'taux', 'total': 'somme', 'item': 'article'},
         docstring='Additionne les prix, taxe comprise.')
    saved = pool.bb('translate', 'total_price@eng', 'fra', str(draft), '-m', 'en français')
    assert saved.returncode == 0, saved.stderr
    assert 'Name: prix_total@fra' in saved.stdout
    shown = pool.bb('show', 'prix_total@fra').stdout
    assert 'def prix_total(articles, taux):' in shown and 'Additionne les prix' in shown
    assert pool.bb('latest', 'prix_total@fra').stdout.split('Hash:')[1].split()[0] == func  # same code, same hash


def test_pool_functions_are_named_in_the_target_language(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n')
    leaf = tmp_path / 'double.fra.md'
    pool.bb('translate', 'double@eng', 'fra', str(leaf))
    fill(leaf, {'double': 'doubler', 'n': 'nombre'}, docstring='Le double.')
    assert pool.bb('translate', 'double@eng', 'fra', str(leaf)).returncode == 0
    pool.edit('quad', 'import bb\ndouble = bb.latest("double")\n\n\ndef quad(n):\n    """Four times."""\n'
              '    return double(double(n))\n')
    draft = tmp_path / 'quad.fra.md'
    pool.bb('translate', 'quad@eng', 'fra', str(draft))
    assert '- double (' in draft.read_text() and ') -> doubler' in draft.read_text()  # prefilled
    fill(draft, {'quad': 'quadrupler', 'n': 'nombre'}, docstring='Quatre fois.')
    assert pool.bb('translate', 'quad@eng', 'fra', str(draft)).returncode == 0
    assert 'doubler(doubler(nombre))' in pool.bb('show', 'quadrupler@fra').stdout


def test_bad_names_are_refused(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n')
    draft = tmp_path / 'd.fra.md'
    pool.bb('translate', 'double@eng', 'fra', str(draft))
    fill(draft, {'double': 'doubler', 'n': 'doubler'})
    refused = pool.bb('translate', 'double@eng', 'fra', str(draft))
    assert refused.returncode == 1 and 'given to two names' in refused.stderr
    draft.unlink()
    pool.bb('translate', 'double@eng', 'fra', str(draft))
    fill(draft, {'double': 'len', 'n': 'nombre'})
    assert 'built-in' in pool.bb('translate', 'double@eng', 'fra', str(draft)).stderr
