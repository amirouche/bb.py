# Transcript: Locale-Prefixed Blog

## Metadata
- ID: 02
- Feature: refactor blog engine for locale-prefixed paths
- Date: 2026-02-11
- Status: draft
- Complexity: intermediate

## Overview
The blog engine from transcript 01 is multilingual in its *code* — English and French developers can read and write the same functions in their own language. But the *blog itself* is stubbornly monolingual: posts land at `/hello-world/`, not `/eng/hello-world/` or `/fra/bonjour-le-monde/`. In this transcript we fix that irony by refactoring three functions — `render_post`, `render_tag_page`, and `build_blog` — to produce locale-prefixed output paths.

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)

---

# Chapter 1: The Code Is Multilingual, the Blog Is Not

A French developer opens the blog engine from transcript 01 and reads it
in perfect French: `analyser_entete`, `generer_page_article`,
`construire_blog`. She compiles it, runs it, and gets… `/first-post/`.
Not `/fra/premier-article/`. Not even `/eng/first-post/`.

The code speaks every language. The blog speaks none.

Time to fix that. The refactoring touches three functions across two
layers — not just the top-level builder, but the renderers too. Links
inside post pages and tag pages need the locale prefix, so
`render_post` and `render_tag_page` each gain a `locale` parameter.
The builder threads it through. Three new hashes, one happy
polyglot blog.

## Source Files

### Frontmatter Parser `parse_frontmatter_eng.py`
```python
def parse_frontmatter(text):
    """Parse YAML-like frontmatter from a markdown file."""
    metadata = {}
    if not text.startswith('---'):
        return metadata, text
    parts = text.split('---', 2)
    if len(parts) < 3:
        return metadata, text
    header = parts[1].strip()
    body = parts[2].strip()
    for line in header.split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            metadata[key.strip()] = value.strip()
    return metadata, body
```

### Markdown to HTML Converter `markdown_to_html_eng.py`
```python
def markdown_to_html(text):
    """Convert a subset of markdown to HTML."""
    import re
    lines = text.split('\n')
    html_lines = []
    in_paragraph = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('# '):
            if in_paragraph:
                html_lines.append('</p>')
                in_paragraph = False
            html_lines.append('<h1>' + stripped[2:] + '</h1>')
        elif stripped.startswith('## '):
            if in_paragraph:
                html_lines.append('</p>')
                in_paragraph = False
            html_lines.append('<h2>' + stripped[3:] + '</h2>')
        elif stripped.startswith('- '):
            if in_paragraph:
                html_lines.append('</p>')
                in_paragraph = False
            html_lines.append('<li>' + stripped[2:] + '</li>')
        elif stripped == '':
            if in_paragraph:
                html_lines.append('</p>')
                in_paragraph = False
        else:
            if not in_paragraph:
                html_lines.append('<p>')
                in_paragraph = True
            html_lines.append(stripped)
    if in_paragraph:
        html_lines.append('</p>')
    return '\n'.join(html_lines)
```

### Tag Collector `collect_tags_eng.py`
```python
def collect_tags(posts):
    """Group posts by their tags, returning a dict of tag -> list of posts."""
    tags = {}
    for post in posts:
        post_tags = post.get('tags', '')
        if isinstance(post_tags, str):
            post_tags = [t.strip() for t in post_tags.split(',') if t.strip()]
        for tag in post_tags:
            tag_lower = tag.lower()
            if tag_lower not in tags:
                tags[tag_lower] = []
            tags[tag_lower].append(post)
    return tags
```

### Post Renderer (with locale) `render_post_eng.py`
```python
from bb.pool import object_25f5b25a43080344597fea475adc88ecb0fc56e05b843a8668a1eb1822219df9 as parse_frontmatter
from bb.pool import object_1085d7f5f9104a833c0ec8f9c095a836712e257636da67dbff473bc0bed64ea7 as markdown_to_html


def render_post(file_content, site_title, locale):
    """Render a single blog post to a full HTML page with locale-prefixed links."""
    metadata, body = parse_frontmatter(file_content)
    title = metadata.get('title', 'Untitled')
    date = metadata.get('date', '')
    body_html = markdown_to_html(body)
    return ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
            + '<title>' + site_title + ' - ' + title + '</title>'
            + '</head>\n<body>'
            + '<header><a href="/' + locale + '/">Home</a></header>'
            + '<article>'
            + '<h1>' + title + '</h1>'
            + '<time>' + date + '</time>'
            + body_html
            + '</article>'
            + '</body>\n</html>')
```

### Tag Page Renderer (with locale) `render_tag_page_eng.py`
```python
def render_tag_page(tag, posts, site_title, locale):
    """Render an HTML page listing all posts for a given tag with locale-prefixed links."""
    items = ''
    for post in sorted(posts, key=lambda p: p.get('date', ''), reverse=True):
        items += ('<li><time>' + post.get('date', '') + '</time> '
                  + '<a href="/' + locale + '/' + post.get('slug', '') + '/">'
                  + post.get('title', 'Untitled') + '</a></li>\n')
    return ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
            + '<title>' + site_title + ' - Tag: ' + tag + '</title>'
            + '</head>\n<body>'
            + '<header><a href="/' + locale + '/">Home</a></header>'
            + '<h1>Tag: ' + tag + '</h1>'
            + '<ul>' + items + '</ul>'
            + '</body>\n</html>')
```

### Blog Builder (locale-prefixed) `build_blog_locale_eng.py`
```python
import os
from bb.pool import object_25f5b25a43080344597fea475adc88ecb0fc56e05b843a8668a1eb1822219df9 as parse_frontmatter
from bb.pool import object_1085d7f5f9104a833c0ec8f9c095a836712e257636da67dbff473bc0bed64ea7 as markdown_to_html
from bb.pool import object_8dbc108bd086d4f71d1fbf6b1e68f5addd2ab97101534136f2684b85806d489f as render_post
from bb.pool import object_7be41b434e25f5904709f0c6c3b907bc0768c45259c3de49928346e6debfdc99 as collect_tags
from bb.pool import object_e5a83f1f858159a0fce8b86a9d66794a618343630f7944dc2f0afa553fadd60d as render_tag_page


def build_blog(source_dir, output_dir, site_title, site_url, locale):
    """Build a static blog with locale-prefixed paths from markdown files."""
    locale_dir = os.path.join(output_dir, locale)
    os.makedirs(locale_dir, exist_ok=True)
    posts = []
    for filename in sorted(os.listdir(source_dir)):
        if not filename.endswith('.md'):
            continue
        filepath = os.path.join(source_dir, filename)
        with open(filepath, encoding='utf-8') as fh:
            content = fh.read()
        metadata, body = parse_frontmatter(content)
        slug = metadata.get('slug', filename.replace('.md', ''))
        title = metadata.get('title', 'Untitled')
        date = metadata.get('date', '')
        tags_raw = metadata.get('tags', '')
        posts.append({'slug': slug, 'title': title, 'date': date, 'tags': tags_raw})
        post_dir = os.path.join(locale_dir, slug)
        os.makedirs(post_dir, exist_ok=True)
        html = render_post(content, site_title, locale)
        with open(os.path.join(post_dir, 'index.html'), 'w', encoding='utf-8') as fh:
            fh.write(html)
    tag_groups = collect_tags(posts)
    tags_dir = os.path.join(locale_dir, 'tags')
    os.makedirs(tags_dir, exist_ok=True)
    all_tags = sorted(tag_groups.keys())
    for tag, tag_posts in tag_groups.items():
        tag_dir = os.path.join(tags_dir, tag)
        os.makedirs(tag_dir, exist_ok=True)
        tag_html = render_tag_page(tag, tag_posts, site_title, locale)
        with open(os.path.join(tag_dir, 'index.html'), 'w', encoding='utf-8') as fh:
            fh.write(tag_html)
    index_items = ''
    for post in sorted(posts, key=lambda p: p['date'], reverse=True):
        tag_links = ''
        post_tags = post.get('tags', '')
        if isinstance(post_tags, str):
            post_tags = [t.strip() for t in post_tags.split(',') if t.strip()]
        for tag in post_tags:
            tag_links += ' <a href="/' + locale + '/tags/' + tag.lower() + '/">[' + tag + ']</a>'
        index_items += ('<li><time>' + post['date'] + '</time> '
                        + '<a href="/' + locale + '/' + post['slug'] + '/">'
                        + post['title'] + '</a>' + tag_links + '</li>\n')
    index_html = ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
                  + '<title>' + site_title + '</title></head>\n<body>'
                  + '<h1>' + site_title + '</h1>'
                  + '<nav>Tags: '
                  + ' '.join('<a href="/' + locale + '/tags/' + t + '/">' + t + '</a>'
                             for t in all_tags)
                  + '</nav><ul>' + index_items + '</ul></body>\n</html>')
    with open(os.path.join(locale_dir, 'index.html'), 'w', encoding='utf-8') as fh:
        fh.write(index_html)
    rss_items = ''
    for post in sorted(posts, key=lambda p: p['date'], reverse=True):
        rss_items += ('<item><title>' + post['title'] + '</title>'
                      + '<link>' + site_url + '/' + locale + '/' + post['slug']
                      + '/</link></item>\n')
    rss = ('<?xml version="1.0" encoding="utf-8"?>\n'
           + '<rss version="2.0"><channel>'
           + '<title>' + site_title + '</title>'
           + '<link>' + site_url + '</link>'
           + rss_items + '</channel></rss>')
    with open(os.path.join(locale_dir, 'feed.rss'), 'w', encoding='utf-8') as fh:
        fh.write(rss)
    return posts
```

### French Post Renderer `render_post_fra.py`
```python
from bb.pool import object_25f5b25a43080344597fea475adc88ecb0fc56e05b843a8668a1eb1822219df9 as analyser_entete
from bb.pool import object_1085d7f5f9104a833c0ec8f9c095a836712e257636da67dbff473bc0bed64ea7 as markdown_vers_html


def generer_page_article(contenu_fichier, titre_site, langue):
    """Génère une page HTML complète pour un article de blog avec préfixe de langue."""
    metadonnees, corps = analyser_entete(contenu_fichier)
    titre = metadonnees.get('title', 'Untitled')
    date = metadonnees.get('date', '')
    corps_html = markdown_vers_html(corps)
    return ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
            + '<title>' + titre_site + ' - ' + titre + '</title>'
            + '</head>\n<body>'
            + '<header><a href="/' + langue + '/">Home</a></header>'
            + '<article>'
            + '<h1>' + titre + '</h1>'
            + '<time>' + date + '</time>'
            + corps_html
            + '</article>'
            + '</body>\n</html>')
```

### French Tag Page Renderer `render_tag_page_fra.py`
```python
def generer_page_etiquette(etiquette, articles, titre_site, langue):
    """Génère une page HTML listant tous les articles pour une étiquette donnée avec préfixe de langue."""
    elements = ''
    for article in sorted(articles, key=lambda a: a.get('date', ''), reverse=True):
        elements += ('<li><time>' + article.get('date', '') + '</time> '
                     + '<a href="/' + langue + '/' + article.get('slug', '') + '/">'
                     + article.get('title', 'Untitled') + '</a></li>\n')
    return ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
            + '<title>' + titre_site + ' - Tag: ' + etiquette + '</title>'
            + '</head>\n<body>'
            + '<header><a href="/' + langue + '/">Home</a></header>'
            + '<h1>Tag: ' + etiquette + '</h1>'
            + '<ul>' + elements + '</ul>'
            + '</body>\n</html>')
```

### French Blog Builder `build_blog_locale_fra.py`
```python
import os
from bb.pool import object_25f5b25a43080344597fea475adc88ecb0fc56e05b843a8668a1eb1822219df9 as analyser_entete
from bb.pool import object_1085d7f5f9104a833c0ec8f9c095a836712e257636da67dbff473bc0bed64ea7 as markdown_vers_html
from bb.pool import object_8dbc108bd086d4f71d1fbf6b1e68f5addd2ab97101534136f2684b85806d489f as generer_page_article
from bb.pool import object_7be41b434e25f5904709f0c6c3b907bc0768c45259c3de49928346e6debfdc99 as regrouper_etiquettes
from bb.pool import object_e5a83f1f858159a0fce8b86a9d66794a618343630f7944dc2f0afa553fadd60d as generer_page_etiquette


def construire_blog(repertoire_source, repertoire_sortie, titre_site, url_site, langue):
    """Construit un blog statique avec chemins préfixés par la langue."""
    repertoire_langue = os.path.join(repertoire_sortie, langue)
    os.makedirs(repertoire_langue, exist_ok=True)
    articles = []
    for nom_fichier in sorted(os.listdir(repertoire_source)):
        if not nom_fichier.endswith('.md'):
            continue
        chemin_fichier = os.path.join(repertoire_source, nom_fichier)
        with open(chemin_fichier, encoding='utf-8') as fh:
            contenu = fh.read()
        metadonnees, corps = analyser_entete(contenu)
        slug = metadonnees.get('slug', nom_fichier.replace('.md', ''))
        titre = metadonnees.get('title', 'Untitled')
        date = metadonnees.get('date', '')
        etiquettes_brutes = metadonnees.get('tags', '')
        articles.append({'slug': slug, 'title': titre, 'date': date, 'tags': etiquettes_brutes})
        repertoire_article = os.path.join(repertoire_langue, slug)
        os.makedirs(repertoire_article, exist_ok=True)
        html = generer_page_article(contenu, titre_site, langue)
        with open(os.path.join(repertoire_article, 'index.html'), 'w', encoding='utf-8') as fh:
            fh.write(html)
    groupes_etiquettes = regrouper_etiquettes(articles)
    repertoire_etiquettes = os.path.join(repertoire_langue, 'tags')
    os.makedirs(repertoire_etiquettes, exist_ok=True)
    toutes_etiquettes = sorted(groupes_etiquettes.keys())
    for etiquette, articles_etiquette in groupes_etiquettes.items():
        repertoire_etiquette = os.path.join(repertoire_etiquettes, etiquette)
        os.makedirs(repertoire_etiquette, exist_ok=True)
        html_etiquette = generer_page_etiquette(etiquette, articles_etiquette, titre_site, langue)
        with open(os.path.join(repertoire_etiquette, 'index.html'), 'w', encoding='utf-8') as fh:
            fh.write(html_etiquette)
    elements_index = ''
    for article in sorted(articles, key=lambda a: a['date'], reverse=True):
        liens_etiquettes = ''
        etiquettes_article = article.get('tags', '')
        if isinstance(etiquettes_article, str):
            etiquettes_article = [e.strip() for e in etiquettes_article.split(',') if e.strip()]
        for etiquette in etiquettes_article:
            liens_etiquettes += ' <a href="/' + langue + '/tags/' + etiquette.lower() + '/">[' + etiquette + ']</a>'
        elements_index += ('<li><time>' + article['date'] + '</time> '
                           + '<a href="/' + langue + '/' + article['slug'] + '/">'
                           + article['title'] + '</a>' + liens_etiquettes + '</li>\n')
    html_index = ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
                  + '<title>' + titre_site + '</title></head>\n<body>'
                  + '<h1>' + titre_site + '</h1>'
                  + '<nav>Tags: '
                  + ' '.join('<a href="/' + langue + '/tags/' + e + '/">' + e + '</a>'
                             for e in toutes_etiquettes)
                  + '</nav><ul>' + elements_index + '</ul></body>\n</html>')
    with open(os.path.join(repertoire_langue, 'index.html'), 'w', encoding='utf-8') as fh:
        fh.write(html_index)
    elements_rss = ''
    for article in sorted(articles, key=lambda a: a['date'], reverse=True):
        elements_rss += ('<item><title>' + article['title'] + '</title>'
                         + '<link>' + url_site + '/' + langue + '/' + article['slug']
                         + '/</link></item>\n')
    rss = ('<?xml version="1.0" encoding="utf-8"?>\n'
           + '<rss version="2.0"><channel>'
           + '<title>' + titre_site + '</title>'
           + '<link>' + url_site + '</link>'
           + elements_rss + '</channel></rss>')
    with open(os.path.join(repertoire_langue, 'feed.rss'), 'w', encoding='utf-8') as fh:
        fh.write(rss)
    return articles
```

## Workflow

### Step 1: Add the leaf functions (unchanged from transcript 01)
```bash
$ PARSE_HASH=$(bb add parse_frontmatter_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

```bash
$ MARKDOWN_HASH=$(bb add markdown_to_html_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

```bash
$ TAGS_HASH=$(bb add collect_tags_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 2: Add the refactored post renderer
```bash
$ RENDER_HASH=$(bb add render_post_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 3: Add the refactored tag page renderer
```bash
$ TAGPAGE_HASH=$(bb add render_tag_page_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 4: Add the locale-prefixed blog builder
```bash
$ BUILD_HASH=$(bb add build_blog_locale_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 5: Verify render_post got a new hash (different logic from transcript 01)

The old `render_post` had `<a href="/">Home</a>`. The new one has
`<a href="/" + locale + "/">Home</a>`. Different logic, different hash.

```bash
$ bb show $RENDER_HASH@eng | grep '^def '
def render_post(file_content, site_title, locale):
```

### Step 6: Verify render_tag_page uses locale in links
```bash
$ bb show $TAGPAGE_HASH@eng | grep '^def '
def render_tag_page(tag, posts, site_title, locale):
```

### Step 7: Compile the locale-prefixed builder
```bash
$ bb compile --output blog_locale.py $BUILD_HASH@eng
```

### Step 8: Add French translations and verify same hashes

```bash
$ RENDER_HASH_FRA=$(bb add render_post_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

```bash
$ TAGPAGE_HASH_FRA=$(bb add render_tag_page_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

```bash
$ BUILD_HASH_FRA=$(bb add build_blog_locale_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

### Step 9: Confirm all refactored functions share hashes across languages
```bash
$ test "$RENDER_HASH" = "$RENDER_HASH_FRA" && echo "render_post: same hash"
render_post: same hash
```

```bash
$ test "$TAGPAGE_HASH" = "$TAGPAGE_HASH_FRA" && echo "render_tag_page: same hash"
render_tag_page: same hash
```

```bash
$ test "$BUILD_HASH" = "$BUILD_HASH_FRA" && echo "build_blog: same hash"
build_blog: same hash
```

### Step 10: Show the French version reads naturally
```bash
$ bb show $RENDER_HASH@fra | grep '^def '
def generer_page_article(contenu_fichier, titre_site, langue):
```

```bash
$ bb show $BUILD_HASH@fra | grep '^def '
def construire_blog(repertoire_source, repertoire_sortie, titre_site, url_site, langue):
```

### Step 11: Compile with French identifiers
```bash
# SKIP-TEST: compile demonstration
$ bb compile --output moteur_blog_locale.py $BUILD_HASH@fra
$ head -3 moteur_blog_locale.py
```

The compiled French script uses `construire_blog(..., langue)`,
`generer_page_article(..., langue)`, `generer_page_etiquette(..., langue)` —
all French names. But the output directory structure is identical.

### Step 12: Build with locale-prefixed paths
```bash
# SKIP-TEST: heredoc and execution demonstration
$ mkdir -p posts
$ cat > posts/hello.md << 'ENDOFPOST'
---
title: Hello World
date: 2026-02-01
slug: hello-world
tags: tutorial
---
Welcome to the multilingual blog.
ENDOFPOST
$ python3 blog_locale.py
$ ls output/eng/
hello-world
index.html
tags
feed.rss
$ ls output/eng/hello-world/
index.html
$ grep 'href=' output/eng/hello-world/index.html
<header><a href="/eng/">Home</a></header>
```

Posts now live at `/eng/hello-world/` instead of `/hello-world/`. The
home link points to `/eng/` instead of `/`. A French build would produce
`/fra/bonjour-le-monde/` — each locale gets its own subtree.

## Chapter 1 Results
- Three functions refactored: `render_post`, `render_tag_page`, `build_blog` — all gained a `locale` parameter
- Three unchanged leaf functions reused by hash: `parse_frontmatter`, `markdown_to_html`, `collect_tags`
- The refactoring touched two layers (renderers + builder), not just the entry point
- French translations added for all refactored functions — same hashes, different names
- Output structure changed from `/<slug>/` to `/<locale>/<slug>/`

---

## Summary

| Function | Old Signature | New Signature | Hash Changed? |
|----------|--------------|---------------|--------------|
| `parse_frontmatter` | `(text)` | unchanged | No |
| `markdown_to_html` | `(text)` | unchanged | No |
| `collect_tags` | `(posts)` | unchanged | No |
| `render_post` | `(file_content, site_title)` | `(file_content, site_title, locale)` | Yes |
| `render_tag_page` | `(tag, posts, site_title)` | `(tag, posts, site_title, locale)` | Yes |
| `build_blog` | `(source_dir, output_dir, site_title, site_url)` | `(source_dir, output_dir, site_title, site_url, locale)` | Yes |

## Key Insights

1. **Refactoring propagates through layers**: Adding `locale` to `build_blog` alone would have been incomplete — the generated links inside post pages and tag pages would still point to `/`. Real refactoring required changing `render_post` and `render_tag_page` too.

2. **Old versions survive**: The transcript 01 versions of `render_post`, `render_tag_page`, and `build_blog` still exist in the pool with their original hashes. The pool is append-only — refactoring creates new entries, it never destroys old ones.

3. **Leaf functions are stable**: `parse_frontmatter`, `markdown_to_html`, and `collect_tags` were untouched by the refactoring. Their hashes are identical to transcript 01. Functions with no reason to change don't change.

4. **Multilingual refactoring**: The French translations of the refactored functions produce the same hashes as the English ones. The `locale` parameter is part of the logic (it affects HTML output), but the variable *name* (`locale` vs `langue`) is presentation — stripped during normalization.
