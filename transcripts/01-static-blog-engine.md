# Transcript: Building a Multilingual Static Blog Engine

## Metadata
- ID: 01
- Feature: static blog engine with tags and multilingual support
- Date: 2026-02-11
- Status: draft
- Complexity: intermediate

## Overview
A user builds a static blog engine in three chapters. First, the core engine that converts markdown posts into HTML pages with an index and RSS feed. Then, tag support with dedicated pages per tag. Finally, the engine is made multilingual using `bb add` with French source files, proving that the same blog engine logic works across languages.

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)

---

# Chapter 1: A Static Blog Engine

The user writes four functions that together form a minimal static blog engine: parse frontmatter from markdown files, convert markdown to HTML, render a post page, and orchestrate the full build.

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

### Post Renderer `render_post_eng.py`
```python
from bb.pool import object_25f5b25a43080344597fea475adc88ecb0fc56e05b843a8668a1eb1822219df9 as parse_frontmatter
from bb.pool import object_1085d7f5f9104a833c0ec8f9c095a836712e257636da67dbff473bc0bed64ea7 as markdown_to_html


def render_post(file_content, site_title):
    """Render a single blog post to a full HTML page."""
    metadata, body = parse_frontmatter(file_content)
    title = metadata.get('title', 'Untitled')
    date = metadata.get('date', '')
    body_html = markdown_to_html(body)
    return ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
            + '<title>' + site_title + ' - ' + title + '</title>'
            + '</head>\n<body>'
            + '<header><a href="/">Home</a></header>'
            + '<article>'
            + '<h1>' + title + '</h1>'
            + '<time>' + date + '</time>'
            + body_html
            + '</article>'
            + '</body>\n</html>')
```

### Blog Builder `build_blog_eng.py`
```python
import os
from bb.pool import object_25f5b25a43080344597fea475adc88ecb0fc56e05b843a8668a1eb1822219df9 as parse_frontmatter
from bb.pool import object_1085d7f5f9104a833c0ec8f9c095a836712e257636da67dbff473bc0bed64ea7 as markdown_to_html
from bb.pool import object_df0eaf3e5dd509f4c933266d045c17170d7b5e12c1e5b0b698a8901e5c14b052 as render_post


def build_blog(source_dir, output_dir, site_title, site_url):
    """Build a static blog from markdown files in source_dir."""
    os.makedirs(output_dir, exist_ok=True)
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
        posts.append({'slug': slug, 'title': title, 'date': date})
        post_dir = os.path.join(output_dir, slug)
        os.makedirs(post_dir, exist_ok=True)
        html = render_post(content, site_title)
        with open(os.path.join(post_dir, 'index.html'), 'w', encoding='utf-8') as fh:
            fh.write(html)
    index_items = ''
    for post in sorted(posts, key=lambda p: p['date'], reverse=True):
        index_items += ('<li><time>' + post['date'] + '</time> '
                        + '<a href="' + post['slug'] + '/">'
                        + post['title'] + '</a></li>\n')
    index_html = ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
                  + '<title>' + site_title + '</title></head>\n<body>'
                  + '<h1>' + site_title + '</h1><ul>'
                  + index_items + '</ul></body>\n</html>')
    with open(os.path.join(output_dir, 'index.html'), 'w', encoding='utf-8') as fh:
        fh.write(index_html)
    rss_items = ''
    for post in sorted(posts, key=lambda p: p['date'], reverse=True):
        rss_items += ('<item><title>' + post['title'] + '</title>'
                      + '<link>' + site_url + '/' + post['slug']
                      + '/</link></item>\n')
    rss = ('<?xml version="1.0" encoding="utf-8"?>\n'
           + '<rss version="2.0"><channel>'
           + '<title>' + site_title + '</title>'
           + '<link>' + site_url + '</link>'
           + rss_items + '</channel></rss>')
    with open(os.path.join(output_dir, 'feed.rss'), 'w', encoding='utf-8') as fh:
        fh.write(rss)
    return posts
```

## Workflow

### Step 1: Add the frontmatter parser
```bash
$ PARSE_HASH=$(bb add parse_frontmatter_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 2: Add the markdown-to-HTML converter
```bash
$ MARKDOWN_HASH=$(bb add markdown_to_html_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 3: Add the post renderer
```bash
$ RENDER_HASH=$(bb add render_post_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 4: Add the blog builder
```bash
$ BUILD_HASH=$(bb add build_blog_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 5: Verify the stored functions
```bash
$ bb show $PARSE_HASH@eng | head -1
def parse_frontmatter(text):
```

```bash
$ bb show $MARKDOWN_HASH@eng | head -1
def markdown_to_html(text):
```

```bash
$ bb show $RENDER_HASH@eng | grep '^def '
def render_post(file_content, site_title):
```

### Step 6: Compile the blog builder to a standalone script
```bash
$ bb compile --output blog_engine.py $BUILD_HASH@eng
```

The compiled script inlines all dependencies (`parse_frontmatter`,
`markdown_to_html`, `render_post`) into a single runnable Python file.

### Step 7: Create sample posts and build the blog
```bash
# SKIP-TEST: heredoc demonstration
$ mkdir -p posts
$ cat > posts/first-post.md << 'ENDOFPOST'
---
title: My First Post
date: 2026-02-01
slug: first-post
---
Welcome to my blog. This is the first post built with a **bb-powered** static blog engine.
ENDOFPOST
$ cat > posts/second-post.md << 'ENDOFPOST'
---
title: On Static Sites
date: 2026-02-08
slug: on-static-sites
---
Static sites are fast, secure, and simple. This engine proves you can build one with composable functions stored in a content-addressed pool.
ENDOFPOST
$ python3 blog_engine.py
```

## Chapter 1 Results
- Four functions stored in bb's pool, each independently addressable by hash
- `render_post` depends on `parse_frontmatter` and `markdown_to_html` via bb imports
- `build_blog` depends on all three via bb imports
- `bb compile` produces a standalone script with all dependencies inlined
- The blog generates `index.html`, per-post pages, and `feed.rss`

---

# Chapter 2: Adding Tags

The user extends the blog engine with tag support. Each post can declare
tags in its frontmatter. The engine generates a dedicated page per tag
listing all related posts, and the index page shows tags alongside each entry.

## Source Files

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

### Tag Page Renderer `render_tag_page_eng.py`
```python
def render_tag_page(tag, posts, site_title):
    """Render an HTML page listing all posts for a given tag."""
    items = ''
    for post in sorted(posts, key=lambda p: p.get('date', ''), reverse=True):
        items += ('<li><time>' + post.get('date', '') + '</time> '
                  + '<a href="/' + post.get('slug', '') + '/">'
                  + post.get('title', 'Untitled') + '</a></li>\n')
    return ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
            + '<title>' + site_title + ' - Tag: ' + tag + '</title>'
            + '</head>\n<body>'
            + '<header><a href="/">Home</a></header>'
            + '<h1>Tag: ' + tag + '</h1>'
            + '<ul>' + items + '</ul>'
            + '</body>\n</html>')
```

### Updated Blog Builder `build_blog_with_tags_eng.py`
```python
import os
from bb.pool import object_25f5b25a43080344597fea475adc88ecb0fc56e05b843a8668a1eb1822219df9 as parse_frontmatter
from bb.pool import object_1085d7f5f9104a833c0ec8f9c095a836712e257636da67dbff473bc0bed64ea7 as markdown_to_html
from bb.pool import object_df0eaf3e5dd509f4c933266d045c17170d7b5e12c1e5b0b698a8901e5c14b052 as render_post
from bb.pool import object_7be41b434e25f5904709f0c6c3b907bc0768c45259c3de49928346e6debfdc99 as collect_tags
from bb.pool import object_da4b01118cdbdc7bab7dabf52630f6db1ca25a259e7457e2fd7daeae6e269e40 as render_tag_page


def build_blog(source_dir, output_dir, site_title, site_url):
    """Build a static blog with tag pages from markdown files."""
    os.makedirs(output_dir, exist_ok=True)
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
        post_dir = os.path.join(output_dir, slug)
        os.makedirs(post_dir, exist_ok=True)
        html = render_post(content, site_title)
        with open(os.path.join(post_dir, 'index.html'), 'w', encoding='utf-8') as fh:
            fh.write(html)
    tag_groups = collect_tags(posts)
    tags_dir = os.path.join(output_dir, 'tags')
    os.makedirs(tags_dir, exist_ok=True)
    all_tags = sorted(tag_groups.keys())
    for tag, tag_posts in tag_groups.items():
        tag_dir = os.path.join(tags_dir, tag)
        os.makedirs(tag_dir, exist_ok=True)
        tag_html = render_tag_page(tag, tag_posts, site_title)
        with open(os.path.join(tag_dir, 'index.html'), 'w', encoding='utf-8') as fh:
            fh.write(tag_html)
    index_items = ''
    for post in sorted(posts, key=lambda p: p['date'], reverse=True):
        tag_links = ''
        post_tags = post.get('tags', '')
        if isinstance(post_tags, str):
            post_tags = [t.strip() for t in post_tags.split(',') if t.strip()]
        for tag in post_tags:
            tag_links += ' <a href="/tags/' + tag.lower() + '/">[' + tag + ']</a>'
        index_items += ('<li><time>' + post['date'] + '</time> '
                        + '<a href="' + post['slug'] + '/">'
                        + post['title'] + '</a>' + tag_links + '</li>\n')
    index_html = ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
                  + '<title>' + site_title + '</title></head>\n<body>'
                  + '<h1>' + site_title + '</h1>'
                  + '<nav>Tags: '
                  + ' '.join('<a href="/tags/' + t + '/">' + t + '</a>'
                             for t in all_tags)
                  + '</nav><ul>' + index_items + '</ul></body>\n</html>')
    with open(os.path.join(output_dir, 'index.html'), 'w', encoding='utf-8') as fh:
        fh.write(index_html)
    rss_items = ''
    for post in sorted(posts, key=lambda p: p['date'], reverse=True):
        rss_items += ('<item><title>' + post['title'] + '</title>'
                      + '<link>' + site_url + '/' + post['slug']
                      + '/</link></item>\n')
    rss = ('<?xml version="1.0" encoding="utf-8"?>\n'
           + '<rss version="2.0"><channel>'
           + '<title>' + site_title + '</title>'
           + '<link>' + site_url + '</link>'
           + rss_items + '</channel></rss>')
    with open(os.path.join(output_dir, 'feed.rss'), 'w', encoding='utf-8') as fh:
        fh.write(rss)
    return posts
```

## Workflow

### Step 8: Add the tag collector
```bash
$ TAGS_HASH=$(bb add collect_tags_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 9: Add the tag page renderer
```bash
$ TAGPAGE_HASH=$(bb add render_tag_page_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 10: Add the updated blog builder with tags
```bash
$ BUILD_V2_HASH=$(bb add build_blog_with_tags_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 11: Compile and test with tagged posts
```bash
# SKIP-TEST: compile and heredoc demonstration
$ bb compile --output blog_engine_v2.py $BUILD_V2_HASH@eng
$ mkdir -p posts_v2
$ cat > posts_v2/first-post.md << 'ENDOFPOST'
---
title: My First Post
date: 2026-02-01
slug: first-post
tags: python, tutorial
---
Welcome to my blog. This is the first post built with a **bb-powered** static blog engine.
ENDOFPOST
$ cat > posts_v2/static-sites.md << 'ENDOFPOST'
---
title: On Static Sites
date: 2026-02-08
slug: on-static-sites
tags: python, web
---
Static sites are fast, secure, and simple. This engine proves you can build one with composable functions.
ENDOFPOST
$ cat > posts_v2/bb-intro.md << 'ENDOFPOST'
---
title: Beyond Babel Introduction
date: 2026-02-10
slug: bb-intro
tags: tutorial, multilingual
---
Beyond Babel lets developers share functions across human languages. The same logic, different names.
ENDOFPOST
$ python3 blog_engine_v2.py
```

### Step 12: Verify the generated tag pages
```bash
# SKIP-TEST: depends on compiled build step
$ ls output/tags/
multilingual
python
tutorial
web
$ cat output/tags/python/index.html | grep '<li>'
<li><time>2026-02-08</time> <a href="/on-static-sites/">On Static Sites</a></li>
<li><time>2026-02-01</time> <a href="/first-post/">My First Post</a></li>
```

## Chapter 2 Results
- Two new functions (`collect_tags`, `render_tag_page`) stored in bb's pool
- A new version of `build_blog` wires everything together, including tag generation
- Each tag gets its own directory at `/tags/<tag>/index.html`
- The index page now shows tag links next to each post
- Tags are normalized to lowercase for consistent URLs
- The old `build_blog` (chapter 1) still exists untouched in the pool

---

# Chapter 3: Making It Multilingual

The user adds French translations for every function using `bb add` with
French source files. Because bb hashes logic, not names, each French version
shares the same hash as its English counterpart. A French developer can now
use the blog engine with French function and variable names while the
compiled output is identical.

## Source Files

### Frontmatter Parser `parse_frontmatter_fra.py`
```python
def analyser_entete(texte):
    """Analyse l'en-tête YAML d'un fichier markdown."""
    metadonnees = {}
    if not texte.startswith('---'):
        return metadonnees, texte
    parties = texte.split('---', 2)
    if len(parties) < 3:
        return metadonnees, texte
    entete = parties[1].strip()
    corps = parties[2].strip()
    for ligne in entete.split('\n'):
        if ':' in ligne:
            cle, valeur = ligne.split(':', 1)
            metadonnees[cle.strip()] = valeur.strip()
    return metadonnees, corps
```

### Markdown to HTML Converter `markdown_to_html_fra.py`
```python
def markdown_vers_html(texte):
    """Convertit un sous-ensemble de markdown en HTML."""
    import re
    lignes = texte.split('\n')
    lignes_html = []
    dans_paragraphe = False
    for ligne in lignes:
        ligne_nettoyee = ligne.strip()
        if ligne_nettoyee.startswith('# '):
            if dans_paragraphe:
                lignes_html.append('</p>')
                dans_paragraphe = False
            lignes_html.append('<h1>' + ligne_nettoyee[2:] + '</h1>')
        elif ligne_nettoyee.startswith('## '):
            if dans_paragraphe:
                lignes_html.append('</p>')
                dans_paragraphe = False
            lignes_html.append('<h2>' + ligne_nettoyee[3:] + '</h2>')
        elif ligne_nettoyee.startswith('- '):
            if dans_paragraphe:
                lignes_html.append('</p>')
                dans_paragraphe = False
            lignes_html.append('<li>' + ligne_nettoyee[2:] + '</li>')
        elif ligne_nettoyee == '':
            if dans_paragraphe:
                lignes_html.append('</p>')
                dans_paragraphe = False
        else:
            if not dans_paragraphe:
                lignes_html.append('<p>')
                dans_paragraphe = True
            lignes_html.append(ligne_nettoyee)
    if dans_paragraphe:
        lignes_html.append('</p>')
    return '\n'.join(lignes_html)
```

### Post Renderer `render_post_fra.py`
```python
from bb.pool import object_25f5b25a43080344597fea475adc88ecb0fc56e05b843a8668a1eb1822219df9 as analyser_entete
from bb.pool import object_1085d7f5f9104a833c0ec8f9c095a836712e257636da67dbff473bc0bed64ea7 as markdown_vers_html


def generer_page_article(contenu_fichier, titre_site):
    """Génère une page HTML complète pour un article de blog."""
    metadonnees, corps = analyser_entete(contenu_fichier)
    titre = metadonnees.get('title', 'Untitled')
    date = metadonnees.get('date', '')
    corps_html = markdown_vers_html(corps)
    return ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
            + '<title>' + titre_site + ' - ' + titre + '</title>'
            + '</head>\n<body>'
            + '<header><a href="/">Home</a></header>'
            + '<article>'
            + '<h1>' + titre + '</h1>'
            + '<time>' + date + '</time>'
            + corps_html
            + '</article>'
            + '</body>\n</html>')
```

### Tag Collector `collect_tags_fra.py`
```python
def regrouper_etiquettes(articles):
    """Regroupe les articles par étiquette, retourne un dict étiquette -> liste d'articles."""
    etiquettes = {}
    for article in articles:
        etiquettes_article = article.get('tags', '')
        if isinstance(etiquettes_article, str):
            etiquettes_article = [e.strip() for e in etiquettes_article.split(',') if e.strip()]
        for etiquette in etiquettes_article:
            etiquette_min = etiquette.lower()
            if etiquette_min not in etiquettes:
                etiquettes[etiquette_min] = []
            etiquettes[etiquette_min].append(article)
    return etiquettes
```

### Tag Page Renderer `render_tag_page_fra.py`
```python
def generer_page_etiquette(etiquette, articles, titre_site):
    """Génère une page HTML listant tous les articles pour une étiquette donnée."""
    elements = ''
    for article in sorted(articles, key=lambda a: a.get('date', ''), reverse=True):
        elements += ('<li><time>' + article.get('date', '') + '</time> '
                     + '<a href="/' + article.get('slug', '') + '/">'
                     + article.get('title', 'Untitled') + '</a></li>\n')
    return ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
            + '<title>' + titre_site + ' - Tag: ' + etiquette + '</title>'
            + '</head>\n<body>'
            + '<header><a href="/">Home</a></header>'
            + '<h1>Tag: ' + etiquette + '</h1>'
            + '<ul>' + elements + '</ul>'
            + '</body>\n</html>')
```

### Updated Blog Builder `build_blog_with_tags_fra.py`
```python
import os
from bb.pool import object_25f5b25a43080344597fea475adc88ecb0fc56e05b843a8668a1eb1822219df9 as analyser_entete
from bb.pool import object_1085d7f5f9104a833c0ec8f9c095a836712e257636da67dbff473bc0bed64ea7 as markdown_vers_html
from bb.pool import object_df0eaf3e5dd509f4c933266d045c17170d7b5e12c1e5b0b698a8901e5c14b052 as generer_page_article
from bb.pool import object_7be41b434e25f5904709f0c6c3b907bc0768c45259c3de49928346e6debfdc99 as regrouper_etiquettes
from bb.pool import object_da4b01118cdbdc7bab7dabf52630f6db1ca25a259e7457e2fd7daeae6e269e40 as generer_page_etiquette


def construire_blog(repertoire_source, repertoire_sortie, titre_site, url_site):
    """Construit un blog statique avec pages d'étiquettes à partir de fichiers markdown."""
    os.makedirs(repertoire_sortie, exist_ok=True)
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
        repertoire_article = os.path.join(repertoire_sortie, slug)
        os.makedirs(repertoire_article, exist_ok=True)
        html = generer_page_article(contenu, titre_site)
        with open(os.path.join(repertoire_article, 'index.html'), 'w', encoding='utf-8') as fh:
            fh.write(html)
    groupes_etiquettes = regrouper_etiquettes(articles)
    repertoire_etiquettes = os.path.join(repertoire_sortie, 'tags')
    os.makedirs(repertoire_etiquettes, exist_ok=True)
    toutes_etiquettes = sorted(groupes_etiquettes.keys())
    for etiquette, articles_etiquette in groupes_etiquettes.items():
        repertoire_etiquette = os.path.join(repertoire_etiquettes, etiquette)
        os.makedirs(repertoire_etiquette, exist_ok=True)
        html_etiquette = generer_page_etiquette(etiquette, articles_etiquette, titre_site)
        with open(os.path.join(repertoire_etiquette, 'index.html'), 'w', encoding='utf-8') as fh:
            fh.write(html_etiquette)
    elements_index = ''
    for article in sorted(articles, key=lambda a: a['date'], reverse=True):
        liens_etiquettes = ''
        etiquettes_article = article.get('tags', '')
        if isinstance(etiquettes_article, str):
            etiquettes_article = [e.strip() for e in etiquettes_article.split(',') if e.strip()]
        for etiquette in etiquettes_article:
            liens_etiquettes += ' <a href="/tags/' + etiquette.lower() + '/">[' + etiquette + ']</a>'
        elements_index += ('<li><time>' + article['date'] + '</time> '
                           + '<a href="' + article['slug'] + '/">'
                           + article['title'] + '</a>' + liens_etiquettes + '</li>\n')
    html_index = ('<!DOCTYPE html>\n<html>\n<head><meta charset="UTF-8">'
                  + '<title>' + titre_site + '</title></head>\n<body>'
                  + '<h1>' + titre_site + '</h1>'
                  + '<nav>Tags: '
                  + ' '.join('<a href="/tags/' + e + '/">' + e + '</a>'
                             for e in toutes_etiquettes)
                  + '</nav><ul>' + elements_index + '</ul></body>\n</html>')
    with open(os.path.join(repertoire_sortie, 'index.html'), 'w', encoding='utf-8') as fh:
        fh.write(html_index)
    elements_rss = ''
    for article in sorted(articles, key=lambda a: a['date'], reverse=True):
        elements_rss += ('<item><title>' + article['title'] + '</title>'
                         + '<link>' + url_site + '/' + article['slug']
                         + '/</link></item>\n')
    rss = ('<?xml version="1.0" encoding="utf-8"?>\n'
           + '<rss version="2.0"><channel>'
           + '<title>' + titre_site + '</title>'
           + '<link>' + url_site + '</link>'
           + elements_rss + '</channel></rss>')
    with open(os.path.join(repertoire_sortie, 'feed.rss'), 'w', encoding='utf-8') as fh:
        fh.write(rss)
    return articles
```

## Workflow

### Step 13: Add the French frontmatter parser
```bash
$ PARSE_HASH_FRA=$(bb add parse_frontmatter_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

### Step 14: Verify same hash
```bash
$ test "$PARSE_HASH" = "$PARSE_HASH_FRA" && echo "Same hash!"
Same hash!
```

### Step 15: Add the French markdown converter
```bash
$ MARKDOWN_HASH_FRA=$(bb add markdown_to_html_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

### Step 16: Add the French post renderer
```bash
$ RENDER_HASH_FRA=$(bb add render_post_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

### Step 17: Add the French tag collector
```bash
$ TAGS_HASH_FRA=$(bb add collect_tags_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

### Step 18: Add the French tag page renderer
```bash
$ TAGPAGE_HASH_FRA=$(bb add render_tag_page_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

### Step 19: Add the French blog builder
```bash
$ BUILD_V2_HASH_FRA=$(bb add build_blog_with_tags_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

### Step 20: Verify all translations share the same hashes
```bash
$ test "$PARSE_HASH" = "$PARSE_HASH_FRA" && echo "parse_frontmatter: same hash"
parse_frontmatter: same hash
$ test "$MARKDOWN_HASH" = "$MARKDOWN_HASH_FRA" && echo "markdown_to_html: same hash"
markdown_to_html: same hash
$ test "$RENDER_HASH" = "$RENDER_HASH_FRA" && echo "render_post: same hash"
render_post: same hash
$ test "$TAGS_HASH" = "$TAGS_HASH_FRA" && echo "collect_tags: same hash"
collect_tags: same hash
$ test "$BUILD_V2_HASH" = "$BUILD_V2_HASH_FRA" && echo "build_blog: same hash"
build_blog: same hash
```

### Step 21: Show functions in both languages
```bash
$ bb show $PARSE_HASH@eng | head -1
def parse_frontmatter(text):
```

```bash
$ bb show $PARSE_HASH@fra | head -1
def analyser_entete(texte):
```

```bash
$ bb show $TAGS_HASH@eng | head -1
def collect_tags(posts):
```

```bash
$ bb show $TAGS_HASH@fra | head -1
def regrouper_etiquettes(articles):
```

### Step 22: Compile with French identifiers
```bash
# SKIP-TEST: compile demonstration
$ bb compile --output moteur_blog.py $BUILD_V2_HASH@fra
$ head -5 moteur_blog.py
```

The compiled French script uses `construire_blog`, `analyser_entete`,
`markdown_vers_html`, `regrouper_etiquettes` etc. - all French names.
But the output HTML is identical to what the English version produces.

### Step 23: Both compiled engines produce identical output
```bash
# SKIP-TEST: compile and diff demonstration
$ python3 blog_engine_v2.py
$ mv output output_eng
$ python3 moteur_blog.py
$ mv output output_fra
$ diff -r output_eng output_fra
```

No differences. The logic is the same - only the source code names differ.

## Chapter 3 Results
- Every function now has both `eng` and `fra` language mappings
- All English/French pairs share the same hash (logic is identical)
- `bb compile` with `@eng` produces English variable names in the output
- `bb compile` with `@fra` produces French variable names in the output
- Both compiled scripts generate **byte-identical** HTML output

---

## Summary

| Function | English Name | French Name | Hash |
|----------|-------------|-------------|------|
| Frontmatter parser | `parse_frontmatter` | `analyser_entete` | `25f5b25a...` |
| Markdown converter | `markdown_to_html` | `markdown_vers_html` | `1085d7f5...` |
| Post renderer | `render_post` | `generer_page_article` | `df0eaf3e...` |
| Tag collector | `collect_tags` | `regrouper_etiquettes` | `7be41b43...` |
| Tag page renderer | `render_tag_page` | `generer_page_etiquette` | `da4b0111...` |
| Blog builder (v1) | `build_blog` | - | `3e77c64b...` |
| Blog builder (v2) | `build_blog` | `construire_blog` | `41c10ff9...` |

## Key Insights

1. **Composability**: Each function is independently stored and versioned. The blog builder references its dependencies via bb imports, not file paths. Swapping out `markdown_to_html` for a better implementation only requires updating one import hash.

2. **Incremental evolution**: Chapter 1's `build_blog` still exists in the pool. Chapter 2 created a new function with a different hash (different logic). Nothing was overwritten - the pool is append-only.

3. **Translation is metadata, not logic**: Adding French versions with `bb add` stores the French names as an additional mapping. The hash doesn't change because the hash captures logic, not presentation.

4. **Multilingual collaboration**: A French developer can `bb show HASH@fra` to read the blog engine in French, modify it with French variable names, and `bb add modified.py@fra` to contribute back. An English developer sees none of the French names unless they ask for them.

5. **Identical output guarantee**: Since both language versions compile from the same normalized logic, `bb compile HASH@eng` and `bb compile HASH@fra` produce functionally identical programs. The only difference is identifier names in the source code - the runtime behavior is the same.
