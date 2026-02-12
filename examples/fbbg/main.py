#!/usr/bin/env python3
"""
Compiled bb function: 239e1a42e5653fb780211f0df5b27df1488c891807b1c7cdad5c64194627b76c
"""

from datetime import datetime
from pathlib import Path
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring
import re


def html_markup_fragment_d3f87c4a(text):
    """Convert simple markdown to HTML with proper paragraph support.

Supports:
- Headings: # h1, ## h2, ### h3, #### h4, ##### h5
- Bold: **bold text**
- Italic: *italic text*
- Links: [text](url)
- Paragraphs: Text blocks separated by blank lines

Args:
    text: Markdown-formatted text string

Returns:
    HTML string

Examples:
    >>> html_markup_fragment("# Hello World")
    '<h1>Hello World</h1>'

    >>> html_markup_fragment("This is **bold** and *italic* text")
    '<p>This is <strong>bold</strong> and <em>italic</em> text</p>'

    >>> html_markup_fragment("Visit [Google](https://google.com)")
    '<p>Visit <a href="https://google.com">Google</a></p>'

    >>> html_markup_fragment("## Section\\n\\nSome **important** content.")
    '<h2>Section</h2>\\n<p>Some <strong>important</strong> content.</p>'

    >>> html_markup_fragment("First paragraph.\\n\\nSecond paragraph.")
    '<p>First paragraph.</p>\\n<p>Second paragraph.</p>'"""
    blocks = text.split('\n\n')
    result = []
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        heading_match = re.match('^(#{1,5})\\s+(.+)$', block, re.DOTALL)
        if heading_match:
            level = len(heading_match.group(1))
            heading_text = heading_match.group(2)
            heading_text = re.sub('\\[([^\\]]+)\\]\\(([^\\)]+)\\)', '<a href="\\2">\\1</a>', heading_text)
            heading_text = re.sub('\\*\\*([^\\*]+)\\*\\*', '<strong>\\1</strong>', heading_text)
            heading_text = re.sub('\\*([^\\*]+)\\*', '<em>\\1</em>', heading_text)
            result.append(f'<h{level}>{heading_text}</h{level}>')
        else:
            block = block.replace('\n', ' ')
            block = re.sub('\\[([^\\]]+)\\]\\(([^\\)]+)\\)', '<a href="\\2">\\1</a>', block)
            block = re.sub('\\*\\*([^\\*]+)\\*\\*', '<strong>\\1</strong>', block)
            block = re.sub('\\*([^\\*]+)\\*', '<em>\\1</em>', block)
            result.append(f'<p>{block}</p>')
    return '\n'.join(result)


def html_new_5bca7e3b(title, language, body):
    """Build a complete HTML5 document.

Creates a valid HTML5 document with proper structure including
doctype, html lang attribute, head with title, and body content.

Args:
    title: Page title string
    language: Language code (e.g., 'en', 'fr', 'es', 'de')
    body: HTML fragment for body content

Returns:
    Complete HTML5 document as string

Examples:
    >>> doc = html_new('My Page', 'en', '<h1>Hello World</h1><p>Welcome!</p>')
    >>> '<!DOCTYPE html>' in doc
    True
    >>> '<html lang="en">' in doc
    True
    >>> '<title>My Page</title>' in doc
    True
    >>> '<h1>Hello World</h1>' in doc
    True

    >>> doc = html_new('Ma Page', 'fr', '<h1>Bonjour</h1>')
    >>> '<html lang="fr">' in doc
    True"""
    return f'''<!DOCTYPE html>\n<html lang="{language}">\n<head>\n    <meta charset="UTF-8">\n    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n    <title>{title}</title>\n    <link rel="preconnect" href="https://fonts.googleapis.com">\n    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>\n    <link href="https://fonts.googleapis.com/css2?family=League+Gothic&display=swap" rel="stylesheet">\n    <style>\n        * {{\n            margin: 0;\n            padding: 0;\n            box-sizing: border-box;\n        }}\n        body {{\n            font-family: Baskerville, Georgia, serif;\n            background-color: #e5e5e5;\n            color: #000;\n            line-height: 1.6;\n            padding: 40px 20px;\n            max-width: 800px;\n            margin: 0 auto;\n        }}\n        h1, h2, h3, h4, h5 {{\n            font-family: 'League Gothic', 'URW Gothic', 'Nimbus Sans L', sans-serif;\n            color: #0000FF;\n            text-transform: uppercase;\n            letter-spacing: 0.05em;\n            margin-bottom: 20px;\n            font-weight: normal;\n        }}\n        h1 {{\n            font-size: 4em;\n            border-bottom: 4px solid #0000FF;\n            padding-bottom: 10px;\n            margin-bottom: 30px;\n        }}\n        h2 {{\n            font-size: 3em;\n            margin-top: 40px;\n        }}\n        h3 {{\n            font-size: 2.5em;\n        }}\n        p {{\n            font-size: 1.5em;\n            margin-bottom: 20px;\n        }}\n        a {{\n            color: #0000FF;\n            text-decoration: none;\n            border-bottom: 2px solid #0000FF;\n            font-weight: normal;\n        }}\n        a:hover {{\n            background-color: #0000FF;\n            color: #e5e5e5;\n        }}\n        ul {{\n            list-style: none;\n            margin: 20px 0;\n        }}\n        li {{\n            font-size: 1.5em;\n            margin-bottom: 15px;\n            padding: 10px;\n            background-color: #fff;\n            border-left: 4px solid #0000FF;\n        }}\n        strong {{\n            color: #0000FF;\n        }}\n    </style>\n</head>\n<body>\n{body}\n</body>\n</html>'''


def rss_f826b728(items, channel_title, channel_link, channel_description):
    """Generate RSS 2.0 feed from list of items.

Creates a valid RSS feed with title (h1), publication date, and URL
for each item.

Args:
    items: List of dictionaries with:
        - title: Article title (h1 heading)
        - url: Absolute URL to the article
        - date: Publication date (datetime object or ISO string)
    channel_title: Feed title
    channel_link: Feed home URL
    channel_description: Feed description

Returns:
    RSS XML string (formatted and indented)

Examples:
    >>> from datetime import datetime
    >>> items = [
    ...     {
    ...         'title': 'First Post',
    ...         'url': 'https://example.com/post1',
    ...         'date': datetime(2025, 2, 11, 10, 0)
    ...     },
    ...     {
    ...         'title': 'Second Post',
    ...         'url': 'https://example.com/post2',
    ...         'date': '2025-02-10T12:00:00'
    ...     }
    ... ]
    >>> feed = rss(items, 'My Blog', 'https://example.com', 'My personal blog')
    >>> '<title>First Post</title>' in feed
    True
    >>> '<link>https://example.com/post1</link>' in feed
    True"""
    rss_elem = Element('rss', version='2.0')
    channel = SubElement(rss_elem, 'channel')
    SubElement(channel, 'title').text = channel_title
    SubElement(channel, 'link').text = channel_link
    SubElement(channel, 'description').text = channel_description
    for item_data in items:
        item = SubElement(channel, 'item')
        SubElement(item, 'title').text = item_data['title']
        SubElement(item, 'link').text = item_data['url']
        SubElement(item, 'guid', isPermaLink='true').text = item_data['url']
        date_value = item_data['date']
        if isinstance(date_value, str):
            date_value = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
        pub_date = date_value.strftime('%a, %d %b %Y %H:%M:%S +0000')
        SubElement(item, 'pubDate').text = pub_date
    xml_string = tostring(rss_elem, encoding='unicode')
    dom = minidom.parseString(xml_string)
    pretty_xml = dom.toprettyxml(indent='  ', encoding='utf-8').decode('utf-8')
    lines = [line for line in pretty_xml.split('\n') if line.strip()]
    return '\n'.join(lines)


def fbbg_239e1a42(www_dir, base_url='https://example.com'):
    """Generate HTML files and RSS feeds from markdown files in www directory.

Recursively processes all .md files in www/{{lang}}/{{year}}/{{month}}/
directory structure and generates:
- Corresponding .html files for each article
- RSS feed (feed.rss) for each language

Args:
    www_dir: Path to www directory (string or Path)
    base_url: Base URL for generated links (default: 'https://example.com')

Returns:
    Dictionary with:
        - generated: List of generated HTML and RSS file paths
        - errors: List of (file_path, error_message) tuples

Directory structure:
    www/
    ├── eng/2026/02/article-name/index.md → index.html
    ├── eng/feed.rss (generated)
    ├── fra/2026/02/nom-article/index.md → index.html
    └── fra/feed.rss (generated)

Language code mapping:
    eng → en, fra → fr, spa → es, deu → de, etc.

Examples:
    >>> result = fbbg('www')
    >>> len(result['generated']) >= 4  # 2 HTML + 2 RSS
    True
    >>> any('feed.rss' in f for f in result['generated'])
    True"""
    lang_map = {'eng': 'en', 'fra': 'fr', 'spa': 'es', 'deu': 'de', 'ita': 'it', 'por': 'pt', 'rus': 'ru', 'jpn': 'ja', 'zho': 'zh', 'ara': 'ar', 'nld': 'nl', 'pol': 'pl'}
    www_path = Path(www_dir)
    generated = []
    errors = []
    articles_by_lang = {}
    for md_file in www_path.rglob('*.md'):
        try:
            content = md_file.read_text(encoding='utf-8')
            html_body = html_markup_fragment_d3f87c4a(content)
            title_match = re.search('<h1>([^<]+)</h1>', html_body)
            title = title_match.group(1) if title_match else 'Untitled'
            parts = md_file.parts
            lang_3char = None
            for part in parts:
                if part in lang_map:
                    lang_3char = part
                    break
            language = lang_map.get(lang_3char, 'en')
            footer = f'\n<footer style="margin-top: 60px; padding-top: 20px; border-top: 4px solid #0000FF;">\n    <p><a href="/{lang_3char}/">← {lang_3char.upper()}</a> · <a href="/">Beyond Babel</a></p>\n</footer>'
            html_body_with_footer = html_body + footer
            page_title = f'Beyond Babel · {title}'
            html_doc = html_new_5bca7e3b(page_title, language, html_body_with_footer)
            html_file = md_file.with_suffix('.html')
            html_file.write_text(html_doc, encoding='utf-8')
            generated.append(str(html_file))
            try:
                year = int(parts[parts.index(lang_3char) + 1])
                month = int(parts[parts.index(lang_3char) + 2])
                date = datetime(year, month, 1)
            except (ValueError, IndexError):
                date = datetime.now()
            relative_path = html_file.relative_to(www_path)
            url = f'{base_url}/{relative_path}'
            if lang_3char not in articles_by_lang:
                articles_by_lang[lang_3char] = []
            articles_by_lang[lang_3char].append({'title': title, 'url': url, 'date': date})
        except Exception as e:
            errors.append((str(md_file), str(e)))
    for lang_3char, articles in articles_by_lang.items():
        try:
            language = lang_map.get(lang_3char, 'en')
            feed_path = www_path / lang_3char / 'feed.rss'
            index_path = www_path / lang_3char / 'index.html'
            articles.sort(key=lambda x: x['date'], reverse=True)
            feed_xml = rss_f826b728(articles, channel_title=f'Beyond Babel - {lang_3char.upper()}', channel_link=f'{base_url}/{lang_3char}/', channel_description=f'Beyond Babel articles in {lang_3char}')
            feed_path.write_text(feed_xml, encoding='utf-8')
            generated.append(str(feed_path))
            index_body = f'<h1><a href="{base_url}/" style="text-decoration:none;color:inherit;border:none;">Beyond Babel</a></h1>\n<ul>\n'
            for article in articles:
                date_str = article['date'].strftime('%Y-%m')
                rel_url = article['url'].replace(f'{base_url}/{lang_3char}/', '').replace('index.html', '')
                index_body += f'''<li><span style="display:inline-block;width:100px">{date_str}</span> <a href="{rel_url}">{article['title']}</a></li>\n'''
            index_body += '</ul>'
            index_html = html_new_5bca7e3b('Beyond Babel', language, index_body)
            index_path.write_text(index_html, encoding='utf-8')
            generated.append(str(index_path))
        except Exception as e:
            errors.append((f'RSS feed for {lang_3char}', str(e)))
    try:
        greetings = {'eng': 'Hello', 'fra': 'Bonjour', 'spa': 'Hola', 'deu': 'Hallo', 'ita': 'Ciao', 'por': 'Olá', 'rus': 'Привет', 'jpn': 'こんにちは', 'zho': '你好', 'ara': 'مرحبا'}
        main_index_body = f'<h1><a href="{base_url}/" style="text-decoration:none;color:inherit;border:none;">Beyond Babel</a></h1>\n<ul>\n'
        for lang_3char in sorted(articles_by_lang.keys()):
            greeting = greetings.get(lang_3char, 'Hello')
            lang_name = lang_3char.upper()
            main_index_body += f'<li><a href="{lang_3char}/">{greeting} - {lang_name}</a></li>\n'
        main_index_body += '</ul>'
        main_index_html = html_new_5bca7e3b('Beyond Babel', 'en', main_index_body)
        main_index_path = www_path / 'index.html'
        main_index_path.write_text(main_index_html, encoding='utf-8')
        generated.append(str(main_index_path))
    except Exception as e:
        errors.append(('Main index', str(e)))
    return {'generated': generated, 'errors': errors}


if __name__ == "__main__":
    import sys
    # Entry point: fbbg_239e1a42
    if len(sys.argv) > 1:
        args = []
        for arg in sys.argv[1:]:
            try:
                args.append(int(arg))
            except ValueError:
                try:
                    args.append(float(arg))
                except ValueError:
                    args.append(arg)
        result = fbbg_239e1a42(*args)
        print(result)
    else:
        print(f"Usage: {sys.argv[0]} www_dir base_url")
