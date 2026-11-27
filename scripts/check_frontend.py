"""Validate templates/index.html markup and inline JavaScript syntax."""

import os
import subprocess
import sys
from html.parser import HTMLParser


class IndexParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.styles = []
        self.scripts = []
        self.links = []
        self.external_scripts = []
        self.ids = []
        self._in_style = False
        self._in_script = False
        self._buf = []

    def handle_starttag(self, tag, attrs):
        attr_map = dict(attrs)
        if "id" in attr_map:
            self.ids.append(attr_map["id"])
        if tag == "link":
            self.links.append(attr_map.get("href", ""))
        elif tag == "style":
            self._in_style = True
            self._buf = []
        elif tag == "script":
            if "src" in attr_map:
                self.external_scripts.append(attr_map["src"])
            else:
                self._in_script = True
                self._buf = []

    def handle_data(self, data):
        if self._in_style or self._in_script:
            self._buf.append(data)

    def handle_endtag(self, tag):
        if tag == "style" and self._in_style:
            self.styles.append("".join(self._buf))
            self._in_style = False
        elif tag == "script" and self._in_script:
            self.scripts.append("".join(self._buf))
            self._in_script = False


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    index_path = os.path.join(root, "templates", "index.html")
    with open(index_path, "r", encoding="utf-8") as f:
        html = f.read()

    parser = IndexParser()
    parser.feed(html)
    parser.close()

    assert parser.styles and parser.styles[0].strip(), "Missing inline <style> in templates/index.html"
    assert parser.scripts and parser.scripts[0].strip(), "Missing inline <script> in templates/index.html"
    assert not any("style.css" in href for href in parser.links), "Unexpected external style.css reference"
    assert not any("app.js" in src for src in parser.external_scripts), "Unexpected external app.js reference"
    assert len(parser.ids) == len(set(parser.ids)), f"Duplicate element IDs found: {parser.ids}"

    js = "\n".join(parser.scripts)
    proc = subprocess.run(["node", "--check"], input=js, text=True, capture_output=True)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        return proc.returncode

    print("Frontend HTML/inline JS validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
