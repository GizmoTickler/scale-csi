#!/usr/bin/env python3
"""Regenerate the "All values" table in charts/scale-csi/README.md.

The table is derived from charts/scale-csi/values.yaml: one row per leaf key,
with its default and the first sentence of the comment block directly above
it. Only the text between the BEGIN/END markers in the README is replaced.

    python3 hack/chart-values-table.py           # rewrite the README
    python3 hack/chart-values-table.py --check   # exit 1 if it is stale

Standard library only, so it runs anywhere Python 3.8+ does.
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VALUES = os.path.join(ROOT, "charts", "scale-csi", "values.yaml")
README = os.path.join(ROOT, "charts", "scale-csi", "README.md")
BEGIN = "<!-- BEGIN GENERATED VALUES TABLE (hack/chart-values-table.py) -->"
END = "<!-- END GENERATED VALUES TABLE -->"

KEY_RE = re.compile(r"^(?P<indent> *)(?P<key>[A-Za-z0-9_./-]+):(?:\s+(?P<value>.*))?$")


def split_inline_comment(value):
    """Split 'value  # comment' (outside quotes) into (value, comment)."""
    quote = None
    for i, ch in enumerate(value):
        if ch in "\"'":
            quote = None if quote == ch else (quote or ch)
        elif ch == "#" and quote is None and (i == 0 or value[i - 1] == " "):
            return value[:i].rstrip(), value[i + 1:].strip()
    return value.rstrip(), ""


# Internal work-item tags that prefix some comments, e.g. "(C10)", "E2:".
TAG_RE = re.compile(r"^(\([A-Z][A-Z0-9/-]*\d[a-z]?\)\s*|[A-Z][A-Z0-9/-]*\d[a-z]?:\s*)+")


def first_sentence(lines):
    text = " ".join(l.strip() for l in lines if l.strip())
    text = re.sub(r"\s+", " ", text).strip()
    text = TAG_RE.sub("", text)
    if not text:
        return ""
    match = re.search(r"(.+?[.!?])(\s|$)", text)
    sentence = match.group(1) if match else text
    sentence = sentence[:1].upper() + sentence[1:]
    if len(sentence) > 220:
        sentence = sentence[:217].rsplit(" ", 1)[0] + "…"
    return sentence


def md_escape(text):
    return text.replace("|", "\\|").replace("<", "&lt;").replace(">", "&gt;")


def parse(path):
    with open(path, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    rows = []
    stack = []  # (indent, key)
    comments = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            comments = []
            i += 1
            continue
        if stripped.startswith("#"):
            comments.append(stripped.lstrip("#"))
            i += 1
            continue
        m = KEY_RE.match(line)
        if not m or stripped.startswith("- "):
            comments = []
            i += 1
            continue
        indent = len(m.group("indent"))
        while stack and stack[-1][0] >= indent:
            stack.pop()
        key = m.group("key")
        value, inline = split_inline_comment(m.group("value") or "")
        path_key = ".".join([k for _, k in stack] + [key])
        # Look ahead to the next meaningful line.
        j = i + 1
        while j < len(lines) and (not lines[j].strip() or lines[j].strip().startswith("#")):
            j += 1
        nxt = lines[j] if j < len(lines) else ""
        nxt_indent = len(nxt) - len(nxt.lstrip(" "))
        description = first_sentence(comments) or first_sentence([inline])
        comments = []
        if value:
            rows.append((path_key, value, description))
        elif nxt.strip().startswith("- ") and nxt_indent >= indent:
            # A list of items (maps or scalars): show it as a block, do not descend.
            items = []
            k = j
            while k < len(lines):
                raw = lines[k]
                if raw.strip() and not raw.strip().startswith("#"):
                    ind = len(raw) - len(raw.lstrip(" "))
                    if ind < indent or (ind == indent and not raw.strip().startswith("- ")):
                        break
                    if ind == nxt_indent and raw.strip().startswith("- "):
                        items.append(raw.strip()[2:])
                k += 1
            if all(":" not in it for it in items):
                default = "[" + ", ".join(items) + "]"
            else:
                default = f"list of {len(items)} (see values.yaml)"
            rows.append((path_key, default, description))
            i = k
            continue
        elif nxt_indent > indent:
            stack.append((indent, key))
        else:
            rows.append((path_key, "", description))
        i += 1
    return rows


def render(rows):
    out = [BEGIN, "", "| Key | Default | Description |", "|---|---|---|"]
    for key, default, desc in rows:
        escaped_default = default.replace("|", "\\|")
        default_md = "`" + escaped_default + "`" if default else ""
        out.append(f"| `{key}` | {default_md} | {md_escape(desc)} |")
    out += ["", END]
    return "\n".join(out)


def main():
    rows = parse(VALUES)
    with open(README, encoding="utf-8") as fh:
        readme = fh.read()
    if BEGIN not in readme or END not in readme:
        sys.exit(f"{README} has no generated-table markers")
    head, rest = readme.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    updated = head + render(rows) + tail
    if "--check" in sys.argv:
        if updated != readme:
            print("charts/scale-csi/README.md values table is stale; run hack/chart-values-table.py")
            sys.exit(1)
        print(f"values table up to date ({len(rows)} keys)")
        return
    with open(README, "w", encoding="utf-8") as fh:
        fh.write(updated)
    print(f"wrote {len(rows)} keys")


if __name__ == "__main__":
    main()
