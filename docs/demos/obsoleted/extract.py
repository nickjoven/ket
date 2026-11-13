"""Deterministic claim extractor for the `obsoleted` demo.

Reads one RFC's text and returns the status-code reason phrases it defines,
as `(code, phrase, section)` triples. It stands in for an LLM extraction
step: same input bytes, same claims, every run. Its own source file is put
into the store, and the CID that comes back names "the extractor" in every
extraction record, so a change to this file is a different derivation.
"""

import re

# Numbered section headings: "10.4.14 413 Request Entity Too Large" (2068,
# 2616), "6.5.11.  413 Payload Too Large" (7231), "15.5.14.  413 Content Too
# Large" (9110). Table-of-contents lines end in dot leaders and page
# numbers, so the end anchor skips them.
_PHRASE = r"[A-Z][A-Za-z'\-]*(?: [A-Z][A-Za-z'\-]*)*"
_HEADING = re.compile(
    r"^ {0,3}(\d+(?:\.\d+)*)\.? +([1-5]\d\d) (" + _PHRASE + r"|\(Unused\))\s*$"
)
# RFC 1945 lists codes as unnumbered, indented lines ("   302 Moved
# Temporarily") inside its top-level "Status Code Definitions" section. The
# indented form is only accepted there: elsewhere it matches things like a
# street address in an authors' section.
_INDENTED = re.compile(r"^   ([1-5]\d\d) (" + _PHRASE + r")\s*$")
_SECTION = re.compile(r"^(\d+)\.? +(\S.*)$")


def extract(text: str):
    """Return sorted, de-duplicated (code, phrase, section) triples."""
    out = {}
    top = ""
    in_status_section = False  # inside a top-level "Status Code Definitions"
    for line in text.splitlines():
        m = _HEADING.match(line)
        if m:
            section, code, phrase = m.group(1), int(m.group(2)), m.group(3)
            if phrase != "(Unused)":
                out.setdefault(code, (code, phrase, section))
            continue
        s = _SECTION.match(line)
        if s:
            in_status_section = s.group(2).startswith("Status Code Definitions")
            top = s.group(1)
            continue
        m = _INDENTED.match(line)
        if m and in_status_section:
            code, phrase = int(m.group(1)), m.group(2)
            out.setdefault(code, (code, phrase, top))
    return [out[c] for c in sorted(out)]
