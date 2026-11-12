#!/usr/bin/env python3
"""obsoleted: naive chunk-RAG vs a ket substrate, on the HTTP RFC series.

Mechanics tier: scripted walkers, no LLM, no API key. Every number this
prints is a deterministic function of the pinned corpus (only node CIDs
change between runs, because nodes carry timestamps).

Four walkers read overlapping slices of 13 HTTP RFCs and record what they
find. The same events happen to both arms:

  1. build      a 2015-era corpus (1945 .. 7538) is walked
  2. rumour     walker-3 writes a confident, wrong summary; walker-4 builds on it
  3. late       RFC 9110/9111/9112 arrive after the store was built
  4. edit       someone edits the shared working copy of rfc9110.txt

Then 44 questions ("what is the reason phrase for status N?") are answered
three ways and scored against an answer key taken from RFC 9110:

  naive        chunks + lexical similarity (stand-in for embeddings) + a reader
  ket, blind   the ket store, read without edge kinds (what ket's MCP read
               tools return today: issue #27)
  ket          the ket store, read with edge kinds enforced: claims grounded
               in a superseded document, proposals, and refuted claims are
               excluded; disagreement between current sources is reported

Usage: obsoleted.py <corpus_dir> <work_dir>   (run via ../obsoleted.sh)
"""

import collections
import json
import math
import os
import re
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from extract import extract  # noqa: E402

WALKERS = ["walker-1", "walker-2", "walker-3", "walker-4"]
LATE = {9110, 9111, 9112}
TOP_K = 8
CHUNK_CHARS = 800


# --------------------------------------------------------------------------
# output helpers

def say(msg):
    print(f"\n\033[1m{msg}\033[0m")


def dim(msg):
    print(f"\033[2m{msg}\033[0m")


# --------------------------------------------------------------------------
# ket CLI (the binary on PATH; KET_HOME points at the work store)

def ket(*args, stdin=None):
    r = subprocess.run(["ket", *args], input=stdin, capture_output=True, text=True)
    if r.returncode not in (0, 1):
        raise SystemExit(f"ket {' '.join(args)} failed: {r.stderr.strip()}")
    return r


def ket_json(*args):
    r = ket("--json", *args)
    if r.returncode != 0:
        raise SystemExit(f"ket {' '.join(args)} failed: {r.stderr.strip()}")
    return json.loads(r.stdout)


def put_bytes(text):
    return ket("put", "-", stdin=text).stdout.strip()


def create(kind, agent, parents, content=None, content_file=None):
    args = ["dag", "create", "--kind", kind, "--agent", agent]
    for cid, edge in parents:
        args += ["--parent", f"{cid}:{edge}"]
    if content_file:
        args += ["--content-file", content_file]
    else:
        args += ["--", content]
    out = ket_json(*args)
    return out["node_cid"], out["content_cid"]


def canon(obj):
    """Canonical JSON: identical claims are identical bytes, hence one CID."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


# --------------------------------------------------------------------------
# corpus

def load_corpus(corpus_dir):
    docs = {}
    with open(os.path.join(HERE, "corpus.tsv")) as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            n, cid, size, title, obs = line.rstrip("\n").split("\t")
            docs[int(n)] = {
                "rfc": int(n), "cid": cid, "title": title,
                "obsoletes": [int(x) for x in obs.split(",") if x],
                "path": os.path.join(corpus_dir, f"rfc{n}.txt"),
            }
    return docs


def load_answers():
    key = {}
    with open(os.path.join(HERE, "answers.tsv")) as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            code, phrase, section = line.rstrip("\n").split("\t")[:3]
            if phrase != "(Unused)":
                key[int(code)] = phrase
    return key


def read(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def assignments(rfcs):
    """Each document goes to two walkers: overlapping slices, as in practice."""
    out = collections.defaultdict(list)
    for i, n in enumerate(sorted(rfcs)):
        out[WALKERS[i % 4]].append(n)
        out[WALKERS[(i + 1) % 4]].append(n)
    return out


# --------------------------------------------------------------------------
# naive arm: chunks + lexical similarity + a reader

STOP = set("a an and are as at be by for from how in is it of on or that the "
           "this to was what which with".split())


def tokens(text):
    return [t for t in re.findall(r"[a-z0-9]+", text.lower()) if t not in STOP]


class NaiveIndex:
    """A shared chunk index. Upserts by (source, chunk number), like most
    vector stores keyed on document id; it keeps no versions and no status."""

    def __init__(self):
        self.chunks = {}          # id -> dict(text, source, path, offset)
        self.bytes_processed = 0  # every walker's reading and chunking

    def add_document(self, rfc, path):
        text = read(path)
        self.bytes_processed += len(text.encode())
        paras, offset = [], 0
        for block in re.split(r"(\n\s*\n)", text):
            paras.append((offset, block))
            offset += len(block)
        buf, start, i = "", 0, 0
        for off, block in paras:
            if buf and len(buf) + len(block) > CHUNK_CHARS:
                self._put(f"rfc{rfc}#{i}", buf, f"RFC {rfc}", path, start)
                buf, i = "", i + 1
            if not buf:
                start = off
            buf += block
        if buf.strip():
            self._put(f"rfc{rfc}#{i}", buf, f"RFC {rfc}", path, start)

    def add_note(self, note_id, text, agent):
        self.bytes_processed += len(text.encode())
        self._put(note_id, text, f"note by {agent}", None, 0)

    def _put(self, cid, text, source, path, offset):
        self.chunks[cid] = {"text": text, "source": source, "path": path,
                            "offset": offset}

    def search(self, query, must=None, k=TOP_K):
        """Hybrid retrieval, the generous baseline: an exact-identifier filter
        (keep chunks that contain `must`, e.g. the status code) and BM25
        (k1=1.2, b=0.75) ranking over the question. BM25 stands in for
        embedding similarity: the failure this demo is about (similar is not
        current) does not depend on which similarity is used."""
        k1, b = 1.2, 0.75
        docs = {cid: collections.Counter(tokens(c["text"]))
                for cid, c in self.chunks.items()}
        n = len(docs)
        avg = sum(sum(tf.values()) for tf in docs.values()) / max(n, 1)
        df = collections.Counter()
        for tf in docs.values():
            df.update(tf.keys())
        q = set(tokens(query))
        scored = []
        for cid, tf in docs.items():
            if must is not None and must not in tf:
                continue
            dl = sum(tf.values())
            score = 0.0
            for t in q:
                f = tf.get(t, 0)
                if not f:
                    continue
                idf = math.log(1 + (n - df[t] + 0.5) / (df[t] + 0.5))
                score += idf * f * (k1 + 1) / (f + k1 * (1 - b + b * dl / avg))
            scored.append((score, cid))
        scored.sort(key=lambda x: (-x[0], x[1]))
        return [cid for _, cid in scored[:k]]


def naive_read(index, code, hits):
    """The reader: the first phrase attached to the code, in rank order.
    Understands the three layouts in the corpus: '413 Content Too Large'
    (headings), '413 (Payload Too Large)' (prose), and RFC 2068/2616's
    status table, '"413"  ; Section 10.4.14: Request Entity Too Large'."""
    phrase = r"([A-Z][A-Za-z'\-]*(?: [A-Z][A-Za-z'\-]*)*)"
    pats = [re.compile(rf'"{code}"\s*;\s*Section [0-9.]+:\s*' + phrase),
            re.compile(rf"(?<![\d.]){code}(?!\d)\s*(?:\(|is\s+|-\s+)?" + phrase)]
    for cid in hits:
        # Undo line wrapping inside a paragraph; a blank line still ends a phrase.
        text = re.sub(r"\n[ \t]*\n", " | ", index.chunks[cid]["text"])
        text = re.sub(r"\s+", " ", text)
        for pat in pats:
            m = pat.search(text)
            if m:
                return m.group(1), cid
    return None, None


# --------------------------------------------------------------------------
# ket arm

class KetArm:
    def __init__(self, extractor_cid):
        self.extractor_cid = extractor_cid
        self.doc_node = {}         # rfc -> doc node cid
        self.memo = {}             # (doc cid, extractor cid) -> extraction node
        self.claims = collections.defaultdict(list)  # rfc -> [claim node cids]
        self.extractions_run = 0
        self.extractions_reused = 0
        self.bytes_processed = 0

    def add_document(self, doc):
        parents = [(self.doc_node[o], "supersedes")
                   for o in doc["obsoletes"] if o in self.doc_node]
        node, content = create("memory", "librarian", parents,
                               content_file=doc["path"])
        if content != doc["cid"]:
            raise SystemExit(f"RFC {doc['rfc']}: stored bytes {content} != pinned {doc['cid']}")
        self.doc_node[doc["rfc"]] = node

    def walk(self, agent, doc):
        key = (doc["cid"], self.extractor_cid)
        if key in self.memo:          # same bytes, same extractor: already derived
            self.extractions_reused += 1
            return
        self.extractions_run += 1
        text = read(doc["path"])
        self.bytes_processed += len(text.encode())
        ext, _ = create("context", agent, [(self.doc_node[doc["rfc"]], "derives")],
                        content=canon({"extraction": {"doc": doc["cid"],
                                                      "extractor": self.extractor_cid}}))
        self.memo[key] = ext
        for code, phrase, section in extract(text):
            claim = canon({"claim": "status-reason", "code": code,
                           "phrase": phrase, "rfc": doc["rfc"], "section": section})
            node, _ = create("reasoning", agent,
                             [(self.doc_node[doc["rfc"]], "grounds"), (ext, "derives")],
                             content=claim)
            self.claims[doc["rfc"]].append(node)


class Projection:
    """Read model rebuilt from the store alone: the log names every node,
    `dag show` returns it with its edge kinds, `get` returns its content.
    Nothing here is kept between runs; delete it and rebuild it."""

    def __init__(self):
        self.nodes = {}
        self.children = collections.defaultdict(list)  # parent -> [(child, edge)]
        for entry in ket_json("log", "-n", "1000000"):
            if entry["event"] != "dag:create":
                continue
            cid = entry["detail"].split()[0]
            n = ket_json("dag", "show", cid)
            kinds = n.get("parent_kinds") or ["derives"] * len(n["parents"])
            n["edges"] = list(zip(n["parents"], kinds))
            n["content"] = None
            if n["kind"] != "memory":           # documents are large; skip them
                n["content"] = ket("get", n["output_cid"]).stdout
            self.nodes[cid] = n
            for p, k in n["edges"]:
                self.children[p].append((cid, k))

    def json_content(self, cid):
        try:
            return json.loads(self.nodes[cid]["content"])
        except (TypeError, ValueError):
            return None

    def superseded(self, cid):
        return any(k == "supersedes" for _, k in self.children[cid])

    def refuted(self, cid):
        return any(k == "refutes" for _, k in self.children[cid])

    def claim_nodes(self):
        for cid, n in self.nodes.items():
            c = self.json_content(cid)
            if isinstance(c, dict) and c.get("claim") == "status-reason":
                yield cid, n, c

    def descendants(self, roots):
        seen, stack = set(), list(roots)
        while stack:
            for child, _ in self.children[stack.pop()]:
                if child not in seen:
                    seen.add(child)
                    stack.append(child)
        return seen

    # Read with edge kinds enforced.
    def current_claims(self, code):
        out = []
        for cid, n, c in self.claim_nodes():
            if c["code"] != code:
                continue
            grounds = [p for p, k in n["edges"] if k == "grounds"]
            if not grounds:                     # proposals and other ungrounded claims
                continue
            if any(self.superseded(g) for g in grounds):
                continue
            if self.refuted(cid):
                continue
            out.append((cid, c))
        return out


def ket_answer(proj, code):
    claims = proj.current_claims(code)
    phrases = sorted({c["phrase"] for _, c in claims})
    if not phrases:
        return None, [], "none"
    if len(phrases) > 1:
        return phrases, claims, "conflict"
    return phrases[0], claims, "answer"


def blind_answer(proj, code):
    """Status ignored: every claim for the code counts, the most-asserted
    phrase wins (ties broken alphabetically)."""
    votes = collections.Counter(c["phrase"] for _, _, c in proj.claim_nodes()
                                if c["code"] == code)
    if not votes:
        return None
    return sorted(votes.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


# --------------------------------------------------------------------------
# scoring

def classify(answer, code, key, stale_phrases):
    if answer is None:
        return "none"
    if answer == key[code]:
        return "correct"
    if answer in stale_phrases.get(code, ()):
        return "stale"
    return "wrong"


def main():
    corpus_dir, work = sys.argv[1], sys.argv[2]
    docs = load_corpus(corpus_dir)
    key = load_answers()
    os.environ["KET_HOME"] = os.path.join(work, ".ket")
    ket("init")

    # The shared folder the walkers read from: a working copy of the corpus.
    shared = os.path.join(work, "shared")
    os.makedirs(shared, exist_ok=True)
    for d in docs.values():
        dst = os.path.join(shared, os.path.basename(d["path"]))
        shutil.copyfile(d["path"], dst)
        d["path"] = dst

    # Phrases each code has had in a document that something else obsoletes.
    obsoleted = {o for d in docs.values() for o in d["obsoletes"]}
    stale_phrases = collections.defaultdict(set)
    for n in obsoleted & set(docs):
        for code, phrase, _ in extract(read(docs[n]["path"])):
            if code in key and phrase != key[code]:
                stale_phrases[code].add(phrase)

    extractor_cid = ket("put", os.path.join(HERE, "extract.py")).stdout.strip()
    naive, arm = NaiveIndex(), KetArm(extractor_cid)

    # ---- 1. build ----------------------------------------------------------
    early = sorted(n for n in docs if n not in LATE)
    say(f"1. Build: 4 walkers, {len(early)} RFCs (1945 .. 7538), each RFC read by two walkers")
    for n in early:
        arm.add_document(docs[n])
    for agent, slice_ in sorted(assignments(early).items()):
        for n in slice_:
            naive.add_document(n, docs[n]["path"])
            arm.walk(agent, docs[n])
    dim(f"   naive: {len(naive.chunks)} chunks indexed")
    dim(f"   ket:   {len(early)} documents pinned by CID, "
        f"{sum(len(v) for v in arm.claims.values())} claims; "
        f"extractions run {arm.extractions_run}, reused {arm.extractions_reused}")

    # ---- 2. rumour ---------------------------------------------------------
    say("2. Rumour: walker-3 writes a confident summary that is wrong today; walker-4 builds on it")
    rumour = {413: "Request Entity Too Large", 414: "Request-URI Too Long",
              416: "Requested Range Not Satisfiable"}
    note_text = ("Note (walker-3): in current HTTP, status code "
                 + "; ".join(f"{c} is {p}" for c, p in sorted(rumour.items())) + ".")
    synth_text = ("Summary (walker-4), client errors for current HTTP: "
                  + "; ".join(f"{c} {p}" for c, p in sorted(rumour.items()))
                  + ". Source: walker-3's note.")
    naive.add_note("note:walker-3", note_text, "walker-3")
    naive.add_note("note:walker-4", synth_text, "walker-4")
    rumour_nodes = []
    for code, phrase in sorted(rumour.items()):
        node, _ = create("reasoning", "walker-3", [(arm.doc_node[2616], "proposes")],
                         content=canon({"claim": "status-reason", "code": code,
                                        "phrase": phrase, "basis": "summary"}))
        rumour_nodes.append(node)
    synth_node, _ = create("reasoning", "walker-4", [(r, "derives") for r in rumour_nodes],
                           content=synth_text)
    dim(f"   naive: the note and the summary are now chunks like any other")
    dim(f"   ket:   3 claims with edge `proposes` (not grounded in a document), "
        f"1 summary that `derives` from them")

    # ---- 3. late arrival ---------------------------------------------------
    late = sorted(LATE)
    say("3. Late arrival: RFC 9110, 9111, 9112 (June 2022) are added after the store was built")
    for n in late:
        arm.add_document(docs[n])
        sup = [o for o in docs[n]["obsoletes"] if o in docs]
        dim(f"   ket:   RFC {n} supersedes {', '.join(f'RFC {o}' for o in sup)}")
    for agent, slice_ in sorted(assignments(late).items()):
        for n in slice_:
            naive.add_document(n, docs[n]["path"])
            arm.walk(agent, docs[n])
    dim(f"   naive: {len(naive.chunks)} chunks; old and new chunks rank side by side")

    # ---- check the rumour against current sources --------------------------
    proj = Projection()
    say("   A checker compares every `proposes` claim with the current, grounded claims")
    refuted = []
    for r in rumour_nodes:
        c = proj.json_content(r)
        current = proj.current_claims(c["code"])
        phrases = {cc["phrase"] for _, cc in current}
        if phrases and c["phrase"] not in phrases:
            ref, _ = create("reasoning", "checker",
                            [(r, "refutes")] + [(cid, "grounds") for cid, _ in current],
                            content=canon({"check": "status-reason", "code": c["code"],
                                           "claimed": c["phrase"],
                                           "current": sorted(phrases)}))
            refuted.append(r)
            dim(f"   refutes: {c['code']} \"{c['phrase']}\" (current: {', '.join(sorted(phrases))})")
    proj = Projection()
    flagged = proj.descendants(refuted)
    flagged.discard(None)
    flagged_non_checks = [f for f in flagged if proj.nodes[f]["agent"] != "checker"]
    dim(f"   downstream of the refuted claims: {len(flagged_non_checks)} node(s) flagged "
        f"(walker-4's summary) — computed by walking edges; ket has no built-in "
        f"propagation yet (issue #24)")

    # ---- 4. edit -----------------------------------------------------------
    say("4. Edit: someone annotates the shared working copy of rfc9110.txt")
    p9110 = docs[9110]["path"]
    original = read(p9110)
    lines = original.split("\n")
    lines.insert(3, "   [local annotation: reviewed for the 2026 API guidelines]")
    with open(p9110, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    drift = ket_json("dag", "drift", p9110, docs[9110]["cid"])
    drifted = bool(drift["drifted"])
    basis = arm.doc_node[9110]
    dependent = [c for c, _ in proj.children[basis] if proj.json_content(c)
                 and (proj.json_content(c) or {}).get("claim") == "status-reason"]
    total_claims = sum(1 for _ in proj.claim_nodes())
    dim(f"   ket:   drift on rfc9110.txt: {drifted}; claims grounded in it: "
        f"{len(dependent)} of {total_claims} flagged for re-check")
    old = {canon({"claim": "status-reason", "code": c, "phrase": p, "rfc": 9110, "section": s})
           for c, p, s in extract(original)}
    new = {canon({"claim": "status-reason", "code": c, "phrase": p, "rfc": 9110, "section": s})
           for c, p, s in extract(read(p9110))}
    dim(f"   ket:   re-extraction from the edited bytes: {len(old & new)} of {len(old)} "
        f"claims byte-identical (same CIDs), {len(new - old)} new, {len(old - new)} gone "
        f"-> cleared without review")
    stale_cites = 0
    nine = [c for c in naive.chunks.values() if c["path"] == p9110]
    edited = read(p9110)
    for c in nine:
        if edited[c["offset"]:c["offset"] + len(c["text"])] != c["text"]:
            stale_cites += 1
    dim(f"   naive: no signal; {stale_cites} of {len(nine)} RFC 9110 chunk citations "
        f"(file + offset) no longer point at the text they quote")

    # ---- questions ---------------------------------------------------------
    say(f"Questions: \"What is the reason phrase for HTTP status code N?\" x {len(key)}, "
        f"answer key from RFC 9110")
    results = {"naive": collections.Counter(), "ket_blind": collections.Counter(),
               "ket": collections.Counter()}
    from_rumour = 0
    rows = []
    for code in sorted(key):
        hits = naive.search(f"What is the reason phrase for HTTP status code {code}?",
                            must=str(code))
        n_ans, n_src = naive_read(naive, code, hits)
        n_cls = classify(n_ans, code, key, stale_phrases)
        results["naive"][n_cls] += 1
        if n_src and n_src.startswith("note:"):
            from_rumour += 1
        b_ans = blind_answer(proj, code)
        results["ket_blind"][classify(b_ans, code, key, stale_phrases)] += 1
        k_ans, k_claims, k_kind = ket_answer(proj, code)
        if k_kind == "conflict":
            k_cls = "conflict"
        else:
            k_cls = classify(k_ans, code, key, stale_phrases)
        results["ket"][k_cls] += 1
        rows.append({"code": code, "key": key[code],
                     "naive": [n_ans, n_cls, n_src], "ket_blind": b_ans,
                     "ket": [k_ans, k_cls,
                             [proj.nodes[c]["parents"][0] for c, _ in k_claims]]})

    def line(name, c):
        tot = sum(c.values())
        parts = [f"{k} {c[k]}" for k in ("correct", "stale", "wrong", "conflict", "none") if c[k]]
        return f"   {name:<11} {c['correct']:>2}/{tot} correct   ({', '.join(parts)})"

    print(line("naive", results["naive"]))
    print(line("ket, blind", results["ket_blind"]))
    print(line("ket", results["ket"]))
    dim(f"   naive answers taken from the rumour or the summary built on it: {from_rumour}")

    print(f"   {'code':<5}{'answer key':<30}{'naive':<58}{'ket, blind':<38}ket")
    for r in rows:
        if "correct" in (r["naive"][1], r["ket"][1]) and r["naive"][1] == r["ket"][1] \
                and r["ket_blind"] == r["key"]:
            continue
        n_ans, n_cls, n_src = r["naive"]
        k_ans, k_cls, _ = r["ket"]
        if isinstance(k_ans, list):
            k_ans = " | ".join(k_ans)
        b_cls = classify(r["ket_blind"], r["code"], key, stale_phrases)
        naive_col = f"{n_ans} ({n_cls}" + (", from the rumour)" if (n_src or "").startswith("note:") else ")")
        print(f"   {r['code']:<5}{r['key']:<30}{naive_col:<58}"
              f"{str(r['ket_blind']) + ' (' + b_cls + ')':<38}{k_ans} ({k_cls})")

    # ---- audit -------------------------------------------------------------
    say("Audit: where did the answer for 413 come from?")
    rfc_of = {node: n for n, node in arm.doc_node.items()}
    _, claims413, _ = ket_answer(proj, 413)
    if claims413:
        cid = claims413[0][0]
        node, c = proj.nodes[cid], proj.json_content(cid)
        doc = [p for p, k in node["edges"] if k == "grounds"][0]
        ext = [p for p, k in node["edges"] if k == "derives"][0]
        doc_cid = proj.nodes[doc]["output_cid"]
        ok = ket("verify", doc_cid).returncode == 0
        print(f"   ket:   \"{c['phrase']}\"")
        print(f"          <- claim {cid[:12]}, {node['agent']}, {node['timestamp'][:19]}Z, "
              f"RFC {c['rfc']} section {c['section']}")
        print(f"          <- derives: extraction {ext[:12]} by extractor {arm.extractor_cid[:12]}")
        print(f"          <- grounds: RFC {c['rfc']}, bytes {doc_cid[:12]} (re-hashed: "
              f"{'OK' if ok else 'FAILED'})")
        frontier, seen = [doc], set()
        while frontier:
            cur = frontier.pop(0)
            olds = [p for p, k in proj.nodes[cur]["edges"] if k == "supersedes"]
            if olds and cur not in seen:
                seen.add(cur)
                print(f"             RFC {rfc_of[cur]} supersedes "
                      + ", ".join(f"RFC {rfc_of[o]}" for o in olds))
                frontier += olds
    hits = naive.search("What is the reason phrase for HTTP status code 413?", must="413")
    _, src = naive_read(naive, 413, hits)
    print(f"   naive: chunk '{src}' (" + (naive.chunks[src]["source"] if src else "no answer")
          + "); the index records no agent, no time, no version and no status")

    # ---- cost --------------------------------------------------------------
    say("Work done")
    print(f"   naive  bytes read and chunked: {naive.bytes_processed:>9,}")
    print(f"   ket    bytes read and extracted: {arm.bytes_processed:>7,}   "
          f"(extractions run {arm.extractions_run}, reused {arm.extractions_reused}: "
          f"same document CID + same extractor CID)")

    report = {
        "corpus": {n: d["cid"] for n, d in sorted(docs.items())},
        "extractor_cid": extractor_cid,
        "scores": {k: dict(v) for k, v in results.items()},
        "naive_answers_from_rumour": from_rumour,
        "rumour_refuted": len(refuted),
        "downstream_flagged": len(flagged_non_checks),
        "edit": {"drift_detected": drifted, "claims_flagged": len(dependent),
                 "claims_total": total_claims,
                 "claims_unchanged_after_reextraction": len(old & new),
                 "naive_citations_broken": stale_cites, "naive_citations_9110": len(nine)},
        "work": {"naive_bytes": naive.bytes_processed, "ket_bytes": arm.bytes_processed,
                 "ket_extractions_run": arm.extractions_run,
                 "ket_extractions_reused": arm.extractions_reused},
        "rows": rows,
    }
    path = os.path.join(work, "results.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=1, sort_keys=True)
    cid = ket("put", path).stdout.strip()
    say(f"Results: results.json, stored in the run's ket store as {cid}")
    dim("   (the run directory is temporary; set KEEP=<dir> to keep the store and results)")


if __name__ == "__main__":
    main()
