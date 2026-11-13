"""Live tier for the `obsoleted` demo: LLM walkers and an LLM reader.

The mechanics tier's scripted extractor and regex reader are replaced by
Claude API calls:

  walkers  read a whole RFC and return the status codes it defines, as
           structured JSON (code, reason phrase, section)
  reader   the naive arm's generation step: it answers each question from the
           retrieved chunks only, and names the chunk it used

Everything an LLM call depends on is stored by CID in the run's ket store:
the extractor (model, effort, prompt, schema) is one blob, and every request
and response is a blob. Each extraction node names its request and response.
The current models do not take sampling parameters, so a live run is not
bit-reproducible. Instead, responses are cached on disk, keyed by the
request's CID. A second run with the same requests makes no API calls and
re-scores exactly, and OBSOLETED_LIVE_REPLAY=1 refuses any request that is
not already cached.

Environment:
  ANTHROPIC_API_KEY        required unless replaying (any credential the SDK resolves works)
  OBSOLETED_MODEL          default claude-opus-5-5
  OBSOLETED_EFFORT         default low (simple extraction and lookup)
  OBSOLETED_LIVE_YES=1     confirm spending; without it the run prints an estimate and stops
  OBSOLETED_LIVE_REPLAY=1  use cached responses only; never call the API
"""

import concurrent.futures
import json
import os

MODEL = os.environ.get("OBSOLETED_MODEL", "claude-opus-5-5")
EFFORT = os.environ.get("OBSOLETED_EFFORT", "low")
FALLBACK_BETA = "server-side-fallback-2026-07-01"
PRICES = {  # USD per million tokens (input, output), first-party API
    "claude-opus-5-5": (4.0, 20.0),
    "claude-sonnet-5-5": (2.0, 10.0),
    "claude-haiku-5-5": (0.10, 0.50),
}
WORKERS = 4

EXTRACT_SYSTEM = ("You read IETF RFC documents and report facts exactly as the "
                  "document states them.")
EXTRACT_PROMPT = (
    "List every HTTP status code that this document itself defines in its own "
    "status-code definitions, with the reason phrase exactly as this document "
    "writes it and the number of the section that defines it. Leave out codes "
    "the document only mentions or defers to another document. If it defines "
    "none, return an empty list.")
EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {"claims": {"type": "array", "items": {
        "type": "object",
        "properties": {"code": {"type": "integer"},
                       "phrase": {"type": "string"},
                       "section": {"type": "string"}},
        "required": ["code", "phrase", "section"],
        "additionalProperties": False}}},
    "required": ["claims"],
    "additionalProperties": False,
}

READ_SYSTEM = "You answer questions using only the passages you are given."
READ_PROMPT = (
    "Answer from the passages above only. Give the reason phrase for the "
    "status code exactly as a passage writes it, and the id of the passage "
    "you took it from. If no passage contains it, give empty strings.")
READ_SCHEMA = {
    "type": "object",
    "properties": {"phrase": {"type": "string"}, "passage_id": {"type": "string"}},
    "required": ["phrase", "passage_id"],
    "additionalProperties": False,
}


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def normalize(phrase):
    """'"Content Too Large".' -> 'Content Too Large'; '' -> None."""
    p = " ".join((phrase or "").split()).strip(" \"'`.")
    return p or None


class Live:
    def __init__(self, put, cache_dir):
        self.put = put                    # bytes/str -> CID, via the run's ket store
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.replay = os.environ.get("OBSOLETED_LIVE_REPLAY") == "1"
        self.client = None
        self.extractor_cid = put(canon({"role": "extractor", "model": MODEL, "effort": EFFORT,
                                        "system": EXTRACT_SYSTEM, "prompt": EXTRACT_PROMPT,
                                        "schema": EXTRACT_SCHEMA}))
        self.reader_cid = put(canon({"role": "reader", "model": MODEL, "effort": EFFORT,
                                     "system": READ_SYSTEM, "prompt": READ_PROMPT,
                                     "schema": READ_SCHEMA}))
        self.calls = self.cached = self.refusals = 0
        self.input_tokens = self.output_tokens = 0
        self.served_by = set()

    # ---- requests ----------------------------------------------------------

    @staticmethod
    def _request(system, user, schema, max_tokens):
        return {
            "model": MODEL,
            "max_tokens": max_tokens,
            "system": system,
            "messages": [{"role": "user", "content": user}],
            "output_config": {"effort": EFFORT,
                              "format": {"type": "json_schema", "schema": schema}},
            "betas": [FALLBACK_BETA],
            "fallbacks": "default",
        }

    def extract_request(self, rfc, text):
        user = f'<document rfc="{rfc}">\n{text}\n</document>\n\n{EXTRACT_PROMPT}'
        return self._request(EXTRACT_SYSTEM, user, EXTRACT_SCHEMA, 16000)

    def read_request(self, index, code, hits):
        passages = "\n\n".join(
            f'<passage id="{cid}" source="{index.chunks[cid]["source"]}">\n'
            f'{index.chunks[cid]["text"].strip()}\n</passage>' for cid in hits)
        user = (f"{passages}\n\nQuestion: What is the reason phrase for HTTP status "
                f"code {code}?\n\n{READ_PROMPT}")
        return self._request(READ_SYSTEM, user, READ_SCHEMA, 4000)

    # ---- cost gate ---------------------------------------------------------

    def cached_path(self, request):
        return os.path.join(self.cache_dir, self.put(canon(request)) + ".json")

    def gate(self, requests, n_reads):
        """Stop unless the spend is confirmed (or nothing new would be sent)."""
        missing = [r for r in requests if not os.path.exists(self.cached_path(r))]
        est_in = (sum(len(r["messages"][0]["content"]) for r in missing) / 3.5
                  + n_reads * 3500)
        est_out = len(missing) * 6000 + n_reads * 1500  # includes thinking at low effort
        price = PRICES.get(MODEL)
        cost = (f"about ${(est_in * price[0] + est_out * price[1]) / 1e6:.2f}"
                if price else "unknown (no price on file for this model)")
        print(f"   live tier: {MODEL}, effort {EFFORT}; {len(requests) - len(missing)} of "
              f"{len(requests)} extraction requests cached; {n_reads} reader requests "
              f"(cached ones are free)")
        print(f"   estimated new spend: ~{est_in / 1e3:,.0f}k input + ~{est_out / 1e3:,.0f}k "
              f"output tokens, {cost}")
        if self.replay:
            return
        if os.environ.get("OBSOLETED_LIVE_YES") != "1":
            raise SystemExit("obsoleted: set OBSOLETED_LIVE_YES=1 to make these API calls, "
                             "or OBSOLETED_LIVE_REPLAY=1 to use cached responses only")

    # ---- calls -------------------------------------------------------------

    def _client(self):
        if self.client is None:
            import anthropic
            self.client = anthropic.Anthropic()
        return self.client

    def call(self, request):
        """Return (parsed JSON or None, provenance). Responses are cached by request CID."""
        req_cid = self.put(canon(request))
        path = os.path.join(self.cache_dir, req_cid + ".json")
        if os.path.exists(path):
            with open(path) as f:
                resp = json.load(f)
            self.cached += 1
        else:
            if self.replay:
                raise SystemExit(f"obsoleted: replay mode, and request {req_cid[:12]} is not cached")
            import anthropic
            try:
                msg = self._client().beta.messages.create(**request)
            except anthropic.APIStatusError as e:
                raise SystemExit(f"obsoleted: API error {e.status_code}: {e.message}")
            except anthropic.APIConnectionError as e:
                raise SystemExit(f"obsoleted: could not reach the API: {e}")
            resp = msg.to_dict()
            resp["_request_id"] = msg._request_id
            with open(path + ".part", "w") as f:
                json.dump(resp, f, sort_keys=True)
            os.replace(path + ".part", path)
            self.calls += 1
            usage = resp.get("usage") or {}      # spend of this run: fresh calls only
            self.input_tokens += usage.get("input_tokens", 0) or 0
            self.output_tokens += usage.get("output_tokens", 0) or 0
        self.served_by.add(resp.get("model"))
        resp_cid = self.put(canon(resp))
        prov = {"request": req_cid, "response": resp_cid, "model": resp.get("model"),
                "request_id": resp.get("_request_id")}
        if resp.get("stop_reason") == "refusal":
            self.refusals += 1
            return None, prov
        text = next((b["text"] for b in resp.get("content", []) if b.get("type") == "text"), "")
        try:
            return json.loads(text), prov
        except ValueError:
            return None, prov

    def prefetch(self, requests):
        """Warm the cache concurrently; results are then read back in order."""
        todo = [r for r in requests if not os.path.exists(self.cached_path(r))]
        if not todo:
            return
        with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
            list(pool.map(self.call, todo))

    # ---- the two roles -----------------------------------------------------

    def extract(self, text, rfc):
        data, prov = self.call(self.extract_request(rfc, text))
        claims = []
        for c in (data or {}).get("claims", []):
            phrase = normalize(c.get("phrase"))
            if isinstance(c.get("code"), int) and phrase:
                claims.append((c["code"], phrase, str(c.get("section", "")).rstrip(".")))
        claims = sorted({c[0]: c for c in claims}.values())
        return claims, prov

    def read(self, index, code, hits):
        if not hits:
            return None, None
        data, _ = self.call(self.read_request(index, code, hits))
        if not data:
            return None, None
        pid = data.get("passage_id") or None
        return normalize(data.get("phrase")), (pid if pid in index.chunks else None)

    def summary(self):
        price = PRICES.get(MODEL)
        cost = (f", about ${(self.input_tokens * price[0] + self.output_tokens * price[1]) / 1e6:.2f}"
                f" at list price" if price else "")
        return (f"{self.calls} API calls, {self.cached} answered from cache, "
                f"{self.refusals} refusals; this run spent {self.input_tokens:,} input + "
                f"{self.output_tokens:,} output tokens{cost}; served by "
                f"{', '.join(sorted(m for m in self.served_by if m))}")
