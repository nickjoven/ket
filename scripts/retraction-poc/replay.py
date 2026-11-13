"""Replay a pinned XMRV/CFS snapshot with unresolved external/cycle-cut support.

Offline by default. --refresh writes newly fetched public responses to a separate
capture directory; it never overwrites the pinned snapshot in the repository.

Run from the repository root after cargo build -p ket-cli:
    python3 scripts/retraction-poc/replay.py --ket ./target/debug/ket --output /tmp/ket24-papers

The output directory must be new or empty. map.json identifies source IDs,
support gaps, deterministic cycle cuts, notices and the pinned capture hash.
report-W*.json are unmodified CLI results. --sample-only is a separate
upper-bound sensitivity model and must not substitute for the default result.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.request

PARSER = argparse.ArgumentParser()
PINNED = Path(__file__).resolve().parents[2] / 'fixtures/retraction-poc/verified'
PARSER.add_argument('--capture', type=Path, default=PINNED, help='Input capture directory (offline by default)')
PARSER.add_argument('--output', '--out', dest='out', type=Path, required=True)
PARSER.add_argument('--ket', default='ket', help='Path to a ket binary that implements dag invalidated')
PARSER.add_argument('--refresh', type=Path, help='Fetch pinned public URLs into a NEW capture directory, then replay it')
PARSER.add_argument('--sample-only', action='store_true', help='Upper-bound sensitivity run, NOT conservative evidence')
ARGS = PARSER.parse_args()
ROOT, OUT = ARGS.capture, ARGS.out
EXPECTED_SOURCES = {
    'openalex-cohort.json', 'openalex-W2160675481.json', 'openalex-W2026087575.json',
    'crossref-W2160675481-original.json', 'crossref-W2160675481-updates.json',
    'crossref-W2026087575-original.json', 'crossref-W2026087575-updates.json',
}

def load_manifest(root):
    manifest = json.loads((root / 'manifest.json').read_text())
    names = [source['file'] for source in manifest['sources']]
    if manifest.get('schema_version') != 1 or len(names) != len(set(names)) or not EXPECTED_SOURCES.issubset(names):
        raise ValueError('capture manifest must uniquely list every consumed source with schema_version 1')
    for name in names:
        if Path(name).is_absolute() or not (root / name).resolve().is_relative_to(root.resolve()):
            raise ValueError('capture source path must remain inside capture directory')
    return manifest

def overlaps(left, right):
    left, right = left.resolve(), right.resolve()
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)

if OUT.exists() and (not OUT.is_dir() or any(OUT.iterdir())):
    PARSER.error('--output must be nonexistent or an empty directory; existing CAS state would change the analysis')
if overlaps(OUT, ROOT):
    PARSER.error('--output and --capture directories must not overlap')
if ARGS.refresh:
    if ARGS.refresh.exists():
        PARSER.error('--refresh destination must not already exist')
    if overlaps(ARGS.refresh, ROOT) or overlaps(ARGS.refresh, OUT) or overlaps(ARGS.refresh, PINNED):
        PARSER.error('--refresh must be separate from input capture, pinned snapshot, and output directories')
    new_manifest = load_manifest(ROOT)
    ARGS.refresh.mkdir(parents=True)
    for source in new_manifest['sources']:
        request = urllib.request.Request(source['url'], headers={'User-Agent': 'ket24-public-metadata-poc/1.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(request, timeout=40) as response:
            raw = response.read()
            source['status'] = response.status
        destination = ARGS.refresh / source['file']
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(raw)
        source['bytes'] = len(raw)
        source['sha256'] = hashlib.sha256(raw).hexdigest()
    new_manifest['captured_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (ARGS.refresh / 'manifest.json').write_text(json.dumps(new_manifest, indent=2) + '\n')
    ROOT = ARGS.refresh
def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()

def put(value):
    raw = canonical(value)
    result = subprocess.run([ARGS.ket, '--home', str(HOME), '--json', 'put', '-'], input=raw, capture_output=True, check=True)
    return json.loads(result.stdout)['cid']

def node(content, links, meta):
    output_cid = put(content)
    document = {'kind': 'reasoning', 'parents': [cid for cid, _ in links],
                'output_cid': output_cid, 'agent': 'ket24-retraction-poc',
                'timestamp': '2026-11-04T00:00:00Z', 'meta': sorted(meta)}
    if links and any(kind != 'derives' for _, kind in links):
        document['parent_kinds'] = [kind for _, kind in links]
    return put(document)

manifest = load_manifest(ROOT)
for source in manifest['sources']:
    raw = (ROOT / source['file']).read_bytes()
    if len(raw) != source['bytes'] or hashlib.sha256(raw).hexdigest() != source['sha256']:
        raise ValueError('capture hash mismatch: ' + source['file'])

# Validate the immutable input before creating an output store.
OUT.mkdir(parents=True, exist_ok=True)
HOME = OUT / '.ket'
(HOME / 'cas').mkdir(parents=True, exist_ok=True)

works = json.loads((ROOT / 'openalex-cohort.json').read_text())['results']
for seed in ['W2160675481', 'W2026087575']:
    works.append(json.loads((ROOT / ('openalex-' + seed + '.json')).read_text()))
papers = {work['id']: work for work in works}
paper_cids, external_cids, active, cuts = {}, {}, set(), []
counts = {'in_sample_edges': 0, 'external_edges': 0}

def unresolved(source_id, reason):
    key = reason + ':' + source_id
    if key not in external_cids:
        # A raw evidence object, deliberately NOT a DagNode. The support edge
        # remains addressable but the engine must classify it as unknown.
        external_cids[key] = put({'schema_version': 1, 'type': 'unresolved_support', 'source_id': source_id, 'reason': reason})
    return external_cids[key]

def build(source_id):
    if source_id in paper_cids:
        return paper_cids[source_id]
    active.add(source_id)
    work = papers[source_id]
    links = []
    for parent_id in sorted(set(work.get('referenced_works', []))):
        if parent_id not in papers:
            counts['external_edges'] += 1
            if ARGS.sample_only:
                continue
            parent_cid = unresolved(parent_id, 'out_of_sample')
        elif parent_id in active:
            cuts.append({'child': source_id, 'parent': parent_id, 'reason': 'citation_cycle'})
            parent_cid = unresolved(parent_id, 'citation_cycle')
        else:
            parent_cid = build(parent_id)
            counts['in_sample_edges'] += 1
        links.append((parent_cid, 'grounds'))
    paper_cids[source_id] = node({'schema_version': 1, 'type': 'paper', 'openalex': work}, links,
                                 [('source_id', source_id), ('model', 'sample_only_upper_bound' if ARGS.sample_only else 'conservative')])
    active.remove(source_id)
    return paper_cids[source_id]

for source_id in sorted(papers):
    build(source_id)

notices = []
evidence = []
for seed in ['W2160675481', 'W2026087575']:
    seed_id = 'https://openalex.org/' + seed
    original_doi = papers[seed_id]['doi'].removeprefix('https://doi.org/').lower()
    updates = json.loads((ROOT / ('crossref-' + seed + '-updates.json')).read_text())['message']['items']
    for item in updates:
        assertions = [assertion for assertion in item.get('update-to', []) if assertion.get('DOI', '').lower() == original_doi]
        evidence.append({'seed': seed_id, 'notice_doi': item.get('DOI'), 'assertions': assertions})
        if not any(a.get('type') == 'retraction' and a.get('source') == 'retraction-watch' for a in assertions):
            continue
        notice_cid = node({'schema_version': 1, 'type': 'notice', 'crossref': item}, [(paper_cids[seed_id], 'refutes')],
                          [('source', 'crossref'), ('notice_doi', item['DOI'])])
        notices.append({'seed': seed_id, 'cid': notice_cid, 'notice_doi': item['DOI'], 'assertions': assertions})

result = {'schema_version': 1, 'model': 'sample_only_upper_bound' if ARGS.sample_only else 'conservative',
          'timestamp': '2026-11-04T00:00:00Z', 'paper_count': len(papers),
          'paper_cids': paper_cids, 'unresolved_cids': external_cids, 'counts': counts,
          'cycle_cuts': cuts, 'notices': notices, 'evidence': evidence,
          'capture_manifest_sha256': hashlib.sha256((ROOT / 'manifest.json').read_bytes()).hexdigest(),
          'limitations': ['Cohort selected on citing BOTH seeds; not a random or complete subfield.',
                          'Citation approximates support; no citation-intent classification.',
                          'Each invocation refutes the explicit seed; co-parent seed outside closure is not automatically refuted.',
                          'External references remain unresolved; sample-only run is sensitivity upper bound.',
                          'Crossref retraction evidence checked for the two seeds only; other cohort works only checked via captured OpenAlex is_retracted flags. No complete subfield retraction audit is claimed.']}
(OUT / 'map.json').write_bytes(canonical(result) + b'\n')
(OUT / 'source-map.json').write_bytes(canonical({'schema_version': 1, 'paper_cids': paper_cids, 'unresolved_cids': external_cids}) + b'\n')
(OUT / 'graph-provenance.json').write_bytes(canonical({'schema_version': 1, 'counts': counts, 'cycle_cuts': cuts,
    'original_edges': [{'child': source_id, 'parents': sorted(set(work.get('referenced_works', [])))} for source_id, work in sorted(papers.items())],
    'capture_manifest_sha256': result['capture_manifest_sha256'], 'limitations': result['limitations']}) + b'\n')
for seed in ['W2160675481', 'W2026087575']:
    cid = paper_cids['https://openalex.org/' + seed]
    report = subprocess.run([ARGS.ket, '--home', str(HOME), '--json', 'dag', 'invalidated', cid, '--with-recorded-refutations', '--include-intact'], capture_output=True, check=True)
    (OUT / ('report-' + seed + '.json')).write_bytes(report.stdout)
    decoded = json.loads(report.stdout)
    statuses = {}
    for item in decoded['nodes']:
        statuses[item['status']] = statuses.get(item['status'], 0) + 1
    print(seed, cid, statuses, 'complete', decoded['complete'], flush=True)
print('build', len(papers), counts, 'unresolved_unique', len(external_cids), 'cycle_cuts', len(cuts), 'notices', len(notices), flush=True)
