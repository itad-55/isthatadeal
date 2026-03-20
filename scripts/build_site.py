#!/usr/bin/env python3
"""
build_site.py
──────────────────────────────────────────────────────────────────────────────
Builds index.html and checker.html from their templates + data sources:
  1. StatCan data (data/statcan_data.json) — official, updated monthly
  2. Flipp averages (data/flipp_averages.json) — proprietary, grows weekly

Run after update_data.py and collect_flipp_prices.py.
──────────────────────────────────────────────────────────────────────────────
"""

import json, os

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT     = os.path.dirname(SCRIPT_DIR)
DATA_DIR      = os.path.join(REPO_ROOT, 'data')
STATCAN_FILE  = os.path.join(DATA_DIR, 'statcan_data.json')
FLIPP_FILE    = os.path.join(DATA_DIR, 'flipp_averages.json')
CHECKER_TMPL  = os.path.join(SCRIPT_DIR, 'checker_template.html')
INDEX_TMPL    = os.path.join(SCRIPT_DIR, 'template.html')
CHECKER_OUT   = os.path.join(REPO_ROOT, 'checker.html')
INDEX_OUT     = os.path.join(REPO_ROOT, 'index.html')

def load_json(path, default=None):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default

def main():
    statcan = load_json(STATCAN_FILE, {})
    flipp   = load_json(FLIPP_FILE, {'cuts': {}})

    period  = statcan.get('period', 'unknown')
    statcan_blob = json.dumps(statcan, separators=(',',':'))
    flipp_blob   = json.dumps(flipp.get('cuts', {}), separators=(',',':'))

    n_flipp = len(flipp.get('cuts', {}))
    print(f"StatCan period: {period}")
    print(f"Flipp cuts with averages: {n_flipp}")

    # Build index.html
    with open(INDEX_TMPL) as f:
        tmpl = f.read()
    html = tmpl.replace('__STATCAN_DATA_BLOB__', statcan_blob)
    html = html.replace('__FLIPP_DATA_BLOB__', flipp_blob)
    html = html.replace('__DATA_PERIOD__', period)
    with open(INDEX_OUT, 'w') as f:
        f.write(html)
    print(f"Built index.html ({len(html)//1024}KB)")

    # Build checker.html
    with open(CHECKER_TMPL) as f:
        tmpl = f.read()
    html = tmpl.replace('__STATCAN_DATA_BLOB__', statcan_blob)
    html = html.replace('__FLIPP_DATA_BLOB__', flipp_blob)
    html = html.replace('__DATA_PERIOD__', period)
    with open(CHECKER_OUT, 'w') as f:
        f.write(html)
    print(f"Built checker.html ({len(html)//1024}KB)")

if __name__ == '__main__':
    main()
