#!/usr/bin/env python3
"""
update_data.py
──────────────────────────────────────────────────────────────────────────────
Downloads the latest Statistics Canada retail price data (Table 18-10-0245),
parses it, and writes a fresh index.html to the repo root.

Run locally:  python3 scripts/update_data.py
Run by CI:    automatically via GitHub Actions on the 5th of each month.
──────────────────────────────────────────────────────────────────────────────
"""

import urllib.request, zipfile, io, json, os, re
import xml.etree.ElementTree as ET

SDMX_URL  = "https://www150.statcan.gc.ca/n1/tbl/csv/18100245-eng.zip"
SDMX_URL2 = "https://www150.statcan.gc.ca/n1/tbl/sdmx/18100245-SDMX.zip"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT  = os.path.dirname(SCRIPT_DIR)
TEMPLATE   = os.path.join(SCRIPT_DIR, "template.html")
OUTPUT     = os.path.join(REPO_ROOT,  "index.html")

# ── Download & parse ───────────────────────────────────────────────────────────
def download_sdmx(retries=3, timeout=120):
    print(f"Downloading StatCan SDMX data…")
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(SDMX_URL2, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; IsThatADeal/1.0)'
            })
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = r.read()
            print(f"Downloaded {len(data)//1024}KB")
            return zipfile.ZipFile(io.BytesIO(data))
        except Exception as e:
            print(f"  Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                import time
                time.sleep(10)
    print("StatCan download failed after all retries — skipping update, keeping existing data.")
    return None

def parse_structure(zf):
    """Returns (prod_codes, geo_codes) dicts mapping id→name."""
    struct_name = next(n for n in zf.namelist() if 'Structure' in n)
    root = ET.parse(io.BytesIO(zf.read(struct_name))).getroot()
    ns  = 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure'
    com = 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
    prod_codes, geo_codes = {}, {}
    for cl in root.iter(f'{{{ns}}}Codelist'):
        cl_id = cl.attrib.get('id','')
        for code in cl.findall(f'{{{ns}}}Code'):
            cid  = code.attrib.get('id','')
            name_el = code.find(f'{{{com}}}Name')
            name = name_el.text if name_el is not None else ''
            if 'Product' in cl_id:                          prod_codes[cid] = name
            elif 'GEO' in cl_id or 'Geography' in cl_id:   geo_codes[cid]  = name
    return prod_codes, geo_codes

def parse_data(zf, prod_codes, geo_codes):
    data_files = sorted(n for n in zf.namelist()
                        if re.match(r'18100245_\d+\.xml', n))
    all_series = {}
    for fname in data_files:
        root = ET.parse(io.BytesIO(zf.read(fname))).getroot()
        for series in root.iter('Series'):
            geo = series.attrib.get('Geography','')
            pid = series.attrib.get('Products','')
            obs_list = []
            for obs in series.findall('Obs'):
                p = obs.attrib.get('TIME_PERIOD','')
                v = obs.attrib.get('OBS_VALUE','')
                if v and v not in ('','.'):
                    obs_list.append((p, float(v)))
            if obs_list:
                obs_list.sort()
                all_series.setdefault(geo, {})[pid] = obs_list

    latest = max(obs[-1][0] for gd in all_series.values() for obs in gd.values())
    print(f"Latest period in data: {latest}")

    output = {}
    for geo_id, prods in all_series.items():
        geo_name = geo_codes.get(geo_id, f'Geo{geo_id}')
        output[geo_name] = {}
        for prod_id, obs_list in prods.items():
            prod_name = prod_codes.get(prod_id, f'Prod{prod_id}')
            latest_val = next((v for p,v in reversed(obs_list) if p <= latest), None)
            history = obs_list[-36:]
            vals = [v for _,v in history]
            output[geo_name][prod_name] = {
                'latest': round(latest_val,2) if latest_val else None,
                'avg':    round(sum(vals)/len(vals),2),
                'lo':     round(min(vals),2),
                'hi':     round(max(vals),2),
            }
    return latest, output

# ── Build HTML ─────────────────────────────────────────────────────────────────
def build_html(period, data):
    with open(TEMPLATE) as f:
        template = f.read()

    blob = json.dumps({'period': period, 'data': data}, separators=(',',':'))
    html = template.replace('__STATCAN_DATA_BLOB__', blob)
    html = html.replace('__DATA_PERIOD__', period)

    with open(OUTPUT, 'w') as f:
        f.write(html)
    print(f"Written {OUTPUT} ({len(html)//1024}KB)")

# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    zf = download_sdmx()
    if zf is None:
        print("Using existing statcan_data.json unchanged.")
        import sys; sys.exit(0)
    prod_codes, geo_codes = parse_structure(zf)
    period, data = parse_data(zf, prod_codes, geo_codes)
    build_html(period, data)
    print("Done!")
