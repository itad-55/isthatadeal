#!/usr/bin/env python3
"""
send_digest.py
─────────────────────────────────────────────────────────────────────────────
Reads flipp_history.csv + statcan_data.json, scores every current flyer price
against the Ontario average, picks the top deals, builds an HTML email, and
creates a draft campaign in MailerLite ready for you to review and send.

Run manually or via GitHub Actions every Monday morning.
Requires: MAILERLITE_API_KEY environment variable
─────────────────────────────────────────────────────────────────────────────
"""

import csv, json, os, urllib.request, urllib.error
from datetime import date, timedelta
from collections import defaultdict

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT    = os.path.dirname(SCRIPT_DIR)
DATA_DIR     = os.path.join(REPO_ROOT, 'data')
HISTORY_CSV      = os.path.join(DATA_DIR, 'flipp_history.csv')
STATCAN_FILE     = os.path.join(DATA_DIR, 'statcan_data.json')
FLIPP_FILE       = os.path.join(DATA_DIR, 'flipp_averages.json')
BASELINES_FILE   = os.path.join(DATA_DIR, 'retail_baselines.json')
EMAIL_TEMPLATE   = os.path.join(SCRIPT_DIR, 'email_template.html')

MAILERLITE_API_KEY = os.environ.get('MAILERLITE_API_KEY', '')
ONTARIO_GROUP_ID   = '182297132531713316'
FROM_EMAIL         = 'deals@isthatadeal.ca'
FROM_NAME          = 'Is That a Deal?'

import zoneinfo
TODAY     = date.today()
try:
    _eastern = zoneinfo.ZoneInfo('America/Toronto')
    from datetime import datetime as _dt
    TODAY = _dt.now(_eastern).date()
except Exception:
    pass  # fall back to UTC if zoneinfo unavailable

STORE_FLYER_URLS = {
    'food basics':                  'https://www.foodbasics.ca/flyer',
    'freshco':                      'https://www.freshco.com/flyer',
    'chalo freshco':                'https://www.freshco.com/flyer',
    'metro':                        'https://www.metro.ca/en/online-grocery/flyer',
    'no frills':                    'https://www.nofrills.ca/en/print-flyer',
    'walmart':                      'https://www.walmart.ca/en/flyer',
    'loblaws':                      'https://www.loblaws.ca/en/print-flyer',
    'sobeys':                       'https://www.sobeys.com/flyer',
    'real canadian superstore':     'https://www.realcanadiansuperstore.ca/en/print-flyer',
    'superstore':                   'https://www.realcanadiansuperstore.ca/en/print-flyer',
    'fortinos':                     'https://www.fortinos.ca/en/print-flyer',
    'zehrs':                        'https://www.loblaws.ca/en/print-flyer',
    'your independent grocer':      'https://www.loblaws.ca/en/print-flyer',
    'independent grocer':           'https://www.loblaws.ca/en/print-flyer',
    'valumart':                     'https://www.loblaws.ca/en/print-flyer',
    'valu-mart':                    'https://www.loblaws.ca/en/print-flyer',
    'giant tiger':                  'https://www.gianttiger.com/en/flyer',
}

def store_link(store_name, expiry_text):
    """Return store line HTML with flyer link if available."""
    url = STORE_FLYER_URLS.get(store_name.lower().strip())
    if url:
        store_html = f'<a href="{url}" style="color:inherit;text-decoration:underline;text-underline-offset:2px">{store_name} ↗</a>'
    else:
        store_html = store_name
    return store_html + (' · ' + expiry_text if expiry_text else '')
WEEK_AGO  = TODAY - timedelta(days=7)
SITE_URL      = 'https://isthatadeal.ca'

# ── Canonical Flipp URL builder ───────────────────────────────────────────────
import re as _re, datetime as _dt

_POSTAL_TO_CITY = {
    'M': 'toronto-on',
    'L': 'toronto-on',   # GTA/Brampton/Mississauga — use toronto-on as canonical
    'K': 'ottawa-on',
    'N': 'waterloo-on',
}

def _fmt_flipp_date(iso_str):
    """Convert ISO datetime string to Flipp date slug, e.g. 'thursday-mar-19'."""
    if not iso_str:
        return ''
    try:
        dt = _dt.datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        et = dt.astimezone(_dt.timezone(_dt.timedelta(hours=-4)))
        day = et.strftime('%A').lower()
        month = et.strftime('%b').lower()
        num = str(et.day)
        return f'{day}-{month}-{num}'
    except Exception:
        return ''

def make_flipp_url(item_id, store, postal_code, valid_from, valid_to, flyer_id=''):
    """Build canonical Flipp item URL in the city/item/id-store-flyer-dates format."""
    if not item_id:
        return ''
    city = _POSTAL_TO_CITY.get((postal_code or '')[:1], 'toronto-on')
    from_slug = _fmt_flipp_date(valid_from)
    to_slug   = _fmt_flipp_date(valid_to)
    if from_slug and to_slug:
        store_slug = _re.sub(r'[^a-z0-9]+', '-', (store or '').lower()).strip('-')
        slug = f'{item_id}-{store_slug}-weekly-flyer-valid-{from_slug}-{to_slug}'
        return f'https://flipp.com/en-ca/{city}/item/{slug}'
    # Fallback to legacy format if dates unavailable
    if flyer_id:
        return f'https://flipp.com/en-ca/item/{item_id}?flyer_id={flyer_id}'
    return f'https://flipp.com/en-ca/item/{item_id}'
VERIFY_FILE   = 'digest_verify_2833151ff2ae0f3d.html'  # secret review URL — do not share

# ── Load StatCan averages ─────────────────────────────────────────────────────
def load_statcan():
    with open(STATCAN_FILE) as f:
        raw = json.load(f)
    ont = raw.get('data', {}).get('Ontario', {})
    # key → {avg, lo, hi}
    return {k: v for k, v in ont.items() if v.get('avg')}

# ── Load Flipp averages ───────────────────────────────────────────────────────
def load_flipp():
    if not os.path.exists(FLIPP_FILE):
        return {}
    with open(FLIPP_FILE) as f:
        raw = json.load(f)
    return raw.get('cuts', {})

# ── Load manually-verified retail baselines ───────────────────────────────────
def load_retail_baselines():
    if not os.path.exists(BASELINES_FILE):
        return {}
    with open(BASELINES_FILE) as f:
        raw = json.load(f)
    return raw.get('cuts', {})

# ── Score recent flyer prices ─────────────────────────────────────────────────

# Realistic retail price ranges ($/kg) for Ontario grocery items
# Used to catch obviously wrong data points
REALISTIC_RANGES = {
    'beef':     (5.0,  80.0),
    'pork':     (4.0,  50.0),
    'chicken':  (3.5,  25.0),  # floor lowered: drumsticks at $1.99/lb = $4.39/kg is a real deal
    'turkey':   (5.0,  25.0),
    'lamb':     (10.0, 60.0),  # floor lowered: bone-in leg at $5.99/lb = $13.21/kg is real
    'veal':     (6.0,  60.0),
    'salmon':   (7.0,  45.0),
    'shrimp':   (5.0,  40.0),
    'default':  (0.50, 100.0),
}

def realistic_range(cut_key):
    k = cut_key.lower()
    for prefix, rng in REALISTIC_RANGES.items():
        if prefix in k:
            return rng
    return REALISTIC_RANGES['default']

def score_deals(statcan, flipp, baselines=None, limit=10):
    """
    Read flipp_history.csv, find rows from the last 7 days,
    compare each price_per_kg to the StatCan or Flipp average,
    return sorted list of deals (limit controls how many to return).
    """
    # Build unified averages lookup: cut_key or statcan_key → avg
    # Override raw_unit for cut_keys that were historically mis-recorded
    # (CSV may still have old 'kg' values before the fix was deployed)
    PKG_OVERRIDES = {'shrimp', 'turkey_breast', 'pork_ham', 'canned_tuna_170g', 'canned_salmon_213g'}

    # Map Flipp cut_keys to StatCan keys where names differ
    STATCAN_ALIASES = {
        # Package / unit items that don't follow the "per kilogram" pattern
        'canned_tuna_170g':   'Canned tuna, 170 grams',
        'canned_salmon_213g': 'Canned salmon, 213 grams',
        'shrimp':             'Shrimp, 300 grams',
        # Produce — per kg in StatCan ↔ snake_case Flipp key
        'sweet_potato':       'Sweet potatoes, per kilogram',
        'tomatoes':           'Tomatoes, per kilogram',
        'grapes':             'Grapes, per kilogram',
        # Meat & fish — per kg in StatCan
        'chicken_whole':      'Whole chicken, per kilogram',
        'chicken_breast':     'Chicken breasts, per kilogram',
        'chicken_thigh_bonein': 'Chicken thigh, per kilogram',
        'chicken_drumsticks': 'Chicken drumsticks, per kilogram',
        'salmon_fillet':      'Salmon, per kilogram',
        # Ground beef — StatCan has one combined entry; applies to regular/medium/lean
        'beef_ground_regular': 'Ground beef, per kilogram',
        'beef_ground_medium':  'Ground beef, per kilogram',
        'beef_ground_lean':    'Ground beef, per kilogram',
        'beef_sirloin':        'Beef top sirloin cuts, per kilogram',
    }

    averages = {}
    # Priority 3 (lowest): Flipp historical sale-price averages
    for key, v in flipp.items():
        if v.get('avg') and v.get('observations', 0) >= 4:
            averages[key] = {'avg': v['avg'], 'name': v['name'], 'source': 'flipp'}
    # Priority 2: StatCan official Ontario averages (overwrites Flipp)
    for key, v in statcan.items():
        averages[key] = {'avg': v['avg'], 'name': key, 'source': 'statcan'}
    # Add aliases so Flipp cut_keys can match StatCan entries
    for flipp_key, statcan_key in STATCAN_ALIASES.items():
        if statcan_key in averages:
            averages[flipp_key] = averages[statcan_key].copy()
    # Priority 1 (highest): manually-verified retail shelf prices
    if baselines:
        for key, v in baselines.items():
            if v.get('median_kg'):
                name = averages[key]['name'] if key in averages else v.get('name', key)
                averages[key] = {'avg': v['median_kg'], 'name': name, 'source': 'retail_baseline'}

    # StatCan display names
    DISPLAY = {
        "Beef stewing cuts, per kilogram": "Beef stewing cuts",
        "Beef striploin cuts, per kilogram": "Beef striploin steak",
        "Beef top sirloin cuts, per kilogram": "Beef top sirloin steak",
        "Beef rib cuts, per kilogram": "Beef rib cuts",
        "Ground beef, per kilogram": "Ground beef",
        "Pork loin cuts, per kilogram": "Pork loin cuts",
        "Pork rib cuts, per kilogram": "Pork rib cuts",
        "Pork shoulder cuts, per kilogram": "Pork shoulder cuts",
        "Whole chicken, per kilogram": "Whole chicken",
        "Chicken breasts, per kilogram": "Chicken breasts",
        "Chicken thigh, per kilogram": "Chicken thighs",
        "Chicken drumsticks, per kilogram": "Chicken drumsticks",
        "Salmon, per kilogram": "Salmon",
        "Bacon, 500 grams": "Bacon (500g)",
        "Butter, 454 grams": "Butter (454g)",
        "Block cheese, 500 grams": "Block cheese (500g)",
        "Eggs, 1 dozen": "Eggs (dozen)",
        "Canned tuna, 170 grams": "Canned tuna (170g)",
        "Canned salmon, 213 grams": "Canned salmon (213g)",
        "Milk, 4 litres": "Milk (4L)",
    }

    deals = []
    rejected = []  # items collected but not included — written to review page
    cutoff = WEEK_AGO.isoformat()

    if not os.path.exists(HISTORY_CSV):
        print("No flipp_history.csv found")
        return []

    with open(HISTORY_CSV, newline='') as f:
        for row in csv.DictReader(f):
            if row['date'] < cutoff:
                continue
            key = row['cut_key']
            if key not in averages:
                continue
            try:
                # For pkg items that were mis-recorded as kg, use raw_price
                if key in PKG_OVERRIDES:
                    price = float(row['raw_price'])
                else:
                    price = float(row['price_per_kg'])
            except ValueError:
                continue

            avg    = averages[key]['avg']
            pct    = ((price - avg) / avg) * 100
            name   = DISPLAY.get(averages[key]['name'], row['cut_name'])
            store  = row['store']
            source = averages[key]['source']

            # Sanity check 0a — skip stores not broadly available across Ontario
            # Use exact startswith/equality checks to avoid 'chalo freshco' matching 'freshco'
            ONTARIO_STORES = [
                'no frills', 'food basics', 'walmart', 'loblaws', 'metro',
                'sobeys', 'freshco', 'real canadian superstore', 'fortinos',
                'zehrs', 'your independent grocer', 'giant tiger', 'superstore',
                'independent grocer', 'valumart', 'valu-mart',
            ]
            EXCLUDED_STORES = ['chalo freshco', 't&t', 'iga', 'provigo', 'maxi', 'marche']
            store_lower = store.lower().strip()
            is_excluded = any(store_lower.startswith(ex) for ex in EXCLUDED_STORES)
            is_allowed  = any(store_lower == ok or store_lower.startswith(ok) for ok in ONTARIO_STORES)
            if is_excluded or not is_allowed:
                print(f"  Skipping {row['cut_name']} @ {store}: store not broadly available in Ontario")
                continue

            # Sanity check 0b — skip deli/processed/pre-cooked/frozen branded products
            # These are not basic groceries and contaminate price averages
            item_name_lower = row.get('item_name', '').lower()
            DELI_KEYWORDS = ['cooked', 'sliced', 'deli', 'artisan', 'cured', 'smoked',
                             'lunch meat', 'lunchmeat', 'bologna', 'salami', 'prosciutto',
                             'pepperoni', 'pastrami', 'corned beef', 'roast beef deli',
                             'schneiders', 'maple leaf deli', 'butterball deli',
                             'flamingo', 'poitrine de dinde', 'great value tuna',
                             'seaquest',
                             'canned', 'chunk light', 'flaked',
                             # Pre-cooked / restaurant-branded / frozen processed
                             'irresistible', 'swiss chalet', "montana's", 'plaisirs gastronomiques',
                             "pinty's", 'repas', 'meal',
                             'rotisserie', 'breaded', 'marinated', 'seasoned', 'stuffed',
                             'pre-cooked', 'fully cooked', 'ready to cook', 'ready-to-cook',
                             'frozen', 'heat and serve', 'microwave']
            if any(kw in item_name_lower for kw in DELI_KEYWORDS):
                print(f"  Skipping {row['cut_name']} @ {row['store']}: deli/processed product")
                rejected.append({'reason_key': 'processed', 'cut_name': row['cut_name'],
                                 'item_name': row.get('item_name', ''), 'store': store,
                                 'price': price, 'avg': avg, 'pct': round(pct, 1), 'source': source,
                                 'note': 'deli / processed / branded'})
                continue

            # Skip frozen salmon/fish — StatCan tracks fresh fillets, not frozen bags
            SALMON_KEYS = {'salmon_fillet', 'salmon_whole', 'tilapia', 'cod', 'tuna_steak'}
            if key in SALMON_KEYS and 'frozen' in item_name_lower:
                print(f"  Skipping {row['cut_name']} @ {row['store']}: frozen seafood excluded")
                rejected.append({'reason_key': 'processed', 'cut_name': row['cut_name'],
                                 'item_name': row.get('item_name', ''), 'store': store,
                                 'price': price, 'avg': avg, 'pct': round(pct, 1), 'source': source,
                                 'note': 'frozen seafood excluded'})
                continue

            # Sanity check 1 — realistic price range for this type of product
            lo, hi = realistic_range(key)
            if not (lo <= price <= hi):
                print(f"  Skipping {row['cut_name']} @ {row['store']}: ${price:.2f}/kg outside realistic range ${lo}-${hi}/kg")
                rejected.append({'reason_key': 'range', 'cut_name': row['cut_name'],
                                 'item_name': row.get('item_name', ''), 'store': store,
                                 'price': price, 'avg': avg, 'pct': round(pct, 1), 'source': source,
                                 'note': f'${price:.2f}/kg outside range ${lo}\u2013${hi}/kg'})
                continue

            # Sanity check 2 — if more than 65% below average, flag it
            # (could be a unit mismatch — e.g. price per 100g read as per kg)
            if pct < -82:
                print(f"  Skipping {row['cut_name']} @ {row['store']}: ${price:.2f}/kg is {pct:.1f}% below avg — likely bad data")
                rejected.append({'reason_key': 'bad_data', 'cut_name': row['cut_name'],
                                 'item_name': row.get('item_name', ''), 'store': store,
                                 'price': price, 'avg': avg, 'pct': round(pct, 1), 'source': source,
                                 'note': f'{pct:.0f}% below avg — possible unit mismatch'})
                continue

            # Only include genuine deals (15%+ below average)
            if pct < -15:
                deals.append({
                    'key':       key,
                    'name':      name,
                    'store':     store,
                    'item_name': row.get('item_name', '').strip().title(),
                    'price':     price,
                    'avg':       avg,
                    'pct':       pct,
                    'source':    source,
                    'date':      row['date'],
                    'valid_to':  row.get('valid_to', ''),
                    'raw_unit':  'pkg' if key in PKG_OVERRIDES else row.get('raw_unit', ''),
                    'flipp_url': make_flipp_url(
                                      row.get('item_id', ''),
                                      row.get('store', ''),
                                      row.get('postal_code', ''),
                                      row.get('valid_from', ''),
                                      row.get('valid_to', ''),
                                      row.get('flyer_id', ''),
                                  ),
                    'retailer_url': row.get('retailer_url', ''),
                })
            elif pct < 0:
                # Close miss — passed all filters but not quite 15% below avg
                rejected.append({'reason_key': 'close_miss', 'cut_name': row['cut_name'],
                                 'item_name': row.get('item_name', ''), 'store': store,
                                 'price': price, 'avg': avg, 'pct': round(pct, 1), 'source': source,
                                 'note': f'only {pct:.1f}% below avg (need −15%)'})

    # Dedupe: keep best price per cut
    best = {}
    for d in deals:
        if d['key'] not in best or d['pct'] < best[d['key']]['pct']:
            best[d['key']] = d

    # Sort by % below average, take top 10
    # Hard filter — drop items whose flyer has already expired
    import datetime, zoneinfo
    try:
        _eastern = zoneinfo.ZoneInfo('America/Toronto')
        today_str = datetime.datetime.now(_eastern).date().isoformat()
    except Exception:
        today_str = datetime.date.today().isoformat()

    print(f"  [filter] today_str={today_str}")
    for d in best.values():
        vt = (d.get('valid_to') or '')[:10]
        print(f"  [filter] {d['name']} @ {d['store']}: valid_to={repr(vt)}")

    active = [d for d in best.values()
              if not (d.get('valid_to') or '')[:10]  # no date = keep (unknown expiry)
              or (d.get('valid_to') or '')[:10] > today_str]  # strictly future = keep

    print(f"  [filter] {len(best)} candidates → {len(active)} after expiry filter")
    return sorted(active, key=lambda x: x['pct'])[:limit], rejected

# ── Emoji for product categories ──────────────────────────────────────────────
def format_valid_to(valid_to):
    if not valid_to:
        return ''
    try:
        # Format: 2026-03-19T03:59:59+00:00 → "Ends Mar 19"
        d = valid_to[:10]  # grab just the date part
        from datetime import datetime
        dt = datetime.strptime(d, '%Y-%m-%d')
        return 'Ends ' + dt.strftime('%b %-d')
    except Exception:
        return ''

def emoji_for(name):
    n = name.lower()
    if any(w in n for w in ['beef','steak','brisket','striploin','sirloin','flank','rib','stewing','ground beef']): return '🥩'
    if any(w in n for w in ['chicken','turkey','poultry','wing','drumstick','thigh']): return '🍗'
    if any(w in n for w in ['pork','bacon','ham','ribs','belly']): return '🥓'
    if any(w in n for w in ['salmon','fish','shrimp','tuna','seafood','cod','tilapia']): return '🐟'
    if any(w in n for w in ['milk','butter','cheese','egg','cream','yogurt','cottage']): return '🥛'
    if any(w in n for w in ['sweet potato','yam']): return '🍠'
    if any(w in n for w in ['potato',]): return '🥔'
    if any(w in n for w in ['apple',]): return '🍎'
    if any(w in n for w in ['banana',]): return '🍌'
    if any(w in n for w in ['orange',]): return '🍊'
    if any(w in n for w in ['grape',]): return '🍇'
    if any(w in n for w in ['strawberr',]): return '🍓'
    if any(w in n for w in ['blueberr',]): return '🫐'
    if any(w in n for w in ['watermelon',]): return '🍉'
    if any(w in n for w in ['avocado',]): return '🥑'
    if any(w in n for w in ['mango',]): return '🥭'
    if any(w in n for w in ['pineapple',]): return '🍍'
    if any(w in n for w in ['lemon',]): return '🍋'
    if any(w in n for w in ['lime',]): return '🍋'
    if any(w in n for w in ['tomato',]): return '🍅'
    if any(w in n for w in ['pepper',]): return '🫑'
    if any(w in n for w in ['corn',]): return '🌽'
    if any(w in n for w in ['carrot',]): return '🥕'
    if any(w in n for w in ['broccoli',]): return '🥦'
    if any(w in n for w in ['cauliflower',]): return '🥦'
    if any(w in n for w in ['mushroom',]): return '🍄'
    if any(w in n for w in ['onion',]): return '🧅'
    if any(w in n for w in ['lettuce','spinach','salad','romaine']): return '🥬'
    if any(w in n for w in ['asparagus',]): return '🌿'
    if any(w in n for w in ['zucchini','cucumber','celery']): return '🥒'
    if any(w in n for w in ['bread','bagel','muffin','tortilla']): return '🍞'
    if any(w in n for w in ['pasta','rice','noodle']): return '🍝'
    if any(w in n for w in ['oil',]): return '🫙'
    if any(w in n for w in ['bean','lentil']): return '🫘'
    if any(w in n for w in ['peanut butter',]): return '🥜'
    return '🛒'

# ── Verdict label ─────────────────────────────────────────────────────────────
def verdict(pct):
    if pct < -20: return ('Great deal', '#0A7A3E', '✓✓')
    if pct < -15:  return ('Good deal',  '#0A6060', '✓')
    return ('Fair', '#8A5A00', '~')

# ── Build HTML email ──────────────────────────────────────────────────────────
def build_email_html(deals, period, show_verify=False):
    week_str = TODAY.strftime('%B %d, %Y')

    best = deals[0] if deals else None
    if best:
        subject = f"This week: {best['name']} is {abs(best['pct']):.0f}% below average"
    else:
        subject = f"This week's Ontario grocery deals — {week_str}"

    deal_rows = ''
    for i, d in enumerate(deals):
        label, color, check = verdict(d['pct'])
        em = emoji_for(d['name'])
        _iname = (d.get('item_name') or '')
        # Truncate at " Or " — Flipp often bundles multiple items
        for _sep in [' Or ', ' OR ', ' or ']:
            if _sep in _iname:
                _iname = _iname.split(_sep)[0].strip()
                break
        item_name_raw = _iname[:55] + ('...' if len(_iname) > 55 else '')
        expiry = format_valid_to(d.get('valid_to', ''))
        # Prefer retailer_url for store link if available
        retailer_url = d.get('retailer_url', '')
        if retailer_url:
            store_html = f'<a href="{retailer_url}" style="color:inherit;text-decoration:underline;text-underline-offset:2px">{d["store"]} ↗</a>'
            store_line = store_html + (f' · {expiry}' if expiry else '')
        else:
            store_line = store_link(d['store'], expiry)
        _furl = d.get('flipp_url', '')
        if show_verify:
            if _furl:
                flipp_verify = f' · <a href="{_furl}" style="color:inherit;text-decoration:underline;text-underline-offset:2px;font-size:11px">verify ↗</a>'
            else:
                flipp_verify = ' · <span style="color:#C00;font-size:11px">No Flipp verify link available</span>'
        else:
            flipp_verify = ''
        # Improve price/unit display logic
        raw_unit = d.get('raw_unit', 'kg')
        is_per_kg = raw_unit not in ('pkg', 'unit', 'each')
        # If the raw_unit is 'lb', show price as $/lb and $/kg
        if raw_unit == 'lb':
            lb_price = f'${d["price"]:.2f}/lb'
            kg_price = f'${d["price"]*2.20462:.2f}/kg'
            primary_price = lb_price
            kg_span = f'  <span style="font-size:18px;font-weight:400;color:rgba(255,255,255,0.5)">{kg_price}</span>'
            kg_span2 = f'<span style="font-size:14px;font-weight:400;color:#8A8680;font-family:monospace">{kg_price}</span> '
        elif is_per_kg:
            lb_price = f'${d["price"]/2.20462:.2f}/lb'
            kg_price = f'${d["price"]:.2f}/kg'
            primary_price = lb_price
            kg_span = f'  <span style="font-size:18px;font-weight:400;color:rgba(255,255,255,0.5)">{kg_price}</span>'
            kg_span2 = f'<span style="font-size:14px;font-weight:400;color:#8A8680;font-family:monospace">{kg_price}</span> '
        else:
            primary_price = f'${d["price"]:.2f}'
            kg_span = ''
            kg_span2 = ''
        pct_below  = f'{abs(d["pct"]):.0f}'
        # Label text depends on what we're comparing against
        src = d.get('source', 'flipp')
        if src == 'retail_baseline':
            pct_label_long  = 'below typical Ontario shelf price'
            pct_label_short = 'below shelf avg'
        elif src == 'statcan':
            pct_label_long  = 'below the Ontario average'
            pct_label_short = 'below avg'
        else:
            pct_label_long  = 'below typical flyer prices'
            pct_label_short = 'below flyer avg'

        if i == 0:
            deal_rows += (
                f'<tr><td style="padding:12px 14px 10px">'
                f'<table width="100%" cellpadding="0" cellspacing="0" style="background:#0D0D0D;border-radius:12px">'
                f'<tr><td class="dw-inner" style="padding:18px 16px">'
                f'<div class="dw-label" style="font-size:15px;letter-spacing:0.1em;text-transform:uppercase;color:rgba(255,255,255,0.4);font-family:monospace;margin-bottom:12px">Deal of the week</div>'
                f'<div style="font-size:28px;margin-bottom:10px">{em}</div>'
                f'<div class="dw-name" style="font-size:28px;font-weight:700;color:#FAFAF7;margin-bottom:5px;line-height:1.2">{d["name"]}</div>'
                f'<div class="dw-desc" style="font-size:17px;color:rgba(255,255,255,0.55);margin-bottom:7px;line-height:1.3">{item_name_raw}</div>'
                f'<div class="dw-meta" style="font-size:16px;color:rgba(255,255,255,0.4);font-family:monospace;margin-bottom:16px">{store_line}{flipp_verify}</div>'
                f'<div class="dw-price" style="font-size:40px;font-weight:700;color:#FAFAF7;font-family:monospace;margin-bottom:3px">{primary_price}</div>'
                f'<div style="font-size:18px;color:rgba(255,255,255,0.45);font-family:monospace;margin-bottom:12px">{kg_price if is_per_kg or raw_unit=="lb" else ""}</div>'
                f'<div class="dw-pct" style="font-size:19px;font-weight:700;color:#5DCAA5;font-family:monospace">{check} {pct_below}% {pct_label_long}</div>'
                f'</td></tr></table></td></tr>'
                f'<tr><td style="padding:10px 14px 6px">'
                f'<div class="also-label" style="font-size:15px;letter-spacing:0.08em;text-transform:uppercase;color:#8A8680;font-family:monospace">Also worth buying this week</div>'
                f'</td></tr>'
            )
        else:
            deal_rows += (
                f'<tr><td class="li-row" style="padding:14px 18px;border-top:1px solid #E8E6DF">'
                f'<table width="100%" cellpadding="0" cellspacing="0"><tr>'
                f'<td width="64" style="vertical-align:top;padding-right:16px">'
                f'<div style="width:56px;height:56px;border-radius:12px;background:#F2F1EC;text-align:center;line-height:56px;font-size:30px">{em}</div>'
                f'</td>'
                f'<td style="vertical-align:top">'
                f'<div class="li-name" style="font-size:24px;font-weight:700;color:#0D0D0D;margin-bottom:4px;line-height:1.2">{d["name"]}</div>'
                f'<div class="li-desc" style="font-size:17px;color:#555555;margin-bottom:4px;line-height:1.3">{item_name_raw}</div>'
                f'<div class="li-meta" style="font-size:16px;color:#8A8680;font-family:monospace;margin-bottom:12px">{store_line}{flipp_verify}</div>'
                f'<div class="li-price" style="font-size:28px;font-weight:700;color:#0D0D0D;font-family:monospace;margin-bottom:2px">{primary_price}</div>'
                f'<div style="font-size:15px;color:#8A8680;font-family:monospace;margin-bottom:5px">{kg_price if is_per_kg or raw_unit=="lb" else ""}</div>'
                f'<div style="font-size:16px;font-weight:700;color:{color};font-family:monospace">{check} {pct_below}% {pct_label_short}</div>'
                f'</td></tr></table></td></tr>'
            )

    if not deal_rows:
        deal_rows = '<tr><td style="padding:2rem;text-align:center;color:#8A8680">No deals 15%+ below average this week.</td></tr>'

    # Load static template and fill placeholders
    with open(EMAIL_TEMPLATE) as f:
        tmpl = f.read()

    html = tmpl.replace('{{WEEK_STR}}', week_str)
    html = html.replace('{{PERIOD}}', period)
    html = html.replace('{{SUBJECT}}', subject)
    html = html.replace('{{DEAL_ROWS}}', deal_rows)

    return subject, html


def create_draft(subject, html_content):
    if not MAILERLITE_API_KEY:
        print("No MAILERLITE_API_KEY — saving email HTML to data/digest_draft.html instead")
        with open(os.path.join(DATA_DIR, 'digest_draft.html'), 'w') as f:
            f.write(html_content)
        print("Saved to data/digest_draft.html")
        return

    payload = json.dumps({
        'type':     'regular',
        'status':   'draft',
        'name':     f'Weekly Digest {TODAY.isoformat()}',
        'language': {'id': 1},
        'emails': [{
            'subject':   subject,
            'from_name': FROM_NAME,
            'from':      FROM_EMAIL,
            'reply_to':  FROM_EMAIL,
            'content':   html_content,
        }],
        'groups': [ONTARIO_GROUP_ID],
    }).encode('utf-8')

    req = urllib.request.Request(
        'https://connect.mailerlite.com/api/campaigns',
        data    = payload,
        headers = {
            'Content-Type':  'application/json',
            'Accept':        'application/json',
            'Authorization': f'Bearer {MAILERLITE_API_KEY}',
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
        campaign_id = data.get('data', {}).get('id', '?')
        print(f"✓ Draft campaign created in MailerLite — ID: {campaign_id}")
        print(f"  Review at: https://dashboard.mailerlite.com/campaigns/{campaign_id}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"✗ MailerLite API error {e.code}: {body[:300]}")



import sys

# ── Rejection audit table (appended to review page) ──────────────────────────
def build_rejection_html(rejected):
    """
    Build an HTML audit table of items that were collected this week but
    did not make the digest.  Appended to digest_review.html so you can
    spot false-positive filter rejections every week without reading logs.
    """
    from collections import defaultdict

    SECTIONS = [
        ('close_miss',  'Close misses (< 15% below avg)',    '#fff3cd', '#856404'),
        ('range',       'Price outside expected range ⚠️',   '#f8d7da', '#721c24'),
        ('bad_data',    'Likely bad data ⛔',                '#f8d7da', '#721c24'),
        ('processed',   'Deli / processed / frozen branded', '#e8f4f8', '#2c5f6e'),
    ]

    by_reason = defaultdict(list)
    for r in rejected:
        by_reason[r['reason_key']].append(r)

    total = sum(len(v) for v in by_reason.values())

    html = f'''
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
            margin:40px auto;max-width:900px;padding:20px 24px;
            border-top:3px solid #ddd">
  <h2 style="color:#333;margin-bottom:4px">🔍 Audit: Not Included This Week</h2>
  <p style="color:#888;font-size:14px;margin-top:0">
    {total} item(s) collected but filtered out.
    Review weekly to catch false positives — especially the orange sections.
  </p>
'''

    for reason_key, label, bg, fg in SECTIONS:
        items = by_reason.get(reason_key)
        if not items:
            continue
        items_sorted = sorted(items, key=lambda x: x.get('pct') or 0)
        html += f'''
  <h3 style="color:{fg};background:{bg};padding:8px 14px;border-radius:4px;
             margin-top:28px;margin-bottom:0;font-size:15px">
    {label} &mdash; {len(items)} item(s)
  </h3>
  <table style="width:100%;border-collapse:collapse;font-size:13px;margin-bottom:8px">
    <thead>
      <tr style="background:#f5f5f5">
        <th style="padding:6px 8px;text-align:left;border-bottom:1px solid #ddd;font-weight:600">Flipp description</th>
        <th style="padding:6px 8px;text-align:left;border-bottom:1px solid #ddd;font-weight:600">Store</th>
        <th style="padding:6px 8px;text-align:right;border-bottom:1px solid #ddd;font-weight:600">$/kg</th>
        <th style="padding:6px 8px;text-align:right;border-bottom:1px solid #ddd;font-weight:600">Avg</th>
        <th style="padding:6px 8px;text-align:right;border-bottom:1px solid #ddd;font-weight:600">vs avg</th>
        <th style="padding:6px 8px;text-align:left;border-bottom:1px solid #ddd;font-weight:600">Source</th>
        <th style="padding:6px 8px;text-align:left;border-bottom:1px solid #ddd;font-weight:600">Note</th>
      </tr>
    </thead>
    <tbody>'''
        for i, r in enumerate(items_sorted):
            row_bg    = '#fff' if i % 2 == 0 else '#fafafa'
            price_str = f"${r['price']:.2f}" if r.get('price') is not None else '—'
            avg_str   = f"${r['avg']:.2f}"   if r.get('avg')   is not None else '—'
            pct_val   = r.get('pct')
            pct_str   = f"{pct_val:+.1f}%"   if pct_val  is not None else '—'
            item_disp = (r.get('item_name') or r.get('cut_name') or '').title()
            html += f'''
      <tr style="background:{row_bg}">
        <td style="padding:5px 8px;border-bottom:1px solid #eee;max-width:280px;word-break:break-word">{item_disp}</td>
        <td style="padding:5px 8px;border-bottom:1px solid #eee">{r.get('store','')}</td>
        <td style="padding:5px 8px;border-bottom:1px solid #eee;text-align:right;font-variant-numeric:tabular-nums">{price_str}</td>
        <td style="padding:5px 8px;border-bottom:1px solid #eee;text-align:right;font-variant-numeric:tabular-nums;color:#888">{avg_str}</td>
        <td style="padding:5px 8px;border-bottom:1px solid #eee;text-align:right;font-variant-numeric:tabular-nums">{pct_str}</td>
        <td style="padding:5px 8px;border-bottom:1px solid #eee;color:#888">{r.get('source','')}</td>
        <td style="padding:5px 8px;border-bottom:1px solid #eee;color:#888">{r.get('note','')}</td>
      </tr>'''
        html += '\n    </tbody>\n  </table>\n'

    html += '</div>\n'
    return html


def main():
    review_mode = '--review-page' in sys.argv
    # Only build digest on Thursdays (weekday() == 3) unless forced or review mode
    force = os.environ.get('FORCE_DIGEST', '').lower() == 'true'
    if not review_mode and TODAY.weekday() != 3 and not force:
        print(f"Today is {TODAY.strftime('%A')} — digest only runs on Thursdays. Skipping.")
        return
    print(f"Building digest for week of {TODAY.isoformat()}...{' (review mode)' if review_mode else ''}")

    statcan    = load_statcan()
    flipp      = load_flipp()
    baselines  = load_retail_baselines()
    print(f"StatCan products: {len(statcan)}  |  Flipp averages: {len(flipp)}  |  Retail baselines: {len(baselines)}")

    if review_mode:
        deals, rejected = score_deals(statcan, flipp, baselines=baselines, limit=50)
    else:
        deals, rejected = score_deals(statcan, flipp, baselines=baselines)  # limit=10

    print(f"Deals found: {len(deals)}")
    for d in deals:
        print(f"  {d['name']} @ {d['store']}: ${d['price']:.2f}/kg ({d['pct']:+.1f}%)")

    if not deals:
        print("No deals this week — skipping draft creation.")
        return

    with open(STATCAN_FILE) as f:
        period = json.load(f).get('period', 'unknown')

    if review_mode:
        _, html_review = build_email_html(deals, period, show_verify=True)
        review_path = os.path.join(DATA_DIR, 'digest_review.html')
        with open(review_path, 'w') as f:
            f.write(html_review + build_rejection_html(rejected))
        print(f"✓ Saved review page to data/digest_review.html")
        print(f"  Open data/digest_review.html in your browser to review the top 50 deals.")
        return

    subject, html        = build_email_html(deals, period, show_verify=False)
    _,       html_verify = build_email_html(deals, period, show_verify=True)
    print(f"\nSubject: {subject}")
    create_draft(subject, html)

    # Save public version as thisweek.html
    thisweek_path = os.path.join(DATA_DIR, 'digest_thisweek.html')
    with open(thisweek_path, 'w') as f:
        f.write(html)
    print(f"✓ Saved to data/digest_thisweek.html")

    # Save verify version to secret URL for pre-send review
    verify_path = os.path.join(DATA_DIR, VERIFY_FILE)
    with open(verify_path, 'w') as f:
        f.write(html_verify)
    print(f"✓ Saved verify copy to data/{VERIFY_FILE}")
    print(f"  Review at: {SITE_URL}/{VERIFY_FILE.replace('digest_', '')}")

if __name__ == '__main__':
    main()
