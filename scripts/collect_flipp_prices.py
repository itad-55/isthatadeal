#!/usr/bin/env python3
"""
collect_flipp_prices.py
──────────────────────────────────────────────────────────────────────────────
Queries the Flipp/Wishabi API for current Ontario grocery flyer prices,
normalises them to $/kg where possible, and appends to data/flipp_history.csv.

Run weekly via GitHub Actions alongside update_data.py.
After enough weeks of data, compute_flipp_averages.py reads this CSV and
generates data/flipp_averages.json which the site can use to expand the checker.

API used:
  GET https://backflipp.wishabi.com/flipp/items/search
  Params: q=<search term>, postal_code=<Ontario postal code>
  No authentication required.
──────────────────────────────────────────────────────────────────────────────
"""

import urllib.request, urllib.parse, json, csv, os, re, time
from datetime import date

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT    = os.path.dirname(SCRIPT_DIR)
DATA_DIR     = os.path.join(REPO_ROOT, 'data')
HISTORY_CSV  = os.path.join(DATA_DIR, 'flipp_history.csv')
AVERAGES_JSON = os.path.join(DATA_DIR, 'flipp_averages.json')

os.makedirs(DATA_DIR, exist_ok=True)

FLIPP_SEARCH = 'https://backflipp.wishabi.com/flipp/items/search'

# Ontario postal codes — spread across the province for better coverage
POSTAL_CODES = ['M5V3L9', 'L4T0A1', 'K1A0A1', 'N2L3G1', 'L2S3A1']

TODAY = date.today().isoformat()

# ── Cuts to track ─────────────────────────────────────────────────────────────
# Each entry: (our_key, display_name, search_terms, unit_hint)
# unit_hint: 'kg' = sold per kg/lb, 'pkg' = sold per package/unit
CUTS = [
    # ── Beef ──────────────────────────────────────────────────────────────────
    ('beef_ground_regular',    'Ground beef (regular)',        ['ground beef regular', 'ground beef'],          'kg'),
    ('beef_ground_medium',     'Ground beef (medium)',         ['ground beef medium'],                          'kg'),
    ('beef_ground_lean',       'Ground beef (lean)',           ['ground beef lean'],                            'kg'),
    ('beef_ground_extra_lean', 'Ground beef (extra lean)',     ['ground beef extra lean'],                      'kg'),
    ('beef_flank',             'Flank steak',                  ['flank steak'],                                 'kg'),
    ('beef_striploin',         'Beef striploin / NY strip',    ['striploin steak', 'new york strip'],           'kg'),
    ('beef_ribeye',            'Beef ribeye steak',            ['ribeye steak', 'rib eye steak'],               'kg'),
    ('beef_tenderloin',        'Beef tenderloin',              ['beef tenderloin'],                             'kg'),
    ('beef_sirloin',           'Beef top sirloin',             ['sirloin steak', 'top sirloin'],                'kg'),
    ('beef_inside_round',      'Beef inside round roast',      ['inside round roast'],                         'kg'),
    ('beef_blade_roast',       'Beef blade roast',             ['blade roast', 'chuck roast'],                  'kg'),
    ('beef_short_ribs',        'Beef short ribs',              ['beef short ribs'],                             'kg'),
    ('beef_brisket',           'Beef brisket',                 ['beef brisket'],                                'kg'),
    ('beef_stewing',           'Beef stewing cubes',           ['stewing beef', 'beef stewing'],                'kg'),
    # ── Pork ──────────────────────────────────────────────────────────────────
    ('pork_chop_bonein',       'Pork chops (bone-in)',         ['pork chops bone in', 'pork chops'],            'kg'),
    ('pork_chop_boneless',     'Pork chops (boneless)',        ['pork chops boneless'],                         'kg'),
    ('pork_tenderloin',        'Pork tenderloin',              ['pork tenderloin'],                             'kg'),
    ('pork_shoulder',          'Pork shoulder roast',          ['pork shoulder', 'pork butt'],                  'kg'),
    ('pork_back_ribs',         'Pork back ribs',               ['pork back ribs', 'baby back ribs'],            'kg'),
    ('pork_side_ribs',         'Pork side ribs',               ['pork side ribs', 'spare ribs'],                'kg'),
    ('pork_belly',             'Pork belly',                   ['pork belly'],                                  'kg'),
    ('pork_ground',            'Ground pork',                  ['ground pork'],                                 'kg'),
    ('pork_ham',               'Ham roast',                    ['ham roast', 'boneless ham'],                   'pkg'),
    # ── Chicken ───────────────────────────────────────────────────────────────
    ('chicken_whole',          'Whole chicken',                ['whole chicken'],                               'kg'),
    ('chicken_breast',         'Chicken breast (boneless)',    ['chicken breast boneless', 'chicken breasts'],  'kg'),
    ('chicken_breast_bonein',  'Chicken breast (bone-in)',     ['chicken breast bone in', 'chicken breast split', 'split chicken breast'], 'kg'),
    ('chicken_thigh_bonein',   'Chicken thighs (bone-in)',     ['chicken thighs bone in', 'chicken thighs'],   'kg'),
    ('chicken_thigh_boneless', 'Chicken thighs (boneless)',    ['chicken thighs boneless'],                     'kg'),
    ('chicken_drumsticks',     'Chicken drumsticks',           ['chicken drumsticks', 'chicken legs'],          'kg'),
    ('chicken_wings',          'Chicken wings',                ['chicken wings'],                               'kg'),
    ('chicken_ground',         'Ground chicken',               ['ground chicken'],                              'kg'),
    # ── Turkey ────────────────────────────────────────────────────────────────
    ('turkey_whole',           'Whole turkey',                 ['whole turkey'],                                'kg'),
    ('turkey_breast',          'Turkey breast',                ['turkey breast'],                               'pkg'),
    ('turkey_ground',          'Ground turkey',                ['ground turkey'],                               'kg'),
    # ── Lamb & other ──────────────────────────────────────────────────────────
    ('lamb_leg',               'Leg of lamb',                  ['leg of lamb', 'lamb leg'],                     'kg'),
    ('lamb_chops',             'Lamb chops',                   ['lamb chops'],                                  'kg'),
    ('veal_cutlets',           'Veal cutlets',                 ['veal cutlets', 'veal'],                        'kg'),
    # ── Fish & seafood ────────────────────────────────────────────────────────
    ('salmon_fillet',          'Salmon fillets',               ['salmon fillet', 'atlantic salmon'],            'kg'),
    ('salmon_whole',           'Whole salmon',                 ['whole salmon'],                                'kg'),
    ('tilapia',                'Tilapia',                      ['tilapia'],                                     'kg'),
    ('cod',                    'Cod fillets',                  ['cod fillet', 'cod fish'],                      'kg'),
    ('shrimp',                 'Shrimp (300g)',                 ['shrimp', 'prawns'],                            'pkg'),
    ('tuna_steak',             'Tuna steak',                   ['tuna steak'],                                  'kg'),
    ('canned_tuna_170g',       'Canned tuna (170g)',            ['canned tuna', 'tuna flakes', 'chunk tuna'],    'pkg'),
    ('canned_salmon_213g',     'Canned salmon (213g)',          ['canned salmon', 'sockeye salmon can'],         'pkg'),
    # ── Dairy ─────────────────────────────────────────────────────────────────
    ('milk_4l',                'Milk (4L)',                    ['milk 4 litre', 'milk 4l'],                     'pkg'),
    ('milk_2l',                'Milk (2L)',                    ['milk 2 litre', 'milk 2l'],                     'pkg'),
    ('butter_454g',            'Butter (454g)',                ['butter 454', 'butter 1lb'],                    'pkg'),
    ('cheese_block_500g',      'Block cheese (500g)',          ['block cheese 500', 'cheddar cheese 500'],      'pkg'),
    ('eggs_dozen',             'Eggs (1 dozen)',               ['eggs dozen', 'large eggs 12'],                 'pkg'),
    ('eggs_18',                'Eggs (18-pack)',               ['eggs 18', '18 eggs'],                          'pkg'),
    ('yogurt_500g',            'Yogurt (500g)',                ['yogurt 500', 'greek yogurt 500'],               'pkg'),
    ('cream_cheese',           'Cream cheese',                 ['cream cheese'],                                'pkg'),
    ('sour_cream',             'Sour cream',                   ['sour cream'],                                  'pkg'),
    ('cottage_cheese',         'Cottage cheese',               ['cottage cheese'],                              'pkg'),
    ('heavy_cream',            'Heavy cream',                  ['heavy cream', 'whipping cream'],               'pkg'),
    # ── Bread & bakery ────────────────────────────────────────────────────────
    ('bread_white',            'White bread',                  ['white bread', 'sandwich bread'],               'pkg'),
    ('bread_whole_wheat',      'Whole wheat bread',            ['whole wheat bread', 'multigrain bread'],       'pkg'),
    ('bagels',                 'Bagels',                       ['bagels'],                                      'pkg'),
    ('english_muffins',        'English muffins',              ['english muffins'],                             'pkg'),
    ('tortillas',              'Tortillas',                    ['tortillas', 'flour tortillas'],                'pkg'),
    # ── Fresh produce — vegetables ────────────────────────────────────────────
    ('broccoli',               'Broccoli',                     ['broccoli'],                                    'pkg'),
    ('cauliflower',            'Cauliflower',                  ['cauliflower'],                                 'pkg'),
    ('carrots_bag',            'Carrots (bag)',                ['carrots 2lb', 'carrots bag', 'carrots 1kg'],   'pkg'),
    ('potatoes_bag',           'Potatoes (bag)',               ['potatoes 10lb', 'potatoes 5lb', 'potatoes bag'], 'pkg'),
    ('sweet_potato',           'Sweet potatoes',               ['sweet potato', 'sweet potatoes'],              'kg'),
    ('tomatoes',               'Tomatoes (loose)',             ['tomatoes on the vine', 'roma tomatoes'],       'kg'),
    ('cucumber',               'Cucumber',                     ['cucumber'],                                    'pkg'),
    ('peppers',                'Bell peppers',                 ['bell peppers', 'sweet peppers'],               'kg'),
    ('onions_bag',             'Onions (bag)',                 ['onions 3lb', 'onions bag', 'onions 2kg'],      'pkg'),
    ('celery',                 'Celery',                       ['celery'],                                      'pkg'),
    ('lettuce_romaine',        'Romaine lettuce',              ['romaine lettuce', 'romaine hearts'],           'pkg'),
    ('salad_mix',              'Salad mix',                    ['salad mix', 'spring mix'],                     'pkg'),
    ('spinach',                'Spinach',                      ['baby spinach', 'spinach'],                     'pkg'),
    ('mushrooms',              'Mushrooms',                    ['mushrooms 227', 'white mushrooms'],            'pkg'),
    ('corn',                   'Corn on the cob',              ['corn on the cob', 'sweet corn'],               'pkg'),
    ('asparagus',              'Asparagus',                    ['asparagus'],                                   'kg'),
    ('zucchini',               'Zucchini',                     ['zucchini'],                                    'kg'),
    # ── Fresh produce — fruit ─────────────────────────────────────────────────
    ('bananas',                'Bananas',                      ['bananas'],                                     'kg'),
    ('apples_bag',             'Apples (bag)',                 ['apples 3lb', 'gala apples', 'apples bag'],     'pkg'),
    ('oranges_bag',            'Oranges (bag)',                ['oranges bag', 'navel oranges bag'],            'pkg'),
    ('strawberries',           'Strawberries',                 ['strawberries 1lb', 'strawberries 454'],        'pkg'),
    ('blueberries',            'Blueberries',                  ['blueberries'],                                 'pkg'),
    ('grapes',                 'Grapes',                       ['grapes'],                                      'kg'),
    ('watermelon',             'Watermelon',                   ['watermelon'],                                  'pkg'),
    ('avocado',                'Avocados',                     ['avocado'],                                     'pkg'),
    ('mango',                  'Mangoes',                      ['mango', 'mangoes'],                            'pkg'),
    ('pineapple',              'Pineapple',                    ['pineapple'],                                   'pkg'),
    ('lemons',                 'Lemons',                       ['lemons'],                                      'pkg'),
    ('limes',                  'Limes',                        ['limes'],                                       'pkg'),
    # ── Pantry staples ────────────────────────────────────────────────────────
    ('pasta_500g',             'Pasta (500g)',                 ['pasta 500g', 'spaghetti 500', 'penne 500'],    'pkg'),
    ('rice_2kg',               'White rice (2kg)',             ['white rice 2kg', 'rice 2kg'],                  'pkg'),
    ('olive_oil_1l',           'Olive oil (1L)',               ['olive oil 1 litre', 'olive oil 1l'],           'pkg'),
    ('canola_oil_3l',          'Canola oil (3L)',              ['canola oil 3 litre', 'canola oil 3l'],         'pkg'),
    ('canned_tomatoes',        'Canned tomatoes (796mL)',      ['canned tomatoes 796', 'diced tomatoes 796'],   'pkg'),
    ('canned_beans',           'Canned beans (540mL)',         ['canned beans 540', 'black beans 540'],         'pkg'),
    ('dried_lentils_1kg',      'Dried lentils (1kg)',          ['dried lentils 1kg', 'lentils 1kg'],            'pkg'),
    ('dried_chickpeas_1kg',    'Dried chickpeas (1kg)',        ['dried chickpeas 1kg', 'chickpeas 1kg'],        'pkg'),
    ('peanut_butter_1kg',      'Peanut butter (1kg)',          ['peanut butter 1kg'],                           'pkg'),
    ('pasta_sauce',            'Pasta sauce (650mL)',          ['pasta sauce 650', 'tomato sauce 650'],         'pkg'),
    # ── Frozen ────────────────────────────────────────────────────────────────
    ('frozen_veg_750g',        'Frozen vegetables (750g)',     ['frozen vegetables 750', 'frozen mixed veg'],   'pkg'),
    ('frozen_peas',            'Frozen peas (750g)',           ['frozen peas 750'],                             'pkg'),
    ('frozen_fries',           'Frozen french fries (750g)',   ['frozen french fries 750', 'frozen fries 750'], 'pkg'),
    # ── Bacon & sausage ───────────────────────────────────────────────────────
    ('bacon_500g',             'Bacon (500g)',                 ['bacon 500g', 'bacon 375g', 'bacon 500'],       'pkg'),
    ('sausage_500g',           'Pork sausage (500g)',          ['pork sausage 500', 'italian sausage 500', 'breakfast sausage 500'], 'pkg'),
    # ── Beverages ─────────────────────────────────────────────────────────────
    ('coffee_ground_300g',     'Ground coffee (300g)',         ['ground coffee 300', 'ground coffee 325', 'ground coffee 326'], 'pkg'),
]
# ── Keywords that indicate a product is NOT a basic grocery ──────────────────
# Used to skip pre-cooked, frozen, deli, and restaurant-branded items
# ── Item IDs permanently blacklisted due to known bad data ──────────────────
# Add item IDs here when Flipp persistently returns contaminated data
# that cannot be filtered by keywords alone.
BLACKLISTED_ITEM_IDS = {
    '1000407057',  # Loblaws lamb leg: $5.99 labelled as /kg (should be /lb = $13.21/kg)
    '1004049334',  # Sobeys "Fresh Pork Shoulder Blade Roast" collected under beef_blade_roast
    '1004068593',  # FreshCo salmon: Flipp stores $9.99/lb but labels unit as 'kg' — price in title says 22.02/kg
    '1004033505',  # Chalo FreshCo same bad salmon item (same flyer, wrong unit)
}

PROCESSED_KEYWORDS = [
    'cooked', 'frozen', 'breaded', 'marinated', 'seasoned', 'stuffed',
    'rotisserie', 'pre-cooked', 'fully cooked', 'ready to cook', 'ready-to-cook',
    'heat and serve', 'microwave',
    # Deli / sliced
    'sliced', 'deli', 'cured', 'smoked', 'lunch meat', 'lunchmeat',
    'bologna', 'salami', 'prosciutto', 'pepperoni', 'pastrami',
    # Restaurant / branded pre-cooked
    'swiss chalet', "montana's", 'plaisirs gastronomiques', 'irresistible',
    # Frozen / party-pack brands and ready-to-eat formats
    "pinty", 'repas', 'meal', 'janes', 'flamingo', 'bucket',
    # Canned / processed fish
    'canned', 'chunk light', 'flaked',
]

# Maps cut_key -> set of words that MUST NOT appear in the item name.
# Use to prevent wrong-species contamination (e.g. pork items under beef cuts).
CUT_REJECT_KEYWORDS = {
    'beef_blade_roast':   {'pork', 'porc'},
    'beef_ground_regular': {'pork', 'porc'},
    'beef_ground_lean':   {'pork', 'porc'},
    'beef_ground_medium': {'pork', 'porc'},
    'beef_inside_round':  {'pork', 'porc'},
    'beef_sirloin':       {'pork', 'porc'},
    'beef_striploin':     {'pork', 'porc'},
    'beef_stewing':       {'pork', 'porc'},
    'beef_brisket':       {'pork', 'porc'},
    'beef_flank':         {'pork', 'porc'},
    'chicken_breast_bonein': {'boneless', 'désossé'},
    'mango': {'dried', 'séché'},
}

# Maps cut_key -> set of PROCESSED_KEYWORDS to ignore for that cut.
# Use when a cut's ingredient would otherwise be flagged by a keyword
# (e.g. bacon is cured & smoked by nature; frozen items contain 'frozen').
CUT_KEYWORD_EXEMPTIONS = {
    # Intentionally-frozen cuts — 'frozen' in the name is expected
    'frozen_veg_750g': {'frozen'},
    'frozen_peas':     {'frozen'},
    'frozen_fries':    {'frozen'},
    # Bacon is cured, smoked, and sliced by definition — still a basic grocery
    'bacon_500g':      {'cured', 'smoked', 'sliced'},
    # Sausage may be smoked or seasoned — still a basic grocery
    'sausage_500g':    {'smoked', 'seasoned'},
}

def is_processed(item_name, cut_key=None):
    """Return True if the item name suggests a processed/pre-cooked product.
    For cuts in CUT_KEYWORD_EXEMPTIONS, specific keywords are ignored so those
    items are still collected into the historical database.
    """
    name_lower = (item_name or '').lower()
    exempt = CUT_KEYWORD_EXEMPTIONS.get(cut_key, set())
    keywords = [kw for kw in PROCESSED_KEYWORDS if kw not in exempt]
    return any(kw in name_lower for kw in keywords)

# ── Grocery store filter ───────────────────────────────────────────────────────
GROCERY_STORES = {
    'no frills', 'food basics', 'loblaws', 'metro', 'sobeys',
    'freshco', 'walmart', 'superstore', 'giant tiger', 'maxi',
    'provigo', 'iga', 'zehrs', 'valumart', 'save on foods',
    "t&t", "fortinos", "independent"
}

def is_grocery(item):
    merchant = (item.get('merchant') or item.get('merchant_name') or '').lower()
    return any(s in merchant for s in GROCERY_STORES)

# ── Price extraction ───────────────────────────────────────────────────────────
def extract_price_per_kg(item, unit_hint):
    """
    Returns (price_per_kg, raw_price, raw_unit) or (None, None, None).
    Handles $/kg, $/100g, $/lb price formats found in Flipp data.

    Key insight: many Canadian flyers show $/lb as the featured price
    (e.g. Walmart) with $/kg in small print. Flipp returns the featured
    price which is often $/lb. We need to detect this to avoid recording
    a $/lb price as $/kg (which would be ~2.2x too low).
    """
    price = item.get('current_price') or item.get('sale_price')
    if price is None:
        return None, None, None

    price = float(price)
    name  = (item.get('name') or '').lower()
    desc  = (item.get('description') or '').lower()
    text  = name + ' ' + desc

    # Check ALL available text fields for unit clues
    price_text   = (item.get('price_text') or item.get('current_price_text') or '').lower()
    display_text = (item.get('display_text') or '').lower()
    pre_price    = (item.get('pre_price_text') or '').lower()
    post_price   = (item.get('post_price_text') or '').lower()
    all_text     = text + ' ' + price_text + ' ' + display_text + ' ' + pre_price + ' ' + post_price

    # ── Fixed-weight packages (e.g. "10 LB BAG", "2 lb Bag", "1.36 kg", "300g") ──
    # Must check BEFORE the generic /lb detection, because "10 LB BAG" contains
    # \blb\b but the $X is the total bag price, not a per-lb rate.
    if unit_hint == 'pkg':
        pkg_kg_m = re.search(r'(\d+(?:\.\d+)?)\s*kg\b', all_text)
        pkg_g_m  = re.search(r'(\d+)\s*g\b', all_text)   # e.g. 300g, 375g, 500g
        pkg_lb_m = re.search(r'(\d+(?:\.\d+)?)\s*(?:lb|lbs)\b', all_text)
        if pkg_kg_m:
            pkg_kg = float(pkg_kg_m.group(1))
            if pkg_kg > 0:
                return round(price / pkg_kg, 2), price, f'bag_{pkg_kg}kg'
        elif pkg_g_m:
            pkg_g = float(pkg_g_m.group(1))
            if pkg_g >= 50:  # sanity floor — ignore stray small numbers (e.g. "1g fat")
                return round(price / (pkg_g / 1000), 2), price, f'bag_{int(pkg_g)}g'
        elif pkg_lb_m:
            pkg_lb = float(pkg_lb_m.group(1))
            if pkg_lb > 0:
                return round(price / (pkg_lb * 0.453592), 2), price, f'bag_{pkg_lb}lb'
        # pkg item with no weight in name — skip (can't compute $/kg)
        return None, None, None

    # Explicit unit detection — check everything we have
    if re.search(r'/\s*100\s*g', all_text):
        return round(price * 10, 2), price, '100g'

    # ── Catch mis-labelled units: item name shows "$X.XX/kg" but listed price
    # is clearly per-lb.  E.g. FreshCo salmon: price=9.99, name contains "22.02/kg".
    # If the text says $Y/kg and Y is between 1.8x and 2.5x of the listed price,
    # the listed price is almost certainly per-lb.
    embedded_kg = re.search(r'(\d+(?:\.\d+)?)\s*/\s*kg', text)
    if embedded_kg:
        embedded_val = float(embedded_kg.group(1))
        ratio = embedded_val / price if price > 0 else 0
        if 1.8 < ratio < 2.5:
            # The embedded value IS the real $/kg — the listed price is $/lb
            print(f"  ⚠ Unit mismatch: listed ${price} but item says {embedded_val}/kg (ratio {ratio:.2f}) — treating as /lb")
            return round(price * 2.20462, 2), price, 'lb'

    if re.search(r'/\s*kg|per\s+kg|\bkg\b', all_text):
        return round(price, 2), price, 'kg'
    if re.search(r'/\s*lb|per\s+lb|\bper\s+pound|\blb\b', all_text):
        return round(price * 2.20462, 2), price, 'lb'

    # Only apply $/lb fallback for loose produce (like zucchini) if the item is not packaged/processed
    lb_first_stores = {
        'no frills', 'food basics', 'loblaws', 'metro', 'sobeys',
        'freshco', 'walmart', 'superstore', 'giant tiger', 'maxi',
        'provigo', 'iga', 'zehrs', 'valumart', 'save on foods',
        "t&t", "fortinos", "independent"
    }
    store = (item.get('merchant') or item.get('merchant_name') or '').strip().lower()
    # Only fallback for zucchini and similar loose produce
    loose_produce = ['zucchini', 'asparagus', 'bananas', 'grapes', 'peppers', 'tomatoes', 'sweet potato']
    # Exclude packaged/processed items
    if unit_hint == 'kg' and store in lb_first_stores:
        # Only fallback for loose produce, not for packaged
        if any(prod in name for prod in loose_produce) and not re.search(r'swirls|noodles|spirals|pack|pkg|tray|bag|container|pre-cut|precut|sliced|sticks|snack|kit|mix|medley|steam|frozen|prepared|zoodles|spiralized', all_text):
            return round(price * 2.20462, 2), price, 'defaulted_lb'

    # No fallback to $/kg
    return None, None, None

# ── Flipp search ───────────────────────────────────────────────────────────────
def search_flipp(query, postal_code):
    params  = urllib.parse.urlencode({'q': query, 'postal_code': postal_code})
    url     = f'{FLIPP_SEARCH}?{params}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; IsThatADeal/1.0)',
        'Accept':     'application/json',
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        return data.get('items', [])
    except Exception as e:
        print(f"    Flipp error ({query}, {postal_code}): {e}")
        return []

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    # Load existing history to avoid exact duplicates
    existing = set()
    if os.path.exists(HISTORY_CSV):
        with open(HISTORY_CSV, newline='') as f:
            for row in csv.DictReader(f):
                vt = (row.get('valid_to') or '')[:10]
                existing.add((row['date'], row['cut_key'], row['store'], row['raw_price'], vt))

    new_rows  = []
    fieldnames = ['date', 'cut_key', 'cut_name', 'store', 'item_name',
                  'raw_price', 'raw_unit', 'price_per_kg', 'postal_code', 'valid_to',
                  'item_id', 'flyer_id', 'retailer_url']

    for key, display_name, queries, unit_hint in CUTS:
        print(f"\n{display_name}")
        seen_items = set()  # dedupe within this cut across postal codes

        for postal in POSTAL_CODES:
            for query in queries:
                items = search_flipp(query, postal)
                time.sleep(0.4)  # be polite

                for item in items:
                    if not is_grocery(item):
                        continue

                    item_id   = str(item.get('id') or item.get('flyer_item_id') or '')
                    flyer_id  = str(item.get('flyer_id') or item.get('flyerId') or '')
                    item_name = (item.get('name') or '').strip()
                    store     = (item.get('merchant') or item.get('merchant_name') or '').strip()

                    # Try to get the retailer-specific URL (external_url, retailer_url, or see_it_url)
                    retailer_url = (
                        item.get('external_url') or
                        item.get('retailer_url') or
                        item.get('see_it_url') or
                        item.get('seeItUrl') or
                        ''
                    )

                    dedup_key = (item_id, store)
                    if dedup_key in seen_items:
                        continue
                    if item_id in BLACKLISTED_ITEM_IDS:
                        print(f"  ✗ Skipping blacklisted item {item_id}: {item_name[:50]}")
                        continue
                    seen_items.add(dedup_key)

                    price_kg, raw_price, raw_unit = extract_price_per_kg(item, unit_hint)
                    if price_kg is None:
                        continue

                    # Skip pre-cooked, frozen, and processed branded items
                    # (pass cut key so intentionally-frozen cuts are not blocked)
                    if is_processed(item_name, key):
                        print(f"  ✗ Skipping processed/frozen: {item_name[:60]}")
                        continue

                    # Reject wrong-species contamination (e.g. pork showing under beef cuts)
                    reject_words = CUT_REJECT_KEYWORDS.get(key, set())
                    name_lower = item_name.lower()
                    if any(rw in name_lower for rw in reject_words):
                        print(f"  ✗ Skipping wrong-species item for {key}: {item_name[:60]}")
                        continue

                    raw_price_str = str(raw_price)
                    valid_to   = (item.get('valid_to') or item.get('flyer_valid_to') or
                                 item.get('valid_until') or '')
                    valid_to_date = valid_to[:10]
                    dup_check = (TODAY, key, store, raw_price_str, valid_to_date)
                    if dup_check in existing:
                        continue

                    row = {
                        'date':         TODAY,
                        'cut_key':      key,
                        'cut_name':     display_name,
                        'store':        store,
                        'item_name':    item_name,
                        'raw_price':    raw_price_str,
                        'raw_unit':     raw_unit,
                        'price_per_kg': price_kg,
                        'postal_code':  postal,
                        'valid_to':     valid_to,
                        'item_id':      item_id,
                        'flyer_id':     flyer_id,
                        'retailer_url': retailer_url,
                    }
                    new_rows.append(row)
                    existing.add(dup_check)
                    print(f"  ✓ {store}: {item_name} — ${raw_price}/{raw_unit} (${price_kg}/kg) [retailer_url: {retailer_url}]")

                if new_rows and len(queries) > 1:
                    break  # found results from first query, skip fallback queries

    # Append to CSV
    write_header = not os.path.exists(HISTORY_CSV)
    with open(HISTORY_CSV, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)

    print(f"\n✓ Appended {len(new_rows)} new price observations to flipp_history.csv")

    # Recompute averages from full history
    compute_averages()

def compute_averages():
    """
    Reads the full flipp_history.csv and computes averages per cut.
    Writes data/flipp_averages.json — used by the site to expand the checker.
    Min 4 observations required before we trust an average.
    """
    if not os.path.exists(HISTORY_CSV):
        return

    from collections import defaultdict
    prices_by_cut = defaultdict(list)
    names_by_cut  = {}

    with open(HISTORY_CSV, newline='') as f:
        for row in csv.DictReader(f):
            # Skip processed/frozen items that may have been recorded before this filter
            if is_processed(row.get('item_name', '')):
                continue
            val = row.get('price_per_kg')
            # Skip if missing, empty, or not a valid float
            if val is None:
                continue
            val_str = str(val).strip()
            if val_str == '' or val_str.lower() == 'none':
                continue
            try:
                pkg = float(val_str)
                if not (0.5 < pkg < 200):  # sanity filter
                    continue
            except (ValueError, TypeError):
                continue
            try:
                prices_by_cut[row['cut_key']].append(pkg)
                names_by_cut[row['cut_key']] = row['cut_name']
            except (KeyError, TypeError):
                continue

    averages = {}
    for key, prices in prices_by_cut.items():
        if len(prices) < 4:
            print(f"  Skipping {key} — only {len(prices)} observations (need 4+)")
            continue
        prices.sort()
        # Trim top and bottom 10% to reduce outlier impact
        trim = max(1, len(prices) // 10)
        trimmed = prices[trim:-trim] if len(prices) > 4 else prices
        averages[key] = {
            'name':         names_by_cut[key],
            'avg':          round(sum(trimmed) / len(trimmed), 2),
            'lo':           round(prices[0], 2),
            'hi':           round(prices[-1], 2),
            'observations': len(prices),
            'source':       'flipp_historical',
        }
        print(f"  {names_by_cut[key]}: ${averages[key]['avg']:.2f}/kg avg ({len(prices)} obs)")

    with open(AVERAGES_JSON, 'w') as f:
        json.dump({'computed_on': TODAY, 'cuts': averages}, f, indent=2)
    print(f"\n✓ Wrote {len(averages)} cut averages to flipp_averages.json")

if __name__ == '__main__':
    main()
