#!/usr/bin/env python3
"""Regression tests for is_processed() and extract_price_per_kg() (gram-weight fix)."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from collect_flipp_prices import is_processed, CUT_KEYWORD_EXEMPTIONS, CUTS, extract_price_per_kg

all_ok = True
def check(label, got, expected):
    global all_ok
    ok = got == expected
    all_ok = all_ok and ok
    print(f"{'PASS' if ok else 'FAIL'}  {label}: got {got!r}  (expected {expected!r})")

# ── 1. Cut keys present ────────────────────────────────────────────────────────
print("=== Cut keys ===")
CUT_KEYS = {key for key, *_ in CUTS}
for k in ('bacon_500g', 'sausage_500g', 'coffee_ground_300g', 'frozen_veg_750g', 'frozen_peas', 'frozen_fries'):
    check(f"cut key {k!r}", k in CUT_KEYS, True)

# ── 2. is_processed keyword exemptions ────────────────────────────────────────
print("\n=== is_processed ===")
ip_tests = [
    ('Frozen peas 750g',               'frozen_peas',        False),
    ('Frozen vegetables 750g',         'frozen_veg_750g',    False),
    ('Frozen french fries 750g',       'frozen_fries',       False),
    ('Maple Leaf Bacon 500g',          'bacon_500g',         False),
    ('Smoked bacon 500g',              'bacon_500g',         False),
    ('Sliced bacon 500g',              'bacon_500g',         False),
    ('Cured bacon 375g',               'bacon_500g',         False),
    ('Fully cooked bacon strips',      'bacon_500g',         True),
    ('Breaded bacon',                  'bacon_500g',         True),
    ('Italian sausage 500g',           'sausage_500g',       False),
    ('Smoked pork sausage 500g',       'sausage_500g',       False),
    ('Seasoned breakfast sausage 500', 'sausage_500g',       False),
    ('Fully cooked sausage',           'sausage_500g',       True),
    ('Ground coffee 300g',             'coffee_ground_300g', False),
    ('Frozen chicken breast',          'chicken_breast',     True),
    ('Smoked salmon fillet',           'salmon_fillet',      True),
    ('Rotisserie chicken',             'chicken_whole',      True),
    ('Canned salmon',                  'salmon_fillet',      True),
    ('Frozen peas 750g',               None,                 True),
]
for name, key, expected in ip_tests:
    check(f"is_processed({name!r}, {key!r})", is_processed(name, key), expected)

# ── 3. extract_price_per_kg — gram weight pkg items ───────────────────────────
print("\n=== extract_price_per_kg (gram weights) ===")
def fake_item(name, price):
    return {'name': name, 'current_price': price}

# 300g coffee at $5.99 → $5.99 / 0.300kg = $19.97/kg
price_kg, raw, unit = extract_price_per_kg(fake_item('Ground coffee 300g', 5.99), 'pkg')
check("coffee 300g price_per_kg", price_kg, 19.97)
check("coffee 300g raw_price", raw, 5.99)
check("coffee 300g raw_unit", unit, 'bag_300g')

# 500g bacon at $6.99 → $6.99 / 0.500kg = $13.98/kg
price_kg, raw, unit = extract_price_per_kg(fake_item('Maple Leaf Bacon 500g', 6.99), 'pkg')
check("bacon 500g price_per_kg", price_kg, 13.98)
check("bacon 500g raw_unit", unit, 'bag_500g')

# 375g bacon at $5.49 → $5.49 / 0.375kg = $14.64/kg
price_kg, raw, unit = extract_price_per_kg(fake_item('Bacon 375g', 5.49), 'pkg')
check("bacon 375g price_per_kg", price_kg, 14.64)

# 1 lb bacon → uses lb path, not g path (kg wins when both present in fallback order)
price_kg, raw, unit = extract_price_per_kg(fake_item('Bacon 1 lb', 5.49), 'pkg')
check("bacon 1lb raw_unit starts with bag_", unit is not None and unit.startswith('bag_'), True)

# Item with no weight → None
price_kg, raw, unit = extract_price_per_kg(fake_item('Ground coffee', 5.99), 'pkg')
check("pkg no weight → None", price_kg, None)

# Sanity floor: stray small number (e.g. "0g fat" label) should not match
price_kg, raw, unit = extract_price_per_kg(fake_item('Something 5g trace', 2.99), 'pkg')
check("pkg tiny gram stray → None", price_kg, None)

print()
print('All tests passed!' if all_ok else 'SOME TESTS FAILED — DO NOT COMMIT')
sys.exit(0 if all_ok else 1)
