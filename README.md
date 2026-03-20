# isthatadeal.ca

Canadian grocery price checker powered by two data sources:

1. **Statistics Canada** (Table 18-10-0245) — official monthly retail averages for ~110 products across all provinces
2. **Flipp/Wishabi** — Ontario grocery flyer prices scraped weekly and accumulated into proprietary historical averages for cuts StatCan doesn't track (flank steak, ground beef by fat content, chicken wings, etc.)

## File structure

```
isthatadeal/
├── index.html                  — homepage (generated)
├── checker.html                — standalone checker (generated)
├── data/
│   ├── statcan_data.json       — latest StatCan data (generated)
│   ├── flipp_history.csv       — raw weekly Flipp price observations (grows over time)
│   └── flipp_averages.json     — computed averages from Flipp history (generated)
├── scripts/
│   ├── update_data.py          — downloads & parses StatCan SDMX data
│   ├── collect_flipp_prices.py — queries Flipp API, appends to flipp_history.csv
│   ├── build_site.py           — builds index.html + checker.html from templates + data
│   ├── template.html           — index.html template
│   └── checker_template.html   — checker.html template
└── .github/workflows/
    └── update.yml              — runs every Sunday automatically
```

## How it works

Every Sunday, GitHub Actions:
1. Queries Flipp for ~35 meat cuts across Ontario postal codes → appends to `flipp_history.csv`
2. Downloads latest StatCan SDMX data → updates `statcan_data.json`
3. Recomputes Flipp averages from full history → updates `flipp_averages.json`
4. Rebuilds `index.html` and `checker.html` from templates
5. FTPs updated files to your hosting
6. Commits everything to the repo

The checker shows StatCan data for the ~15 tracked cuts, plus Flipp-derived averages for everything else. Flipp averages are labelled differently and only shown once we have 4+ observations (so they're trustworthy).

## Setup (one-time)

### GitHub Secrets needed (Settings → Secrets → Actions)

| Secret | Value |
|--------|-------|
| `FTP_SERVER` | Your FTP hostname |
| `FTP_USERNAME` | Your FTP username |
| `FTP_PASSWORD` | Your FTP password |

### First run

Trigger manually: GitHub → Actions → "Update Data & Deploy" → Run workflow

The Flipp averages will be empty at first (not enough observations). After 4+ weeks of Sunday runs, cuts like flank steak and ground beef (lean) will start appearing in the checker with real Ontario price averages.

## Data sources

- Statistics Canada Table 18-10-0245: https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810024501
- Flipp/Wishabi API: https://backflipp.wishabi.com/flipp/items/search
