#!/bin/bash
# ────────────────────────────────────────────────────────────────
# Run all scrapers in sequence
# ────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo
echo "==============================================================="
echo " Running all scrapers in sequence"
echo "==============================================================="

# 1. Legislative periods
echo
echo "===== Step 1: Legislative Periods ====="
"$SCRIPT_DIR/run_legislative_period_scraper.sh"
if [ $? -ne 0 ]; then
    echo "===== Legislative periods scraper failed – aborting ====="
    exit 1
fi

# 2. Politicians
echo
echo "===== Step 2: Politicians ====="
"$SCRIPT_DIR/run_politician_scraper.sh"
if [ $? -ne 0 ]; then
    echo "===== Politician scraper failed – aborting ====="
    exit 1
fi

# 3. Politician content (default 5 batches)
echo
echo "===== Step 3: Politician Content ====="
"$SCRIPT_DIR/run_politician_content_scraper.sh" ${1:-5}
if [ $? -ne 0 ]; then
    echo "===== Politician content scraper failed – aborting ====="
    exit 1
fi

echo
echo "===== ALL SCRAPERS FINISHED SUCCESSFULLY ====="
exit 0 