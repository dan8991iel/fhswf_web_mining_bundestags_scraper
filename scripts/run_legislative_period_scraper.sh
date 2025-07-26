#!/bin/bash
# ────────────────────────────────────────────────────────────────
# Run the legislative periods spider
# ────────────────────────────────────────────────────────────────
echo
echo "=====  SCRAPY: legislative_periods  ====="
echo

uv run scrapy crawl legislative_periods 