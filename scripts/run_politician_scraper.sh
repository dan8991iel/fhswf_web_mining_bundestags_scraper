#!/bin/bash
# ────────────────────────────────────────────────────────────────
# Run the politician spider
# ────────────────────────────────────────────────────────────────
echo
echo "=====  SCRAPY: politician_spider  ====="
echo

uv run scrapy crawl politician_spider 