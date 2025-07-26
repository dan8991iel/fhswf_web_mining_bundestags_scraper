#!/bin/bash
# ────────────────────────────────────────────────────────────────
# Run the politician content spider
# ────────────────────────────────────────────────────────────────
echo
echo "=====  SCRAPY: politician_content_spider  ====="
echo

# ---- config --------------------------------------------------
BATCH_SIZE=2000            # must equal spider's BATCH_SIZE
TOTAL_BATCHES=${1:-5}      # arg 1 or default

echo "============================================================="
echo " Running $TOTAL_BATCHES batches  (size $BATCH_SIZE)"
echo "============================================================="

for ((B=0; B<TOTAL_BATCHES; B++)); do
    echo
    echo "=====  Batch $B / $TOTAL_BATCHES ====="
    uv run scrapy crawl politician_content_spider -a batch=$B
    if [ $? -ne 0 ]; then
        echo "===== Scrapy returned an error in batch $B – aborting ====="
        exit 1
    fi
done

echo
echo "=====  ALL BATCHES FINISHED  ====="
exit 0 