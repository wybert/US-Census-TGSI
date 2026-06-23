#!/bin/bash
# Stage the US-CT-TSGI aggregated statistics for deposit to figshare.
#
# Produces a self-contained upload directory containing the 42 Parquet files
# plus README, data dictionary, license, a manifest, and SHA-256 checksums.
# The Parquet files are ~121 GB total; figshare's free tier is 20 GB, so you
# may need a figshare+ / institutional account, or deposit a subset
# (e.g. yearly + monthly = ~28 GB) and host the daily files separately.
#
# Usage:
#   bash data_release/package_for_figshare.sh [STAGE_DIR]
# Default STAGE_DIR: <workspace>/figshare_upload
set -euo pipefail

REPO="/n/home11/xiaokangfu/xiaokang/US-Census-TGSI"
AGG="/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/aggregated_sentiment_stats"
STAGE="${1:-/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/figshare_upload}"

mkdir -p "$STAGE"
echo "Staging to: $STAGE"

# 1) Metadata (small, copied)
cp "$REPO/data_release/README.md"          "$STAGE/"
cp "$REPO/data_release/data_dictionary.csv" "$STAGE/"
cp "$REPO/data_release/LICENSE.txt"         "$STAGE/"

# 2) Data files: hardlink if same filesystem (instant, no extra space), else copy.
echo "Linking $(ls "$AGG"/*.parquet | wc -l) Parquet files..."
for f in "$AGG"/{daily,monthly,yearly}_*.parquet; do
  ln "$f" "$STAGE/$(basename "$f")" 2>/dev/null || cp "$f" "$STAGE/"
done

# 3) Manifest + checksums (for integrity verification by downloaders)
echo "Computing SHA-256 checksums (this can take a while over 121 GB)..."
( cd "$STAGE" && sha256sum *.parquet > SHA256SUMS.txt )
( cd "$STAGE" && \
  printf "file,bytes,sha256\n" > MANIFEST.csv && \
  for f in *.parquet; do
    printf "%s,%s,%s\n" "$f" "$(stat -c %s "$f")" "$(awk -v F="$f" '$2==F{print $1}' SHA256SUMS.txt)" >> MANIFEST.csv
  done )

echo "Done. Upload the contents of $STAGE to a new figshare item."
echo "Total size: $(du -sh "$STAGE" | cut -f1)"
echo "After publishing, paste the figshare DOI into paper/main.tex (Data Records)."
