#!/usr/bin/env bash
# rsync-download-from-hpc.sh
# Download data from FASRC HPC for paper figures and dataset publication.
# Uses a single rsync call to avoid repeated password prompts.
# Usage: bash scripts/rsync-download-from-hpc.sh [LOCAL_DEST]

set -euo pipefail

read -p "Enter your FASRC username: " HPC_USER
HPC_HOST="${HPC_HOST:-login.rc.fas.harvard.edu}"
WS=/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace
DST="${1:-$HOME/US-Census-TGSI-export}"

echo "=== Downloading from ${HPC_USER}@${HPC_HOST} ==="
echo "=== Remote workspace: ${WS} ==="
echo "=== Local destination: ${DST} ==="
mkdir -p "$DST"

# Build a temporary file list (paths relative to workspace root)
FILELIST=$(mktemp)
cat > "$FILELIST" <<'EOF'
# Final data product (aggregated sentiment at census block level)
data/aggregated_sentiment_stats/
# Intermediate data for paper figures
data/all_years_tweet_count_with_pop_CR.parquet
data/sentiment_places_data_joined.parquet
data/all_years_tract_summary.parquet
data/spatial_representation/
sentiment_by_tract/
# External validation data
data/census_pop/pop data/
data/census_data/
data/500-Cities-Places/
EOF

rsync -avz --progress --files-from="$FILELIST" \
  "${HPC_USER}@${HPC_HOST}:${WS}/" "$DST/"

rm -f "$FILELIST"
echo "=== Done! All data saved to ${DST} ==="
