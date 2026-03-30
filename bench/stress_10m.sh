#!/bin/bash
set -e

# Configuration
NUM_ROWS=10000000
CSV_PATH="data/stress_10m.csv"
BINARY="./target/release/lsm"

echo "=== AQE 10M RECORD STRESS BENCHMARK ==="

# 1. Cleanup
echo "[1/5] Cleaning up old data..."
rm -rf data/*.aqe data/segments/* 2>/dev/null || true

# 2. Build
echo "[2/5] Building release binary..."
cargo build --release

# 3. Generate Data
echo "[3/5] Generating $NUM_ROWS rows (this may take a minute)..."
python3 tools/gen_data.py --num $NUM_ROWS --output $CSV_PATH

# 4. Ingest and Compact
echo "[4/5] Loading CSV into row-oriented SSTables..."
$BINARY load $CSV_PATH

echo "[4/5] Compacting into columnar segments..."
$BINARY compact

# 5. Run Heavy Queries
echo "[5/5] Running stress queries..."

# Q1: High Cardinality Group By (or standard u8)
echo "--------------------------------"
echo "QUERY: GROUP BY country (Unfiltered)"
sed -i 's/accuracy_target = .*/accuracy_target = 0.99/' config.toml
$BINARY query "SELECT COUNT(*) FROM logs GROUP BY country"

# Q2: Filtered SUM (Status=1 is ~20% of data)
echo "--------------------------------"
echo "QUERY: SUM(level) WHERE status = 1"
sed -i 's/accuracy_target = .*/accuracy_target = 0.999/' config.toml
$BINARY query "SELECT SUM(level) FROM logs WHERE status = 1"

# Q3: Sparse Filtered COUNT (Level > 900 is ~10% of data)
echo "--------------------------------"
echo "QUERY: COUNT(*) WHERE level > 900"
sed -i 's/accuracy_target = .*/accuracy_target = 0.70/' config.toml
$BINARY query "SELECT COUNT(*) FROM logs WHERE level > 900"

echo "--------------------------------"
echo "Benchmark Complete."
