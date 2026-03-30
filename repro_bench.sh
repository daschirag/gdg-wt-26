#!/bin/bash
set -e

export RUSTFLAGS="-Awarnings" 

# NOTE: main.rs expects positional arguments: query <SQL>
# accuracy/k are read from config.toml. I'll override them by updating config.toml before each run if needed.

# 1. Q1: GROUP BY country (Unfiltered)
echo "Running Q1..."
sed -i 's/accuracy_target = .*/accuracy_target = 0.99/' config.toml
cargo run --release -- query "SELECT COUNT(*) FROM logs GROUP BY country" 

# 2. Q2: SUM level WHERE level > 0 (Highly Filtered)
echo "Running Q2..."
sed -i 's/accuracy_target = .*/accuracy_target = 0.999/' config.toml
cargo run --release -- query "SELECT SUM(level) FROM logs WHERE level > 0" 

# 3. Q3: COUNT level WHERE level > 500 (Sampled or Filtered)
echo "Running Q3..."
sed -i 's/accuracy_target = .*/accuracy_target = 0.70/' config.toml
cargo run --release -- query "SELECT COUNT(*) FROM logs WHERE level > 500" 
