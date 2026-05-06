#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
budget_path="$repo_root/benchmarks/perf-gate.json"
tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/bpu-bench-gate.XXXXXX")"
base_worktree="$tmp_root/base-worktree"
baseline_dir="$tmp_root/baseline"
candidate_dir="$tmp_root/candidate"

cleanup() {
  if git -C "$repo_root" worktree list | grep -Fq "$base_worktree"; then
    git -C "$repo_root" worktree remove --force "$base_worktree" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp_root"
}
trap cleanup EXIT

if [[ -n "${PERF_GATE_BASE_REF:-}" ]]; then
  base_ref="$PERF_GATE_BASE_REF"
elif [[ "${GITHUB_EVENT_NAME:-}" == "pull_request" ]]; then
  base_branch="${GITHUB_BASE_REF:-main}"
  base_ref="$(git -C "$repo_root" merge-base HEAD "origin/$base_branch")"
elif git -C "$repo_root" rev-parse --verify HEAD^ >/dev/null 2>&1; then
  base_ref="$(git -C "$repo_root" rev-parse HEAD^)"
else
  echo "perf gate skipped: no suitable base ref"
  exit 0
fi

micro_bench="$(jq -r '.micro.bench' "$budget_path")"
micro_count="$(jq -r '.micro.count // 1' "$budget_path")"
micro_benchtime="$(jq -r '.micro.benchtime // ""' "$budget_path")"
e2e_profile="$(jq -r '.e2e.profile' "$budget_path")"
e2e_mining="$(jq -r '.e2e.mining' "$budget_path")"
e2e_nodes="$(jq -r '.e2e.nodes' "$budget_path")"
e2e_topology="$(jq -r '.e2e.topology' "$budget_path")"
e2e_tx_origin="$(jq -r '.e2e.tx_origin' "$budget_path")"
e2e_batch_size="$(jq -r '.e2e.batch_size // 64' "$budget_path")"
e2e_txs_per_block="$(jq -r '.e2e.txs_per_block' "$budget_path")"
e2e_blocks="$(jq -r '.e2e.blocks' "$budget_path")"
e2e_block_interval="$(jq -r '.e2e.block_interval' "$budget_path")"

run_gate_reports() {
  local worktree="$1"
  local outdir="$2"
  local -a micro_args

  micro_args=(
    go run ./cmd/bpu-cli bench micro
    --bench "$micro_bench"
    --count "$micro_count"
    --report "$outdir/micro.json"
    --markdown "$outdir/micro.md"
  )
  if [[ -n "$micro_benchtime" ]]; then
    micro_args+=(--benchtime "$micro_benchtime")
  fi

  mkdir -p "$outdir"
  (
    cd "$worktree"
    "${micro_args[@]}"
    go run ./cmd/bpu-cli bench e2e \
      --profile "$e2e_profile" \
      --mining "$e2e_mining" \
      --nodes "$e2e_nodes" \
      --topology "$e2e_topology" \
      --tx-origin "$e2e_tx_origin" \
      --batch-size "$e2e_batch_size" \
      --txs-per-block "$e2e_txs_per_block" \
      --block-interval "$e2e_block_interval" \
      --blocks "$e2e_blocks" \
      --report "$outdir/e2e.json" \
      --markdown "$outdir/e2e.md"
  )
}

echo "perf gate base ref: $base_ref"
git -C "$repo_root" worktree add --detach "$base_worktree" "$base_ref" >/dev/null

# The first perf-gate merge has to compare against a base commit that predates
# the budget file and gate wiring. Seed the contract once, then require the
# base branch to produce reports on subsequent runs.
if [[ ! -f "$base_worktree/benchmarks/perf-gate.json" ]]; then
  echo "perf gate bootstrap: base ref predates benchmarks/perf-gate.json, skipping comparison"
  exit 0
fi

run_gate_reports "$base_worktree" "$baseline_dir"
run_gate_reports "$repo_root" "$candidate_dir"

(
  cd "$repo_root"
  go run ./cmd/bpu-cli bench gate compare \
    --budget "$budget_path" \
    --baseline-dir "$baseline_dir" \
    --candidate-dir "$candidate_dir"
)
