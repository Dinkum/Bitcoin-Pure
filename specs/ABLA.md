# ABLA.md
# Adaptive Block Size Limit Algorithm for BPU

## Status
This document is normative consensus specification.

## Purpose
ABLA defines the per-block consensus maximum serialized block size limit.
It replaces any fixed or window-based block size cap.

For each height `h`, ABLA derives:
- `epsilon(h)` = control size
- `beta(h)` = elastic buffer
- `L(h)` = `epsilon(h) + beta(h)`

A block at height `h` is valid only if its serialized size in bytes is
less than or equal to `L(h)`.

## Definitions

### Height and Size
- `h` = block height.
- `block_size(h)` = serialized size in bytes of the full block at height `h`,
  using the exact consensus block serialization.

### State
For each height `h`, ABLA state is:
- `epsilon(h)` : uint64
- `beta(h)`    : uint64
- `L(h)`       : uint64, where `L(h) = epsilon(h) + beta(h)`

ABLA state is consensus-derived.
It is not serialized in the block and is not committed in the header.

## Constants

### Floor values
- `EPSILON_FLOOR = 16_000_000`
- `BETA_FLOOR    = 16_000_000`
- `LIMIT_FLOOR   = 32_000_000`

### Response parameters
These constants instantiate ABLA with the same parameter values used by BCH mainnet ABLA:

- `ZETA_NUM   = 3`
- `ZETA_DEN   = 2`
- `GAMMA_DEN  = 37_938`
- `THETA_DEN  = 37_938`
- `DELTA      = 10`

Interpretation:
- `zeta  = 3 / 2`
- `gamma = 1 / 37938`
- `theta = 1 / 37938`
- `delta = 10`

### Safety clips
- `EPSILON_MAX = 2_837_960_626_724_546_304`
- `BETA_MAX    = 9_459_868_755_748_488_064`

These bounds guarantee that:
- `epsilon(h)` fits in uint64,
- `beta(h)` fits in uint64,
- `L(h)` fits in uint64,
- the specified arithmetic can be implemented safely with 128-bit intermediates.

## Initialization

ABLA is active from genesis.

Initial state:
- `epsilon(0) = EPSILON_FLOOR`
- `beta(0)    = BETA_FLOOR`
- `L(0)       = LIMIT_FLOOR`

## Arithmetic Rules

### Integer domains
- All persisted ABLA state values are unsigned 64-bit integers.
- All intermediate calculations MUST be performed exactly using integer arithmetic with enough precision to avoid overflow.
- Implementations MUST support at least 128-bit unsigned intermediate arithmetic, or arbitrary-precision integers.

### Division semantics
For nonnegative integers:
- `floor_div(a, b)` = integer division rounded downward, where `b > 0`.
- `ceil_div(a, b)`  = `(a + b - 1) / b`, rounded downward, where `b > 0`.

No floating-point arithmetic is permitted anywhere in consensus ABLA evaluation.

### Clamp helper
For any state transition input:
- `x = min(block_size(h-1), L(h-1))`

This clamp is consensus-required.

## Per-Block Update Rule

For each height `h > 0`, let:
- `E = epsilon(h-1)`
- `B = beta(h-1)`
- `Y = L(h-1) = E + B`
- `x = min(block_size(h-1), Y)`

Then derive the state for height `h` as follows.

### Case 1 — Positive response branch
If:

- `3 * x > 2 * E`

then compute:

- `dE_num = E * (3*x - 2*E)`
- `dE_den = 2 * (E + 3*B) * GAMMA_DEN`
- `dE = floor_div(dE_num, dE_den)`

- `E_next = E + dE`

- `decay = floor_div(B, THETA_DEN)`
- `B_next = B - decay + DELTA * dE`

Then apply floor and clip:

- `epsilon(h) = min(max(E_next, EPSILON_FLOOR), EPSILON_MAX)`
- `beta(h)    = min(max(B_next, BETA_FLOOR), BETA_MAX)`

### Case 2 — Non-positive response branch
Else (`3 * x <= 2 * E`) compute:

- `shrink_num = 2*E - 3*x`
- `shrink_den = 2 * GAMMA_DEN`
- `dE = ceil_div(shrink_num, shrink_den)`

- `E_next = E - dE`

- `decay = floor_div(B, THETA_DEN)`
- `B_next = B - decay`

Then apply floor and clip:

- `epsilon(h) = min(max(E_next, EPSILON_FLOOR), EPSILON_MAX)`
- `beta(h)    = min(max(B_next, BETA_FLOOR), BETA_MAX)`

### Final limit
After deriving `epsilon(h)` and `beta(h)`:

- `L(h) = epsilon(h) + beta(h)`

A block at height `h` is valid only if:

- `block_size(h) ≤ L(h)`

## Consensus Invariants
For all heights `h`:
- `epsilon(h) ≥ EPSILON_FLOOR`
- `beta(h) ≥ BETA_FLOOR`
- `L(h) = epsilon(h) + beta(h)`
- `L(h) ≥ LIMIT_FLOOR`

## Reorganization Semantics
ABLA state is chain-local.

On any reorganization:
- ABLA state MUST be recomputed along the selected active chain exactly as specified here.
- Nodes MAY cache derived ABLA state per block for efficiency.
- Cached state is not consensus data; the derived values are.

## Notes
- ABLA responds upward only when recent block size pressure is high enough.
- `epsilon` is the slow control term.
- `beta` is a decaying elastic buffer that preserves temporary surge headroom.
- This document defines the exact consensus algorithm for BPU.
- Non-consensus mining policy MAY choose to mine smaller blocks than `L(h)`, but no policy may treat a block larger than `L(h)` as valid.
