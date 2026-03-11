# ASERT Difficulty Adjustment Specification

## 1. Scope

This document defines the consensus difficulty adjustment algorithm for Bitcoin Pure.

The algorithm is ASERT: **Absolutely Scheduled Exponentially Rising Targets**.

This specification defines:

- the consensus constants used by ASERT,
- the chain-policy rules that determine the ASERT anchor,
- the normative target computation algorithm,
- the exact integer and shift semantics required for cross-implementation consensus,
- the block validation rule for `nBits` under ASERT.

This specification is normative.

---

## 2. Definitions

- **Target**: the unsigned integer threshold that a block hash must be less than or equal to in order to satisfy proof of work.
- **Compact target / `nBits`**: the compact block-header encoding of a target.
- **Anchor**: the reference point from which ASERT computes the required target schedule.
- **Anchor height**: the block height of the anchor block.
- **Anchor bits**: the `nBits` value of the anchor block.
- **Anchor parent time**: the header timestamp (`nTime`) of the parent of the anchor block.
- **Candidate block**: the block currently being validated.
- **Parent block**: the direct parent of the candidate block.
- **Ideal block time**: the target inter-block interval in seconds.
- **Halflife**: the ASERT response constant in seconds.

---

## 3. Consensus Constants

The following constants are consensus-critical on mainnet:

- `IDEAL_BLOCK_TIME = 600` seconds
- `HALFLIFE = 86400` seconds
- `RADIX = 65536`
- `MAX_BITS = chain maximum target in compact form, as defined by chain parameters`
- `MAX_TARGET = bits_to_target(MAX_BITS)`

Interpretation:

- for every `HALFLIFE` seconds the chain is **behind** schedule, difficulty halves,
- for every `HALFLIFE` seconds the chain is **ahead of** schedule, difficulty doubles.

No floating-point arithmetic is permitted anywhere in ASERT computation.

---

## 4. External Normative Dependencies

This specification depends on the following consensus definitions, which MUST be defined elsewhere and treated as normative inputs to ASERT:

- `bits_to_target(bits) -> uint256`
- `target_to_bits(target) -> uint32`

These functions define conversion between compact target encoding and the canonical unsigned integer target representation.

If those functions are ambiguous, ASERT is ambiguous.

---

## 5. Chain Policy: Activation and Anchor

### 5.1 Mainnet activation model

On Bitcoin Pure mainnet, ASERT is active from block height `1`.

The genesis block is fixed by chain parameters and is not itself validated by ASERT.

### 5.2 Mainnet anchor model

For Bitcoin Pure mainnet, the ASERT anchor is the genesis block.

The anchor tuple is defined as:

- `anchor_height = 0`
- `anchor_bits = genesis.nBits`
- `anchor_parent_time = genesis.nTime - IDEAL_BLOCK_TIME`

This defines a virtual parent timestamp for the genesis block.

This rule ensures that if block `1` is mined exactly on schedule, its required target equals the genesis target.

### 5.3 Future upgrade-based anchor model

If a future consensus upgrade re-anchors or replaces the difficulty algorithm, that upgrade MUST define a new anchor selection rule explicitly.

A valid upgrade-style ASERT anchor rule is:

- the first block whose activation condition is true is the fork block,
- the parent of that fork block is the anchor block,
- the anchor uses:
  - the anchor block’s height,
  - the anchor block’s `nBits`,
  - the anchor block parent’s `nTime`.

This section is informative for future upgrades and is not part of current mainnet consensus.

---

## 6. Informative Closed-Form Equation

The ASERT schedule may be described by the following mathematical form:

`required_target = anchor_target * 2 ^ ((time_delta - IDEAL_BLOCK_TIME * (height_delta + 1)) / HALFLIFE)`

Where:

- `anchor_target = bits_to_target(anchor_bits)`
- `time_delta = eval_time - anchor_parent_time`
- `height_delta = eval_height - anchor_height`

This equation is informative only.

The normative consensus rule is the integer pseudocode in Section 7.

---

## 7. Normative Target Computation

### 7.1 Function contract

The following function computes the required `nBits` for a candidate block from the candidate block’s parent.

Inputs:

- `anchor_height`
- `anchor_parent_time`
- `anchor_bits`
- `eval_height`
- `eval_time`

Where:

- `eval_height` is the height of the candidate block’s parent,
- `eval_time` is the timestamp of the candidate block’s parent.

Output:

- the required `nBits` value for the candidate block.

### 7.2 Pseudocode

```text
ALGORITHM ASERT_REQUIRED_BITS is
    INPUT:
        anchor_height
        anchor_parent_time
        anchor_bits
        eval_height
        eval_time

    OUTPUT:
        required_bits

    PRECONDITION:
        eval_height >= anchor_height
        0 < bits_to_target(anchor_bits) <= MAX_TARGET

    CONSTANTS:
        IDEAL_BLOCK_TIME = 600
        HALFLIFE = 86400
        RADIX = 65536
        MAX_BITS
        MAX_TARGET = bits_to_target(MAX_BITS)

    anchor_target ← bits_to_target(anchor_bits)

    time_delta ← eval_time - anchor_parent_time
    height_delta ← eval_height - anchor_height

    exponent_fp ← trunc_div(
        ((time_delta - IDEAL_BLOCK_TIME * (height_delta + 1)) * RADIX),
        HALFLIFE
    )

    num_shifts ← arithmetic_shift_right(exponent_fp, 16)
    frac ← exponent_fp - num_shifts * RADIX

    factor ← arithmetic_shift_right(
                  195766423245049 * frac
                + 971821376 * pow(frac, 2)
                + 5127 * pow(frac, 3)
                + pow(2, 47),
                48
              ) + RADIX

    next_target ← anchor_target * factor

    IF num_shifts < 0 THEN
        next_target ← shift_right(next_target, -num_shifts)
    ELSE
        next_target ← shift_left(next_target, num_shifts)
    END IF

    next_target ← shift_right(next_target, 16)

    IF next_target == 0 THEN
        RETURN target_to_bits(1)
    END IF

    IF next_target > MAX_TARGET THEN
        RETURN MAX_BITS
    END IF

    RETURN target_to_bits(next_target)
END ALGORITHM
````

---

## 8. Required Operator Semantics

This section is consensus-critical.

### 8.1 Integer-only arithmetic

All ASERT calculations MUST use integer arithmetic only.

Floating-point arithmetic MUST NOT be used.

### 8.2 `trunc_div`

`trunc_div(a, b)` MUST divide signed integers and truncate toward zero.

Examples:

* `trunc_div(7, 3) = 2`
* `trunc_div(-7, 3) = -2`
* `trunc_div(7, -3) = -2`
* `trunc_div(-7, -3) = 2`

Division semantics that round toward negative infinity are invalid.

### 8.3 `arithmetic_shift_right`

`arithmetic_shift_right(x, n)` on signed integers MUST preserve the sign bit.

It MUST behave as floor-division by `2^n` for signed fixed-point decomposition.

Logical right shift on signed values is invalid here.

### 8.4 `shift_left`

`shift_left(x, n)` MUST behave as if performed on an unbounded non-negative integer.

Implementations using limited-width integer types MUST detect overflow and MUST emulate the result of an unlimited-width computation.

If an implementation can prove during computation that the final result would exceed `MAX_TARGET`, it MAY return `MAX_BITS` early.

### 8.5 Polynomial width

The polynomial computation of `factor` MUST be performed using at least 64-bit unsigned arithmetic, or wider.

Signed 64-bit arithmetic alone is insufficient.

### 8.6 `pow(x, y)`

`pow(x, y)` in this specification denotes exact integer exponentiation.

It does not denote floating-point exponentiation.

---

## 9. Block Validation Rule

For any candidate block `B` at height `h > 0` with parent block `P`:

1. Determine the active ASERT anchor according to Section 5.

2. Compute:

   `required_bits = ASERT_REQUIRED_BITS(anchor_height, anchor_parent_time, anchor_bits, P.height, P.nTime)`

3. The candidate block is valid under ASERT only if:

   * `B.nBits == required_bits`, and
   * `0 < bits_to_target(B.nBits) <= MAX_TARGET`, and
   * `uint256(hash256(B.header)) <= bits_to_target(B.nBits)`

There are no emergency difficulty adjustments, no special minimum-difficulty exceptions, and no timestamp-gap reset rules on mainnet unless another consensus specification explicitly adds them.

---

## 10. Genesis and Early-Chain Behavior

The genesis block is exempt from ASERT validation.

Block `1` is the first block validated under ASERT.

Given the genesis-anchor rule in Section 5.2:

* if `block_1.nTime = genesis.nTime + IDEAL_BLOCK_TIME`, then the required target for block `1` equals `genesis.nBits`,
* if block production runs ahead of schedule, target tightens smoothly,
* if block production falls behind schedule, target loosens smoothly,
* rounding error does not accumulate across blocks because ASERT is anchored to an absolute schedule.

---

## 11. Consensus Safety Requirements

An implementation is non-conforming if any of the following are true:

* it uses floating point for exponent or target computation,
* it uses floor division instead of truncation toward zero,
* it uses logical right shift where arithmetic right shift is required,
* it allows silent overflow to alter consensus results,
* it computes the target polynomial using insufficient integer width,
* it validates a candidate block against its own timestamp instead of its parent’s evaluation state,
* it uses the anchor block timestamp instead of the anchor parent timestamp.

Any such behavior may cause a chain split.

---

## 12. Recommended Conformance Tests

Every implementation SHOULD include test coverage for at least the following cases:

* block `1` exactly on schedule,
* block `1` early,
* block `1` late,
* several consecutive on-schedule blocks,
* large positive `time_delta`,
* large negative `time_delta`,
* `num_shifts < 0`,
* `num_shifts = 0`,
* `num_shifts > 0`,
* underflow to target `1`,
* overflow or saturation to `MAX_TARGET`,
* negative intermediate exponent values,
* cross-checks against a big-integer reference implementation.

At least one reference implementation using arbitrary-precision integers SHOULD be maintained for test-vector generation.

---

## 13. Implementation Guidance

Consensus code should expose ASERT as a pure function over:

* chain constants,
* anchor tuple,
* parent height,
* parent timestamp.

Do not mix:

* anchor selection,
* header encoding,
* proof-of-work hash checking,
* and target computation

into a single opaque routine.

Those concerns should remain separate even though all are consensus-critical.

---

## 14. Summary

Bitcoin Pure uses an integer-only ASERT difficulty schedule with:

* 600-second ideal spacing,
* 86400-second halflife,
* genesis-based absolute anchoring,
* Bitcoin-style compact target encoding,
* no floating point,
* no emergency reset rules on mainnet.

The pseudocode in Section 7 and the operator semantics in Section 8 are the normative source of truth for ASERT target computation.


[1]: https://upgradespecs.bitcoincashnode.org/2020-11-15-asert/ "2020-NOV-15 ASERT Difficulty Adjustment Algorithm (aserti3-2d) - Bitcoin Cash upgrade specifications"
