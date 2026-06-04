"""
Row hashing for SCD2 change detection.

When merging incoming rows into a Type-2 dimension, we need a cheap way
to decide whether a record has changed. The classical approach is to
compare every tracked column individually. A hash of the concatenated
tracked columns reduces that to a single equality check.

This module defines the canonical hashing contract (algorithm, encoding,
null handling, separator). Each engine implements `add_row_hash` natively
but must produce the same hash for the same input — otherwise the
pandas and spark code paths could disagree about what "changed" means.

Canonical algorithm:

    md5( col1_value || U+241F || col2_value || U+241F || ... )

  - Values are stringified with str(); NaN/None become the literal "<NULL>".
  - Separator is U+241F (SYMBOL FOR UNIT SEPARATOR), chosen because no
    realistic football data field contains it.
  - md5 is fine: this is a change-detection hash, not a cryptographic
    primitive. Collisions for our row counts are astronomically unlikely.
"""

from __future__ import annotations

import hashlib

# Unit-separator code point. Used between values so column boundaries
# can't be ambiguous (e.g. "Foo" + "Bar" vs "Foob" + "ar").
HASH_SEPARATOR = "\u241f"

# Sentinel for missing values. Picking a fixed string avoids the trap
# where None vs NaN vs empty string would hash differently.
NULL_SENTINEL = "<NULL>"


def hash_row(values: list[object]) -> str:
    """
    Reference implementation. Engines must produce the same output for
    the same logical inputs (in the same column order).

    Used by tests to cross-check the pandas and spark engines.
    """
    parts: list[str] = []
    for v in values:
        if v is None:
            parts.append(NULL_SENTINEL)
            continue
        # pandas NaN is a float; isinstance(v, float) and v != v catches it
        # without importing numpy/pandas here (keeps this module pure).
        if isinstance(v, float) and v != v:
            parts.append(NULL_SENTINEL)
            continue
        parts.append(str(v))
    joined = HASH_SEPARATOR.join(parts)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()
