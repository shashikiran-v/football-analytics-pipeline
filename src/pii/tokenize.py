"""
PII tokenisation via salted SHA-256.

Design philosophy
-----------------
The pipeline ingests data that contains identifiable information about
real people — primarily player names and dates of birth. Even when the
data is "public" (e.g. famous footballers), the principle of least
privilege says downstream analytical layers shouldn't see the raw
identifiers if they don't need them.

The tokeniser:

  - Reads PII columns named declaratively in `configs/sources.yaml`
    under each source's `pii.hash_columns` field (see ADR-0012).
  - Replaces each value with a SHA-256 hash of (salt + value), keeping
    only the first 8 hex chars to balance collision resistance and
    output compactness.
  - Is deterministic: same salt + same value → same token, so joins
    on tokenised columns still work.

Why salted SHA-256 (not HMAC, not Faker, not encryption)?
---------------------------------------------------------
- Salted SHA-256 is the smallest reasonable cryptographic primitive
  that resists trivial rainbow-table attacks. 8 hex chars (32 bits)
  is enough to distinguish ~10^5 distinct values with low collision
  probability — sufficient for the brief's data scale.
- HMAC is cryptographically stronger but the additional guarantees
  (key separation, length-extension resistance) aren't needed for
  the threat model: we're protecting against accidental disclosure
  and casual reverse-engineering, not adversaries with controlled-
  input attacks.
- Faker produces realistic-looking fakes but is non-deterministic by
  default, breaking joins. It also doesn't reduce reversibility — an
  attacker who can run Faker can produce the same outputs.
- Encryption is reversible by design, which is the opposite of what
  we want for analytical layers. Encryption is appropriate for
  fields that may need to be unmasked under controlled access (e.g.
  GDPR data-subject-access requests), but that's a separate concern.

ADR-0012 covers the design choice and rejected alternatives in detail.

Reversibility
-------------
The transformation is NOT reversible from the token alone, but IS
reversible if you have access to:
  1. The salt (read from `PII_SALT` env var, never committed)
  2. A lookup table mapping plaintext → token (which we don't build —
     adding one would defeat the point)

In production this means an operator needs the secrets-manager salt
AND a brute-force search over likely plaintext to undo the tokens.
For famous footballers in our sample data that's tractable; for the
production brief's data scale (~30K players) it's noticeably harder.

Production hardening (out of scope here, captured for the design doc):

  - Use HMAC-SHA256 instead of plain SHA-256 to resist length-extension
    style attacks. Trivial code change.
  - Rotate the salt periodically; tokens from prior salts can be
    re-tokenised with the new salt via a one-shot ETL job.
  - Store the salt in a secrets manager (AWS Secrets Manager, HashiCorp
    Vault) rather than an env var.
  - Add an audit log entry for every tokenisation operation so
    downstream privacy investigations can trace what data was touched.

For the brief, salted SHA-256 with an env-var salt is the right scope.
"""

from __future__ import annotations

import hashlib
import os
from typing import Any

from src.utils.logging import get_logger

log = get_logger(__name__)

# Length of the hex prefix kept in the final token. 8 hex chars =
# 32 bits = ~4.3 billion possible values. For player counts under
# 10^5, collision probability is negligible (see ADR-0012).
TOKEN_HEX_LENGTH = 8

# Prefix that goes on every tokenised value, so it's visually obvious
# in dim_players output that the value has been tokenised rather than
# being some other 8-character string.
TOKEN_PREFIX = "pii_"


def get_salt() -> str:
    """
    Read the PII salt from the environment variable named in config.

    Raises ValueError if PII is enabled but the salt env var is unset
    or empty. The pipeline should refuse to tokenise with an empty
    salt — that would degenerate to unsalted SHA-256, which is
    trivially reversible via public rainbow tables.

    The env var name itself comes from `config.pii.salt_env_var`
    (default "PII_SALT"). This lets different deployments use
    different env var names without rebuilding code.
    """
    from src.utils.config import get_config

    env_var = get_config().pii.salt_env_var
    salt = os.environ.get(env_var, "")
    if not salt:
        raise ValueError(
            f"PII tokenisation is enabled but the salt env var "
            f"{env_var!r} is unset or empty. Set it before running "
            f"the pipeline, e.g. `export {env_var}=<random-string>`. "
            f"Never commit the salt value."
        )
    return salt


def tokenize_value(value: Any, salt: str) -> str | None:
    """
    Tokenise a single value with the given salt.

    Args:
        value: The plaintext value. May be a string, int, float, date,
               or None. None passes through (preserving missingness).
               Anything else is str()-converted before hashing.
        salt:  The salt string. Must be non-empty (validate via get_salt()).

    Returns:
        A token of the form "pii_<8-hex-chars>", or None if the input
        was None. The token is deterministic for a given (value, salt)
        pair — same input always produces same output.

    Notes:
        * Empty strings are tokenised (not passed through as None).
          An empty-string PII value is unusual but if it occurs, we
          want it represented as a deterministic token, not None.
        * Numeric values are str()-converted first. This is fine for
          PII purposes (we want the value, not the type) but it means
          tokenize_value(42, salt) and tokenize_value("42", salt)
          produce the same token. In practice PII columns are
          declared as strings in sources.yaml, so this doesn't bite.
    """
    if value is None:
        return None

    # Pandas can deliver NaN for missing numeric/date values; treat
    # those as None too. (NaN != NaN in IEEE 754, hence this dance.)
    if isinstance(value, float) and value != value:
        return None

    payload = (salt + str(value)).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return f"{TOKEN_PREFIX}{digest[:TOKEN_HEX_LENGTH]}"


def tokenize_column(values: list[Any], salt: str) -> list[str | None]:
    """
    Tokenise every value in a column. Used by engine adapters that
    need to apply the tokeniser to a Pandas Series or Spark column.

    Returns a new list of the same length as the input. None and NaN
    inputs produce None outputs.
    """
    return [tokenize_value(v, salt) for v in values]


def tokenize_columns_in_dataframe(
    df: Any,  # actually pd.DataFrame at runtime, but kept generic for engine portability
    columns: list[str],
) -> Any:
    """
    Apply tokenisation to the named columns of a DataFrame in place.

    The function reads the salt once (via get_salt()) and applies
    tokenize_value to every cell of every named column. Columns not
    present in the DataFrame are silently skipped with a warning —
    this is forgiving on purpose, because sources.yaml may declare
    columns that some Bronze partitions don't actually contain
    (e.g. when an upstream schema drift drops a column).

    Args:
        df:      A Pandas DataFrame. Modified in place AND returned.
        columns: Column names to tokenise. Columns not present are
                 logged and skipped.

    Returns:
        The same DataFrame, with named columns tokenised.

    Why in-place + return? Idiomatic Pandas users expect either, so
    we support both. Callers can chain or modify-then-use freely.
    """
    import pandas as pd  # local import keeps this module engine-agnostic

    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            f"tokenize_columns_in_dataframe expects a Pandas DataFrame, "
            f"got {type(df).__name__}. Spark support is via a separate "
            f"engine method (see src/engines/spark_engine.py)."
        )

    salt = get_salt()

    for col in columns:
        if col not in df.columns:
            log.warning(
                "pii_column_not_in_dataframe",
                column=col,
                available_columns=list(df.columns),
                reason="skipping; sources.yaml may declare columns not present in this partition",
            )
            continue
        df[col] = df[col].apply(lambda v, s=salt: tokenize_value(v, s))
        log.info(
            "pii_column_tokenised",
            column=col,
            rows=len(df),
        )

    return df
