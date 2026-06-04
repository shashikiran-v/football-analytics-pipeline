# ADR-0012: PII Tokenisation Design

## Status

Accepted — 2026-06-04

## Context

The pipeline ingests data that contains identifiable information about
real people — primarily player names, dates of birth, cities of birth,
and image URLs. Even when the underlying data is "public" (famous
footballers in the case of the Kaggle dataset), the principle of
least privilege says downstream analytical layers shouldn't see raw
identifiers if they don't need them.

The brief implicitly assumes professional handling of sensitive data;
this ADR documents how the pipeline implements that handling.

Five questions:

1. **Where in the pipeline does tokenisation happen?**
2. **Which columns get tokenised, and how is that configured?**
3. **What's the tokenisation function — what algorithm, what output shape?**
4. **What threat model are we defending against?**
5. **What production hardening is deliberately out of scope?**

## Decision

### Tokenise in Silver as a post-transform step

PII columns are tokenised in Silver, between the projection step and
the SCD2 merge in `src/silver/dimensions.py`. Bronze keeps raw values
(it's the audit-grade landing zone — see ADR-0003); Silver onwards
sees only tokens.

**Placement matters: tokenisation happens BEFORE the SCD2 hash is
computed.** This is the subtle but critical detail. SCD2's change
detection works by hashing tracked columns and comparing against the
existing current version's hash. If we tokenised AFTER the hash, the
hash would be computed against raw values, then tokenisation would
happen, and on re-runs the comparison would be apples-to-oranges. By
tokenising first, the dimension's source-of-truth IS the tokenised
data; SCD2's deterministic hash then works correctly across multi-
batch runs (tokenisation never spuriously looks like a change because
same-input-same-salt always produces the same token).

**Rejected: tokenise at Bronze write.** Would lose audit-grade
preservation of the source bytes. If a downstream investigation
needed to verify exactly what the source said, we'd have nothing
to verify against. Bronze remains the immutable source-of-truth.

**Rejected: a separate "Platinum" layer between Silver and Gold.**
Two parquet copies, more storage, more lake complexity. Adds
infrastructure without clearly improving the privacy story; the
right pattern is to tokenise at the earliest layer that doesn't
need raw values (Silver), not to add a layer specifically for
tokenisation.

### Declarative configuration via `sources.yaml`

Each source declares its PII columns in a `pii.hash_columns` block:

```yaml
players:
  pii:
    hash_columns:
      - first_name
      - last_name
      - name
      - date_of_birth
      - city_of_birth
```

The Pydantic source registry parses this via `PIISpec`
(`src/ingestion/registry.py`), and the Silver builder
(`_maybe_apply_pii_tokenisation` in `src/silver/dimensions.py`)
reads the declared columns at runtime.

Top-level config flags control whether tokenisation actually runs:

```yaml
pii:
  enabled: ${PII_ENABLED:true}    # default ON in production
  salt_env_var: PII_SALT           # name of the env var carrying the salt
```

This matches the rest of the codebase's "declarative config + Pydantic
typed access" pattern (see ADR-0002 for the source registry framework).

**Rejected: hard-code PII columns in `src/pii/`.** Would couple PII
policy to specific source schemas. New sources would require Python
code changes; current pattern is YAML-only.

**Rejected: a separate `pii.yaml` policy file.** Could express more
sophisticated policies (per-environment masking levels, role-based
visibility, retention rules). Genuine value at scale, overkill for
this brief. Deferred. The config reserves a `pii.config_path` field
for the future expansion.

### Salted SHA-256, 8-hex-char output

The tokeniser produces `pii_<8-hex-chars>` from `SHA-256(salt + value)`:

```python
def tokenize_value(value: Any, salt: str) -> str | None:
    if value is None:
        return None
    payload = (salt + str(value)).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return f"pii_{digest[:8]}"
```

Three design choices in this small function:

1. **SHA-256, not MD5/SHA-1/HMAC.** SHA-256 is the smallest standard
   cryptographic primitive widely considered safe for indefinite
   future use. MD5 and SHA-1 are deprecated for collision resistance.
   HMAC is cryptographically cleaner but the additional guarantees
   (key separation, length-extension resistance) aren't needed for
   this threat model — see "Threat model" below.

2. **8 hex chars (32 bits) of output.** 4.3 billion possible values.
   Birthday-paradox collision probability for 10^5 distinct values
   (well above the brief's ~30K players) is ~10^-6 — i.e. one in a
   million chance that any two players in the dataset collide.
   Acceptable for analytics; intolerable would be ~10^-3 or worse.
   Going to 16 hex chars (64 bits) is trivial and gains another
   factor of 4 billion, but the output is twice as wide for no
   meaningful gain at our scale.

3. **`pii_` prefix.** Visual marker so it's immediately obvious in
   dim_players output that a value has been tokenised, rather than
   being some other arbitrary 8-character string. Also makes
   `WHERE name LIKE 'pii_%'` a trivial sanity check.

**Rejected: Faker library.** Produces realistic-looking fakes ("John
Smith" for "Bukayo Saka"). Non-deterministic by default — breaks
joins across dimensions and facts (a player would have different
tokens in `dim_players` vs `fact_appearances`). Also: an attacker
with access to Faker can produce the same outputs deterministically
if seeded, so the "looks reversible-protected" framing is misleading.

**Rejected: symmetric encryption (AES-GCM, ChaCha20).** Reversible
by design — opposite of what we want for analytical layers. The
threat model below explains: we're protecting against accidental
disclosure, not building a re-identification system. Encryption is
appropriate when fields may need legitimate unmasking under
controlled access (e.g. GDPR data-subject-access requests), but
that's a separate concern from "what does Silver expose to BI."

### Salt from env var; empty salt rejected

The salt is read from the env var named in `config.pii.salt_env_var`
(default `PII_SALT`). If the env var is unset or empty, the tokeniser
raises:

```python
ValueError("PII tokenisation is enabled but the salt env var
'PII_SALT' is unset or empty. ...")
```

This is a hard fail, not a fallback to an unsalted hash. An unsalted
SHA-256 of a common name is trivially reversible via public rainbow
tables — `sha256("Bukayo Saka")` is a permanent fingerprint anyone
can compute. Salting makes the tokens specific to this deployment.

**Test environment defaults PII OFF** via the conftest autouse
fixture (`tests/conftest.py`), so the 435 pre-existing tests assert
on plaintext names. PII-specific tests in `tests/test_pii.py`
opt back in with `monkeypatch.setenv("PII_ENABLED", "true")` plus
`monkeypatch.setenv("PII_SALT", "test-salt")`.

This is the same "production defaults on, test environment defaults
off, dedicated tests opt back in" pattern used in mature codebases
for cross-cutting concerns (think structured logging, telemetry,
auth context). Tests that don't care about PII don't break when
the PII story changes; tests that do care assert explicitly.

## Threat model

What we're defending against:

1. **Casual disclosure.** A BI user with read access to
   `dim_players` can't tell that a particular row is "Bukayo Saka"
   just by looking. Even reading the entire dimension doesn't
   reveal names.

2. **Accidental data leakage.** A parquet file exported to a less-
   trusted environment doesn't contain plaintext names. If someone
   forwards it via email, posts it to Slack, or commits it to the
   wrong git repo, the names aren't there.

3. **Limited reverse-engineering effort.** An attacker who gets a
   token but not the salt has to either:
   - Mount a brute-force attack against the salt (unsalted SHA-256
     would be a one-shot rainbow-table lookup)
   - Have an a-priori candidate list AND the salt to verify

What we're NOT defending against:

1. **Adversarial reverse-engineering with the salt.** Anyone with
   `PII_SALT` AND a candidate list (e.g. "the top 1000 Premier
   League players") can re-tokenise every candidate and match
   tokens to identities. The salt is the secret; without it, even
   common names are protected. With it, common names are trivially
   reversible.

2. **Side-channel inference.** Combining tokenised name + plaintext
   club + plaintext position narrows the candidate set significantly.
   "An Arsenal right winger" reveals an obvious candidate even with
   the name tokenised. True k-anonymity would also generalise
   quasi-identifiers; we don't, because it would destroy analytical
   value.

3. **Insider compromise.** A user with both the salt AND access to
   dim_players can re-identify trivially. This is a process problem
   (rotate access, audit logs), not a cryptographic one.

## Production hardening (deliberately out of scope)

For the brief, the design above is appropriate. A production
deployment at scale should add:

1. **Use HMAC-SHA256 instead of plain salted SHA-256.** Resists
   length-extension attacks (irrelevant for short inputs, but free
   protection). Trivial code change:
   ```python
   import hmac
   digest = hmac.new(salt.encode(), value.encode(), hashlib.sha256).hexdigest()
   ```

2. **Rotate the salt periodically.** Old tokens become unreversible
   to fresh attackers, even if the new salt is leaked. Rotation
   requires a one-shot ETL job to re-tokenise the lake with the
   new salt, preserving join keys; this is non-trivial but
   tractable.

3. **Store the salt in a secrets manager.** AWS Secrets Manager,
   HashiCorp Vault, GCP Secret Manager. Env-var-from-OS-environment
   is fine for a dev demo; production needs audit logging on salt
   access.

4. **Audit log every tokenisation operation.** Privacy investigations
   benefit enormously from being able to say "yes, we tokenised this
   row at this timestamp using salt version X." Today our audit DAO
   logs the pipeline run; an extension would log per-column PII
   touchpoints.

5. **Differential-privacy noise injection for aggregate queries.**
   Top-scorers-by-season Gold table doesn't directly reveal player
   names if the names are tokenised — but aggregate counts can still
   leak presence/absence of specific individuals if an attacker can
   query selectively. DP noise on aggregates is the standard
   countermeasure; deferred.

6. **Tiered tokenisation levels.** A "redact" level (replace with
   constant null), a "tokenise" level (current implementation), and
   a "preserve under access control" level (raw values, ACL-gated)
   for fields that some roles legitimately need. Today every PII
   column is treated identically; production would differentiate.

7. **Reversibility map for legitimate access.** GDPR data-subject-
   access requests (DSARs) require returning a user's full record.
   If a user asks "what data do you hold on me," we need to be able
   to find their tokenised rows. One pattern: keep a secure
   `pii_lookup` table (encrypted, ACL-gated) mapping plaintext to
   token, used only for DSARs.

None of these change the basic algorithm; they're operational
hardening around it.

## Consequences

**Gained:**

- **Configurable PII handling at the source-registry layer.** Adding
  a new source's PII columns is a YAML change, not Python code.
- **Production-on, test-off pattern** so existing tests aren't
  affected by enabling PII. New PII-specific tests opt in
  explicitly via env vars.
- **Deterministic same-input-same-token** means joins across
  dimensions and facts still work even when the underlying values
  are tokenised.
- **Hard fail on empty salt** prevents the most common misconfig
  (PII enabled but no salt set).
- **Tokenisation BEFORE SCD2 hash** so re-runs don't spuriously
  detect tokenised-vs-tokenised as a change.
- **Clear documentation of what we ARE and AREN'T defending
  against** (threat model section above). No false sense of
  cryptographic security.

**Given up:**

- **Reversibility is bounded.** A user who needs to reverse a
  token (e.g. for legitimate DSAR fulfillment) needs the salt
  AND a candidate list. We don't build a lookup table because
  building one would defeat the point.
- **Quasi-identifier protection is absent.** Position + club +
  age narrows identity even with name tokenised. True k-anonymity
  would also generalise these, but at significant analytical cost.
- **No tiered tokenisation levels.** Every PII column is treated
  identically; production would differentiate `redact` vs
  `tokenise` vs `preserve-under-ACL`.
- **No audit log for PII touchpoints.** The pipeline's audit DAO
  records batch runs but not per-column PII operations. Production
  investigations would benefit from this granularity.

## See also

- Implementation:
  - `src/pii/tokenize.py` (the tokeniser)
  - `src/pii/__init__.py` (public surface)
  - `src/silver/dimensions.py` (the `_maybe_apply_pii_tokenisation`
    wiring point)
  - `tests/test_pii.py` (21 tests covering single-value behaviour,
    column behaviour, env var integration, DataFrame helper, and
    end-to-end Silver integration)
  - `tests/conftest.py` (autouse fixture defaults PII OFF in tests)
  - `configs/sources.yaml` (per-source `pii.hash_columns` declarations)
  - `configs/config.yaml` (`pii.enabled` and `pii.salt_env_var`)

- Related:
  - ADR-0002 (Source Registry as a Framework) — the declarative
    YAML pattern this ADR extends
  - ADR-0003 (Bronze Storage and Partitioning) — why Bronze keeps
    raw values; the rationale for tokenising at Silver, not earlier
  - ADR-0005 (SCD Type 2 Implementation) — the hash-based merge
    that tokenisation must run BEFORE
