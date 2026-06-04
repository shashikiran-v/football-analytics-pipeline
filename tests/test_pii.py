"""
Tests for src.pii.tokenize.

Three test classes:

1. TestTokenizeValue — unit tests of the tokeniser function itself.
   Determinism, NULL handling, salt sensitivity.

2. TestTokenizeColumnsInDataframe — DataFrame-level integration.
   Multi-column tokenisation, missing-column tolerance.

3. TestPIIIntegrationWithSilver — Silver layer end-to-end.
   PII columns in dim_players are tokenised when enabled;
   passthrough when disabled.

Why a separate file: the conftest auto-fixture sets PII_ENABLED=false
for every test (so the existing 400+ tests don't choke on tokenised
names). Tests in this file explicitly re-enable PII via
monkeypatch.setenv where needed.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.pii.tokenize import (
    TOKEN_HEX_LENGTH,
    TOKEN_PREFIX,
    get_salt,
    tokenize_column,
    tokenize_columns_in_dataframe,
    tokenize_value,
)

# ---------------------------------------------------------------------------
# tokenize_value — single-value behaviour
# ---------------------------------------------------------------------------


class TestTokenizeValue:
    """Single-value tokenisation."""

    def test_token_format(self):
        """Tokens start with the configured prefix and have the right length."""
        result = tokenize_value("Bukayo Saka", salt="test-salt")
        assert result.startswith(TOKEN_PREFIX)
        # Token = prefix + hex chars
        assert len(result) == len(TOKEN_PREFIX) + TOKEN_HEX_LENGTH
        # Hex chars are 0-9, a-f
        hex_part = result[len(TOKEN_PREFIX) :]
        assert all(c in "0123456789abcdef" for c in hex_part)

    def test_determinism_same_input_same_output(self):
        """Same input + same salt = same output. Critical for joinability."""
        t1 = tokenize_value("Bukayo Saka", salt="test-salt")
        t2 = tokenize_value("Bukayo Saka", salt="test-salt")
        assert t1 == t2

    def test_different_inputs_different_outputs(self):
        """Different inputs (almost always) produce different tokens.
        Collision possible but vanishingly rare at our scale."""
        t1 = tokenize_value("Bukayo Saka", salt="test-salt")
        t2 = tokenize_value("Cole Palmer", salt="test-salt")
        assert t1 != t2

    def test_different_salts_different_outputs(self):
        """Different salts produce different tokens for the same input.
        This is what protects against rainbow-table attacks."""
        t1 = tokenize_value("Bukayo Saka", salt="salt-one")
        t2 = tokenize_value("Bukayo Saka", salt="salt-two")
        assert t1 != t2

    def test_none_passes_through(self):
        """None input -> None output. Missing PII stays missing."""
        assert tokenize_value(None, salt="test-salt") is None

    def test_empty_string_is_tokenised(self):
        """Empty string is a valid value that gets a deterministic token,
        NOT mapped to None. An empty PII field is rare but legitimate."""
        result = tokenize_value("", salt="test-salt")
        assert result is not None
        assert result.startswith(TOKEN_PREFIX)

    def test_nan_passes_through(self):
        """Pandas delivers NaN for missing numeric/date values; treat
        the same as None (because NaN != NaN, isnan checks are awkward
        without numpy). Reasonable behaviour for a date column with
        missing values."""
        import numpy as np

        assert tokenize_value(np.nan, salt="test-salt") is None

    def test_numeric_inputs_are_stringified(self):
        """A numeric input is str()-converted before hashing. So
        tokenize_value(42) and tokenize_value("42") produce the same
        token — fine because PII columns are declared as string in
        the source registry."""
        t_int = tokenize_value(42, salt="test-salt")
        t_str = tokenize_value("42", salt="test-salt")
        assert t_int == t_str

    def test_unicode_handling(self):
        """Tokens are computed against UTF-8 encoded bytes. Names with
        non-ASCII characters (every other footballer) tokenise cleanly."""
        result = tokenize_value("José Mourinho", salt="test-salt")
        assert result is not None
        assert result.startswith(TOKEN_PREFIX)


# ---------------------------------------------------------------------------
# tokenize_column — list-level behaviour
# ---------------------------------------------------------------------------


class TestTokenizeColumn:
    def test_preserves_length(self):
        """A column of N values becomes a column of N tokens."""
        result = tokenize_column(
            ["Saka", "Palmer", "Mbappe", None, ""],
            salt="test-salt",
        )
        assert len(result) == 5

    def test_none_positions_preserved(self):
        """None values stay None at the same positions."""
        result = tokenize_column(["A", None, "B"], salt="test-salt")
        assert result[1] is None
        assert result[0] is not None
        assert result[2] is not None

    def test_consistent_tokens_across_columns(self):
        """Same value at different positions in different columns
        produces the same token. Required for cross-column joins."""
        col1 = tokenize_column(["Saka", "Palmer"], salt="test-salt")
        col2 = tokenize_column(["Mbappe", "Saka"], salt="test-salt")
        # "Saka" is at position 0 of col1 and position 1 of col2
        assert col1[0] == col2[1]


# ---------------------------------------------------------------------------
# get_salt — env var integration
# ---------------------------------------------------------------------------


class TestGetSalt:
    def test_reads_env_var(self, monkeypatch):
        """The salt comes from the env var named in config."""
        monkeypatch.setenv("PII_SALT", "from-env")
        # Clear config cache so the change is visible
        from src.utils.config import get_config

        get_config.cache_clear()
        assert get_salt() == "from-env"

    def test_empty_salt_raises(self, monkeypatch):
        """Empty salt is rejected — it would degenerate to unsalted
        SHA-256, which is trivially reversible via public rainbow tables."""
        monkeypatch.setenv("PII_SALT", "")
        from src.utils.config import get_config

        get_config.cache_clear()
        with pytest.raises(ValueError, match="salt env var"):
            get_salt()

    def test_unset_salt_raises(self, monkeypatch):
        """Unset salt is rejected — same reasoning as empty."""
        monkeypatch.delenv("PII_SALT", raising=False)
        from src.utils.config import get_config

        get_config.cache_clear()
        with pytest.raises(ValueError, match="salt env var"):
            get_salt()


# ---------------------------------------------------------------------------
# tokenize_columns_in_dataframe — DataFrame integration
# ---------------------------------------------------------------------------


class TestTokenizeColumnsInDataframe:
    def test_named_columns_tokenised(self, monkeypatch):
        """Listed columns are tokenised; others are left alone."""
        monkeypatch.setenv("PII_SALT", "test-salt")
        from src.utils.config import get_config

        get_config.cache_clear()
        df = pd.DataFrame(
            {
                "player_id": [1, 2, 3],
                "name": ["Saka", "Palmer", "Mbappe"],
                "age": [22, 22, 25],
            }
        )
        result = tokenize_columns_in_dataframe(df, ["name"])

        # name was tokenised
        assert all(v.startswith(TOKEN_PREFIX) for v in result["name"])
        # player_id and age were not
        assert list(result["player_id"]) == [1, 2, 3]
        assert list(result["age"]) == [22, 22, 25]

    def test_multiple_columns_tokenised(self, monkeypatch):
        """Multi-column tokenisation works in one pass."""
        monkeypatch.setenv("PII_SALT", "test-salt")
        from src.utils.config import get_config

        get_config.cache_clear()
        df = pd.DataFrame(
            {
                "first_name": ["Bukayo", "Cole"],
                "last_name": ["Saka", "Palmer"],
                "club_id": [1, 2],
            }
        )
        result = tokenize_columns_in_dataframe(df, ["first_name", "last_name"])

        assert all(v.startswith(TOKEN_PREFIX) for v in result["first_name"])
        assert all(v.startswith(TOKEN_PREFIX) for v in result["last_name"])

    def test_missing_columns_skipped_with_warning(self, monkeypatch):
        """If sources.yaml declares a PII column that isn't in the
        current Bronze partition (e.g. schema drift), we don't crash —
        we log and skip. Production environments expect schema drift
        to be tolerated as long as the rest of the source still works."""
        monkeypatch.setenv("PII_SALT", "test-salt")
        from src.utils.config import get_config

        get_config.cache_clear()
        df = pd.DataFrame({"name": ["Saka"], "age": [22]})
        # 'middle_name' isn't in the DataFrame
        result = tokenize_columns_in_dataframe(df, ["name", "middle_name"])
        # name still tokenised
        assert result["name"].iloc[0].startswith(TOKEN_PREFIX)
        # middle_name silently absent (no exception raised)
        assert "middle_name" not in result.columns

    def test_non_dataframe_input_raises(self, monkeypatch):
        """Type checking — the function is Pandas-only by design."""
        monkeypatch.setenv("PII_SALT", "test-salt")
        from src.utils.config import get_config

        get_config.cache_clear()
        with pytest.raises(TypeError, match="Pandas DataFrame"):
            tokenize_columns_in_dataframe([1, 2, 3], ["x"])  # type: ignore


# ---------------------------------------------------------------------------
# Silver integration — end-to-end check that PII reaches dim_players
# ---------------------------------------------------------------------------


class TestPIIIntegrationWithSilver:
    """
    The Silver dim_players builder is the production consumer of the
    tokeniser. These tests assert the integration is correctly wired:
    PII columns from sources.yaml are tokenised when the env says
    enabled, and pass through otherwise.

    Imports the actual builder to avoid mocking. The cost is each
    test takes ~1s (loads config, reads sample data); the benefit is
    we catch integration regressions, not just unit regressions.
    """

    def test_dim_players_tokenised_when_pii_enabled(self, monkeypatch):
        """With PII_ENABLED=true and a valid PII_SALT, dim_players'
        name column should contain tokens, not raw plaintext names."""
        monkeypatch.setenv("PII_ENABLED", "true")
        monkeypatch.setenv("PII_SALT", "test-salt-integration")

        from src.bronze.run import run_bronze
        from src.silver.run import run_silver
        from src.utils.config import get_config

        get_config.cache_clear()

        run_bronze(batch_id="pii-on-test", raw_root="data/sample")
        run_silver(batch_id="pii-on-test")

        cfg = get_config()
        dim_players = pd.read_parquet(cfg.paths.silver / "dim_players")

        # The original sample data has player names like 'Bukayo Saka'.
        # If PII is on, ALL name values should be tokens.
        assert all(v.startswith(TOKEN_PREFIX) for v in dim_players["name"]), (
            f"Expected all names to be tokens; got " f"{dim_players['name'].head().tolist()}"
        )
        # Same for the other declared PII columns
        for col in ("first_name", "last_name"):
            if col in dim_players.columns:
                assert all(v.startswith(TOKEN_PREFIX) or v is None for v in dim_players[col])

    def test_dim_players_plaintext_when_pii_disabled(self, monkeypatch):
        """With PII_ENABLED=false, name column should be raw player
        names — the existing test data uses real footballer names."""
        monkeypatch.setenv("PII_ENABLED", "false")

        from src.bronze.run import run_bronze
        from src.silver.run import run_silver
        from src.utils.config import get_config

        get_config.cache_clear()

        run_bronze(batch_id="pii-off-test", raw_root="data/sample")
        run_silver(batch_id="pii-off-test")

        cfg = get_config()
        dim_players = pd.read_parquet(cfg.paths.silver / "dim_players")

        # At least one name should NOT be a token (i.e. is a real name).
        assert not all(
            v.startswith(TOKEN_PREFIX) for v in dim_players["name"]
        ), "Expected plaintext names when PII is disabled"
