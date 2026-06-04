"""
Tests for src.utils.checksums.

Covers:
  - file MD5: known values for known content, streaming correctness
              for files larger than a single chunk, edge cases
              (empty file, missing file)
  - schema hash: determinism, order-invariance, sensitivity to type
                 changes, sentinel value for empty schemas
"""

from __future__ import annotations

import hashlib

import pytest

from src.utils.checksums import (
    ChecksumError,
    file_checksum_md5,
    schema_version_hash,
)

# Known MD5s for canonical reference strings — verifiable with any
# external MD5 tool, which lets the test suite double as documentation
# of the algorithm we're using.
EMPTY_MD5 = "d41d8cd98f00b204e9800998ecf8427e"
HELLO_MD5 = "5d41402abc4b2a76b9719d911017c592"  # "hello"


# ---------------------------------------------------------------------------
# file_checksum_md5
# ---------------------------------------------------------------------------


class TestFileChecksum:
    def test_empty_file_md5(self, tmp_path):
        f = tmp_path / "empty"
        f.write_bytes(b"")
        assert file_checksum_md5(f) == EMPTY_MD5

    def test_known_value_md5(self, tmp_path):
        f = tmp_path / "hello"
        f.write_bytes(b"hello")
        assert file_checksum_md5(f) == HELLO_MD5

    def test_missing_file_raises_checksum_error(self, tmp_path):
        with pytest.raises(ChecksumError, match="non-file"):
            file_checksum_md5(tmp_path / "does_not_exist.csv")

    def test_directory_raises_checksum_error(self, tmp_path):
        # Important: hashing a directory must fail, not silently succeed.
        with pytest.raises(ChecksumError, match="non-file"):
            file_checksum_md5(tmp_path)

    def test_streaming_matches_single_shot(self, tmp_path):
        """
        The critical correctness property of streaming: chunked reads
        must produce the SAME hash as reading the whole file at once.
        We construct a file larger than the default chunk size and
        compare both approaches.
        """
        f = tmp_path / "big"
        # 200 KiB of pseudo-random bytes — comfortably bigger than the
        # 64 KiB chunk size, so the read loop iterates multiple times.
        content = bytes(range(256)) * 800  # ~200 KB, deterministic
        f.write_bytes(content)

        streamed = file_checksum_md5(f)
        single_shot = hashlib.md5(content).hexdigest()
        assert streamed == single_shot

    def test_small_chunk_size_still_correct(self, tmp_path):
        """
        Hash with a tiny chunk size to force many iterations; result
        must still match. This guards against off-by-one errors in
        the streaming loop (e.g. dropping the final partial chunk).
        """
        f = tmp_path / "data"
        content = b"A" * 1000
        f.write_bytes(content)

        chunked = file_checksum_md5(f, chunk_size=7)  # awkward size
        single_shot = hashlib.md5(content).hexdigest()
        assert chunked == single_shot

    def test_accepts_string_path_as_well_as_path_object(self, tmp_path):
        f = tmp_path / "x"
        f.write_bytes(b"hi")
        # Both forms must work — Path objects from internal callers,
        # plain strings from CLI flags or tests.
        from_path = file_checksum_md5(f)
        from_str = file_checksum_md5(str(f))
        assert from_path == from_str

    def test_digest_is_32_lowercase_hex(self, tmp_path):
        f = tmp_path / "x"
        f.write_bytes(b"hi")
        digest = file_checksum_md5(f)
        assert len(digest) == 32
        assert digest == digest.lower()
        assert all(c in "0123456789abcdef" for c in digest)


# ---------------------------------------------------------------------------
# schema_version_hash
# ---------------------------------------------------------------------------


class TestSchemaHash:
    def test_empty_schema_returns_empty_md5(self):
        # Documented sentinel — useful for "no schema observed yet".
        assert schema_version_hash({}) == EMPTY_MD5

    def test_deterministic(self):
        schema = {"player_id": "int", "name": "string"}
        assert schema_version_hash(schema) == schema_version_hash(schema)

    def test_order_invariant(self):
        """
        Same columns and types in a different dict insertion order
        must yield the same hash. dict() in Python preserves insertion
        order, so without sorting this would silently differ.
        """
        a = {"player_id": "int", "name": "string"}
        b = {"name": "string", "player_id": "int"}
        assert schema_version_hash(a) == schema_version_hash(b)

    def test_sensitive_to_added_column(self):
        before = {"player_id": "int"}
        after = {"player_id": "int", "name": "string"}
        assert schema_version_hash(before) != schema_version_hash(after)

    def test_sensitive_to_type_change(self):
        # The most subtle drift: same column name, different declared type.
        # (e.g. player_id was int yesterday, vendor sent it as string today)
        before = {"player_id": "int"}
        after = {"player_id": "string"}
        assert schema_version_hash(before) != schema_version_hash(after)

    def test_sensitive_to_column_rename(self):
        before = {"first_name": "string"}
        after = {"firstname": "string"}
        assert schema_version_hash(before) != schema_version_hash(after)

    def test_digest_is_32_lowercase_hex(self):
        digest = schema_version_hash({"a": "int", "b": "string"})
        assert len(digest) == 32
        assert digest == digest.lower()
        assert all(c in "0123456789abcdef" for c in digest)
