"""PII tokenisation for the Silver layer.

See ADR-0012 for the design choice and src/pii/tokenize.py for
implementation details. Public surface kept small on purpose:
the rest of the pipeline only needs to know about
`tokenize_columns_in_dataframe` and `get_salt`.
"""

from src.pii.tokenize import (
    TOKEN_HEX_LENGTH,
    TOKEN_PREFIX,
    get_salt,
    tokenize_column,
    tokenize_columns_in_dataframe,
    tokenize_value,
)

__all__ = [
    "TOKEN_HEX_LENGTH",
    "TOKEN_PREFIX",
    "get_salt",
    "tokenize_column",
    "tokenize_columns_in_dataframe",
    "tokenize_value",
]
