"""Postgres Connector error classes."""


class PostgresConnectorError(Exception):
    """Base class for all errors."""


class PostgresClientError(PostgresConnectorError):
    """An error specific to the PostgreSQL driver."""
