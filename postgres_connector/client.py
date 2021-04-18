"""A client of PostgreSQL."""

from logging import INFO, getLogger
from typing import Dict
from typing import Generator
from typing import List


from postgres_connector.errors import PostgresClientError

_SELECT_STATEMENT = "SELECT"

logger = getLogger(__name__)
logger.setLevel(INFO)
