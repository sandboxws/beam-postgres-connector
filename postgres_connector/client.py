"""A client of PostgreSQL."""

from logging import INFO, getLogger
from typing import Dict
from typing import Generator
from typing import List


from postgres_connector.errors import PostgresClientError, PostgresConnectorError

_SELECT_STATEMENT = "SELECT"

logger = getLogger(__name__)
logger.setLevel(INFO)


class PostgresClient:
    """A PostgreSQL client object."""

    def __init__(self, config: Dict):
        self._config = config
        self._validate_config(self._config)

    def record_generator(self, query: str, dictionary=True) -> Generator[Dict, None, None]:
        """
        Generate dict record from raw data under PostgeSQL

        Args:
            query: query with select statement
            dictionary: the type of result is dict if true else tuple

        Returns:
            dict record

        Raises:
            ~postgres_connector.errors.PostgresClientError
        """
        self._validate_query(query, [_SELECT_STATEMENT])

        with _PostgresConnection(self._config) as conn:
            # buffered is false because it can be assumed that the data size is too large
            cur = conn.cursor(buffered=False, dictionary=dictionary)

            try:
                cur.execute(query)
                logger.info(f"Successfully execute query: {query}")

                for record in cur:
                    yield record
            except PostgresConnectorError as e:
                raise PostgresClientError(
                    f"Failed to execute query: {query}, Raise exception: {e}")

            cur.close()

    def counts_estimator(self, query: str) -> int:
        """
        Returns an estimate of the total number of records.

        Args:
            query: query with select statement

        Returns:
            the total number of records

        Raises:
            ~postgres_connector.errors.PostgresClientError
        """
        self._validate_query(query, [_SELECT_STATEMENT])
        count_query = f"SELECT COUNT(*) AS count FROM ({query}) as subq"

        with _PostgresConnection(self._config) as conn:
            # buffered is false because it can be assumed that the data size is too large
            cur = conn.cursor(buffered=False, dictionary=True)

            try:
                cur.execute(count_query)
                logger.info(f"Successfully execute query: {count_query}")

                record = cur.fetchone()
            except PostgresConnectorError as e:
                raise PostgresClientError(
                    f"Failed to execute query: {count_query}, Raise exception: {e}")

            cur.close()

            return record["count"]

    def rough_counts_estimator(self, query: str) -> int:
        """
        Make a rough estimate of the total number of records.
        To avoid waiting time by select counts query when the data size is too large.

        Args:
            query: query with select statement

        Returns:
            the total number of records

        Raises:
            ~postgres_connector.errors.PostgresClientError
        """
        self._validate_query(query, [_SELECT_STATEMENT])
        count_query = f"EXPLAIN SELECT * FROM ({query}) as subq"

        with _PostgresConnection(self._config) as conn:
            # buffered is false because it can be assumed that the data size is too large
            cur = conn.cursor(buffered=False, dictionary=True)

            try:
                cur.execute(count_query)
                logger.info(f"Successfully execute query: {count_query}")

                records = cur.fetchall()

                total_number = 0

                for record in records:
                    if record["select_type"] in ("PRIMARY", "SIMPLE"):
                        total_number = record["rows"]
            except PostgresConnectorError as e:
                raise PostgresClientError(
                    f"Failed to execute query: {count_query}, Raise exception: {e}")

            cur.close()

            if total_number <= 0:
                raise PostgresConnectorError(
                    f"Failed to estimate total number of records. Query: {count_query}")
            else:
                return total_number

    @staticmethod
    def _validate_config(config: Dict):
        required_keys = {"host", "port", "database", "user", "password"}
        if not config.keys() == required_keys:
            raise PostgresClientError(
                f"Config is not satisfied. required: {required_keys}, actual: {config.keys()}")

    @staticmethod
    def _validate_query(query: str, statements: List[str]):
        query = query.lstrip()

        for statement in statements:
            if statement and not query.lower().startswith(statement.lower()):
                raise PostgresClientError(
                    f"Query expected to start with {statement} statement. Query: {query}")

class _PostgresConnection:
    """A wrapper object to connect PostgreSQL."""

    def __init__(self, _config: Dict):
        self._config = _config

    def __enter__(self):
        try:
            self.conn = conn = psycopg2.connect(**self._config)
            return self.conn
        except PostgresConnectorError as e:
            raise PostgresClientError(f"Failed to connect postgres, Raise exception: {e}")

    def __exit__(self, exception_type, exception_value, traceback):
        self.conn.close()
