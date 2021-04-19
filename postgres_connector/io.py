"""Apache BEAM I/O connector of PostgreSQL."""

from typing import Dict
from typing import Union

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.options.value_provider import ValueProvider
from apache_beam.pvalue import PCollection
from apache_beam.transforms.core import PTransform

from postgres_connector import splitters
from postgres_connector.client import PostgresClient
from postgres_connector.source import PostgresSource


class ReadFromPostgres(PTransform):
    """Create PCollection from PostgreSQL."""

    def __init__(
        self,
        query: Union[str, ValueProvider],
        host: Union[str, ValueProvider],
        database: Union[str, ValueProvider],
        user: Union[str, ValueProvider],
        password: Union[str, ValueProvider],
        port: Union[int, ValueProvider] = 3306,
        splitter=splitters.NoSplitter(),
    ):
        super().__init__()
        self._query = query
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port
        self._splitter = splitter

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | iobase.Read(
            PostgresSource(self._query, self._host, self._database, self._user, self._password, self._port, self._splitter)
        )
