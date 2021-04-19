"""An Apache Beam bounded source for PostgreSQL"""

from typing import Union

from apache_beam.io import iobase
from apache_beam.options.value_provider import ValueProvider

from postgres_connector.splitters import BaseSplitter
from postgres_connector.client import PostgresClient
from postgres_connector.utils import clean_query
from postgres_connector.utils import get_runtime_value


class PostgresSource(iobase.BoundedSource):
    """A source object of mysql."""

    def __init__(
        self,
        query: Union[str, ValueProvider],
        host: Union[str, ValueProvider],
        database: Union[str, ValueProvider],
        user: Union[str, ValueProvider],
        password: Union[str, ValueProvider],
        port: Union[int, ValueProvider],
        splitter: BaseSplitter,
    ):
        super().__init__()
        self._query = query
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port

        self._is_initialized = False

        self._config = {
            "host": self._host,
            "database": self._database,
            "user": self._user,
            "password": self._password,
            "port": self._port,
        }

        self._splitter = splitter

    def estimate_size(self):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        if not self._is_initialized:
            self._initialize()

        return self._splitter.estimate_size()

    def get_range_tracker(self, start_position, stop_position):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        if not self._is_initialized:
            self._initialize()

        return self._splitter.get_range_tracker(start_position, stop_position)

    def read(self, range_tracker):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        for record in self._splitter.read(range_tracker):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.split`"""
        if not self._is_initialized:
            self._initialize()

        for split in self._splitter.split(desired_bundle_size, start_position, stop_position):
            yield split

    def _initialize(self):
        for k, v in self._config.items():
            self._config[k] = get_runtime_value(v)

        self.query = clean_query(get_runtime_value(self._query))
        self.client = PostgresClient(self._config)
        self._splitter.build_source(self)

        self._is_initialized = True
