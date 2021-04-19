import re
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Callable, Iterator

from apache_beam.io import iobase
from apache_beam.io.range_trackers import (LexicographicKeyRangeTracker,
                                           OffsetRangeTracker,
                                           UnsplittableRangeTracker)
from dateutil.relativedelta import relativedelta


class BaseSplitter(metaclass=ABCMeta):
    """Splitters abstract class."""

    def build_source(self, source):
        """Build source on runtime."""
        self.source = source

    @abstractmethod
    def estimate_size(self):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        raise NotImplementedError()

    @abstractmethod
    def get_range_tracker(self, start_position, stop_position):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        raise NotImplementedError()

    @abstractmethod
    def read(self, range_tracker):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        raise NotImplementedError()

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.split`"""
        raise NotImplementedError()

class DateSplitter(BaseSplitter):
    """Split bounded source by dates."""
    pass

class IdsSplitter(BaseSplitter):
    """Split bounded source by any ids."""

    def __init__(self, generate_ids_fn: Callable[[], Iterator], batch_size: int = 1000000):
        self._generate_ids_fn = generate_ids_fn
        self._batch_size = batch_size

    def estimate_size(self):
        # TODO: unify the method of estimating
        return 0

    def get_range_tracker(self, start_position, stop_position):
        self._validate_query()
        return LexicographicKeyRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        if range_tracker.start_position() is None:
            ids = ",".join([f"'{id}'" for id in self._generate_ids_fn()])
        else:
            ids = range_tracker.start_position()

        query = self.source.query.format(ids=ids)
        for record in self.source.client.record_generator(query):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        self._validate_query()

        ids = []
        for generated_id in self._generate_ids_fn():
            ids.append(generated_id)
            if len(ids) == self._batch_size:
                yield self._create_bundle_source(desired_bundle_size, self.source, ids)
                ids.clear()
        print('***********')
        print(start_position)
        print(stop_position)
        print(desired_bundle_size)
        print(len(ids))
        print('***********')
        yield self._create_bundle_source(desired_bundle_size, self.source, ids)

    def _validate_query(self):
        condensed_query = self.source.query.lower().replace(" ", "")
        if re.search(r"notin\({ids}\)", condensed_query):
            raise ValueError(f"Not support 'not in' phrase: {self.source.query}")
        if not re.search(r"in\({ids}\)", condensed_query):
            example = "SELECT * FROM tests WHERE id IN ({ids})"
            raise ValueError(f"Require 'in' phrase and 'ids' key on query: {self.source.query}, e.g. '{example}'")

    @staticmethod
    def _create_bundle_source(desired_bundle_size, source, ids):
        if isinstance(ids, list):
            ids_str = ",".join([f"'{id}'" for id in ids])
        elif isinstance(ids, str):
            ids_str = ids
        else:
            raise ValueError(f"Unexpected ids: {ids}")

        return iobase.SourceBundle(
            weight=desired_bundle_size, source=source, start_position=ids_str, stop_position=None
        )

class LimitOffsetSplitter(BaseSplitter):
    """Split bounded source by limit and offset."""

    def __init__(self, batch_size: int = 1000000):
        self._batch_size = batch_size
        self._counts = 0

    def estimate_size(self):
        self._counts = self.source.client.counts_estimator(self.source.query)
        return self._counts

    def get_range_tracker(self, start_position, stop_position):
        if self._counts == 0:
            self._counts = self.source.client.counts_estimator(self.source.query)
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self._counts

        return LexicographicKeyRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        offset, limit = range_tracker.start_position(), range_tracker.stop_position()
        query = f"SELECT * FROM ({self.source.query}) as subq LIMIT {limit} OFFSET {offset}"
        for record in self.source.client.record_generator(query):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        if self._counts == 0:
            self._counts = self.source.client.counts_estimator(self.source.query)
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self._counts

        last_position = 0
        for offset in range(start_position, stop_position, self._batch_size):
            yield iobase.SourceBundle(
                weight=desired_bundle_size, source=self.source, start_position=offset, stop_position=self._batch_size
            )
            last_position = offset + self._batch_size

        yield iobase.SourceBundle(
            weight=desired_bundle_size,
            source=self.source,
            start_position=last_position + 1,
            stop_position=stop_position,
        )


class NoSplitter(BaseSplitter):
    """No split bounded source so not work parallel."""

    def estimate_size(self):
        return self.source.client.rough_counts_estimator(self.source.query)

    def get_range_tracker(self, start_position, stop_position):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = OffsetRangeTracker.OFFSET_INFINITY

        return UnsplittableRangeTracker(OffsetRangeTracker(start_position, stop_position))

    def read(self, range_tracker):
        for record in self.source.client.record_generator(self.source.query):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        print('***********')
        print(start_position)
        print(stop_position)
        print(desired_bundle_size)
        print('***********')
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = OffsetRangeTracker.OFFSET_INFINITY

        yield iobase.SourceBundle(
            weight=desired_bundle_size, source=self.source, start_position=start_position, stop_position=stop_position
        )
