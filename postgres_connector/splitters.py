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
    pass

class LimitOffsetSplitter(BaseSplitter):
    """Split bounded source by limit and offset."""
    pass


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
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = OffsetRangeTracker.OFFSET_INFINITY

        yield iobase.SourceBundle(
            weight=desired_bundle_size, source=self.source, start_position=start_position, stop_position=stop_position
        )
