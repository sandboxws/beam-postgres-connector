from abc import ABCMeta
from abc import abstractmethod


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
