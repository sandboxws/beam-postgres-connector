import re
from abc import ABCMeta
from abc import abstractmethod
from datetime import datetime
from typing import Callable
from typing import Iterator

from apache_beam.io import iobase
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.io.range_trackers import UnsplittableRangeTracker
from dateutil.relativedelta import relativedelta

class DateSplitter(BaseSplitter):
    """Split bounded source by dates."""
    pass
