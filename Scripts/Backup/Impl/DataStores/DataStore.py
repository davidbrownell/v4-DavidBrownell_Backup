# ----------------------------------------------------------------------
# |
# |  DataStore.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-25 10:33:24
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022-23
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Contains the DataStore object"""

from abc import ABC, abstractmethod
from enum import auto, Enum


# ----------------------------------------------------------------------
class ItemType(Enum):
    """Type of file-system-like item"""

    File                                    = auto()
    Dir                                     = auto()
    SymLink                                 = auto()


# ----------------------------------------------------------------------
class DataStore(ABC):
    """Abstraction for systems that are able to store data"""

    # ----------------------------------------------------------------------
    @abstractmethod
    def ExecuteInParallel(self) -> bool:
        """Return True if processing should be executed in parallel"""
        raise Exception("Abstract method")  # pragma: no cover
