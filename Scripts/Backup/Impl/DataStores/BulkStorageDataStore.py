# ----------------------------------------------------------------------
# |
# |  BulkStorageDataStore.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-12-09 11:52:39
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022-23
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Contains the BulkStorageDataStore object"""

from abc import abstractmethod
from pathlib import Path

from Common_Foundation.Streams.DoneManager import DoneManager

from .DataStore import DataStore


# ----------------------------------------------------------------------
class BulkStorageDataStore(DataStore):
    """Abstraction for data stores that can upload content in bulk but not easily retrieve it (such as cloud storage)"""

    # ----------------------------------------------------------------------
    @abstractmethod
    def Upload(
        self,
        dm: DoneManager,
        local_path: Path,
    ) -> None:
        """Uploads all content in the provided path and its descendants"""
        raise Exception("Abstract method")  # pragma: no cover
