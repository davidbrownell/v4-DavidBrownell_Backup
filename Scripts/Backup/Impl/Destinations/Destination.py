# ----------------------------------------------------------------------
# |
# |  Destination.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-12 10:51:40
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Contains the Destination object"""

from abc import abstractmethod, ABC
from enum import Enum

from Common_Foundation.Streams.DoneManager import DoneManager

from ..Snapshot import Snapshot


# ----------------------------------------------------------------------
class ValidateType(str, Enum):
    """Controls how validation is performed"""

    standard                                = "standard"                    # File names and sizes are validated
    complete                                = "complete"                    # File names, sizes, and hash values are validated


# ----------------------------------------------------------------------
class Destination(ABC):
    """Abstraction for the destination of Backup operations"""

    # ----------------------------------------------------------------------
    @abstractmethod
    def GetMirroredSnapshot(
        self,
        dm: DoneManager,                    # pylint: disable=unused-argument
    ) -> Snapshot:
        """Returns the `Snapshot` of content that was previously mirrored"""

        raise Exception("Abstract method")

    # ----------------------------------------------------------------------
    @abstractmethod
    def ProcessMirroredSnapshot(
        self,
        dm: DoneManager,                    # pylint: disable=unused-argument
        local_snapshot: Snapshot,           # pylint: disable=unused-argument
        mirrored_snapshot: Snapshot,        # pylint: disable=unused-argument
    ) -> None:
        """\
        At the conclusion of this method, the contents associated with the mirrored `Snapshot` will
        match the contents associated with the local `Snapshot`.
        """

        raise Exception("Abstract method")

    # ----------------------------------------------------------------------
    @abstractmethod
    def CleanPreviousRun(
        self,
        dm: DoneManager,
    ) -> None:
        """Cleans any data produced by a previous run"""

        raise Exception("Abstract method")

    # ----------------------------------------------------------------------
    @abstractmethod
    def Validate(
        self,
        dm: DoneManager,
        validate_type: ValidateType,
    ) -> None:
        """Validates what is expected to be stored (based on the mirrored snapshot) with what is actually stored."""

        raise Exception("Abstract method")
