# ----------------------------------------------------------------------
# |
# |  Mirror_BuildVerificationTest.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-11-21 08:50:08
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Build Verification Test for mirror functionality"""

# Note that this file is using Python's unittest library rather than pytest so that it can be invoked
# in both the standard and dev configurations.

import sys
import unittest

from io import StringIO
from pathlib import Path

from Common_Foundation.ContextlibEx import ExitStack
from Common_Foundation.Shell.All import CurrentShell
from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation import PathEx


# ----------------------------------------------------------------------
sys.path.insert(0, str(PathEx.EnsureDir(Path(__file__).parent.parent.parent)))
with ExitStack(lambda: sys.path.pop(0)):
    from Backup.Impl.Mirror import Backup
    from Backup.Impl import TestHelpers


# ----------------------------------------------------------------------
class FileSystemMirrorSuite(unittest.TestCase):
    # ----------------------------------------------------------------------
    def test_Backup(self):
        temp_directory = CurrentShell.CreateTempDirectory()

        with ExitStack(lambda: PathEx.RemoveTree(temp_directory)):
            source_dir = Path(__file__).parent.parent

            with DoneManager.Create(StringIO(), "") as dm:
                Backup(
                    dm,
                    temp_directory,
                    [source_dir],
                    ssd=False,
                    force=False,
                    quiet=False,
                    file_includes=None,
                    file_excludes=None,
                )

            try:
                TestHelpers.CompareFileSystemSourceAndDestination(
                    source_dir,
                    temp_directory,
                    compare_file_contents=True,
                )

            except AssertionError as ex:
                self.assertTrue(False, str(ex))


# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
if __name__ == "__main__":
    try:
        sys.exit(
            unittest.main(
                verbosity=2,
            ),
        )
    except KeyboardInterrupt:
        pass
