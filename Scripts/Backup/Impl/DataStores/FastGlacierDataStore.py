# ----------------------------------------------------------------------
# |
# |  FastGlacierDataStore.py
# |
# |  David Brownell <db@DavidBrownell.com>
# |      2022-12-09 10:54:57
# |
# ----------------------------------------------------------------------
# |
# |  Copyright David Brownell 2022
# |  Distributed under the Boost Software License, Version 1.0. See
# |  accompanying file LICENSE_1_0.txt or copy at
# |  http://www.boost.org/LICENSE_1_0.txt.
# |
# ----------------------------------------------------------------------
"""Contains the FastGlacierDataStore object"""

from pathlib import Path
from typing import Optional

from Common_Foundation.Streams.DoneManager import DoneManager
from Common_Foundation import SubprocessEx
from Common_Foundation.Types import overridemethod

from .BulkStorageDataStore import BulkStorageDataStore


# ----------------------------------------------------------------------
class FastGlacierDataStore(BulkStorageDataStore):
    """Data store that uses the Fast Glacier application (https://fastglacier.com/)"""

    # ----------------------------------------------------------------------
    def __init__(
        self,
        account_name: str,
        aws_region: str,
        glacier_dir: Optional[Path],
    ):
        super(FastGlacierDataStore, self).__init__()

        self.account_name                   = account_name
        self.aws_region                     = aws_region

        self._glacier_dir                   = glacier_dir or Path()

        self._validated_command_line        = False

    # ----------------------------------------------------------------------
    @overridemethod
    def ExecuteInParallel(self) -> bool:
        return False

    # ----------------------------------------------------------------------
    @overridemethod
    def Upload(
        self,
        dm: DoneManager,
        local_path: Path,
    ) -> None:
        if self._validated_command_line is False:
            with dm.Nested(
                "Validating Fast Glacier on the command line...",
                suffix="\n",
            ) as check_dm:
                result = SubprocessEx.Run("glacier-con --version")

                check_dm.WriteVerbose(result.output)

                if result.returncode != 0 and "glacier-con.exe upload" not in result.output:
                    check_dm.WriteError("Fast Glacier is not available; please make sure it exists in the path and run the script again.\n")
                    return

                self._validated_command_line = True

        with dm.Nested("Uploading to Glacier...") as upload_dm:
            command_line = 'glacier-con upload "{account}" "{local_dir}\\*" "{region}" "{path}"'.format(
                account=self.account_name,
                local_dir=local_path,
                region=self.aws_region,
                path=self._glacier_dir.as_posix(),
            )

            upload_dm.WriteVerbose("Command Line: {}\n\n".format(command_line))

            with upload_dm.YieldStream() as stream:
                upload_dm.result = SubprocessEx.Stream(command_line, stream)
