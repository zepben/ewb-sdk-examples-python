#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from pathlib import Path

# Directory this package lives in, i.e. src/zepben/examples, which is where config.json
# is expected to live (see utils.py::get_client and the various scripts that open "config.json").
CONFIG_DIR = f"{Path(__file__).parent}/"
