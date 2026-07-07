#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import json
from typing import Dict

from zepben.eas import EasClient

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger()


# Default config dir is where config.json sits.
def get_config_dir(argv):
    return argv[1] if len(argv) > 1 else "."


def read_json_config(config_file_path: str) -> Dict:
    file = open(config_file_path)
    config_dict = json.load(file)
    file.close()
    return config_dict


def get_client(config_dir, async_=True):
    config = read_json_config(f"{config_dir}/config.json")
    rpc_port = config.pop("rpc_port")

    return EasClient(**config, port=rpc_port, protocol="https", asynchronous=async_)
