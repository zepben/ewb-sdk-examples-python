#  Copyright 2026 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import json
from pathlib import Path
from typing import Any, Dict, Optional, Union

from zepben.eas import EasClient
from zepben.ewb import connect_with_token


EXAMPLES_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.json"


def load_examples_config(config_path: Union[str, Path, None] = None) -> Dict[str, Any]:
    """
    Load `src/zepben/examples/config.json` relative to this package,
    so study scripts work no matter what the current working directory is.
    """
    path = Path(config_path) if config_path is not None else EXAMPLES_CONFIG_PATH
    return json.loads(path.read_text())


def ca_filename_from_config(config: Dict[str, Any]) -> Optional[str]:
    ca = config.get("ca_filename") or config.get("ca_path")
    if ca:
        return ca
    try:
        import certifi

        return certifi.where()
    except ImportError:
        return None


def connect_rpc_from_config(config: Dict[str, Any]):
    return connect_with_token(
        host=config["host"],
        access_token=config["access_token"],
        rpc_port=config["rpc_port"],
        ca_filename=ca_filename_from_config(config),
        timeout_seconds=config.get("timeout_seconds", 5),
        debug=bool(config.get("debug", False)),
        skip_connection_test=bool(config.get("skip_connection_test", False)),
    )


def create_eas_client(
    *,
    host: str,
    port: int,
    access_token: str,
    verify_certificate: bool = True,
    ca_filename: Optional[str] = None,
) -> EasClient:
    return EasClient(
        host=host,
        port=port,
        protocol="https",
        access_token=access_token,
        verify_certificate=verify_certificate,
        ca_filename=ca_filename,
        asynchronous=True,
        enable_legacy_methods=True,
    )


def create_eas_client_from_config(config: Dict[str, Any]) -> EasClient:
    return create_eas_client(
        host=config["host"],
        port=config["rpc_port"],
        access_token=config["access_token"],
        verify_certificate=config.get("verify_certificate", True),
        ca_filename=ca_filename_from_config(config),
    )


def create_eas_client_for_host(
    *,
    host: str,
    port: int,
    access_token: str,
    ca_filename: Optional[str] = None,
    verify_certificate: bool = True,
) -> EasClient:
    return create_eas_client(
        host=host,
        port=port,
        access_token=access_token,
        verify_certificate=verify_certificate,
        ca_filename=ca_filename or ca_filename_from_config({}),
    )
