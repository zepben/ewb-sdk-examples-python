#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json
import sys

from zepben.eas import FeederLoadAnalysisInput, EasClient

with open("config.json") as f:
    c = json.loads(f.read())


async def main(argv):
    # See connecting_to_grpc_service.py for examples of each connect function
    print("Connecting to EAS..")
    eas_client = EasClient(
        host=c["eas_host"],
        port=c["eas_port"],
        protocol=c["eas_protocol"],
        access_token=c["access_token"],
        verify_certificate=c.get("verify_certificate", True),
        ca_filename=c["ca_path"]
    )
    print("Connection established..")
    # Fire off a feeder load analysis study
    feeder_load_analysis_token = await eas_client.async_run_feeder_load_analysis_report(
        FeederLoadAnalysisInput(
            feeders=["feeder1", "feeder2"],
            substations=None,
            sub_geographical_regions=None,
            geographical_regions=None,
            start_date="2022-04-01",
            end_date="2022-12-31",
            fetch_lv_network=True,
            process_feeder_loads=True,
            process_coincident_loads=True,
            aggregate_at_feeder_level=False,
            output="Test"
        )
    )

    print(f"Feeder Load Analysis study: {feeder_load_analysis_token['data']['runFeederLoadAnalysis']}")

    # Feeder Load Analysis Study results can be retrieved from back end storage set up with EAS.

    await eas_client.aclose()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(sys.argv))
