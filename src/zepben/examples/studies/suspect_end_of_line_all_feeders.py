#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
from datetime import datetime
from itertools import islice
from typing import List

from zepben.eas.client.eas_client import EasClient
from zepben.ewb import NetworkConsumerClient, connect_with_token

from suspect_end_of_line import (
    fetch_feeder_and_trace,
    upload_suspect_end_of_line_study,
    _build_suspect_end_result,
    _line_length_m,
)


with open("../config.json") as f:
    c = json.loads(f.read())


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


async def main():
    # Only process feeders in the following zones
    zone_mrids = ["CPM"]
    print(f"Start time: {datetime.now()}")

    rpc_channel = connect_with_token(
        host=c["host"],
        access_token=c["access_token"],
        rpc_port=c["rpc_port"],
        ca_filename=c.get("ca_filename"),
        timeout_seconds=c.get("timeout_seconds", 5),
        debug=bool(c.get("debug", False)),
        skip_connection_test=bool(c.get("skip_connection_test", False)),
    )
    client = NetworkConsumerClient(rpc_channel)
    hierarchy = (await client.get_network_hierarchy()).throw_on_error()
    substations = hierarchy.value.substations

    print(f"Collecting feeders from zones {', '.join(zone_mrids)}.")
    feeder_mrids = []
    for zone_mrid in zone_mrids:
        if zone_mrid in substations:
            for feeder in substations[zone_mrid].feeders:
                feeder_mrids.append(feeder.mrid)

    print(f"Feeders to be processed: {', '.join(feeder_mrids)}")

    feeder_results = []

    # Process the feeders in batches of 3, using asyncio, for performance
    batches = chunk(feeder_mrids, 3)
    for feeders in batches:
        futures = []
        rpc_channel = connect_with_token(
            host=c["host"],
            access_token=c["access_token"],
            rpc_port=c["rpc_port"],
            ca_filename=c.get("ca_filename"),
            timeout_seconds=c.get("timeout_seconds", 5),
            debug=bool(c.get("debug", False)),
            skip_connection_test=bool(c.get("skip_connection_test", False)),
        )
        print(f"Processing feeders {', '.join(feeders)}")
        for feeder_mrid in feeders:
            futures.append(asyncio.ensure_future(fetch_feeder_and_trace(feeder_mrid, rpc_channel)))

        for future in futures:
            result = await future
            if not result:   # Empty if the feeder failed
                continue
            feeder_mrid, transformers, tx_to_sus_lines, feeder_suspect_lines = result
            total_length_m = sum(_line_length_m(line) for line in feeder_suspect_lines)
            highlight_equipment = set(transformers) | set(feeder_suspect_lines)
            feeder_results.append(
                (feeder_mrid, total_length_m, list(highlight_equipment), tx_to_sus_lines)
            )

    print(f"Created Study for {len(feeder_mrids)} feeders")

    eas_client = EasClient(host=c["host"], port=c["rpc_port"], protocol="https", access_token=c["access_token"])

    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    styles = json.load(open("style_eol.json", "r"))
    results = [
        _build_suspect_end_result(
            feeder_mrid,
            total_length_m,
            equipment,
            transformer_to_suspect_lines,
            styles=styles,
        )
        for feeder_mrid, total_length_m, equipment, transformer_to_suspect_lines in feeder_results
    ]
    await upload_suspect_end_of_line_study(
        eas_client,
        results,
        name=f"Suspect end of line ({', '.join(zone_mrids)})",
        description="Highlights only line segments that have no downstream EnergyConsumers (excludes shared upstream segments).",
        tags=["suspect_end_of_line", "-".join(zone_mrids)],
        styles=styles,
    )
    await eas_client.close()
    print("Uploaded Study")

    print(f"Finish time: {datetime.now()}")


if __name__ == "__main__":
    asyncio.run(main())
