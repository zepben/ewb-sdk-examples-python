#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import csv
import json
import os

from zepben.ewb import connect_with_token, NetworkConsumerClient, Meter, IncludedEnergizedContainers


def _get_client() -> NetworkConsumerClient:
    with open('config.json') as f:
        config = json.load(f)['ewb']

    channel = connect_with_token(**config)
    return NetworkConsumerClient(channel)


async def get_feeder_meter_ids(feeder_name_or_mrid: str, client: NetworkConsumerClient = None) -> list[str]:
    """Fetch all Meters under the feeder identified by name or mrid, return their `name` (company_meter_id)."""
    client = client or _get_client()

    hierarchy_response = await client.get_network_hierarchy()
    hierarchy_response.throw_on_error()
    hierarchy = hierarchy_response.result
    feeder = hierarchy.feeders.get(feeder_name_or_mrid)
    if feeder is None:
        feeder = next((f for f in hierarchy.feeders.values() if f.name == feeder_name_or_mrid), None)
    if feeder is None:
        raise ValueError(f"No feeder found with mrid/name '{feeder_name_or_mrid}'")

    (await client.get_equipment_container(
        feeder.mrid,
        include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS
    )).throw_on_error()

    def _meter_id(meter: Meter) -> str:
        # `name` holds the meter id when populated; otherwise it's embedded in the mrid as "<meter_id>-mt".
        if meter.name:
            return meter.name
        return meter.mrid[:-3] if meter.mrid.endswith("-mt") else meter.mrid

    meter_ids = sorted({_meter_id(meter) for meter in client.service.objects(Meter)})
    return meter_ids


def write_csv(meter_ids: list[str], feeder_name_or_mrid: str) -> str:
    os.makedirs("csvs", exist_ok=True)
    path = f"csvs/{feeder_name_or_mrid}_meter_ids.csv"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["meter_id"])
        for meter_id in meter_ids:
            writer.writerow([meter_id])
    return path


if __name__ == "__main__":
    FEEDER_NAME_OR_MRID = "<FEEDER_CODE>"
    ids = asyncio.run(get_feeder_meter_ids(FEEDER_NAME_OR_MRID))
    csv_path = write_csv(ids, FEEDER_NAME_OR_MRID)
    print(f"Found {len(ids)} meter ids for feeder {FEEDER_NAME_OR_MRID}. Written to {csv_path}")
