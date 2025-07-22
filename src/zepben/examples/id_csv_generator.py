#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json
import os
from dataclasses import dataclass
from typing import Optional
import pandas as pd

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

from zepben.evolve import NetworkConsumerClient, connect_with_token, ConductingEquipment, Feeder, connect_tls

with open("./config.json") as f:
    c = json.loads(f.read())

"""
This is a basic example that shows how to export a CSV of all the conducting equipment in a feeder.
It will output one CSV per feeder in the network.
"""


async def connect():
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)

    network_hierarchy = (await network_client.get_network_hierarchy()).throw_on_error().value
    print("Network hierarchy:")
    for gr in network_hierarchy.geographical_regions.values():
        print(f"- Geographical region: {gr.name}")
        for sgr in gr.sub_geographical_regions:
            print(f"  - Subgeographical region: {sgr.name}")
            for sub in sgr.substations:
                print(f"    - Zone Substation: {sub.name}")
                for fdr in sub.feeders:
                    print(f"      - Processing Feeder: {fdr.name}")
                    await process_nodes(fdr.mrid, channel)


@dataclass
class NetworkObject(object):
    id: str
    type: str
    feeder: str
    voltage: int
    dist_tx_id: Optional[str] = None
    dist_tx_name: Optional[str] = None


async def process_nodes(feeder_mrid: str, channel):
    print("Fetching from server ...")
    network_client = NetworkConsumerClient(channel=channel)
    network_service = network_client.service
    (await network_client.get_equipment_container(feeder_mrid,
                                                  include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()

    print("Processing equipment ...")
    feeder = network_service.get(feeder_mrid, Feeder)
    network_objects = []
    # Fetch all the high voltage conducting equipment from the Feeder
    for equip in feeder.equipment:
        if isinstance(equip, ConductingEquipment):
            no = NetworkObject(equip.mrid, type(equip).__name__, feeder_mrid, equip.base_voltage_value)
            network_objects.append(no)

    # Fetch all the low voltage conducting equipment from the LvFeeders supplied by this Feeder
    for lvf in feeder.normal_energized_lv_feeders:
        head = lvf.normal_head_terminal.conducting_equipment
        for equip in lvf.equipment:
            if isinstance(equip, ConductingEquipment):
                no = NetworkObject(equip.mrid, type(equip).__name__, feeder_mrid, equip.base_voltage_value, head.mrid, head.name)
                network_objects.append(no)

    print(f"Writing csvs/{feeder_mrid}_network_objects.csv")
    network_objects = pd.DataFrame(network_objects)
    os.makedirs("csvs", exist_ok=True)
    network_objects.to_csv(f"csvs/{feeder_mrid}_network_objects.csv", index=False)
    print(f"Finished processing {feeder_mrid}")


if __name__ == "__main__":
    asyncio.run(connect())
