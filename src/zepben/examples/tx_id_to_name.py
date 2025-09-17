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
import os.path
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from zepben.evolve import NetworkConsumerClient, connect_with_token, PowerTransformer

from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS

with open("./config.json") as f:
    c = json.loads(f.read())


async def connect():
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)

    network_hierarchy = (await network_client.get_network_hierarchy()).throw_on_error().value

    for gr in network_hierarchy.geographical_regions.values():
        print(f"- Geographical region: {gr.name}")
        for sgr in gr.sub_geographical_regions:
            print(f"  - Subgeographical region: {sgr.name}")
            for sub in sgr.substations:
                print(f"    - Zone Substation: {sub.name}")
                await process_nodes(sub.mrid, channel)
                for fdr in sub.feeders:
                    print(f"      - Processing Feeder: {fdr.name}")
                    await process_nodes(fdr.mrid, channel)


@dataclass
class NetworkObject(object):
    dist_tx_id: str
    dist_tx_name: str
    container: str
    container_mrid: str
    high_step: Optional[float]
    low_step: Optional[float]
    neutral_step: Optional[float]
    step: Optional[float]
    normal_step: Optional[float]
    step_voltage_increment: Optional[float]


async def process_nodes(container_mrid: str, channel):
    print("Fetching from server ...")
    network_client = NetworkConsumerClient(channel=channel)
    network_service = network_client.service
    (await network_client.get_equipment_container(container_mrid, include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
    container = network_service.get(container_mrid)
    container_name = container.name

    print("Processing equipment ...")
    network_objects = []
    for equip in network_service.objects(PowerTransformer):
        primary_end = equip.get_end_by_num(1)
        tap_changer = primary_end.ratio_tap_changer
        high_step = None
        neutral_step = None
        low_step = None
        step = None
        normal_step = None
        step_voltage_increment = None
        if tap_changer:
            high_step = tap_changer.high_step
            neutral_step = tap_changer.neutral_step
            low_step = tap_changer.low_step
            step = tap_changer.step
            normal_step = tap_changer.normal_step
            step_voltage_increment = tap_changer.step_voltage_increment

        no = NetworkObject(equip.mrid, equip.name, container_name, container_mrid, high_step, low_step, neutral_step, step, normal_step, step_voltage_increment)
        network_objects.append(no)

    network_objects = pd.DataFrame(network_objects)
    network_objects.to_csv(f"csvs/{container_mrid}_transformer_tap_details.csv", index=False, mode='a', header=True)
    print(f"Finished processing {container_mrid}")


if __name__ == "__main__":
    asyncio.run(connect())
