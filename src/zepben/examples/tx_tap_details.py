#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
import os
import pandas as pd

from typing import Dict, Callable, List, Optional
from dataclasses import dataclass
from zepben.evolve import (connect_with_token, NetworkConsumerClient, Feeder, Tracing, downstream, StepActionWithContextValue, \
    NetworkTraceStep, EnergyConsumer, StepContext, IdentifiedObject, Breaker, Fuse, PowerTransformer, TransformerFunctionKind, TreeNode, EquipmentTreeBuilder, \
    ConductingEquipment, NetworkTrace, upstream)

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers


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


def _get_client():
    with open('config.json') as f:
        config = json.load(f)

        # Connect to server
    channel = connect_with_token(
        host=config["host"],
        access_token=config["access_token"],
        rpc_port=config['rpc_port'],
        ca_filename=config['ca_path']
    )
    return NetworkConsumerClient(channel)


async def get_feeders(_client=None) -> Dict[str, Feeder]:
    _feeders = (await (_client or _get_client()).get_network_hierarchy()).result.feeders
    return _feeders


async def get_feeder_equipment(client: NetworkConsumerClient, feeder_mrid: str) -> None:
    """Get all objects under the feeder, including LV Feeders"""
    (await client.get_equipment_container(
        feeder_mrid,
        include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS
    )).throw_on_error()



async def process_nodes(container_mrid: str):
    client = _get_client()
    await get_feeder_equipment(client, container_mrid)

    network_service = client.service
    container = network_service.get(container_mrid)
    container_name = container.name

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

    write_csv(network_objects, container_mrid)


def write_csv(network_objects: List[NetworkObject], feeder_mrid: str):
    network_objects = pd.DataFrame(network_objects)
    os.makedirs("csvs", exist_ok=True)
    network_objects.to_csv(f"csvs/{feeder_mrid}_tx_tap_details.csv", index=False)


def process_feeders_sequentially():
    async def main_async(trace_type: Callable):
        """
        Fetch the equipment container from the given feeder and create a CSV with the relevant information.
        Differences between the functions passable as `trace_type` are documented in the relevant docstrings.

        `trace_type` must be one of the following:
            - `trace_from_energy_consumers`
            - `trace_from_energy_consumers_with_context`
            - `trace_from_feeder_downstream`

        """
        from tqdm import tqdm
        client = _get_client()
        # feeders = ["RW1292"]
        feeders = list(await get_feeders(client)) # Uncomment to process all feeders
        for _feeder in tqdm(feeders):
            await trace_type(_feeder)

    asyncio.run(main_async(process_nodes))


def multi_proc(_feeder):
    asyncio.run(process_nodes(_feeder))

def process_feeders_concurrently():
    # Get a list of feeders before entering main compute section of script.
    feeders = list(asyncio.run(get_feeders()))

    from tqdm.contrib.concurrent import process_map
    process_map(multi_proc, feeders, max_workers=int(os.cpu_count() / 2), chunksize=1)


if __name__ == "__main__":
    # process_feeders_sequentially()
    process_feeders_concurrently()  # Uncomment and comment sequentially above to multi-process, note this is resource intensive and may cause issues.
