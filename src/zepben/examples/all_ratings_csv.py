#  Copyright 2026 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
import os
import pandas as pd

from typing import Dict, Callable, List
from dataclasses import dataclass
from zepben.ewb import connect_with_token, NetworkConsumerClient, Feeder, Tracing, downstream, StepActionWithContextValue, \
    NetworkTraceStep, EnergyConsumer, StepContext, IdentifiedObject, Breaker, Fuse, PowerTransformer, TransformerFunctionKind, TreeNode, EquipmentTreeBuilder, \
    ConductingEquipment, NetworkTrace, upstream, IncludedEnergizedContainers, Conductor, Switch

"""
This is a small script which can be configured to run concurrently to create CSVs of ratings for conductors, transformers, and switches in the network.
Results are output as a CSV per feeder in a ./csvs directory.
"""


@dataclass
class EquipmentWithRating:
    mrid: str
    type: str
    rating_va: float | int | None


def _get_client():
    with open('config.json') as f:
        c = json.load(f)

        # Connect to server
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"])
    return NetworkConsumerClient(channel)


async def get_feeders(_client=None) -> Dict[str, Feeder]:
    _feeders = (await (_client or _get_client()).get_network_hierarchy()).result.feeders
    return _feeders


async def get_feeder_equipment(client: NetworkConsumerClient, feeder_mrid: str) -> None:
    """Get all objects under the feeder, including LV Feeders"""
    (await client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS)).throw_on_error()


async def write_ratings(feeder_mrid: str, client=None):
    """
    Retrieves ratings for transformers, conductors, and switches on this feeder and outputs them to a CSV named csv/<feeder_mrid>_ratings.csv
    """
    client = client or _get_client()
    await get_feeder_equipment(client, feeder_mrid)

    equip_with_ratings = []
    for tx in client.service.objects(PowerTransformer):
        equip_with_ratings.append(EquipmentWithRating(mrid=tx.mrid, type="PowerTransformer", rating_va=tx.get_end_by_num(1).rated_s))
    for conductor in client.service.objects(Conductor):
        if conductor.asset_info is not None:
            equip_with_ratings.append(EquipmentWithRating(mrid=conductor.mrid, type=type(conductor).__name__, rating_va=conductor.asset_info.rated_current))
        else:
            equip_with_ratings.append(EquipmentWithRating(mrid=conductor.mrid, type=type(conductor).__name__, rating_va=None))

    for switch in client.service.objects(Switch):
        equip_with_ratings.append(EquipmentWithRating(mrid=switch.mrid, type=type(switch).__name__, rating_va=switch.rated_current))

    write_csv(equip_with_ratings, feeder_mrid)


def write_csv(equip: List[EquipmentWithRating], feeder_mrid: str):
    network_objects = pd.DataFrame(equip)
    os.makedirs("csvs", exist_ok=True)
    network_objects.to_csv(f"csvs/{feeder_mrid}_ratings.csv", index=False)


def process_feeders_sequentially():
    async def main_async(writer: Callable):
        """
        Fetch the equipment container from the given feeder and create a CSV with the relevant information.
        Differences between the functions passable as `trace_type` are documented in the relevant docstrings.
        """
        from tqdm import tqdm
        client = _get_client()
        feeders = list(await get_feeders(client))
        # feeders = ["<FEEDER_ID>"] # Uncomment to process just one (or configured) feeder(s).
        for _feeder in tqdm(feeders):
            await writer(_feeder, client)

    asyncio.run(main_async(write_ratings))


def multi_proc(_feeder):
    asyncio.run(write_ratings(_feeder))


def process_feeders_concurrently():
    # Get a list of feeders before entering main compute section of script. This will iterate over all feeders in the network.
    feeders = list(asyncio.run(get_feeders()))

    from tqdm.contrib.concurrent import process_map
    # Parallelise up to half the cores available on your machine
    process_map(multi_proc, feeders, max_workers=int(os.cpu_count() / 2))


if __name__ == "__main__":
    # process_feeders_sequentially() # Uncomment and comment concurrently below to process feeder at a time, note concurrently is resource intensive.
    process_feeders_concurrently()
