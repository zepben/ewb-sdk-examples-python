#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
import math
import os
from dataclasses import dataclass
import time
from typing import Dict

import pandas as pd
from tqdm.contrib.concurrent import process_map
from zepben.evolve import connect_with_token, NetworkConsumerClient, Feeder, Tracing, downstream, StepActionWithContextValue, \
    NetworkTraceStep, EnergyConsumer, StepContext, IdentifiedObject, Breaker, Fuse, PowerTransformer, TransformerFunctionKind
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers


@dataclass
class EnergyConsumerDeviceHierarchy:
    energy_consumer_mrid: str
    lv_circuit_name: str
    upstream_switch_mrid: str
    lv_circuit_name: str
    upstream_switch_class: str
    distribution_power_transformer_mrid: str
    distribution_power_transformer_name: str
    regulator_mrid: str
    breaker_mrid: str
    feeder_mrid: str


def _get_client():
    with open('config.json') as f:
        config = json.load(f)

        # Connect to server
    channel = connect_with_token(**config)
    return NetworkConsumerClient(channel)


async def get_feeders() -> Dict[str, Feeder]:
    _feeders = (await _get_client().get_network_hierarchy()).result.feeders
    return _feeders


async def trace_from_feeder(feeder_mrid: str):
    """
    Fetch the equipment container from the given feeder and build an equipment tree of everything downstream of the feeder.
    Use the Equipment tree to traverse upstream of all EC's and get the equipment we are interested in.
    Finally, create a CSV with the relevant information.
    """
    client =  _get_client()
    # Get all objects under the feeder, including Substations and LV Feeders
    (await client.get_equipment_container(
        feeder_mrid,
        include_energized_containers = IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS
    )).throw_on_error()
    feeder = client.service.get(feeder_mrid, Feeder)

    energy_consumers = []

    class StepActionWithContext(StepActionWithContextValue):
        def _apply(self, item: NetworkTraceStep, context: StepContext):
            if isinstance((ec := item.path.to_equipment), EnergyConsumer):
                nonlocal energy_consumers
                data = self.get_context_value(context)
                data.update({'feeder': feeder.mrid, 'energy_consumer_mrid': ec.mrid})

                row = _build_row(data)
                energy_consumers.append(row)

        def compute_next_value(self, next_item: NetworkTraceStep, current_item: NetworkTraceStep, current_value: Dict[str, IdentifiedObject]):
            data = dict(current_value)
            equip = next_item.path.to_equipment
            if isinstance(equip, Breaker):
                data['breaker'] = equip
            elif isinstance(equip, Fuse):
                data['upstream_switch'] = equip
            elif isinstance(equip, PowerTransformer):
                if equip.function == TransformerFunctionKind.distributionTransformer:
                    data['distribution_power_transformer'] = equip
                elif equip.function == TransformerFunctionKind.voltageRegulator:
                    data['regulator'] = equip
            return data

        def compute_initial_value(self, item: NetworkTraceStep):
            return {}

    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(StepActionWithContext('key'))
    ).run(getattr(feeder, 'normal_head_terminal'))

    csv_sfx = "energy_consumers.csv"
    network_objects = pd.DataFrame(energy_consumers)
    os.makedirs("csvs", exist_ok=True)
    network_objects.to_csv(f"csvs/{feeder.mrid}_{csv_sfx}", index=False)


def _build_row(up_data: dict[str, IdentifiedObject | str]) -> EnergyConsumerDeviceHierarchy:
    return EnergyConsumerDeviceHierarchy(
        energy_consumer_mrid = up_data['energy_consumer_mrid'],
        upstream_switch_mrid = (up_data.get('upstream_switch') or NullEquipment).mrid,
        lv_circuit_name = (up_data.get('upstream_switch') or NullEquipment).name,
        upstream_switch_class = type(up_data.get('upstream_switch')).__name__,
        distribution_power_transformer_mrid = (up_data.get('distribution_power_transformer') or NullEquipment).mrid,
        distribution_power_transformer_name = (up_data.get('distribution_power_transformer') or NullEquipment).name,
        regulator_mrid = (up_data.get('regulator') or NullEquipment).mrid,
        breaker_mrid = (up_data.get('breaker') or NullEquipment).mrid,
        feeder_mrid = up_data.get('feeder'),
    )


class NullEquipment:
    """empty class to simplify code below in the case of an equipment not existing in that position of the network"""
    mrid = None
    name = None


def main(_feeder):
    asyncio.run(trace_from_feeder(_feeder))


if __name__ == "__main__":
    start = time.time()
    # Get a list of feeders before entering main compute section of script.
    feeders = list(asyncio.run(get_feeders()))

    print('processing feeders')

    # Process feeders sequentially.
    # for _feeder in tqdm(feeders):
    #     asyncio.run(trace_from_feeder(_feeder))

    # Process feeders concurrently.
    process_map(main, feeders, max_workers=math.floor(os.cpu_count()/2))

    print(f'done in {time.time() - start} seconds')
