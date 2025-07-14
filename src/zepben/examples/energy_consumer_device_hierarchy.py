#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
import os
from dataclasses import dataclass
from multiprocessing import Pool
from typing import Union

import pandas as pd
from zepben.evolve import NetworkConsumerClient, connect_with_token, Tracing, upstream, EnergyConsumer, NetworkTraceStep, StepContext, PowerTransformer, \
    TransformerFunctionKind, Breaker, ConductingEquipment, Fuse, IdentifiedObject, NetworkTrace, Feeder
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizingContainers, IncludedEnergizedContainers


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
    channel = connect_with_token(
        host=config["host"],
        access_token=config["access_token"],
        rpc_port=config['rpc_port'],
        ca_filename=config['ca_path']
    )
    return NetworkConsumerClient(channel)


def _get_equipment_tree_trace(up_data: dict) -> NetworkTrace:
    def step_action(step: NetworkTraceStep, _: StepContext):
        to_equip: ConductingEquipment = step.path.to_equipment
        if isinstance(to_equip, Breaker):
            if not up_data.get('breaker'):
                up_data['breaker'] = to_equip
        elif isinstance(to_equip, Fuse):
            if not up_data.get('upstream_switch'):
                up_data['upstream_switch'] = to_equip
        elif isinstance(to_equip, PowerTransformer):
            if not up_data.get('distribution_power_transformer'):
                up_data['distribution_power_transformer'] = to_equip
            elif not up_data.get('regulator') and to_equip.function == TransformerFunctionKind.voltageRegulator:
                up_data['regulator'] = to_equip

    return (
        Tracing.network_trace()
        .add_condition(upstream())
        .add_step_action(step_action)
    )


async def get_feeders():
    client = _get_client()

    _feeders = (await client.get_network_hierarchy()).result.feeders
    return _feeders


async def trace_from_energy_consumers(feeder):
    """
    Fetch the equipment container from the given feeder, then trace upstream from every EnergyConsumer
    and create a CSV with the relevant information.
    """
    client = _get_client()
    print(f'processing feeder {feeder}')
    # Get all objects under the feeder, including Substations and LV Feeders
    (await client.get_equipment_container(feeder, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
    network = client.service
    f = network.get(feeder, Feeder)

    energy_consumers = []
    for lvf in f.normal_energized_lv_feeders:
        for ce in lvf.equipment:
            if isinstance(ce, EnergyConsumer):
                up_data = {'feeder': feeder, 'energy_consumer_mrid': ce.mrid}

                # Trace upstream from EnergyConsumer.
                await _get_equipment_tree_trace(up_data).run(ce)
                energy_consumers.append(_build_row(up_data))

    csv_sfx = "energy_consumers.csv"
    network_objects = pd.DataFrame(energy_consumers)
    os.makedirs("csvs", exist_ok=True)
    network_objects.to_csv(f"csvs/{f.mrid}_{csv_sfx}", index=False)


class NullEquipment:
    """empty class to simplify code below in the case of an equipment not existing in that position of the network"""
    mrid = None
    name = None


def _build_row(up_data: dict[str, Union[IdentifiedObject, str]]) -> EnergyConsumerDeviceHierarchy:
    return EnergyConsumerDeviceHierarchy(
        energy_consumer_mrid=up_data['energy_consumer_mrid'],
        upstream_switch_mrid=(up_data.get('upstream_switch') or NullEquipment).mrid,
        lv_circuit_name=(up_data.get('upstream_switch') or NullEquipment).name,
        upstream_switch_class=type(up_data.get('upstream_switch')).__name__,
        distribution_power_transformer_mrid=(up_data.get('distribution_power_transformer') or NullEquipment).mrid,
        distribution_power_transformer_name=(up_data.get('distribution_power_transformer') or NullEquipment).name,
        regulator_mrid=(up_data.get('regulator') or NullEquipment).mrid,
        breaker_mrid=(up_data.get('breaker') or NullEquipment).mrid,
        feeder_mrid=up_data.get('feeder'),
    )


def process_target(feeder):
    asyncio.run(trace_from_energy_consumers(feeder))


if __name__ == "__main__":
    # Get a list of feeders before entering main compute section of script.
    feeders = asyncio.run(get_feeders())

    # Spin up a multiprocess pool of $CPU_COUNT processes to handle the workload, otherwise we saturate a single cpu core and it's slow.
    cpus = os.cpu_count()
    print(f'Spawning {cpus} processes')
    pool = Pool(cpus)

    print(f'mapping to process pool')
    pool.map(process_target, feeders)

    print('finishing remaining processes')
    pool.close()
    pool.join()
