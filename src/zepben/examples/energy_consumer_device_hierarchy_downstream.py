#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Dict

import pandas as pd
from zepben.evolve import NetworkConsumerClient, connect_with_token, Tracing, EnergyConsumer, PowerTransformer, \
    TransformerFunctionKind, Breaker, Fuse, IdentifiedObject, EquipmentTreeBuilder, downstream, TreeNode, Feeder
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
    channel = connect_with_token(
        host=config["host"],
        access_token=config["access_token"],
        rpc_port=config['rpc_port'],
        ca_filename=config['ca_path']
    )
    return NetworkConsumerClient(channel)


async def get_feeders() -> Dict[str, Feeder]:
    _feeders = (await _get_client().get_network_hierarchy()).result.feeders
    return _feeders


def process_leaf(up_data: dict, leaf: TreeNode):
    to_equip: IdentifiedObject = leaf.identified_object

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


async def trace_from_feeder(feeder_mrid: str):
    """
    Fetch the equipment container from the given feeder and build an equipment tree of everything downstream of the feeder.
    Use the Equipment tree to traverse upstream of all EC's and get the equipment we are interested in.
    Finally, create a CSV with the relevant information.
    """
    client =  _get_client()
    print(f'processing feeder {feeder_mrid}')

    # Get all objects under the feeder, including Substations and LV Feeders
    await client.get_equipment_container(
        feeder_mrid,
        include_energized_containers = IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS
    )

    feeder = client.service.get(feeder_mrid, Feeder)

    builder = EquipmentTreeBuilder()

    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(builder)
    ).run(getattr(feeder, 'normal_head_terminal'))

    energy_consumers = []
    for up in client.service.objects(EnergyConsumer):
        # iterate up tree from EC.
        up_data = {'feeder': feeder.mrid, 'energy_consumer_mrid': up.mrid}
        def _process(leaf):
            process_leaf(up_data, leaf)
            if leaf.parent:
                _process(leaf.parent)
        try:
            _process(builder.leaves[up.mrid])
        except KeyError:
            # If the up is not in the Equipment tree builders leaves, skip it
            continue

        row = _build_row(up_data)
        energy_consumers.append(row)

    csv_sfx = "energy_consumers.csv"
    network_objects = pd.DataFrame(energy_consumers)
    os.makedirs("csvs", exist_ok=True)
    network_objects.to_csv(f"csvs/{feeder.mrid}_{csv_sfx}", index=False)


class NullEquipment:
    """empty class to simplify code below in the case of an equipment not existing in that position of the network"""
    mrid = None
    name = None


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


if __name__ == "__main__":
    # Get a list of feeders before entering main compute section of script.
    feeders = asyncio.run(get_feeders())

    print('processing feeders')
    for _feeder in feeders:
        asyncio.run(trace_from_feeder(_feeder))
