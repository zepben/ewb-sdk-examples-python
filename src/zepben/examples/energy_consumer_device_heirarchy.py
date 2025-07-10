#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
import os
from dataclasses import dataclass

import pandas as pd
from zepben.evolve import NetworkConsumerClient, connect_with_token, Tracing, upstream, EnergyConsumer, NetworkTraceStep, StepContext, PowerTransformer, \
                          TransformerFunctionKind, Breaker, ConductingEquipment, Fuse, IdentifiedObject
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizingContainers, IncludedEnergizedContainers


@dataclass
class EnergyConsumerDeviceHeirarchy:
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
        rpc_port=config['port'],
        ca_filename=config['ca_filename']
    )
    return NetworkConsumerClient(channel)


def _get_equipment_tree_trace(up_data):

    def step_action(step: NetworkTraceStep, _: StepContext):
        to_equip: ConductingEquipment = step.path.to_equipment
        match to_equip:
            case Breaker():
                if not up_data.get('breaker'):
                    up_data['breaker'] = to_equip
            case Fuse():
                if not up_data.get('upstream_switch'):
                    up_data['upstream_switch'] = to_equip
            case PowerTransformer():
                if not up_data.get('distribution_power_transformer'):
                    up_data['distribution_power_transformer'] = to_equip
                elif not up_data.get('regulator') and to_equip.function == TransformerFunctionKind.voltageRegulator:
                    up_data['regulator'] = to_equip

    return (
        Tracing.network_trace()
        .add_condition(upstream())
        .add_step_action(step_action)
    )


async def main():
    client =  _get_client()


    for feeder in (await client.get_network_hierarchy()).result.feeders:
        # Get all objects under the feeder, including Substations and LV Feeders
        feeder_objects = (
            await client.get_equipment_container(
                feeder,
                include_energizing_containers = IncludedEnergizingContainers.INCLUDE_ENERGIZING_SUBSTATIONS,
                include_energized_containers = IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS
            )
        ).result.objects

        energy_consumers = []

        for up in feeder_objects.values():
            if isinstance(up, EnergyConsumer):
                up_data = {'feeder': feeder, 'energy_consumer_mrid': up.mrid}

                # Trace upstream from EnergyConsumer.
                await _get_equipment_tree_trace(up_data).run(up)
                energy_consumers.append(_build_row(up_data))

        csv_sfx = "energy_consumers.csv"
        print(f"Writing csvs/{feeder}_{csv_sfx}")
        network_objects = pd.DataFrame(energy_consumers)
        os.makedirs("csvs", exist_ok=True)
        network_objects.to_csv(f"csvs/{feeder}_{csv_sfx}", index=False)
        print(f"Finished processing {feeder}")


class NullEquipment:
    """empty class to simplify code below in the case of an equipment not existing in that position of the network"""
    mrid = None
    name = None


def _build_row(up_data: dict[str, IdentifiedObject | str]) -> EnergyConsumerDeviceHeirarchy:
    return EnergyConsumerDeviceHeirarchy(
        energy_consumer_mrid = up_data['energy_consumer_mrid'],
        upstream_switch_mrid = (up_data.get('upstream_switch') or NullEquipment).mrid,
        lv_circuit_name = (up_data.get('upstream_switch') or NullEquipment).name,
        upstream_switch_class = type(up_data.get('upstream_switch')).__name__,
        distribution_power_transformer_mrid = up_data.get('distribution_power_transformer').mrid,
        distribution_power_transformer_name = up_data.get('distribution_power_transformer').name,
        regulator_mrid = (up_data.get('regulator') or NullEquipment).mrid,
        breaker_mrid = (up_data.get('breaker') or NullEquipment).mrid,
        feeder_mrid = up_data.get('feeder'),
    )


if __name__ == "__main__":
    asyncio.run(main())