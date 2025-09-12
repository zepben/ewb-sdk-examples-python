#  Copyright 2025 Zeppelin Bend Pty Ltd
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
    ConductingEquipment, NetworkTrace, upstream, IncludedEnergizedContainers


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


async def get_feeders(_client=None) -> Dict[str, Feeder]:
    _feeders = (await (_client or _get_client()).get_network_hierarchy()).result.feeders
    return _feeders


async def get_feeder_equipment(client: NetworkConsumerClient, feeder_mrid: str) -> None:
    """Get all objects under the feeder, including LV Feeders"""
    (await client.get_equipment_container(
        feeder_mrid,
        include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS
    )).throw_on_error()


async def trace_from_energy_consumers(feeder_mrid: str, client=None):
    """
    Least efficient/the slowest
    Inefficient upstream tracing example.
    Trace upstream from every EnergyConsumer.
    """
    client = client or _get_client()
    await get_feeder_equipment(client, feeder_mrid)

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

    feeder = client.service.get(feeder_mrid, Feeder)

    energy_consumers = []
    for lvf in feeder.normal_energized_lv_feeders:
        for ce in lvf.equipment:
            if isinstance(ce, EnergyConsumer):
                up_data = {'feeder': feeder_mrid, 'energy_consumer_mrid': ce.mrid}

                # Trace upstream from EnergyConsumer.
                await _get_equipment_tree_trace(up_data).run(ce)
                energy_consumers.append(_build_row(up_data))

    write_csv(energy_consumers, feeder.mrid)


async def trace_from_feeder_downstream(feeder_mrid: str, client=None):
    """
    More memory use than `trace_from_feeder_context`, more efficient/faster than `trace_from_energy_consumers`
    Build an equipment tree of everything downstream of the feeder.
    Use the Equipment tree to recurse through parent equipment of all EC's and get the equipment we are interested in.
    """

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

    client = client or _get_client()
    await get_feeder_equipment(client, feeder_mrid)

    builder = EquipmentTreeBuilder()

    feeder = client.service.get(feeder_mrid, Feeder)
    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(builder)
    ).run(getattr(feeder, 'normal_head_terminal'))

    energy_consumers = []

    for leaf in (l for l in builder.leaves if isinstance((ec := l.identified_object), EnergyConsumer)):
        ec_data = {'feeder': feeder.mrid, 'energy_consumer_mrid': ec.mrid}

        def _process(_leaf):
            process_leaf(ec_data, _leaf)
            if _leaf.parent:
                _process(_leaf.parent)

        _process(leaf)

        row = _build_row(ec_data)
        energy_consumers.append(row)

    write_csv(energy_consumers, feeder.mrid)


async def trace_from_feeder_context(feeder_mrid: str, client=None):
    """
    Most efficient/fastest.
    trace downstream from the feeder recording relevant information using `NetworkTrace` `StepContext`.
    """
    client = client or _get_client()
    # Get all objects under the feeder, including Substations and LV Feeders
    await get_feeder_equipment(client, feeder_mrid)

    energy_consumers = []

    feeder = client.service.get(feeder_mrid, Feeder)

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

    write_csv(energy_consumers, feeder.mrid)


def write_csv(energy_consumers: List[EnergyConsumerDeviceHierarchy], feeder_mrid: str):
    network_objects = pd.DataFrame(energy_consumers)
    os.makedirs("csvs", exist_ok=True)
    network_objects.to_csv(f"csvs/{feeder_mrid}_energy_consumers.csv", index=False)


class NullEquipment:
    """empty class to simplify code below in the case of an equipment not existing in that position of the network"""
    mrid = None
    name = None


def _build_row(up_data: dict[str, IdentifiedObject | str]) -> EnergyConsumerDeviceHierarchy:
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
        feeders = ["<FEEDER_ID>"]
        # feeders = list(await get_feeders(client)) # Uncomment to process all feeders
        for _feeder in tqdm(feeders):
            await trace_type(_feeder, client)

    # Uncomment to run other trace functions
    asyncio.run(main_async(trace_from_feeder_context))
    # asyncio.run(main_async(trace_from_feeder_downstream))
    # asyncio.run(main_async(trace_from_energy_consumers))


def process_feeders_concurrently():
    def multi_proc(_feeder):
        # Uncomment to run other trace functions
        asyncio.run(trace_from_feeder_context(_feeder))
        # asyncio.run(trace_from_feeder_downstream(_feeder))
        # asyncio.run(trace_from_energy_consumers(_feeder))

    # Get a list of feeders before entering main compute section of script.
    feeders = list(asyncio.run(get_feeders()))

    from tqdm.contrib.concurrent import process_map
    process_map(multi_proc, feeders, max_workers=int(os.cpu_count() / 2))


if __name__ == "__main__":
    process_feeders_sequentially()
    # process_feeders_concurrently()  # Uncomment and comment sequentially above to multi-process, note this is resource intensive and may cause issues.
