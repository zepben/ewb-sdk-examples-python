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
from zepben.evolve import connect_with_token, NetworkConsumerClient, Feeder, Tracing, downstream, StepActionWithContextValue, \
    NetworkTraceStep, StepContext, IdentifiedObject, Breaker, Fuse, PowerTransformer, TransformerFunctionKind, \
    Conductor

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers


"""
This example follows the same pattern as `energy_consumer_device_hierarchy.py` 
"""


@dataclass
class ConductorTxRating:
    equipment_mrid: str  # mRID of Conductor or PowerTransformer
    equipment_type: str  # Conductor or PowerTransformer
    rating: Optional[float]
    voltage: int | str | None
    lv_circuit_name: Optional[str]
    upstream_switch_mrid: Optional[str]
    upstream_switch_class: Optional[str]
    upstream_switch_rating: Optional[float]
    distribution_power_transformer_mrid: Optional[str]
    distribution_power_transformer_name: Optional[str]
    regulator_mrid: Optional[str]
    feeder_head_mrid: str
    feeder_mrid: str


def _get_client():
    with open('config.json') as f:
        c = json.load(f)

    # Connect to server
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"], ca_filename=c.get("ca_path", None))
    return NetworkConsumerClient(channel)


async def get_feeders(_client=None) -> Dict[str, Feeder]:
    _feeders = (await (_client or _get_client()).get_network_hierarchy()).result.feeders
    return _feeders


async def get_feeder_equipment(client: NetworkConsumerClient, feeder_mrid: str) -> None:
    """Get all objects under the feeder, including LV Feeders"""
    (await client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()


async def trace_from_feeder_context(feeder_mrid: str, client=None):
    """
    Most efficient/fastest.
    trace downstream from the feeder recording relevant information using `NetworkTrace` `StepContext`.
    """
    client = client or _get_client()
    # Get all objects under the feeder, including Substations and LV Feeders
    await get_feeder_equipment(client, feeder_mrid)

    conducting_equipment = []

    feeder = client.service.get(feeder_mrid, Feeder)

    class StepActionWithContext(StepActionWithContextValue):
        def _apply(self, item: NetworkTraceStep, context: StepContext):
            if isinstance((ce := item.path.to_equipment), (Conductor, PowerTransformer)):
                nonlocal conducting_equipment
                data = self.get_context_value(context)
                data.update({'feeder': feeder, 'conducting_equipment': ce})

                row = _build_row(data)
                conducting_equipment.append(row)

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

    # Trace downstream from feeder head performing above _apply function at each step of the trace.
    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(StepActionWithContext('key'))
    ).run(getattr(feeder, 'normal_head_terminal'))

    write_csv(conducting_equipment, feeder.mrid)


def write_csv(energy_consumers: List[ConductorTxRating], feeder_mrid: str):
    network_objects = pd.DataFrame(energy_consumers)
    os.makedirs("csvs", exist_ok=True)
    network_objects.to_csv(f"csvs/{feeder_mrid}_ratings.csv", index=False)


class NullEquipment:
    """empty class to simplify code below in the case of an equipment not existing in that position of the network"""
    mrid = None
    name = None


def _build_row(ce_data: dict[str, IdentifiedObject | str]) -> ConductorTxRating:
    # Fetch rating for power transformers or conductors
    ce = ce_data['conducting_equipment']
    if isinstance(ce, PowerTransformer):
        primary = ce.get_end_by_num(1)
        rating = primary.rated_s
    elif isinstance(ce, Conductor):
        rating = ce.wire_info.rated_current
    else:
        rating = 0.0

    # Extract upstream switch - populated in compute_next_value
    upstream_switch = ce_data.get('upstream_switch')
    upstream_switch_class = type(upstream_switch).__name__ if upstream_switch else None
    upstream_switch_rating = upstream_switch.rated_current if upstream_switch else None

    # Extract feeder and feeder head mRID
    feeder: Feeder | None = ce_data.get('feeder')
    feeder_head = None
    if feeder:
        if feeder.normal_head_terminal:
            feeder_head = feeder.normal_head_terminal.conducting_equipment

    feeder_head_mrid = feeder_head.mrid if feeder_head else None

    if isinstance(ce, PowerTransformer):
        voltage = ",".join([str(e.rated_u) for e in ce.ends])
    else:
        voltage = ce.base_voltage_value

    return ConductorTxRating(
        equipment_mrid=ce.mrid,
        equipment_type=type(ce).__name__,
        rating=rating,
        voltage=voltage,
        lv_circuit_name=(ce_data.get('upstream_switch') or NullEquipment).name,
        upstream_switch_mrid=(ce_data.get('upstream_switch') or NullEquipment).mrid,
        upstream_switch_class=upstream_switch_class,
        upstream_switch_rating=upstream_switch_rating,
        distribution_power_transformer_mrid=(ce_data.get('distribution_power_transformer') or NullEquipment).mrid,
        distribution_power_transformer_name=(ce_data.get('distribution_power_transformer') or NullEquipment).name,
        regulator_mrid=(ce_data.get('regulator') or NullEquipment).mrid,
        feeder_head_mrid=feeder_head_mrid,
        feeder_mrid=ce_data.get('feeder').mrid,
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
        feeders = ["<FEEDER_ID>"]  # TODO: Specify a Feeder ID
        # feeders = list(await get_feeders(client)) # Uncomment to run all feeders
        for _feeder in tqdm(feeders):
            await trace_type(_feeder, client)

    asyncio.run(main_async(trace_from_feeder_context))


def process_feeders_concurrently():
    def multi_proc(_feeder):
        asyncio.run(trace_from_feeder_context(_feeder))

    # Get a list of feeders before entering main compute section of script.
    feeders = list(asyncio.run(get_feeders())) # Uncomment to run all feeders

    from tqdm.contrib.concurrent import process_map
    process_map(multi_proc, feeders, max_workers=int(os.cpu_count() / 2))


if __name__ == "__main__":
    process_feeders_sequentially()  # Make sure you replace <FEEDER_ID> in this function
    # process_feeders_concurrently()  # Uncomment and comment sequentially above to multi-process, note this is resource intensive and may cause issues.
