#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""
For every piece of `ConductingEquipment` downstream of a feeder, records a row with its equipment type,
nominal voltage, length (where applicable), and the nearest upstream protection device (fuse or LV
circuit breaker), distribution transformer, regulator and breaker mrid — so an `element_id` can be
related back to its measurement zones.
"""
import asyncio
import json
import os
import pandas as pd

from typing import Dict
from zepben.ewb import connect_with_token, NetworkConsumerClient, Feeder, Tracing, downstream, StepActionWithContextValue, \
    NetworkTraceStep, StepContext, IdentifiedObject, Breaker, Fuse, PowerTransformer, TransformerFunctionKind, ConductingEquipment, \
    Conductor, IncludedEnergizedContainers

FEEDER_MRID = "27189"


def _get_client() -> NetworkConsumerClient:
    with open('config.json') as f:
        config = json.load(f)
    return NetworkConsumerClient(connect_with_token(**config))


def _build_row(equip: ConductingEquipment, upstream: dict) -> dict:
    return {
        'element_mrid': equip.mrid,
        'element_name': equip.name,
        'element_type': type(equip).__name__,
        'nominal_voltage': equip.base_voltage_value,
        'length': getattr(equip, 'length', None) if isinstance(equip, Conductor) else None,
        'upstream_protection_device_mrid': getattr(upstream.get('upstream_protection_device'), 'mrid', None),
        'distribution_power_transformer_mrid': getattr(upstream.get('distribution_power_transformer'), 'mrid', None),
        'regulator_mrid': getattr(upstream.get('regulator'), 'mrid', None),
        'breaker_mrid': getattr(upstream.get('breaker'), 'mrid', None),
    }


async def trace_element_hierarchy_from_feeder(feeder_mrid: str) -> None:
    client = _get_client()
    (await client.get_equipment_container(
        feeder_mrid,
        include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS
    )).throw_on_error()

    feeder = client.service.get(feeder_mrid, Feeder)
    rows = []

    class StepActionWithContext(StepActionWithContextValue):
        def _apply(self, item: NetworkTraceStep, context: StepContext):
            equip = item.path.to_equipment
            if isinstance(equip, ConductingEquipment):
                rows.append(_build_row(equip, self.get_context_value(context)))

        def compute_next_value(self, next_item: NetworkTraceStep, current_item: NetworkTraceStep, current_value: Dict[str, IdentifiedObject]):
            upstream = dict(current_value)
            equip = next_item.path.to_equipment
            if isinstance(equip, Breaker):
                upstream['breaker'] = equip
                if equip.base_voltage_value is not None and equip.base_voltage_value <= 1000:
                    upstream['upstream_protection_device'] = equip
            elif isinstance(equip, Fuse):
                upstream['upstream_protection_device'] = equip
            elif isinstance(equip, PowerTransformer):
                if equip.function == TransformerFunctionKind.distributionTransformer:
                    upstream['distribution_power_transformer'] = equip
                elif equip.function == TransformerFunctionKind.voltageRegulator:
                    upstream['regulator'] = equip
            return upstream

        def compute_initial_value(self, item: NetworkTraceStep):
            return {}

    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(StepActionWithContext('key'))
    ).run(feeder.normal_head_terminal)

    os.makedirs("csvs", exist_ok=True)
    pd.DataFrame(rows).to_csv(f"csvs/{feeder_mrid}_network_elements.csv", index=False)


if __name__ == "__main__":
    asyncio.run(trace_element_hierarchy_from_feeder(FEEDER_MRID))