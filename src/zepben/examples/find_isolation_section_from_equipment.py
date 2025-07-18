#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""
Example trace showing method to traverse outwards from any given `IdentifiableObject` to the next
`Switch` object, and build a list of all contained equipment (isolate-able section)
"""

import asyncio
import json

from zepben.evolve import (
    NetworkStateOperators, NetworkTraceActionType, NetworkTraceStep, StepContext,
    NetworkConsumerClient, AcLineSegment, connect_with_token, stop_at_open
)
from zepben.evolve import Tracing, Switch
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS


async def main(conductor_mrid: str, feeder_mrid: str):
    with open("config.json") as f:
        c = json.loads(f.read())

    channel = connect_with_token(
        host=c["host"],
        access_token=c["access_token"],
        rpc_port=c["rpc_port"]
    )
    client = NetworkConsumerClient(channel)
    await client.get_equipment_container(
        feeder_mrid, include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS
    )
    network = client.service

    hv_acls = network.get(conductor_mrid, AcLineSegment)

    found_equip = set()

    def queue_condition(step: NetworkTraceStep, context: StepContext, _, __):
        """Queue the next step unless it's a `Switch`"""
        return not isinstance(step.path.to_equipment, Switch)

    def step_action(step: NetworkTraceStep, context: StepContext):
        """Add to our list of equipment, and equipment stepped on during this trace"""
        found_equip.add(step.path.to_equipment.mrid)

    await (
        Tracing.network_trace(
            network_state_operators=NetworkStateOperators.NORMAL,
            action_step_type=NetworkTraceActionType.ALL_STEPS
        ).add_condition(stop_at_open())
        .add_queue_condition(queue_condition)
        .add_step_action(step_action)
        .add_start_item(hv_acls)
    ).run()

    # print a list of all mRID's for all equipment in the isolation area.
    print(found_equip)


if __name__ == "__main__":
    asyncio.run(main(conductor_mrid='50434998', feeder_mrid='RW1292'))
