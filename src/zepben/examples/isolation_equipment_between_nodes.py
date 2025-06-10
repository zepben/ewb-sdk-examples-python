#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""
Example trace showing method to traverse between any 2 given `IdentifiableObject` and build a list
of `ProtectedSwitch` objects found, if any
"""

import asyncio
import json
from typing import Tuple, Type

from zepben.evolve import (
    NetworkStateOperators, NetworkTraceActionType, NetworkTraceStep, StepContext, Tracing,
    NetworkConsumerClient, ProtectedSwitch, Recloser, LoadBreakSwitch, connect_with_token
)
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS


async def main(mrids: Tuple[str, str], io_type: Type[ProtectedSwitch], feeder_mrid):
    with open("config.json") as f:
        c = json.loads(f.read())

    channel = connect_with_token(
        host=c["host"],
        access_token=c["access_token"],
        rpc_port=c["rpc_port"]
    )
    client = NetworkConsumerClient(channel)
    await client.get_equipment_container(
        feeder_mrid,
        include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS
    )
    network = client.service

    nodes = [network.get(_id) for _id in mrids]

    state_operators = NetworkStateOperators.NORMAL

    found_switch = set()
    found_node = []

    def stop_condition(step: NetworkTraceStep, context: StepContext):
        """if we encounter any of the equipment we have specified, we stop the trace and mark the `found_switch` list as valid"""
        if step.path.to_equipment in nodes:
            found_node.append(True)
            return True

    def step_action(step: NetworkTraceStep, context: StepContext):
        """Add any equipment matching the type passed in to the list, this list is invalid unless we trace onto our other node"""
        if isinstance(step.path.to_equipment, io_type):
            found_switch.add(step.path.to_equipment)

    trace = (
        Tracing.network_trace(
            network_state_operators=state_operators,
            action_step_type=NetworkTraceActionType.ALL_STEPS
        ).add_condition(state_operators.upstream())
        .add_stop_condition(stop_condition)
        .add_step_action(step_action)
    )

    queue = iter(nodes)
    while not found_node:  # run an upstream trace for every node specified until we encounter another specified node
        await trace.run(start=next(queue), can_stop_on_start_item=False)

    all(map(print, found_switch))  # print the list of switches
    print(bool(found_switch))  # print whether we found what we were looking for


if __name__ == "__main__":
    asyncio.run(main(mrids=('50735858', '66598892'), io_type=LoadBreakSwitch, feeder_mrid='RW1292'))
    asyncio.run(main(mrids=('50735858', '50295424'), io_type=Recloser, feeder_mrid='RW1292'))
