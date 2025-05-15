#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""
Example trace showing methods to traverse upstream of a given `IdentifiedObject` to find the first occurrence
  of another specified `IdentifiedObject`
"""

import asyncio

from zepben.evolve import NetworkStateOperators, NetworkTraceActionType, Traversal, NetworkTraceStep, StepContext, \
    NetworkConsumerClient, connect_tls, ConductingEquipment
from zepben.evolve import PowerTransformer, UsagePoint, Tracing, Switch
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS


def _trace(start_item, results, stop_condition):
    """Returns a `NetworkTrace` configured with our parameters"""

    def step_action(step: NetworkTraceStep, context: StepContext):
        if context.is_stopping:  # if the trace is stopping, we have found the equipment we're looking for
            results.append(step.path.to_equipment)

    state_operators = NetworkStateOperators.NORMAL

    return (
        Tracing.network_trace(
            network_state_operators=state_operators,
            action_step_type=NetworkTraceActionType.ALL_STEPS
        ).add_condition(state_operators.upstream())
        .add_stop_condition(Traversal.stop_condition(stop_condition))
        .add_step_action(Traversal.step_action(step_action))
        .add_start_item(start_item)
    )


async def main(usage_point_mrid: str, feeder_mrid: str):
    channel = connect_tls(host='ewb.local', rpc_port=50051, ca_filename='ca.crt')
    client = NetworkConsumerClient(channel)
    await client.get_equipment_container(feeder_mrid, include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS)
    network = client.service

    usage_point = network.get(usage_point_mrid, UsagePoint)

    # get the `ConductingEquipment` from the `UsagePoint`
    start_item = next(filter(lambda ce: isinstance(ce, ConductingEquipment), usage_point.equipment))

    results = []

    # Get DSUB from which any given customer is supplied from using a basic upstream trace
    def dsub_stop_condition(step: NetworkTraceStep, context: StepContext):
        return isinstance(step.path.to_equipment, PowerTransformer)

    # Get Circuit Breaker from which any given customer is supplied from using a basic upstream trace
    def circuit_breaker_stop_condition(step: NetworkTraceStep, context: StepContext):
        return isinstance(step.path.to_equipment, Switch)

    await _trace(
        start_item=start_item,
        results=results,
        stop_condition=dsub_stop_condition,
        #stop_condition=circuit_breaker_stop_condition,
    ).run()

    print(results)


if __name__ == "__main__":
    asyncio.run(main(usage_point_mrid='4310990779', feeder_mrid='RW1292'))
