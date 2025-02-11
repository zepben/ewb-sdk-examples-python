#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.


import asyncio
import json

from zepben.evolve import NetworkConsumerClient, PhaseStep, PhaseCode, AcLineSegment, normal_downstream_trace, connect_with_token, EnergyConsumer, \
    PowerTransformer, normal_upstream_trace
from zepben.evolve.services.network.tracing.phases.phase_step import start_at
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

with open("config.json") as f:
    c = json.loads(f.read())


async def main():
    print("Connecting to Server")
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"])

    client = NetworkConsumerClient(channel)
    result = (await client.get_network_hierarchy()).throw_on_error().result
    print("Connection Established")

    for feeder in result.feeders.values():
        if feeder.mrid != "WD24":
            continue
        print()
        print(f"Fetching {feeder.mrid}")
        network = await get_feeder_network(channel, feeder.mrid)

        print()
        print("Downstream Trace Example..")
        # Get the count of customers per transformer
        for io in network.objects(PowerTransformer):
            customers = await get_downstream_customer_count(start_at(io, PhaseCode.ABCN))
            print(f"Transformer {io.mrid} has {customers} Energy Consumer(s)")

        print()
        print("Upstream Trace Example..")
        for ec in network.objects(EnergyConsumer):
            upstream_length = await get_upstream_length(start_at(ec, PhaseCode.ABCN))
            print(f"Energy Consumer {ec.mrid} --> Upstream Length: {upstream_length}")


async def get_feeder_network(channel, feeder_mrid):
    client = NetworkConsumerClient(channel)
    (await client.get_equipment_container(mrid=feeder_mrid,
                                          include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
    return client.service


async def get_downstream_customer_count(ce: PhaseStep) -> int:
    trace = normal_downstream_trace()
    customer_count = 0

    def collect_eq_in():
        async def add_eq(ps, _):
            nonlocal customer_count
            if isinstance(ps.conducting_equipment, EnergyConsumer):
                customer_count += 1
        return add_eq

    trace.add_step_action(collect_eq_in())
    await trace.run(ce)
    return customer_count


async def get_upstream_length(ce: PhaseStep) -> int:
    trace = normal_upstream_trace()
    upstream_length = 0

    def collect_eq_in():
        async def add_eq(ps, _):
            nonlocal upstream_length
            if isinstance(ps.conducting_equipment, AcLineSegment):
                if ps.conducting_equipment.length is not None:
                    upstream_length = upstream_length + ps.conducting_equipment.length
        return add_eq

    trace.add_step_action(collect_eq_in())
    await trace.run(ce)
    return upstream_length

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
