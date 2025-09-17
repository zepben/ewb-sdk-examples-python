#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.


import asyncio
import json

from zepben.evolve import (
    NetworkConsumerClient, PhaseCode, AcLineSegment, connect_with_token, EnergyConsumer,
    PowerTransformer, ConductingEquipment, Tracing, NetworkTraceStep, downstream, upstream,
    UsagePoint
)
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS


async def main():
    with open("config.json") as f:
        c = json.loads(f.read())

    print("Connecting to Server")
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"])

    client = NetworkConsumerClient(channel)
    result = (await client.get_network_hierarchy()).throw_on_error().result
    print("Connection Established")

    for feeder in result.feeders.values():
        if feeder.mrid != "RW1292":
            continue
        print(f"\nFetching {feeder.mrid}")
        network = await get_feeder_network(channel, feeder.mrid)

        print("\nDownstream Trace Example..")
        # Get the count of customers per transformer
        fuse = network.get("114881231")
        customers = await get_downstream_customer_count(fuse, PhaseCode.ABCN)
        print(f"Fuse {fuse.mrid} has {customers} Energy Consumer(s)")

        tx= network.get("94646671")
        customers = await get_downstream_customer_count(tx, PhaseCode.ABCN)
        print(f"TX {tx.mrid} has {customers} Energy Consumer(s)")
        for io in network.objects(PowerTransformer):
            customers = await get_downstream_customer_count(io, PhaseCode.ABCN)
            print(f"Transformer {io.mrid} has {customers} Energy Consumer(s)")

        print("\nUpstream Trace Example..")
        for ec in network.objects(EnergyConsumer):
            upstream_length = await get_upstream_length(ec, PhaseCode.ABCN)
            print(f"Energy Consumer {ec.mrid} --> Upstream Length: {upstream_length}")


async def get_feeder_network(channel, feeder_mrid):
    client = NetworkConsumerClient(channel)
    (await client.get_equipment_container(
        mrid=feeder_mrid,
        include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS
    )).throw_on_error()
    return client.service


async def get_downstream_customer_count(ce: ConductingEquipment, phase_code: PhaseCode) -> int:
    customer_count = 0
    up_count = 0
    def collect_eq_in():
        async def add_eq(ps: NetworkTraceStep, _):
            nonlocal customer_count, up_count
            if isinstance(ps.path.to_equipment, EnergyConsumer):
                customer_count += 1
                ups = [up for up in ps.path.to_equipment.usage_points]
                if ups:
                    print(f"{ps.path.to_equipment}: {ups}")
                    up_count += len(ups)
        return add_eq

    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(collect_eq_in())
    ).run(start=ce, phases=phase_code)

    print(f"Up count for {ce.mrid}: {up_count}")
    return customer_count


async def get_upstream_length(ce: ConductingEquipment, phases: PhaseCode) -> int:
    upstream_length = 0

    def collect_eq_in():
        async def add_eq(ps: NetworkTraceStep, _):
            nonlocal upstream_length
            equip = ps.path.to_equipment
            if isinstance(equip, AcLineSegment):
                if equip.length is not None:
                    upstream_length += equip.length
        return add_eq

    await (
        Tracing.network_trace()
        .add_condition(upstream())
        .add_step_action(collect_eq_in())
    ).run(start=ce, phases=phases)

    return upstream_length

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
