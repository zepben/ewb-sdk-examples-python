#  Copyright 2024 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import csv
import json
import os
from typing import Any

from zepben.evolve import NetworkConsumerClient, PhaseStep, PhaseCode, AcLineSegment, \
    Switch, normal_downstream_trace, FeederDirection, connect_with_token
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

    switch_to_line_type: dict[str, tuple[list[Any], bool]] = {}

    os.makedirs("csvs", exist_ok=True)
    for feeder in result.feeders.values():
        print(f"Fetching {feeder.mrid}")
        network = await get_feeder_network(channel, feeder.mrid)
        if not network:  # Skip feeders that fail to pull down
            print(f"Failed to retrieve feeder {feeder.mrid}")
            continue
        for io in network.objects(Switch):
            loop = False

            for t in io.terminals:
                t_dir = t.normal_feeder_direction
                if t_dir == FeederDirection.BOTH:
                    loop = True

            sw_name = io.name
            sw_id = io.mrid

            # Currently using switch with the following name as a marker for LV circuit heads
            if "Circuit Head Switch" in sw_name:
                switch_to_line_type[sw_id] = (
                    await get_downstream_trace(start_at(io, PhaseCode.ABCN)),
                    loop
                )
        await save_to_csv(switch_to_line_type, feeder.mrid)


async def save_to_csv(data: dict[str, tuple[list[Any], bool]], feeder_mrid):
    filename = f"csvs/conductor_types_{feeder_mrid}.csv"
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Feeder", "Switch", "Line", "Line Type", "Length", "Loop"])

        for switch, (values, loop) in data.items():
            for i in range(0, len(values), 3):
                line_type = values[i + 1] if i + 1 < len(values) else ""
                length = values[i + 2] if i + 2 < len(values) else ""
                switch_data = [feeder_mrid, switch, values[i], line_type, length, loop]
                writer.writerow(switch_data)

    print(f"Data saved to {filename}")


async def get_feeder_network(channel, feeder_mrid):
    client = NetworkConsumerClient(channel)
    result = (await client.get_equipment_container(mrid=feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS))
    if result.was_failure:
        print(f"Failed: {result.thrown}")
        return None
    return client.service


async def get_downstream_trace(ce: PhaseStep) -> list[Any]:
    trace = normal_downstream_trace()
    l_type: [str, str, float] = []

    def collect_eq_in():
        async def add_eq(ps, _):
            if isinstance(ps.conducting_equipment, AcLineSegment):
                l_type.append(ps.conducting_equipment.mrid)
                l_type.append(ps.conducting_equipment.asset_info.name)
                if ps.conducting_equipment.length is not None:
                    l_type.append(ps.conducting_equipment.length)
                else:
                    l_type.append(0)

        return add_eq

    trace.add_step_action(collect_eq_in())
    await trace.run(ce)
    return l_type


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
