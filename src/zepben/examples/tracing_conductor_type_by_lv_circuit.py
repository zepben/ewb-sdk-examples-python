#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import csv
import json
import os
from typing import List, Union, Tuple, Optional, Dict

from zepben.evolve import NetworkConsumerClient, PhaseCode, AcLineSegment, \
    FeederDirection, connect_with_token, Tracing, downstream, NetworkTraceStep, ConductingEquipment, PowerTransformer
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS

LineInfo = Tuple[str, str, Optional[Union[int, float]]]


async def main():
    with open("config.json") as f:
        c = json.loads(f.read())

    print("Connecting to Server")
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"])

    client = NetworkConsumerClient(channel)
    result = (await client.get_network_hierarchy()).throw_on_error().result
    print("Connection Established")

    tx_to_line_type: Dict[str, Tuple[List[LineInfo], bool]] = {}

    os.makedirs("csvs", exist_ok=True)
    for feeder in result.feeders.values():
        print(f"Fetching {feeder.mrid}")
        if not (network := await get_feeder_network(channel, feeder.mrid)):  # Skip feeders that fail to pull down
            print(f"Failed to retrieve feeder {feeder.mrid}")
            continue
        for io in network.objects(PowerTransformer):
            print(io)
            _loop = False

            for t in io.terminals:
                t_dir = t.normal_feeder_direction
                if t_dir == FeederDirection.BOTH:
                    _loop = True


            tx_to_line_type[io.mrid] = (await get_downstream_trace(io, PhaseCode.ABCN), _loop)
        await save_to_csv(tx_to_line_type, feeder.mrid)


async def save_to_csv(data: Dict[str, Tuple[List[LineInfo], bool]], feeder_mrid):
    filename = f"csvs/conductor_types_{feeder_mrid}.csv"
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Feeder", "Transformer", "Line", "Line Type", "Length", "Loop"])

        for transformer, (values, loop) in data.items():
            for value in values:
                line, line_type, length = value
                switch_data = [feeder_mrid, transformer, line, line_type, length, loop]
                writer.writerow(switch_data)

    print(f"Data saved to {filename}")


async def get_feeder_network(channel, feeder_mrid):
    client = NetworkConsumerClient(channel)
    result = (
        await client.get_equipment_container(
            mrid=feeder_mrid,
            include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS
        )
    )
    if result.was_failure:
        print(f"Failed: {result.thrown}")
        return None
    return client.service


async def get_downstream_trace(ce: ConductingEquipment, phase_code: PhaseCode) -> List[LineInfo]:
    l_type: List[LineInfo] = []

    def collect_eq_in(step: NetworkTraceStep, _):
        if isinstance(equip := step.path.to_equipment, AcLineSegment):
            nonlocal l_type
            l_type.append((equip.mrid, equip.asset_info.name, equip.length or 0))

    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(collect_eq_in)
    ).run(start=ce, phases=phase_code)

    return l_type


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
