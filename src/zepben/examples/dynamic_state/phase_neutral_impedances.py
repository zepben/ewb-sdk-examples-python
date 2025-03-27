#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

from zepben.evolve import AcLineSegment, LvFeeder, NetworkConsumerClient, connect_with_token, PerLengthPhaseImpedance, Switch

with open("config.json") as f:
    c = json.loads(f.read())


async def phase_neutral_impedances_per_circuit():
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)
    network = network_client.service
    (await network_client.get_equipment_container("TT0-011",
                                                  include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
    for lvf in network.objects(LvFeeder):
        conductor_code_counts = {}
        if lvf.normal_head_terminal:
            if not isinstance(lvf.normal_head_terminal.conducting_equipment, Switch):
                continue
        null_count = 0
        for equipment in lvf.equipment:
            if isinstance(equipment, AcLineSegment):
                if equipment.per_length_phase_impedance:
                    conductor_code = equipment.per_length_phase_impedance.mrid
                    count = conductor_code_counts.get(conductor_code, 0)
                    count += 1
                    conductor_code_counts[conductor_code] = count
                else:
                    null_count += 1
        conductor_code_counts["UNKNOWN"] = null_count

        for code, count in sorted(conductor_code_counts.items(), key=lambda x: x[1]):
            if code == "UNKNOWN":
                print(f"Code: {code} - Count in {lvf.name}: {count}")
                continue

            plpi = network.get(code, PerLengthPhaseImpedance)
            print(f"Code: {code} - Count in {lvf.name}: {count}")
            for data in plpi.data:
                print(f"Phase: ({data.from_phase.short_name}, {data.to_phase.short_name}) Resistance: {data.r} Reactance: {data.x}")
            print()


async def phase_neutral_impedances():
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)
    network = network_client.service
    (await network_client.get_equipment_container("TT0-011",
                                                  include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
    conductor_code_counts = {}
    null_count = 0
    for plpi in network.objects(PerLengthPhaseImpedance):
        conductor_code = plpi.mrid
        print(f"Code: {conductor_code}:")
        for data in plpi.data:
            print(f"Phase: ({data.from_phase.short_name}, {data.to_phase.short_name}) Resistance: {data.r} Reactance: {data.x}")
            count = conductor_code_counts.get(conductor_code, 0)
            count += 1
            conductor_code_counts[conductor_code] = count
        else:
            null_count += 1
    conductor_code_counts["UNKNOWN"] = null_count

    for code, count in sorted(conductor_code_counts.items(), key=lambda x: x[1]):
        if code == "UNKNOWN":
            print(f"Code: {code} - Count in feeder: {count}")
            continue
        plpi = network.get(code, PerLengthPhaseImpedance)
        print(f"Code: {code} - Count in feeder: {count}")
        for data in plpi.data:
            print(f"Phase: ({data.from_phase.short_name}, {data.to_phase.short_name}) Resistance: {data.r} Reactance: {data.x}")
        print()

    print(f"Num conductors in TT0-011: {len([x for x in network.objects(AcLineSegment)])}")


if __name__ == "__main__":
    asyncio.run(phase_neutral_impedances())
    # asyncio.run(phase_neutral_impedances_per_circuit())
