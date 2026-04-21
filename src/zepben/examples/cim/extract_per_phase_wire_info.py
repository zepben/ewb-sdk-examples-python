#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json

from zepben.ewb import AcLineSegment, NetworkConsumerClient, connect_with_token, SinglePhaseKind, WireInfo, CableInfo, IncludedEnergizedContainers

from zepben.examples import CONFIG_DIR

with open(f"{CONFIG_DIR}/config.json") as f:
    c = json.loads(f.read())


async def extract_wire_info_per_phase(feeder_mrid: str):
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)
    network = network_client.service
    (await network_client.get_equipment_container(feeder_mrid,
                                                  include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS)).throw_on_error()
    for conductor in network.objects(AcLineSegment):
        # Phase wire info is typically the same for all phases, so lazily just check A.
        phase_wire_info = conductor.wire_info_for_phase(SinglePhaseKind.A) or conductor.wire_info_for_phase(SinglePhaseKind.B) or conductor.wire_info_for_phase(SinglePhaseKind.C)
        neutral_wire_info = conductor.wire_info_for_phase(SinglePhaseKind.N)
        print(f"{conductor}")
        if not phase_wire_info and not neutral_wire_info:
            print(f"  No wire info found.")
        print_wire_info("Phase", phase_wire_info)
        if neutral_wire_info is not phase_wire_info:
            print_wire_info("Neutral", neutral_wire_info)

        print(f"")


def print_wire_info(descriptor: str, wire_info: WireInfo | None):
    if wire_info:
        print(f"  {descriptor} {wire_info}: ")
        print(f"    rated_current: {wire_info.rated_current}")
        print(f"    material: {wire_info.material}")
        print(f"    size_description: {wire_info.size_description}")
        print(f"    strand_count: {wire_info.strand_count}")
        print(f"    core_strand_count: {wire_info.core_strand_count}")
        print(f"    insulated: {wire_info.insulated}")
        print(f"    insulation_material: {wire_info.insulation_material}")
        print(f"    insulation_thickness: {wire_info.insulation_thickness}")


if __name__ == "__main__":
    asyncio.run(extract_wire_info_per_phase("YVE-014"))
