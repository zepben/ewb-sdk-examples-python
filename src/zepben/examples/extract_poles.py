#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

from zepben.evolve import NetworkConsumerClient, connect_with_token, Pole

with open("config-local.json") as f:
    c = json.loads(f.read())


async def connect():
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)

    network_hierarchy = (await network_client.get_network_hierarchy()).throw_on_error().value

    print("Network hierarchy:")
    for gr in network_hierarchy.geographical_regions.values():
        print(f"- Geographical region: {gr.name}")
        for sgr in gr.sub_geographical_regions:
            print(f"  - Subgeographical region: {sgr.name}")
            for sub in sgr.substations:
                print(f"    - Zone Substation: {sub.name}")
                for fdr in sub.feeders:
                    print(f"      - Processing Feeder: {fdr.name}")
                    if fdr.mrid == "PTN-011":
                        await process_nodes(fdr.mrid, channel)


async def process_nodes(feeder_mrid: str, channel):
    print("Fetching from server ...")
    network_client = NetworkConsumerClient(channel=channel)
    network_service = network_client.service
    (await network_client.get_equipment_container(feeder_mrid,
                                                  include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()

    print("Processing poles ...")
    for pole in network_service.objects(Pole):
        for psr in pole.power_system_resources:
            print(f"Pole: {pole.mrid}, Equipment: {psr}")


if __name__ == "__main__":
    asyncio.run(connect())
