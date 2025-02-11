#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json

from zepben.evolve import Conductor, PowerTransformer, ConductingEquipment, EnergyConsumer, Switch, \
    connect_with_token, NetworkConsumerClient
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS, INCLUDE_ENERGIZED_FEEDERS, INCLUDE_ENERGIZING_SUBSTATIONS, \
    INCLUDE_ENERGIZING_FEEDERS

with open("config.json") as f:
    c = json.loads(f.read())


async def main():
    # See connecting_to_grpc_service.py for examples of each connect function
    print("Connecting to EWB..")
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"])
    feeder_mrid = "WD24"
    print(f"Fetching {feeder_mrid}")
    # Note you should create a new client for each Feeder you retrieve
    # There is also a NetworkConsumerClient that is asyncio compatible, with the same API.
    client = NetworkConsumerClient(channel=channel)
    network = client.service

    # Fetch feeder and all its LvFeeders
    await client.get_equipment_container(feeder_mrid, include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS)

    print()
    print(f"Total Number of objects: {client.service.len_of()}")
    types = set(type(x) for x in network.objects(ConductingEquipment))
    for t in types:
        print(f"Number of {t.__name__}'s = {len(list(network.objects(t)))}")

    total_length = 0
    for conductor in network.objects(Conductor):
        if conductor.length is not None:
            total_length += conductor.length

    print()
    print(f"Total conductor length in {feeder_mrid}: {total_length:.3f}m")

    print()
    feeder = network.get(feeder_mrid)
    print(f"{feeder.mrid} Transformers:")
    for eq in feeder.equipment:
        if isinstance(eq, PowerTransformer):
            print(f"    {eq} - Vector Group: {eq.vector_group.short_name}, Function: {eq.function.short_name}")
    print()

    print()
    print(f"{feeder_mrid} Energy Consumers:")
    for ec in network.objects(EnergyConsumer):
        print(f"    {ec} - Real power draw: {ec.q}W, Reactive power draw: {ec.p}VAr")
    print()

    print(f"{feeder_mrid} Switches:")
    for switch in network.objects(Switch):
        print(f"    {switch} - Open status: {switch.get_state():04b}")

    # === Some other examples of fetching containers ===

    # Fetch substation equipment and include equipment from HV/MV feeders powered by it
    await client.get_equipment_container("substation ID", include_energized_containers=INCLUDE_ENERGIZED_FEEDERS)

    # Same as above, but also fetch equipment from LV feeders powered by the HV/MV feeders
    await client.get_equipment_container("substation ID", include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS)

    # Fetch feeder equipment without fetching any additional equipment from powering/powered containers
    await client.get_equipment_container("feeder ID")

    # Fetch HV/MV feeder equipment, the equipment from the substation powering it, and the equipment from the LV feeders it powers
    await client.get_equipment_container("feeder ID",
                                   include_energizing_containers=INCLUDE_ENERGIZING_SUBSTATIONS,
                                   include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS)

    # Fetch LV feeder equipment and include equipment from HV/MV feeders powering it
    await client.get_equipment_container("LV feeder ID", include_energizing_containers=INCLUDE_ENERGIZING_FEEDERS)

    # Same as above, but also fetch equipment from the substations powering the HV/MV feeders
    await client.get_equipment_container("LV feeder ID", include_energizing_containers=INCLUDE_ENERGIZING_SUBSTATIONS)


if __name__ == "__main__":
    asyncio.run(main())
