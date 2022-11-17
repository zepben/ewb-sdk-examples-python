#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from zepben.evolve import Conductor, PowerTransformer, connect_with_password, SyncNetworkConsumerClient, ConductingEquipment, EnergyConsumer, Switch, \
    SinglePhaseKind, connected_equipment_trace, current_connected_equipment_trace, ConductingEquipmentStep
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS


def main():
    channel = connect_with_password(host="dev.ewb.essential.zepben.com",
                                    rpc_port=443,
                                    username="<username>@essentialenergy.com.au",
                                    password="<your-password>",
                                    client_id="jTLCsjEHE0bp6u4dhhGMAIjtJBuI1Sr6")   # Client ID will always be this for this instance of EWB.
    feeder_mrid = "CPM3B3"
    print(f"Fetching {feeder_mrid}")
    # Note you should create a new client for each Feeder you retrieve
    # There is also a NetworkConsumerClient that is asyncio compatible, with the same API.
    client = SyncNetworkConsumerClient(channel=channel)
    network = client.service

    # Fetch feeder and all its LvFeeders
    client.get_equipment_container(feeder_mrid, include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS).throw_on_error()

    print(f"Total Number of objects: {client.service.len_of()}")
    types = set(type(x) for x in network.objects(ConductingEquipment))
    for t in types:
        print(f"Number of {t.__name__}'s = {len(list(network.objects(t)))}")

    total_length = 0
    for conductor in network.objects(Conductor):
        total_length += conductor.length

    print(f"Total conductor length in {feeder_mrid}: {total_length:.3f}m")

    feeder = network.get(feeder_mrid)
    print(f"{feeder.mrid} Transformers:")
    for eq in feeder.equipment:
        if isinstance(eq, PowerTransformer):
            print(f"    {eq} - Vector Group: {eq.vector_group.short_name}, Function: {eq.function.short_name}")
    print()

    print(f"{feeder_mrid} Energy Consumers:")
    for ec in network.objects(EnergyConsumer):
        print(f"    {ec} - Real power draw: {ec.q}W, Reactive power draw: {ec.p}VAr")
    print()

    print(f"{feeder_mrid} Switches:")
    for switch in network.objects(Switch):
        print(f"    {switch} - Open status: {switch.get_state():04b}")


if __name__ == "__main__":
    main()
