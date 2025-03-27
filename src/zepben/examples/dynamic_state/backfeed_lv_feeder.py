#  Copyright 2025 Zeppelin Bend Pty Ltd
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json
import time
from datetime import datetime
from typing import Optional, List

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers, NetworkState

from zepben.evolve import EnergyConsumer, AcLineSegment, connect_tls, \
    LvFeeder, NetworkConsumerClient, UpdateNetworkStateClient, SwitchStateEvent, SwitchAction, connect_with_token, Feeder, Junction


with open("config.json") as f:
    c = json.loads(f.read())


async def counts_disconnected_circuits(circuits: List[str]):
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)
    network = network_client.service
    (await network_client.get_equipment_container("TT0-011",
                                                  include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()

    tt0011 = network.get("TT0-011", Feeder)
    for lv_feeder in tt0011.normal_energized_lv_feeders:
        (await network_client.get_equipment_for_container(lv_feeder.mrid,
                                                          include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS,
                                                          network_state=NetworkState.ALL_NETWORK_STATE)).throw_on_error()

    for circuit_mrid in circuits:
        circuit = network.get(circuit_mrid, LvFeeder)
        normal_energy_consumers = []
        current_energy_consumers = []
        normal_usage_points = []
        current_usage_points = []
        current_nmis = []
        normal_nmis = []
        for ce in circuit.equipment:
            if isinstance(ce, EnergyConsumer):
                normal_energy_consumers.append(ce)
            for up in ce.usage_points:
                normal_usage_points.append(up)

        for ce in circuit.current_equipment:
            if isinstance(ce, EnergyConsumer):
                current_energy_consumers.append(ce)
            for up in ce.usage_points:
                current_usage_points.append(up)

        for up in normal_usage_points:
            for name in up.names:
                if name.type.name == "NMI":
                    normal_nmis.append(name.name)

        for up in current_usage_points:
            for name in up.names:
                if name.type.name == "NMI":
                    current_nmis.append(name.name)

        print(f"Num normal equipment {circuit.name}: {len(list(circuit.equipment))}")
        print(f"Num normal supply points {circuit.name}: {len(normal_energy_consumers)}")
        print(f"Num normal NMIs {circuit.name}: {len(normal_nmis)}")
        print(f"Num normal conductors {circuit.name}: {len([x for x in circuit.equipment if isinstance(x, AcLineSegment)])}")
        print(f"Num normal junctions {circuit.name}: {len([x for x in circuit.equipment if isinstance(x, Junction)])}")
        print(f"Num current equipment {circuit.name}: {len(list(circuit.current_equipment))}")
        print(f"Num current supply points {circuit.name}: {len(current_energy_consumers)}")
        print(f"Num current NMIs {circuit.name}: {len(current_nmis)}")
        print(f"Num current conductors {circuit.name}: {len([x for x in circuit.current_equipment if isinstance(x, AcLineSegment)])}")
        print(f"Num current junctions {circuit.name}: {len([x for x in circuit.current_equipment if isinstance(x, Junction)])}")
        print()


async def reset_switches():
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    client = UpdateNetworkStateClient(channel)

    event1 = SwitchStateEvent("event1", datetime.now(), "14397394", SwitchAction.OPEN)
    event2 = SwitchStateEvent("event2", datetime.now(), "20747784", SwitchAction.CLOSE)
    result = await client.set_current_states(int(time.time()), [event1, event2])
    print(f"Update status: {result}")


async def close_switch_for_backfeed():
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    client = UpdateNetworkStateClient(channel)

    event = SwitchStateEvent("event1", datetime.now(), "14397394", SwitchAction.CLOSE)
    result = await client.set_current_states(int(time.time()), [event])
    print(f"Update status: {result}")


async def disconnect_one_circuit():
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    client = UpdateNetworkStateClient(channel)

    event = SwitchStateEvent("event1", datetime.now(), "20747784", SwitchAction.OPEN)
    result = await client.set_current_states(int(time.time()), [event])
    print(f"Update status: {result}")


def run_single_circuit():
    asyncio.run(reset_switches())
    print("Equipment counts prior to disconnect:")
    asyncio.run(counts_disconnected_circuits(["20747918-lvf", "39065908-lvf"]))
    print()
    print("Opening Switch 20747784...")
    print()

    asyncio.run(disconnect_one_circuit())
    print()
    print("Equipment counts after disconnect:")
    asyncio.run(counts_disconnected_circuits(["20747918-lvf", "39065908-lvf"]))
    time.sleep(60.0)

    print()
    print("Closing Switch 14397394...")
    print()
    asyncio.run(close_switch_for_backfeed())
    print("Equipment counts after reconnection:")
    asyncio.run(counts_disconnected_circuits(["20747918-lvf", "39065908-lvf"]))

    time.sleep(60.0)
    asyncio.run(reset_switches())


if __name__ == "__main__":
    run_single_circuit()
