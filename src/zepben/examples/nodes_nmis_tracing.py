#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.


import asyncio
import json
import os
import pandas as pd
from dataclasses import dataclass

from typing import Set
from zepben.evolve import NetworkConsumerClient, PhaseStep, PhaseCode, AcLineSegment, normal_downstream_trace, connect_with_token, EnergyConsumer, \
    PowerTransformer, normal_upstream_trace, LvFeeder, Switch, ConductingEquipment
from zepben.evolve.services.network.tracing.phases.phase_step import start_at
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

with open("config.json") as f:
    c = json.loads(f.read())


async def main():
    print("Connecting to Server")
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"], ca_filename=c["ca_path"])

    client = NetworkConsumerClient(channel)
    result = (await client.get_network_hierarchy()).throw_on_error().result
    print("Connection Established")

    for feeder in result.feeders.values():
        if feeder.mrid != "PTN-014":
            continue
        print()
        print(f"Fetching {feeder.mrid}")
        network = await get_feeder_network(channel, feeder.mrid)

        lv_feeder_heads = [lvf.normal_head_terminal.conducting_equipment for lvf in network.objects(LvFeeder) if lvf.normal_head_terminal]
        print(f"All LV feeder heads: {lv_feeder_heads}")
        print()
        print("Downstream Trace Example..")
        # Get the count of customers per transformer
        for lvf in network.objects(LvFeeder):
            if "PLENTY-RAGLAN" in lvf.name:
                head = lvf.normal_head_terminal
                if head:
                    head_equipment = head.conducting_equipment
                    if isinstance(head_equipment, Switch):
                        print(f"LvFeeder: {lvf.name} Switch: {head_equipment.name} {type(head_equipment).__name__}")
                        nodes = await get_downstream_customer_count(start_at(head_equipment, PhaseCode.ABCN), set(lv_feeder_heads))
                        for node in nodes:
                            print(f"  {node.node_id}, {node.node_type}, {node.upstream_node}, {node.upstream_node_type}")


                nodesdf = pd.DataFrame(nodes)
                os.makedirs("csvs", exist_ok=True)
                nodesdf.to_csv(f"csvs/{lvf.name}-nodes.csv", index=False)
async def get_feeder_network(channel, feeder_mrid):
    client = NetworkConsumerClient(channel)
    (await client.get_equipment_container(mrid=feeder_mrid,
                                          include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
    return client.service


@dataclass
class Node(object):
    # substation: str
    # circuit: str
    node_id: str  # First connectivity node above
    node_type: str  # node or actual type
    upstream_node: str
    upstream_node_type: str
    # longitude: float
    # latitude: float
    # segment_length: float  # Length of conductor
    # conductor_type: str  # conductor code
    # z_n: str  # real and imaginary - neutral
    # z_sl: str  # real and imaginary phase
    # switch_state: str
    # supply_type: str  # light if its a public light otherwise NULL
    # cross_reference_type: str  # same as node_type
    # cross_reference_id: str  # same as node_id or SAP ID?


async def get_downstream_customer_count(ce: PhaseStep, lv_feeder_heads: Set[ConductingEquipment]) -> int:
    trace = normal_downstream_trace()
    customer_count = 0

    nodes = []
    last_node = None
    async def add_eq(ps: PhaseStep, _):
        nonlocal last_node
        nodes.append(Node(ps.conducting_equipment.mrid, type(ps.conducting_equipment).__name__, last_node.mrid if last_node else "", type(last_node).__name__ if last_node else ""))
        if ps.conducting_equipment.num_terminals() == 1:
            last_node = None
        else:
            last_node = ps.conducting_equipment

    

    async def stop_on_lv_circuit_switch(ps: PhaseStep) -> bool:
        return ps.conducting_equipment in lv_feeder_heads
    
    trace.add_step_action(add_eq)
    trace.add_stop_condition(stop_on_lv_circuit_switch)
    await trace.run(ce, can_stop_on_start_item=False)
    return nodes


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
