#  Copyright 2024 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json
import os
from dataclasses import dataclass
from typing import Optional, Tuple
import pandas as pd

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

from zepben.evolve import PowerTransformer, EnergyConsumer, AcLineSegment, LvFeeder, Customer, \
    NetworkConsumerClient, CustomerConsumerClient, connect_with_token, Feeder, connect_insecure, connect_tls, \
    ConductingEquipment, Switch, FeederDirection, Conductor, SinglePhaseKind, normal_upstream_trace, BusbarSection, PhaseCode, Site

from zepben.evolve.services.network.tracing.phases.phase_step import start_at

with open("config.json") as f:
    c = json.loads(f.read())


async def connect():
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)

    network_hierarchy = (await network_client.get_network_hierarchy()).throw_on_error().value

    print("Network hierarchy:")
    for gr in network_hierarchy.geographical_regions.values():
        print(f"- {gr.name}")
        for sgr in gr.sub_geographical_regions:
            print(f"  - {sgr.name}")
            for sub in sgr.substations:
                print(f"    - {sub.name}")
                for fdr in sub.feeders:
                    print(f"      - {fdr.name}")
                    if fdr.mrid in ("COO-023"):
                        await process_nodes(fdr.mrid, channel)


@dataclass
class NetworkNmi(object):
    circuit: Optional[str]
    nmi: str
    node_id: str
    nmi_status: str
    supply_type: Optional[str]
    conductor_type: Optional[str]
    z_n: str  # real and imaginary - neutral
    z_sl: str  # real and imaginary phase
    phases: str
    spid: str
    longitude: Optional[float]
    latitude: Optional[float]
    upstream_longitude: Optional[float]
    upstream_latitude: Optional[float]
    substation: Optional[str]


@dataclass
class Node(object):
    substation: str
    circuit: str
    node_id: str  # First connectivity node above
    node_type: str  # node or actual type
    upstream_node: str
    longitude: float
    latitude: float
    segment_length: float  # Length of conductor
    conductor_type: str  # conductor code
    z_n: str  # real and imaginary - neutral
    z_sl: str  # real and imaginary phase
    switch_state: str
    supply_type: str  # light if its a public light otherwise NULL
    cross_reference_type: str  # same as node_type
    cross_reference_id: str  # same as node_id or SAP ID?


async def process_nodes(feeder_mrid: str, channel):
    print(f"Fetching Feeder {feeder_mrid}")
    network_client = NetworkConsumerClient(channel=channel)
    customer_client = CustomerConsumerClient(channel=channel)
    network_service = network_client.service
    customer_service = customer_client.service
    (await network_client.get_equipment_container(feeder_mrid,
                                                  include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
    for lvf in network_service.objects(LvFeeder):
        (await customer_client.get_customers_for_container(lvf.mrid)).throw_on_error()

    # Create CSVs for each distribution transformer site
    for site in network_service.objects(Site):
        # if site.name != "BLOOM-MYOORA":
        #     continue
        network_nodes = []
        network_nmis = []
        processed_circuits = set()
        seen = set()
        for equipment in site.equipment:
            lvf = None
            # Only process the LvFeeder within this site that starts with circuit switches
            if isinstance(equipment, Switch):
                for x in equipment.normal_lv_feeders:
                    if x.normal_head_terminal in equipment.terminals:
                        lvf = x
                if not lvf:
                    continue
                assert lvf.mrid not in processed_circuits, "We have already processed this LvFeeder, did you change the for loop for selecting the LvFeeder?"
                print(f"Processing equipment {equipment} for LvFeeder {lvf}")
                processed_circuits.add(lvf.mrid)
                substation = site.name
                circuit = lvf.name

                # Turn each ConductingEquipment into a "Node"
                for conducting_equipment in lvf.equipment:
                    # Skip anything not part of the connectivity model (e.g PV)
                    if not isinstance(conducting_equipment, ConductingEquipment):
                        continue
                    conducting_equipment: ConductingEquipment

                    if isinstance(conducting_equipment, Conductor):
                        # TODO: link to a pole once we have it
                        for t in conducting_equipment.terminals:
                            if FeederDirection.UPSTREAM in t.normal_feeder_direction:
                                terminal = t
                        assert terminal, f"Could not find an upstream terminal for {conducting_equipment}. This implies a connectivity issue in the circuit"
                        node_id = terminal.connectivity_node_id
                        node_type = "node"

                    else:
                        try:
                            for name in conducting_equipment.get_names("GIS"):
                                node_id = name.name
                        except KeyError as k:
                            node_id = conducting_equipment.mrid
                        node_type = conducting_equipment.__class__.__name__  # GIS type could be description?

                    if node_id in seen:
                        # Many conductors could connect to the same connectivity node, so if we've already seen the connectivity node
                        # we skip it. It's also possible that some things are in multiple circuits. Eg a circuit head fuse might also be the end
                        # of another circuit (think 00 and 01 circuits), so if we've already processed it as part of another circuit we skip it.
                        continue
                    seen.add(node_id)
                    location = conducting_equipment.location
                    points = location.get_point(0)
                    longitude = points.x_position
                    latitude = points.y_position
                    supply_type = "public_light" if conducting_equipment.description == "public_light" else "NULL"

                    if isinstance(conducting_equipment, Switch):
                        switch_state = "open" if conducting_equipment.is_normally_open() else "closed"
                    else:
                        switch_state = None

                    upstream_equip, segment_length, upstream_conductor = await get_upstream_node(conducting_equipment)

                    conductor_code = None
                    z_sl = None
                    z_n = None

                    if upstream_equip:
                        # Get upstream connectivity
                        # TODO: this will be a conductor mRID - so need to get corresponding connectivity node/pole
                        try:
                            for name in upstream_equip.get_names("GIS"):
                                upstream_node = name.name
                        except KeyError as k:
                            upstream_node = upstream_equip.mrid

                        if upstream_conductor:
                            plpi = upstream_conductor.per_length_phase_impedance
                            if plpi:
                                conductor_code = plpi.mrid
                                pid = plpi.get_data(SinglePhaseKind.A, SinglePhaseKind.A)
                                z_sl = f"{pid.r}+{pid.x}i"
                                pid = plpi.get_data(SinglePhaseKind.N, SinglePhaseKind.N)
                                if pid.r == 0.0 and pid.x == 0.0:  # No data for neutral
                                    z_n = z_sl
                                else:
                                    z_n = f"{pid.r}+{pid.x}i"
                    else:
                        upstream_node = None

                    network_nodes.append(
                        Node(
                            substation=substation,
                            circuit=circuit,
                            node_id=node_id,
                            node_type=node_type,
                            upstream_node=upstream_node,
                            segment_length=segment_length,
                            conductor_type=conductor_code,
                            cross_reference_type=node_id,
                            cross_reference_id=node_type,
                            switch_state=switch_state,
                            longitude=longitude,
                            latitude=latitude,
                            supply_type=supply_type,
                            z_n=z_n,
                            z_sl=z_sl,
                        )
                    )

                    # Extract the "NMIs" from supply points (EnergyConsumers), and store the reference back to the Node
                    if isinstance(conducting_equipment, EnergyConsumer):
                        spid = conducting_equipment.mrid
                        location = upstream_equip.location if upstream_equip.location is not None else None
                        upstream_pp = next(location.points) if location.num_points() > 0 else None

                        terminal = next(conducting_equipment.terminals)
                        for up in conducting_equipment.usage_points:
                            up_nmis = up.get_names("NMI")
                            if not up_nmis:
                                continue
                            nmi = up_nmis[0].name  # Accept only one NMI per UsagePoint
                            for meter in up.end_devices:
                                customer = customer_service.get(meter.customer_mrid, Customer)
                                supply_type = "Life Support" if customer.special_need == "Yes" else "Customer"
                                network_nmis.append(
                                    NetworkNmi(
                                        nmi=nmi,
                                        node_id=node_id,
                                        nmi_status="Active (A)",  # hard coded as we only store active NMIs
                                        supply_type=supply_type,
                                        phases=terminal.phases.short_name,
                                        conductor_type=conductor_code,
                                        spid=spid,
                                        longitude=longitude,
                                        latitude=latitude,
                                        substation=substation,
                                        circuit=circuit,
                                        upstream_longitude=upstream_pp.longitude,
                                        upstream_latitude=upstream_pp.latitude,
                                        z_n=z_n,
                                        z_sl=z_sl
                                    )
                                )

                nodesdf = pd.DataFrame(network_nodes)
                os.makedirs("csvs", exist_ok=True)
                nodesdf.to_csv(f"csvs/{site.name}-nodes.csv", index=False)
                nmisdf = pd.DataFrame(network_nmis)
                nmisdf.to_csv(f"csvs/{site.name}-nmis.csv", index=False)


async def get_upstream_node(ce, phases=PhaseCode.ABCN) -> Tuple[Optional[ConductingEquipment], float, Optional[AcLineSegment]]:
    trace = normal_upstream_trace()
    next_node = []
    length = [0.0]
    upstream_conductor = []
    if isinstance(ce, Switch):
        if ce.is_normally_open():
            print(f"Skipping end of circuitt open switch: {ce}")
            return None, 0.0, None

    async def count_segment_length(ps, is_stopping):
        if isinstance(ps.conducting_equipment, AcLineSegment):
            if ps.conducting_equipment.length:  # Not all lengths are available
                length[0] += ps.conducting_equipment.length
                upstream_conductor.append(ps.conducting_equipment)

    async def stop_on_node(ps):
        next_node.append(ps.conducting_equipment)
        return True

    phase_step = start_at(ce, phases)
    trace.add_stop_condition(stop_on_node)
    trace.add_step_action(count_segment_length)
    await trace.run(phase_step, can_stop_on_start_item=False)

    if not next_node:
        print(f"No next node - maybe top of container? start point was {ce}")
        return None, 0, None

    return next_node[0], length[0], upstream_conductor[0] if upstream_conductor else None


if __name__ == "__main__":
    asyncio.run(connect())
