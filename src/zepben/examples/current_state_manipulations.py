#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import sys
from typing import List, Set

from zepben.evolve import Feeder, PowerTransformer, Switch, assign_equipment_to_feeders, set_phases, NetworkConsumerClient, \
    connect_with_password, BusbarSection, tracing, ConductingEquipment, ConductingEquipmentStep, Terminal, Breaker, EquipmentContainer

"""
Primary question to answer/example for:
1. How to access the CIM model? Show examples of how the static/design and dynamic/current states of the network model is typically accessed by software
 developers?
    a. Can Zepben model be updated with dynamic ADMS state information
    b. if we have current state of network (dynamically updated from ADMS), can we query the model to find all current connected HV feeders in a voltage bus?
     (VVC Example)
    c. How fast can we retrieve a model (dynamic sate) from CIM for near real time applications? (VVC Example)
2. Show how the static and dynamic states of the network model is used by applications.
"""


async def run_simple(client: NetworkConsumerClient):
    print()
    print()
    print("######################")
    print("# FETCH ZONE FEEDERS #")
    print("######################")
    print()
    print()
    await fetch_zone_feeders(client)
    print()
    print()
    print("##################")
    print("# SPUR ISOLATION #")
    print("##################")
    print()
    await isolate_spur_current(client)
    print()
    print()
    print("##################")
    print("# ZONE BUS TRACE #")
    print("##################")
    print()
    await zone_bus_trace(client)
    print()
    print()


async def fetch_zone_feeders(client: NetworkConsumerClient):
    print("fetching network hierarchy...")
    hierarchy = (await client.get_network_hierarchy()).throw_on_error().result
    print("hierarchy fetched, fetching BAS feeders...")
    substation = hierarchy.substations["BAS"]
    for feeder in substation.feeders:
        print(f"   {feeder.mrid}...")
        await client.get_equipment_container(feeder.mrid, Feeder, include_energizing_containers=True, include_energized_containers=True)
    print("CPM feeders fetched.")


async def isolate_spur_current(client: NetworkConsumerClient):
    feeder: Feeder = client.service.get("BAS022", Feeder)
    switch: Switch = client.service.get("171143844", Switch)
    tx: PowerTransformer = client.service.get("171143830", PowerTransformer)

    log_spur("original:", switch, tx)

    clear_feeders({feeder})

    print(f"setting switch open...")

    log_spur("cleaned:", switch, tx)

    # Change the open state of the switch.
    switch.set_open(True)

    await recalculate_feeders({feeder})

    log_spur("currently open:", switch, tx)


def log_spur(desc: str, switch: Switch, tx: PowerTransformer):
    print("==========================")
    print(desc)
    print(
        f"   {str(switch)}: is_normally_open={switch.is_normally_open()}, "
        f"is_open={switch.is_open()}, "
        f"normal_feeders={[it.mrid for it in switch.normal_feeders]}, "
        f"current_feeders={[it.mrid for it in switch.current_feeders]}"
    )
    print(
        f"   {str(tx)}: nominal_phases={[it.phases.name for it in tx.terminals]}, "
        f"normal_phases={[it.normal_phases.as_phase_code().name for it in tx.terminals]}, "
        f"current_phases={[it.current_phases.as_phase_code().name for it in tx.terminals]}, "
        f"normal_feeders={[it.mrid for it in tx.normal_feeders]}, "
        f"current_feeders={[it.mrid for it in tx.current_feeders]}"
    )
    print("==========================")


async def zone_bus_trace(client: NetworkConsumerClient):
    feeder_head_terminals = [it.normal_head_terminal for it in client.service.objects(Feeder) if
                             it.normal_head_terminal is not None and it.normal_head_terminal.conducting_equipment is not None]
    feeder_heads = [it.conducting_equipment for it in feeder_head_terminals]
    feeder_head_other_terms = [ot for it in feeder_head_terminals for ot in it.other_terminals()]

    print(f"creating bus for {[feeder.mrid for it in feeder_heads for feeder in it.normal_feeders]}...")
    # There is no subtrans in the model we pulled down so create a zone bus for all the feeders.
    bus = BusbarSection()
    bus_terminal = Terminal()
    bus.add_terminal(bus_terminal)

    for it in feeder_head_other_terms:
        print(f"   connecting {[feeder.mrid for feeder in it.conducting_equipment.normal_feeders]} to bus...")
        # disconnect the terminal from its energy source and move it to the bus
        client.service.disconnect(it)
        client.service.connect_terminals(bus_terminal, it)

    print("bus created")
    await log_bus("original:", bus, feeder_heads)

    print("Setting BAS022 and BAS023 to currently open")
    client.service.get("15416104", Breaker).set_open(True)  # BAS022 Breaker
    client.service.get("15416128", Breaker).set_open(True)  # BAS023 Breaker

    print("Setting BAS032 and BAS033 to normally open")
    client.service.get("177750988", Breaker).set_normally_open(True)  # BAS032 Breaker
    client.service.get("177750866", Breaker).set_normally_open(True)  # BAS033 Breaker

    await log_bus("opened:", bus, feeder_heads)


async def log_bus(desc: str, bus: ConductingEquipment, feeder_heads: List[ConductingEquipment]):
    print("==========================")
    print(desc)

    # we run a trace on the assumption that the real model may have more equipment between the bus and the feeder heads.
    # e.g. other minor busbars or ac line segments
    trace = tracing.connected_equipment_trace()
    open_heads: List[ConductingEquipment] = []
    closed_heads: List[ConductingEquipment] = []
    normally_open_heads: List[ConductingEquipment] = []
    normally_closed_heads: List[ConductingEquipment] = []

    # stop at all feeder heads
    async def stop_on_feeder_heads(step: ConductingEquipmentStep) -> bool:
        return isinstance(step.conducting_equipment, Switch) and step.conducting_equipment in feeder_heads

    # stop at transformers to prevent tracing out of this zone into others
    async def stop_on_transformers(step: ConductingEquipmentStep) -> bool:
        return isinstance(step.conducting_equipment, PowerTransformer)

    # sort feeder heads based on state
    async def sort_feeder_heads(step: ConductingEquipmentStep):
        if isinstance(step.conducting_equipment, Switch):
            if step.conducting_equipment.is_open():
                open_heads.append(step.conducting_equipment)
            else:
                closed_heads.append(step.conducting_equipment)
            if step.conducting_equipment.is_normally_open():
                normally_open_heads.append(step.conducting_equipment)
            else:
                normally_closed_heads.append(step.conducting_equipment)

    trace.add_stop_condition(stop_on_feeder_heads)
    trace.add_stop_condition(stop_on_transformers)
    trace.if_stopping(sort_feeder_heads)

    await trace.run_from(bus)
    print(f"   disconnected feeders: {[feeder.mrid for it in open_heads for feeder in it.normal_feeders]}")
    print(f"   connected feeders: {[feeder.mrid for it in closed_heads for feeder in it.normal_feeders]}")
    print(f"   normally disconnected feeders: {[feeder.mrid for it in normally_open_heads for feeder in it.normal_feeders]}")
    print(f"   normally connected feeders: {[feeder.mrid for it in normally_closed_heads for feeder in it.normal_feeders]}")
    print("==========================")


async def run_swap_feeder(client: NetworkConsumerClient):
    open_point_id = "13953031"
    isolation_point_id = "13952991"

    # noinspection PyTypeChecker
    open_point: Switch = (await client.get_identified_object(open_point_id)).throw_on_error().value
    # noinspection PyTypeChecker
    isolation_point: Switch = (await client.get_identified_object(isolation_point_id)).throw_on_error().value

    feeder_ids = {it.to_mrid for it in client.service.get_unresolved_references_from(open_point.mrid) if it.resolver.to_class == EquipmentContainer}

    # get the feeders on both sides of the open point.
    print(f"fetching feeders...")
    for feeder in feeder_ids:
        print(f"   {feeder}...")
        (await client.get_equipment_container(feeder)).throw_on_error()
    print(f"done.")

    feeders = set(open_point.normal_feeders)
    log_txs(f"original:", feeders)

    clear_feeders(feeders)

    print(f"setting switches to move feeders...")

    open_point.set_open(False)
    isolation_point.set_open(True)

    print(f"switches updated.")

    await recalculate_feeders(feeders)

    log_txs(f"swapped:", feeders)


def clear_feeders(feeders: Set[Feeder]):
    # remove the phases and feeders to show the difference in open/normal state
    for feeder in feeders:
        print(f"removing phases from {feeder.mrid}...")
        # should use `await remove_phases().run(feeder.normal_head_terminal)`, but it is not working (or just really slow) for some reason...
        for equip in feeder.equipment:
            if isinstance(equip, ConductingEquipment):
                for term in equip.terminals:
                    term.normal_phases.phase_status = 0
                    term.current_phases.phase_status = 0
        print(f"phases removed, removing equipment...")
        for equip in feeder.equipment:
            equip.clear_containers()
            equip.clear_current_containers()
        feeder.clear_equipment()
        feeder.clear_current_equipment()
        print(f"equipment removed.")


async def recalculate_feeders(feeders: Set[Feeder]):
    # recalculate the phases and feeders with the new switch state.
    for feeder in feeders:
        print(f"assigning phases to {feeder.mrid}...")
        await set_phases().run_with_terminal(feeder.normal_head_terminal)
        print(f"phases assigned, assigning equipment...")
        await assign_equipment_to_feeders().run_feeder(feeder)
        print(f"equipment assigned.")


def log_txs(desc: str, feeders: Set[Feeder]):
    print()
    print(desc)

    for feeder in feeders:
        print(f"   {feeder.mrid} txs: {sorted([tx.name for tx in feeder.current_equipment if isinstance(tx, PowerTransformer)])}")

    print()


async def main():
    if len(sys.argv) != 6:
        raise TypeError("you must provided the CLIENT_ID, username, password, host and port to connect")

    # noinspection PyTypeChecker
    async with connect_with_password(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]) as secure_channel:
        await run_simple(NetworkConsumerClient(secure_channel))
        await run_swap_feeder(NetworkConsumerClient(secure_channel))


if __name__ == "__main__":
    asyncio.run(main())
