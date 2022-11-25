#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

# A Traversal is used to iterate over graph-like structures.
# The Evolve SDK contains several factory functions for traversals that cover common use cases.
import asyncio

from zepben.evolve import Switch, connected_equipment_trace, ConductingEquipmentStep, ConductingEquipment, connected_equipment_breadth_trace, \
    normal_connected_equipment_trace, current_connected_equipment_trace, connectivity_trace, ConnectivityResult, connected_equipment, \
    connectivity_breadth_trace, SinglePhaseKind, normal_connectivity_trace, current_connectivity_trace, phase_trace, PhaseCode, PhaseStep, normal_phase_trace, \
    PowerTransformer, current_phase_trace, assign_equipment_to_feeders, Feeder, LvFeeder, assign_equipment_to_lv_feeders

from zepben.evolve.services.network.tracing.phases import phase_step

# For the purposes of this example, we will use the IEEE 13 node feeder.
from zepben.examples.ieee_13_node_test_feeder import network

regulator = network.get("vr_650_632", PowerTransformer)
switch = network.get("sw_671_692", Switch)
hv_feeder = network.get("hv_fdr", Feeder)
lv_feeder = network.get("lv_fdr", LvFeeder)


def reset_switch():
    switch.set_normally_open(False)
    switch.set_open(False)
    print("Switch reset (normally and currently closed)")
    print()


def print_heading(heading):
    print("+" + "-" * (len(heading) + 2) + "+")
    print(f"| {heading} |")
    print("+" + "-" * (len(heading) + 2) + "+")
    print()


async def equipment_traces():
    # Equipment traces iterate over equipment connected in a network.
    print_heading("EQUIPMENT TRACING")

    start_item = ConductingEquipmentStep(regulator)
    visited = set()

    async def print_step(ces: ConductingEquipmentStep, _):
        visited.add(ces.conducting_equipment)
        print(f"\tDepth {ces.step:02d}: {ces.conducting_equipment}")

    # The connected equipment trace iterates through all connected equipment depth-first, and even through open switches.
    # Equipment will be revisited if a shorter path from the starting equipment is found.
    print("Connected Equipment Trace:")
    await connected_equipment_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    # There is also a breadth-first version, which guarantees that each equipment is visited at most once.
    print("Connected Equipment Breadth Trace:")
    await connected_equipment_breadth_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    # The normal connected equipment trace iterates through all equipment normally connected to the starting equipment.
    # By setting the switch from node 671 to 692 to normally open, the traversal will not trace through the switch.
    network.get("sw_671_692", Switch).set_normally_open(True)
    print("Normal Connected Equipment Trace:")
    await normal_connected_equipment_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    # The normal connected equipment trace iterates through all equipment normally connected to the starting equipment.
    # By setting the switch from node 671 to 692 to currently open on at least one phase, the traversal will not trace through the switch.
    switch.set_normally_open(True, phase=SinglePhaseKind.A)
    print("Switch set to normally open on phase A")
    print("Normal Connected Equipment Trace:")
    await current_connected_equipment_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    # The normal connected equipment trace iterates through all equipment normally connected to the starting equipment.
    # By setting the switch from node 671 to 692 to currently open on at least one phase, the traversal will not trace through the switch.
    switch.set_open(True, phase=SinglePhaseKind.B)
    print("Switch set to currently open on phase B")
    print("Normal Connected Equipment Trace:")
    await current_connected_equipment_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    reset_switch()


async def connectivity_traces():
    # Connectivity traces iterate over the connectivity of equipment terminals, rather than the equipment themselves.
    # The tracker ensures that each equipment appears at most once as a destination in a connectivity.
    print_heading("CONNECTIVITY TRACING")

    start_item = connected_equipment(regulator)[0]
    visited = set()

    async def print_connectivity(cr: ConnectivityResult, _: bool):
        visited.add(cr)
        from_phases = "".join(phase_path.from_phase.short_name for phase_path in cr.nominal_phase_paths)
        to_phases = "".join(phase_path.to_phase.short_name for phase_path in cr.nominal_phase_paths)
        print(f"\t{cr.from_terminal.mrid:-<15}-{from_phases:->4}-{to_phases:-<4}-{cr.to_terminal.mrid:->15}")

    print("Connectivity Trace:")
    await connectivity_trace().add_step_action(print_connectivity).run(start_item)
    print(f"Number of connectivities visited: {len(visited)}")
    print()
    visited.clear()

    # A breadth-first connectivity trace is also available.
    print("Connectivity Breadth Trace:")
    await connectivity_breadth_trace().add_step_action(print_connectivity).run(start_item)
    print(f"Number of connectivities visited: {len(visited)}")
    print()
    visited.clear()

    # The normal connectivity trace is analogous to the normal connected equipment trace,
    # and likewise does not go through switches with at least open phase.
    switch.set_normally_open(True, phase=SinglePhaseKind.A)
    print("Switch set to normally open on phase A")
    print("Normal Connectivity Trace:")
    await normal_connectivity_trace().add_step_action(print_connectivity).run(start_item)
    print(f"Number of connectivities visited: {len(visited)}")
    print()
    visited.clear()

    switch.set_open(True, phase=SinglePhaseKind.B)
    print("Switch set to currently open on phase B")
    print("Current Connectivity Trace:")
    await current_connectivity_trace().add_step_action(print_connectivity).run(start_item)
    print(f"Number of connectivities visited: {len(visited)}")
    print()
    visited.clear()

    reset_switch()


async def phase_traces():
    # Phase traces account for which phases each terminal supports.
    print_heading("PHASE TRACING")

    start_item = phase_step.start_at(regulator, PhaseCode.ABCN)
    visited = set()

    async def print_phase_step(phase_step: PhaseStep, _: bool):
        visited.add(phase_step)
        phases = ""
        for spk in PhaseCode.ABCN:
            if spk in phase_step.phases:
                phases += spk.short_name
            else:
                phases += "-"
        print(f'\t{phase_step.previous and phase_step.previous.mrid or "(START)":-<15}-{phases: ^4}-{phase_step.conducting_equipment.mrid:->15}')

    print("Phase Trace:")
    await phase_trace().add_step_action(print_phase_step).run(start_item)
    print(f"Number of phase steps visited: {len(visited)}")
    print()
    visited.clear()

    # For each normally open phase on a switch, the normal phase trace will not trace through that phase for the switch.
    switch.set_normally_open(True, SinglePhaseKind.B)
    print("Normal Phase Trace:")
    await normal_phase_trace().add_step_action(print_phase_step).run(start_item)
    print(f"Number of phase steps visited: {len(visited)}")
    print()
    visited.clear()

    # For each currently open phase on a switch, the normal phase trace will not trace through that phase for the switch.
    switch.set_open(True, SinglePhaseKind.C)
    print("Current Phase Trace:")
    await current_phase_trace().add_step_action(print_phase_step).run(start_item)
    print(f"Number of phase steps visited: {len(visited)}")
    print()
    visited.clear()

    reset_switch()


async def assigning_equipment_to_feeders():
    # Use assign_equipment_to_feeders() and assign_equipment_to_lv_feeders() to assign equipment to HV and LV feeders.
    # assign_equipment_to_feeders() also ensures that HV feeders that power LV feeders are associated.
    print_heading("ASSIGNING EQUIPMENT TO FEEDERS")
    print(f"Equipment in HV feeder: {[eq.mrid for eq in hv_feeder.equipment]}")
    print(f"Equipment in LV feeder: {[eq.mrid for eq in lv_feeder.equipment]}")
    print(f"LV feeders powered by HV feeder: {[lvf.mrid for lvf in hv_feeder.normal_energized_lv_feeders]}")
    print(f"HV feeders powering LV feeder: {[hvf.mrid for hvf in lv_feeder.normal_energizing_feeders]}")
    print()
    await assign_equipment_to_feeders().run(network)
    await assign_equipment_to_lv_feeders().run(network)
    print("Equipment assigned to feeders.")
    print()
    print(f"Equipment in HV feeder: {[eq.mrid for eq in hv_feeder.equipment]}")
    print(f"Equipment in LV feeder: {[eq.mrid for eq in lv_feeder.equipment]}")
    print(f"LV feeders powered by HV feeder: {[lvf.mrid for lvf in hv_feeder.normal_energized_lv_feeders]}")
    print(f"HV feeders powering LV feeder: {[hvf.mrid for hvf in lv_feeder.normal_energizing_feeders]}")
    print()


async def setting_feeder_direction():
    # Use set_direction().run(network) to
    pass


async def main():
    await equipment_traces()
    await connectivity_traces()
    await phase_traces()
    await assigning_equipment_to_feeders()

if __name__ == "__main__":
    asyncio.run(main())
