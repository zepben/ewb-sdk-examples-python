#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

# A Traversal is used to iterate over graph-like structures.
# The Evolve SDK contains several factory functions for traversals that cover common use cases.
import asyncio

from zepben.evolve import Switch, connected_equipment_trace, ConductingEquipmentStep, connected_equipment_breadth_trace, \
    normal_connected_equipment_trace, current_connected_equipment_trace, connectivity_trace, ConnectivityResult, connected_equipment, \
    connectivity_breadth_trace, SinglePhaseKind, normal_connectivity_trace, current_connectivity_trace, phase_trace, PhaseCode, PhaseStep, normal_phase_trace, \
    current_phase_trace, assign_equipment_to_feeders, Feeder, LvFeeder, assign_equipment_to_lv_feeders, set_direction, Terminal, \
    normal_limited_connected_equipment_trace, AcLineSegment, current_limited_connected_equipment_trace, FeederDirection, remove_direction, \
    normal_downstream_trace, current_downstream_trace, TreeNode, Breaker

from zepben.evolve.services.network.tracing.phases import phase_step
from zepben.evolve.services.network.tracing.tracing import normal_upstream_trace, current_upstream_trace, normal_downstream_tree, current_downstream_tree

# For the purposes of this example, we will use the IEEE 13 node feeder.
from zepben.examples.ieee_13_node_test_feeder import network

feeder_head = network.get("br_650", Breaker)
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

    # noinspection PyArgumentList
    start_item = ConductingEquipmentStep(conducting_equipment=feeder_head)
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

    # The normal connected equipment trace iterates through all equipment connected to the starting equipment in the network's normal state.
    # By setting the switch from node 671 to 692 to normally open on at least one phase, the traversal will not trace through the switch.
    # Even if a switch has closed phases, it will not be traced through if one or more of its phases are closed in the network's normal state.
    network.get("sw_671_692", Switch).set_normally_open(True, phase=SinglePhaseKind.A)
    print("Switch set to normally open on phase A")
    print()
    print("Normal Connected Equipment Trace:")
    await normal_connected_equipment_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    # The normal connected equipment trace iterates through all equipment connected to the starting equipment in the network's current state.
    # By setting the switch from node 671 to 692 to currently open on at least one phase, the traversal will not trace through the switch.
    # Even if a switch has closed phases, it will not be traced through if one or more of its phases are closed in the network's current state.
    switch.set_open(True, phase=SinglePhaseKind.B)
    print("Switch set to currently open on phase B")
    print()
    print("Current Connected Equipment Trace:")
    await current_connected_equipment_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    reset_switch()


async def connectivity_traces():
    # Connectivity traces iterate over the connectivity of equipment terminals, rather than the equipment themselves.
    # The tracker ensures that each equipment appears at most once as a destination in a connectivity.
    print_heading("CONNECTIVITY TRACING")

    start_item = connected_equipment(feeder_head)[0]
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
    # and likewise does not go through switches with at least one open phase.
    switch.set_normally_open(True, phase=SinglePhaseKind.A)
    print("Switch set to normally open on phase A")
    print()
    print("Normal Connectivity Trace:")
    await normal_connectivity_trace().add_step_action(print_connectivity).run(start_item)
    print(f"Number of connectivities visited: {len(visited)}")
    print()
    visited.clear()

    switch.set_open(True, phase=SinglePhaseKind.B)
    print("Switch set to currently open on phase B")
    print()
    print("Current Connectivity Trace:")
    await current_connectivity_trace().add_step_action(print_connectivity).run(start_item)
    print(f"Number of connectivities visited: {len(visited)}")
    print()
    visited.clear()

    reset_switch()


async def limited_connected_equipment_traces():
    # Limited connected equipment traces allow you to trace up to a number of steps, and optionally in a specified feeder direction.
    # Running the trace returns a dictionary from each visited equipment to the number of steps away it is from a starting equipment.
    # set_direction() must be run on a network before running directed traces.
    print_heading("LIMITED CONNECTED EQUIPMENT TRACES")

    switch.set_normally_open(True, phase=SinglePhaseKind.A)
    print(f"Switch set to normally open on phase A.")
    print()

    await set_direction().run(network)
    print(f"Feeder direction set for each terminal.")
    print()

    line = network.get("l_632_671", AcLineSegment)
    normal_distances = await normal_limited_connected_equipment_trace().run([line], maximum_steps=2, feeder_direction=FeederDirection.DOWNSTREAM)
    print("Normal limited connected downstream trace from line 632-671 with maximum steps of 2:")
    for eq, distance in normal_distances.items():
        print(f"\tNumber of steps to {eq}: {distance}")
    print(f"Number of equipment traced: {len(normal_distances)}")
    print()

    current_distances = await current_limited_connected_equipment_trace().run([line], maximum_steps=2, feeder_direction=FeederDirection.DOWNSTREAM)
    print("Current limited connected downstream trace from line 632-671 with maximum steps of 2:")
    for eq, distance in current_distances.items():
        print(f"\tNumber of steps to {eq}: {distance}")
    print(f"Number of equipment traced: {len(current_distances)}")
    print()

    remove_direction().run(network)
    print(f"Feeder direction removed for each terminal.")
    print()

    reset_switch()


async def phase_traces():
    # Phase traces account for which phases each terminal supports.
    print_heading("PHASE TRACING")

    feeder_head_phase_step = phase_step.start_at(feeder_head, PhaseCode.ABCN)
    switch_phase_step = phase_step.start_at(switch, PhaseCode.ABCN)
    visited = set()

    async def print_phase_step(step: PhaseStep, _: bool):
        visited.add(step)
        phases = ""
        for spk in PhaseCode.ABCN:
            if spk in step.phases:
                phases += spk.short_name
            else:
                phases += "-"
        print(f'\t{step.previous and step.previous.mrid or "(START)":-<15}-{phases: ^4}-{step.conducting_equipment.mrid:->15}')

    print("Phase Trace:")
    await phase_trace().add_step_action(print_phase_step).run(feeder_head_phase_step)
    print(f"Number of phase steps visited: {len(visited)}")
    print()
    visited.clear()

    # For each normally open phase on a switch, the normal phase trace will not trace through that phase for the switch.
    switch.set_normally_open(True, SinglePhaseKind.B)
    print("Normal Phase Trace:")
    await normal_phase_trace().add_step_action(print_phase_step).run(feeder_head_phase_step)
    print(f"Number of phase steps visited: {len(visited)}")
    print()
    visited.clear()

    # For each currently open phase on a switch, the current phase trace will not trace through that phase for the switch.
    switch.set_open(True, SinglePhaseKind.C)
    print("Current Phase Trace:")
    await current_phase_trace().add_step_action(print_phase_step).run(feeder_head_phase_step)
    print(f"Number of phase steps visited: {len(visited)}")
    print()
    visited.clear()

    # There are also directed phase traces.
    # set_direction() must be run on a network before running directed traces.
    # Note that set_direction() does not trace through switches with at least one open phase,
    # meaning that terminals beyond such a switch are left with a feeder direction of NONE.
    await set_direction().run(network)
    print(f"Feeder direction set for each terminal.")
    print()

    print("Normal Downstream Phase Trace:")
    await normal_downstream_trace().add_step_action(print_phase_step).run(feeder_head_phase_step)
    print(f"Number of phase steps visited: {len(visited)}")
    print()
    visited.clear()

    print("Current Downstream Phase Trace:")
    await current_downstream_trace().add_step_action(print_phase_step).run(feeder_head_phase_step)
    print(f"Number of phase steps visited: {len(visited)}")
    print()
    visited.clear()

    print("Normal Upstream Phase Trace:")
    await normal_upstream_trace().add_step_action(print_phase_step).run(switch_phase_step)
    print(f"Number of phase steps visited: {len(visited)}")
    print()
    visited.clear()

    print("Current Upstream Phase Trace:")
    await current_upstream_trace().add_step_action(print_phase_step).run(switch_phase_step)
    print(f"Number of phase steps visited: {len(visited)}")
    print()
    visited.clear()

    remove_direction().run(network)
    print(f"Feeder direction removed for each terminal.")
    print()

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


async def set_and_remove_feeder_direction():
    # Use set_direction().run(network) to evaluate the feeder direction of each terminal.
    print_heading("SETTING FEEDER DIRECTION")
    switch.set_normally_open(True, phase=SinglePhaseKind.A)
    print(f"Switch set to normally open on phase A. Switch is between feeder head and energy consumer 675.")

    consumer_terminal = network.get("ec_675_t", Terminal)
    print(f"Normal feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.normal_feeder_direction}")
    print(f"Current feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.current_feeder_direction}")
    print(f"Normal feeder direction of energy consumer 675 terminal: {consumer_terminal.normal_feeder_direction}")
    print(f"Current feeder direction of energy consumer 675 terminal: {consumer_terminal.current_feeder_direction}")
    print()
    await set_direction().run(network)
    print("Normal and current feeder direction set.")
    print()
    print(f"Normal feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.normal_feeder_direction}")
    print(f"Current feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.current_feeder_direction}")
    print(f"Normal feeder direction of energy consumer 675 terminal: {consumer_terminal.normal_feeder_direction}")
    print(f"Current feeder direction of energy consumer 675 terminal: {consumer_terminal.current_feeder_direction}")
    print()

    # Use remove_direction().run(network) to remove feeder directions.
    # While set_direction().run(network) must be awaited, remove_direction().run(network) does not, because it is not asynchronous.
    print_heading("REMOVING FEEDER DIRECTION")

    consumer_terminal = network.get("ec_675_t", Terminal)
    print(f"Normal feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.normal_feeder_direction}")
    print(f"Current feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.current_feeder_direction}")
    print(f"Normal feeder direction of energy consumer 675 terminal: {consumer_terminal.normal_feeder_direction}")
    print(f"Current feeder direction of energy consumer 675 terminal: {consumer_terminal.current_feeder_direction}")
    print()
    remove_direction().run(network)
    print("Normal and current feeder direction removed.")
    print()
    print(f"Normal feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.normal_feeder_direction}")
    print(f"Current feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.current_feeder_direction}")
    print(f"Normal feeder direction of energy consumer 675 terminal: {consumer_terminal.normal_feeder_direction}")
    print(f"Current feeder direction of energy consumer 675 terminal: {consumer_terminal.current_feeder_direction}")
    print()

    reset_switch()


async def trees():
    # A downstream tree contains all non-intersecting equipment paths starting from a common equipment and following downstream terminals.
    # The same equipment may appear multiple times in the tree if the network contains multiple downstream paths to the equipment, i.e. loops.
    # Similar to connected equipment traces, either the normal or current state of the network may be used to determine whether to trace through each switch.
    print_heading("DOWNSTREAM TREES")

    def desc_lines(node: TreeNode):
        children = list(node.children)
        for i, child in enumerate(children):
            is_last_child = i == len(children) - 1
            branch_char = "┗" if is_last_child else "┣"
            stem_char = " " if is_last_child else "┃"
            yield f"{branch_char}━{child.conducting_equipment}"
            for line in desc_lines(child):
                yield f"{stem_char} {line}"

    def print_tree(root_node: TreeNode):
        print(root_node.conducting_equipment)
        for line in desc_lines(root_node):
            print(line)

    switch.set_open(True, SinglePhaseKind.C)
    print("Switch set to currently open on phase C.")
    print()

    await set_direction().run(network)
    print("Feeder direction set.")
    print()

    print("Normal Downstream Tree:")
    ndt = await normal_downstream_tree().run(feeder_head)
    print_tree(ndt)
    print()

    print("Current Downstream Tree:")
    cdt = await current_downstream_tree().run(feeder_head)
    print_tree(cdt)
    print()

    remove_direction().run(network)
    print(f"Feeder direction removed for each terminal.")
    print()

    reset_switch()


async def main():
    # All examples are self-contained. Feel free to comment out any of the following lines to isolate specific examples.
    await assigning_equipment_to_feeders()
    await set_and_remove_feeder_direction()
    await equipment_traces()
    await limited_connected_equipment_traces()
    await connectivity_traces()
    await phase_traces()
    await trees()

if __name__ == "__main__":
    asyncio.run(main())
