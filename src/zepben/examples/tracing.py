#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

# A Traversal is used to iterate over graph-like structures.
# The Evolve SDK contains several factory functions for traversals that cover common use cases.
import asyncio

from zepben.evolve import NetworkTraceStep, StepContext, Breaker, Switch, Feeder, LvFeeder, NetworkStateOperators

# For the purposes of this example, we will use the IEEE 13 node feeder.
from zepben.examples.ieee_13_node_test_feeder import network

feeder_head = network.get("br_650", Breaker)
switch = network.get("sw_671_692", Switch)
hv_feeder = network.get("hv_fdr", Feeder)
lv_feeder = network.get("lv_fdr", LvFeeder)


def reset_switch():
    switch.set_normally_open(False)
    switch.set_open(False)
    print("Switch reset (normally and currently closed)\n")


def print_heading(heading):
    print("+" + "-" * (len(heading) + 2) + "+")
    print(f"| {heading} |")
    print("+" + "-" * (len(heading) + 2) + "+\n")


async def network_trace():
    """
    Explanation of :class:`NetworkTrace` and its configurable options.

    More information about the constructors used in this function can be found in the docstring
    of :class:`NetworkTrace`
    """
    start_item = feeder_head

    async def network_traces():
        """
        :class:`NetworkTrace` iterates sequentially over all terminals in a network.

        By default, the trace will run:
            - Depth first, stepping to any equipment marked as 'in_service'.
            - Considering only the normal state of the network.
            - Performing :class:`StepAction`s only once per unique equipment encountered
        """
        from zepben.evolve import Tracing

        await (
            Tracing.network_trace()
        ).run(start_item)

    async def network_trace_state_operators():
        """
        Both the normal and current state of the network can be operated on by passing
        :class:`NetworkStateOperators` to the constructor as `network_state_operators`
        """
        from zepben.evolve import Tracing, NetworkStateOperators

        await (
            Tracing.network_trace(network_state_operators=NetworkStateOperators.NORMAL)
        ).run(start_item)

        await (
            Tracing.network_trace(network_state_operators=NetworkStateOperators.CURRENT)
        ).run(start_item)


    async def network_trace_queue():
        """
        :meth:`TraversalQueue.depth_first` or :meth:`TraversalQueue.breadth_first` can be passed in as
        `queue` to the constructor to control which step is taken next as the `NetworkTrace` traverses
        the network
        """
        from zepben.evolve import Tracing, TraversalQueue

        await (
            Tracing.network_trace(queue=TraversalQueue.depth_first())
        ).run(start_item)

        await (
            Tracing.network_trace(queue=TraversalQueue.breadth_first())
        ).run(start_item)

    async def network_trace_branching():
        """
        :class:`NetworkTrace` can also be configured to run in a branching manner. This intended to be
        used solely for tracing around loops both ways.

        A branching trace has the same defaults as a non_branching trace
        """

        from zepben.evolve import Tracing
        await (
            Tracing.network_trace_branching()
        ).run(start_item)

    # uncomment any of the following to not run them, as they have no StepActions they will do nothing
    # other than silently traverse the network.

    await network_traces()
    await network_trace_state_operators()
    await network_trace_queue()
    await network_trace_branching()


async def network_trace_step_actions():
    """
    Explanation of network trace :class:`StepActions` and :class:`NetworkTraceActionType`
    """
    start_item = feeder_head

    async def lambda_step_action():
        """
        A :class:`NetworkTrace` is useless as configured above, as we haven't specified any :class:`StepAction`s
        to take as we traverse. async functions are supported as step actions To get started, let's demonstrate
        a simple :class:`StepAction defined as a lambda.
        """
        from zepben.evolve import Tracing

        print_heading('NetworkTrace StepAction (as lambda):')
        await (
            Tracing.network_trace()
            .add_step_action(lambda step, _: print(step.path))
        ).run(start_item)

    async def function_step_action():
        """
        Functions can be used if you want type hinting, or more then one line.
        """
        from zepben.evolve import Tracing, NetworkTraceStep, StepContext

        print_heading('NetworkTrace StepAction (as function):')
        def print_step(step: NetworkTraceStep, context: StepContext) -> None:
            print(step.path)

        await (
            Tracing.network_trace()
            .add_step_action(print_step)
        ).run(start_item)

    async def subclassed_step_action():
        """
        And if it suits the need better, subclasses of :class:`StepAction` are also accepted, for this
        approach, please read the documentation of :class:`StepAction` as there are specific methods
        you will need to override.
        """
        from zepben.evolve import Tracing, NetworkTraceStep, StepAction, StepContext

        print_heading('NetworkTrace StepAction (as subclass):')
        class PrintingStepAction(StepAction):
            def __init__(self):
                super().__init__(self._apply)

            def _apply(self, step: NetworkTraceStep, context: StepContext):
                print(step.path)

        await (
            Tracing.network_trace()
            .add_step_action(PrintingStepAction())
        ).run(start_item)

    async def step_action_type():
        """"
        With :class:`StepAction`s you may wish to only execute these for every step taken, or once
        per equipment. This is configured by passing :class:`NetworkTraceActionType to the
        :class:`NetworkTrace` constructor.
        """
        from zepben.evolve import Tracing, NetworkTraceActionType

        print_heading('NetworkTrace (ALL_STEPS):')
        await (
            Tracing.network_trace(
                action_step_type=NetworkTraceActionType.ALL_STEPS
            )
            .add_step_action(lambda step, _: print(step.path))
        ).run(start_item)

        print_heading('NetworkTrace (FIRST_STEP_ON_EQUIPMENT):')
        await (
            Tracing.network_trace(
                action_step_type=NetworkTraceActionType.FIRST_STEP_ON_EQUIPMENT
            )
            .add_step_action(lambda step, _: print(step.path))
        ).run(start_item)

    # comment any of the following to skip running them

    await lambda_step_action()
    await function_step_action()
    await subclassed_step_action()
    await step_action_type()


async def network_trace_conditions():
    """
    Explanation of :class:`Conditions`
    """
    visited = list()

    start_item = feeder_head

    def print_step(ces: NetworkTraceStep, ctx: StepContext):
        visited.append(ces.path.to_equipment)
        print(f"\tDepth {ctx.step_number:02d}: {ces.path.to_equipment}")


    async def conditions_stop_at_open():
        """
        As :class:`NetworkTrace` will traverse all in service connected terminals regardless of open
        state, of we want to stop tracing at open switches etc. we need to add that as a condition.

        The condition is checked against the state specified with `network_state_operators` passed
        to the constructor of :class:`NetworkTrace`
        """
        from zepben.evolve import Tracing, stop_at_open, Switch

        print_heading("Network Trace Stopping at open equipment (NetworkStateOperators.NORMAL):")

        network.get("sw_671_692", Switch).set_normally_open(True)
        print("Switch set to normally open\n")

        await (
            Tracing.network_trace()
            .add_step_action(print_step)
            .add_condition(stop_at_open())
        ).run(start_item)

        print(f"Number of equipment visited: {len(visited)}")
        print()

        visited.clear()
        reset_switch()

    async def conditions_downstream():
        """
        You can specify a direction to trace to achieve a directed network trace.
        Tracing.set_direction() must be run on a network before performing any directed traces
        """
        from zepben.evolve import Tracing, downstream, upstream

        print_heading("Downstream Network Trace:")

        await Tracing.set_direction().run(network, network_state_operators=NetworkStateOperators.NORMAL)
        print("Feeder direction set for each terminal.\n")

        await (
            Tracing.network_trace()
            .add_step_action(print_step)
            .add_condition(downstream())
            #.add_condition(upstream())
        ).run(start_item)

        print(f"Number of equipment visited: {len(visited)}\n")

        await Tracing.clear_direction().run(start_item)
        visited.clear()

    async def conditions_limit_equipment_steps():
        """
        Limited connected equipment traces allow you to trace up to a number of steps.
        Running the trace returns a dictionary from each visited equipment to the number of steps
        away it is from a starting equipment.
        """
        from zepben.evolve import Tracing, downstream, limit_equipment_steps, AcLineSegment

        print_heading("Downstream NetworkTrace with limited equipment steps:")

        line = network.get("l_632_671", AcLineSegment)

        await Tracing.set_direction().run(network, network_state_operators=NetworkStateOperators.NORMAL)
        print("Feeder direction set for each terminal.\n")

        await (
            Tracing.network_trace()
            .add_condition(downstream())
            .add_stop_condition(limit_equipment_steps(limit=2))
            .add_step_action(print_step)
        ).run(line)

        await Tracing.clear_direction().run(start_item)
        print(f"Feeder direction removed for each terminal.")
        print()

        reset_switch()

    await conditions_stop_at_open()
    await conditions_downstream()
    await conditions_limit_equipment_steps()


async def assigning_equipment_to_feeders():
    """
    Use :meth:`assign_equipment_to_feeders` and :meth`assign_equipment_to_lv_feeders` to assign equipment to HV and LV feeders.

    :meth:`assign_equipment_to_feeders` also ensures that HV feeders that power LV feeders are associated.

    As with all tracing, both the normal and current state can be operated on by passing in :class:`NetworkStateOperators.NORMAL`
    or :class:`NetworkStateOperators.CURRENT`. e.g.

    .. code-block:: python

        Tracing.assign_equipment_to_feeders.run(
            network_state_operators=NetworkStateOperators.NORMAL
        )
    """
    from zepben.evolve import Tracing

    print_heading("ASSIGNING EQUIPMENT TO FEEDERS")

    print(f"Equipment in HV feeder: {[eq.mrid for eq in hv_feeder.equipment]}")
    print(f"Equipment in LV feeder: {[eq.mrid for eq in lv_feeder.equipment]}")
    print(f"LV feeders powered by HV feeder: {[lvf.mrid for lvf in hv_feeder.normal_energized_lv_feeders]}")
    print(f"HV feeders powering LV feeder: {[hvf.mrid for hvf in lv_feeder.normal_energizing_feeders]}")
    print()
    await Tracing.assign_equipment_to_feeders().run(network)
    await Tracing.assign_equipment_to_lv_feeders().run(network)
    print("Equipment assigned to feeders.")
    print()
    print(f"Equipment in HV feeder: {[eq.mrid for eq in hv_feeder.equipment]}")
    print(f"Equipment in LV feeder: {[eq.mrid for eq in lv_feeder.equipment]}")
    print(f"LV feeders powered by HV feeder: {[lvf.mrid for lvf in hv_feeder.normal_energized_lv_feeders]}")
    print(f"HV feeders powering LV feeder: {[hvf.mrid for hvf in lv_feeder.normal_energizing_feeders]}")
    print()


async def feeder_direction():
    """
    Examples on using set/clear direction to set or clear feeder directions to or from a network.
    """
    async def set_feeder_direction():
        """
        Use Tracing.set_direction().run(network) to set feeder directions to Terminals in the network.

        .. code-block:: python

            await Tracing.clear_direction().run(
                network
            )

        As with all tracing, both the normal and current state can be operated on by passing in :class:`NetworkStateOperators.NORMAL`
        or :class:`NetworkStateOperators.CURRENT`. e.g.

        .. code-block:: python

            await Tracing.clear_direction().run(
                network,
                network_state_operators=NetworkStateOperators.CURRENT
            )
        """
        from zepben.evolve import Tracing, NetworkStateOperators, Terminal

        print_heading("SETTING FEEDER DIRECTION")

        consumer_terminal = network.get("ec_675_t", Terminal)
        print(f"Normal feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.normal_feeder_direction}")
        print(f"Current feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.current_feeder_direction}")
        print(f"Normal feeder direction of energy consumer 675 terminal: {consumer_terminal.normal_feeder_direction}")
        print(f"Current feeder direction of energy consumer 675 terminal: {consumer_terminal.current_feeder_direction}")
        print()
        await Tracing.set_direction().run(network, network_state_operators=NetworkStateOperators.NORMAL)
        await Tracing.set_direction().run(network, network_state_operators=NetworkStateOperators.CURRENT)
        print("Normal and current feeder direction set.")
        print()
        print(f"Normal feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.normal_feeder_direction}")
        print(f"Current feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.current_feeder_direction}")
        print(f"Normal feeder direction of energy consumer 675 terminal: {consumer_terminal.normal_feeder_direction}")
        print(f"Current feeder direction of energy consumer 675 terminal: {consumer_terminal.current_feeder_direction}")
        print()

    async def clear_feeder_direction():
        """
        Use Tracing.clear_direction().run(network) to clear the feeder direction for Terminals in the network.

        .. code-block:: python

            await Tracing.set_direction().run(
                network
            )

        As with all tracing, both the normal and current state can be operated on by passing in
        :class:`NetworkStateOperators.NORMAL` or :class:`NetworkStateOperators.CURRENT`. e.g.

        .. code-block:: python

            await Tracing.set_direction().run(
                network,
                network_state_operators=NetworkStateOperators.CURRENT
            )
        """
        from zepben.evolve import Tracing, NetworkStateOperators, Terminal

        print_heading("REMOVING FEEDER DIRECTION")

        consumer_terminal = network.get("ec_675_t", Terminal)
        print(f"Normal feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.normal_feeder_direction}")
        print(f"Current feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.current_feeder_direction}")
        print(f"Normal feeder direction of energy consumer 675 terminal: {consumer_terminal.normal_feeder_direction}")
        print(f"Current feeder direction of energy consumer 675 terminal: {consumer_terminal.current_feeder_direction}")
        print()
        await Tracing.clear_direction().run(consumer_terminal, network_state_operators=NetworkStateOperators.NORMAL)
        await Tracing.clear_direction().run(consumer_terminal, network_state_operators=NetworkStateOperators.CURRENT)
        print("Normal and current feeder direction removed.")
        print()
        print(f"Normal feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.normal_feeder_direction}")
        print(f"Current feeder direction of HV feeder head terminal: {hv_feeder.normal_head_terminal.current_feeder_direction}")
        print(f"Normal feeder direction of energy consumer 675 terminal: {consumer_terminal.normal_feeder_direction}")
        print(f"Current feeder direction of energy consumer 675 terminal: {consumer_terminal.current_feeder_direction}")
        print()

        reset_switch()

    from zepben.evolve import SinglePhaseKind

    switch.set_normally_open(True, phase=SinglePhaseKind.A)
    print(f"Switch set to normally open on phase A. Switch is between feeder head and energy consumer 675.")

    await set_feeder_direction()
    await clear_feeder_direction()


async def trees():
    """
    A downstream tree contains all non-intersecting equipment paths starting from a common equipment
    and following downstream terminals. The same equipment may appear multiple times in the tree if
    the network contains multiple downstream paths to the equipment, i.e. loops. As this is backed by
    a NetworkTrace, either the normal or current state of the network may be used to determine whether
    to trace through each switch when combined with `Conditions.stop_at_open`
    """
    from zepben.evolve import Tracing, SinglePhaseKind, EquipmentTreeBuilder, TreeNode, NetworkStateOperators

    print_heading("DOWNSTREAM TREES")

    def desc_lines(node: TreeNode):
        children = list(node.children)
        for i, child in enumerate(children):
            is_last_child = i == len(children) - 1
            branch_char = "┗" if is_last_child else "┣"
            stem_char = " " if is_last_child else "┃"
            yield f"{branch_char}━{child.identified_object}"
            for line in desc_lines(child):
                yield f"{stem_char} {line}"

    def print_tree(root_node: TreeNode):
        print(root_node.identified_object)
        for line in desc_lines(root_node):
            print(line)
        print()

    switch.set_open(True, SinglePhaseKind.C)
    print("Switch set to currently open on phase C.\n")

    await Tracing.set_direction().run(network, network_state_operators=NetworkStateOperators.NORMAL)
    print("Feeder direction set.\n")

    print("Normal Downstream Tree:")
    equip_tree_builder = EquipmentTreeBuilder()
    await (
        Tracing.network_trace()
        .add_step_action(equip_tree_builder)
        .run(feeder_head)
    )
    print_tree(next(equip_tree_builder.roots))

    print("Current Downstream Tree:")
    cur_equip_tree_builder = EquipmentTreeBuilder()
    await (
        Tracing.network_trace(
            network_state_operators=NetworkStateOperators.CURRENT
        ).add_step_action(cur_equip_tree_builder)
    ).run(feeder_head)
    print_tree(next(cur_equip_tree_builder.roots))

    await Tracing.clear_direction().run(feeder_head)
    print(f"Feeder direction removed for each terminal.\n")

    reset_switch()


async def main():
    # All examples are self-contained. Feel free to comment out any of the following lines to isolate specific examples.
    await network_trace()
    await network_trace_step_actions()
    await network_trace_conditions()
    await assigning_equipment_to_feeders()
    await feeder_direction()
    await trees()

if __name__ == "__main__":
    asyncio.run(main())
