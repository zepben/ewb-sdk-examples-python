#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import statistics
from typing import Tuple

import numpy
from zepben.evolve import AcLineSegment, Disconnector, PowerTransformer, TransformerFunctionKind, NetworkService, Terminal, PowerTransformerEnd, EnergyConsumer, \
    PerLengthSequenceImpedance, PhaseCode, EnergyConsumerPhase, SinglePhaseKind, LinearShuntCompensator, ShuntCompensatorInfo, PhaseShuntConnectionKind

FEET_PER_MILE = 5280

###########################
# EQUIPMENT AND TERMINALS #
###########################
# Terminal phases are ABC by default
vr_650_632_t1, vr_650_632_t2 = Terminal(), Terminal()
vr_650_632_e1 = PowerTransformerEnd(terminal=vr_650_632_t1)
vr_650_632_e2 = PowerTransformerEnd(terminal=vr_650_632_t2)
vr_650_632 = PowerTransformer(function=TransformerFunctionKind.voltageRegulator, terminals=[vr_650_632_t1, vr_650_632_t2],
                              power_transformer_ends=[vr_650_632_e1, vr_650_632_e2])

l_632_645_t1, l_632_645_t2 = Terminal(phases=PhaseCode.BCN), Terminal(phases=PhaseCode.BCN)
l_632_645 = AcLineSegment(length=500, terminals=[l_632_645_t1, l_632_645_t2])

l_632_633_t1, l_632_633_t2 = Terminal(phases=PhaseCode.ABCN), Terminal(phases=PhaseCode.ABCN)
l_632_633 = AcLineSegment(length=500, terminals=[l_632_633_t1, l_632_633_t2])

tx_633_634_t1, tx_633_634_t2 = Terminal(phases=PhaseCode.ABCN), Terminal(phases=PhaseCode.ABCN)
tx_633_634_e1 = PowerTransformerEnd(terminal=tx_633_634_t1)
tx_633_634_e2 = PowerTransformerEnd(terminal=tx_633_634_t2)
tx_633_634 = PowerTransformer(terminals=[tx_633_634_t1, tx_633_634_t2], power_transformer_ends=[tx_633_634_e1, tx_633_634_e2])

l_645_646_t1, l_645_646_t2 = Terminal(phases=PhaseCode.BCN), Terminal(phases=PhaseCode.BCN)
l_645_646 = AcLineSegment(length=300, terminals=[l_645_646_t1, l_645_646_t2])

l_650_632_t1, l_650_632_t2 = Terminal(phases=PhaseCode.ABCN), Terminal(phases=PhaseCode.ABCN)
l_650_632 = AcLineSegment(length=2000, terminals=[l_650_632_t1, l_650_632_t2])

l_684_652_t1, l_684_652_t2 = Terminal(phases=PhaseCode.AN), Terminal(phases=PhaseCode.AN)
l_684_652 = AcLineSegment(length=800, terminals=[l_684_652_t1, l_684_652_t2])

l_632_671_t1, l_632_671_t2 = Terminal(phases=PhaseCode.ABCN), Terminal(phases=PhaseCode.ABCN)
l_632_671 = AcLineSegment(length=2000, terminals=[l_632_671_t1, l_632_671_t2])

l_671_684_t1, l_671_684_t2 = Terminal(phases=PhaseCode.ACN), Terminal(phases=PhaseCode.ACN)
l_671_684 = AcLineSegment(length=300, terminals=[l_671_684_t1, l_671_684_t2])

l_671_680_t1, l_671_680_t2 = Terminal(phases=PhaseCode.ABCN), Terminal(phases=PhaseCode.ABCN)
l_671_680 = AcLineSegment(length=1000, terminals=[l_671_680_t1, l_671_680_t2])

sw_671_692_t1, sw_671_692_t2 = Terminal(phases=PhaseCode.ABCN), Terminal(phases=PhaseCode.ABCN)
sw_671_692 = Disconnector(terminals=[sw_671_692_t1, sw_671_692_t2])

l_684_611_t1, l_684_611_t2 = Terminal(phases=PhaseCode.CN), Terminal(phases=PhaseCode.CN)
l_684_611 = AcLineSegment(length=300, terminals=[l_684_611_t1, l_684_611_t2])

l_692_675_t1, l_692_675_t2 = Terminal(phases=PhaseCode.ABCN), Terminal(phases=PhaseCode.ABCN)
l_692_675 = AcLineSegment(length=500, terminals=[l_692_675_t1, l_692_675_t2])

ec_634_t = Terminal(phases=PhaseCode.ABCN)
ec_634_pha = EnergyConsumerPhase(phase=SinglePhaseKind.A, p_fixed=160000, q_fixed=110000)
ec_634_phb = EnergyConsumerPhase(phase=SinglePhaseKind.B, p_fixed=120000, q_fixed=90000)
ec_634_phc = EnergyConsumerPhase(phase=SinglePhaseKind.C, p_fixed=120000, q_fixed=90000)
ec_634 = EnergyConsumer(terminals=[ec_634_t], energy_consumer_phases=[ec_634_pha, ec_634_phb, ec_634_phc])

ec_645_t = Terminal(phases=PhaseCode.ABCN)
ec_645_pha = EnergyConsumerPhase(phase=SinglePhaseKind.A, p_fixed=0, q_fixed=0)
ec_645_phb = EnergyConsumerPhase(phase=SinglePhaseKind.B, p_fixed=170000, q_fixed=125000)
ec_645_phc = EnergyConsumerPhase(phase=SinglePhaseKind.C, p_fixed=0, q_fixed=0)
ec_645 = EnergyConsumer(terminals=[ec_645_t], energy_consumer_phases=[ec_645_pha, ec_645_phb, ec_645_phc])

ec_646_t = Terminal(phases=PhaseCode.ABC)
ec_646_pha = EnergyConsumerPhase(phase=SinglePhaseKind.A, p=0, q=0)
ec_646_phb = EnergyConsumerPhase(phase=SinglePhaseKind.B, p=230000, q=132000)
ec_646_phc = EnergyConsumerPhase(phase=SinglePhaseKind.C, p=0, q=0)
ec_646 = EnergyConsumer(terminals=[ec_646_t], energy_consumer_phases=[ec_646_pha, ec_646_phb, ec_646_phc])

ec_652_t = Terminal(phases=PhaseCode.ABCN)
ec_652_pha = EnergyConsumerPhase(phase=SinglePhaseKind.A, p=128000, q=86000)
ec_652_phb = EnergyConsumerPhase(phase=SinglePhaseKind.B, p=0, q=0)
ec_652_phc = EnergyConsumerPhase(phase=SinglePhaseKind.C, p=0, q=0)
ec_652 = EnergyConsumer(terminals=[ec_652_t], energy_consumer_phases=[ec_652_pha, ec_652_phb, ec_652_phc])

ec_671_t = Terminal(phases=PhaseCode.ABC)
ec_671_pha = EnergyConsumerPhase(phase=SinglePhaseKind.A, p_fixed=385000, q_fixed=220000)
ec_671_phb = EnergyConsumerPhase(phase=SinglePhaseKind.B, p_fixed=385000, q_fixed=220000)
ec_671_phc = EnergyConsumerPhase(phase=SinglePhaseKind.C, p_fixed=385000, q_fixed=220000)
ec_671 = EnergyConsumer(terminals=[ec_671_t], energy_consumer_phases=[ec_671_pha, ec_671_phb, ec_671_phc])

ec_675_t = Terminal(phases=PhaseCode.ABCN)
ec_675_pha = EnergyConsumerPhase(phase=SinglePhaseKind.A, p_fixed=485000, q_fixed=190000)
ec_675_phb = EnergyConsumerPhase(phase=SinglePhaseKind.B, p_fixed=68000, q_fixed=60000)
ec_675_phc = EnergyConsumerPhase(phase=SinglePhaseKind.C, p_fixed=290000, q_fixed=212000)
ec_675 = EnergyConsumer(terminals=[ec_675_t], energy_consumer_phases=[ec_675_pha, ec_675_phb, ec_675_phc])

ec_692_t = Terminal(phases=PhaseCode.ABC)
ec_692_pha = EnergyConsumerPhase(phase=SinglePhaseKind.A, p=0, q=0)
ec_692_phb = EnergyConsumerPhase(phase=SinglePhaseKind.B, p=0, q=0)
ec_692_phc = EnergyConsumerPhase(phase=SinglePhaseKind.C, p=170000, q=151000)
ec_692 = EnergyConsumer(terminals=[ec_692_t], energy_consumer_phases=[ec_692_pha, ec_692_phb, ec_692_phc])

ec_611_t = Terminal(phases=PhaseCode.ABCN)
ec_611_pha = EnergyConsumerPhase(phase=SinglePhaseKind.A, p=0, q=0)
ec_611_phb = EnergyConsumerPhase(phase=SinglePhaseKind.B, p=0, q=0)
ec_611_phc = EnergyConsumerPhase(phase=SinglePhaseKind.C, p=170000, q=80000)
ec_611 = EnergyConsumer(terminals=[ec_611_t], energy_consumer_phases=[ec_611_pha, ec_611_phb, ec_611_phc])

# Distributed load on line 632-671 is unmodelled.

lsc_675_t1, lsc_675_t2 = Terminal(phases=PhaseCode.ABCN), Terminal(phases=PhaseCode.ABCN)
lsc_675_info = ShuntCompensatorInfo(rated_voltage=4160, rated_current=48.077, rated_reactive_power=200000)
lsc_675 = LinearShuntCompensator(terminals=[lsc_675_t1, lsc_675_t2], asset_info=lsc_675_info)

lsc_611_t1, lsc_611_t2 = Terminal(phases=PhaseCode.CN), Terminal(phases=PhaseCode.CN)
lsc_611_info = ShuntCompensatorInfo(rated_voltage=4160, rated_current=24.048, rated_reactive_power=100000)
lsc_611 = LinearShuntCompensator(terminals=[lsc_611_t1, lsc_611_t2], asset_info=lsc_611_info)

###########################
# SETTING LINE IMPEDANCES #
###########################
def plsi_from_z_per_mile(*impedances: Tuple[float, float]):
    r_per_foot, x_per_foot = numpy.mean(impedances, axis=0) / FEET_PER_MILE
    return PerLengthSequenceImpedance(r=r_per_foot, x=x_per_foot)


plsi_601 = plsi_from_z_per_mile((0.3465, 1.0179), (0.3375, 1.0478), (0.3414, 1.0348))
plsi_602 = plsi_from_z_per_mile((0.7526, 1.1814), (0.7475, 1.1983), (0.7436, 1.2112))
plsi_603 = plsi_from_z_per_mile((1.3294, 1.3471), (1.3238, 1.3569))
plsi_604 = plsi_from_z_per_mile((1.3238, 1.3569), (1.3294, 1.3471))
plsi_605 = plsi_from_z_per_mile((1.3292, 1.3475))
plsi_606 = plsi_from_z_per_mile((0.7982, 0.4463), (0.7891, 0.4041), (0.7982, 0.4463))
plsi_607 = plsi_from_z_per_mile((1.3425, 0.5124))

##########################
# BUILDING NETWORK MODEL #
##########################
network = NetworkService()
for io in [vr_650_632, vr_650_632_t1, vr_650_632_t2, vr_650_632_e1, vr_650_632_e2, l_632_645, l_632_645_t1, l_632_645_t2, l_632_633, l_632_633_t1, l_632_633_t2,
           tx_633_634, tx_633_634_t1, tx_633_634_t2, tx_633_634_e1, tx_633_634_e2, l_645_646, l_645_646_t1, l_645_646_t2, l_650_632, l_650_632_t1, l_650_632_t2,
           l_684_652, l_684_652_t1, l_684_652_t2, l_632_671, l_632_671_t1, l_632_671_t2, l_671_684, l_671_684_t1, l_671_684_t2, l_671_680, l_671_680_t1,
           l_671_680_t2, sw_671_692, sw_671_692_t1, sw_671_692_t2, l_684_611, l_684_611_t1, l_684_611_t2, l_692_675, l_692_675_t1, l_692_675_t2, ec_634_t,
           ec_634_pha, ec_634_phb, ec_634_phc, ec_634, ec_645_t, ec_645_pha, ec_645_phb, ec_645_phc, ec_645, ec_646_t, ec_646_pha, ec_646_phb, ec_646_phc,
           ec_646, ec_652_t, ec_652_pha, ec_652_phb, ec_652_phc, ec_652, ec_671_t, ec_671_pha, ec_671_phb, ec_671_phc, ec_671, ec_675_t, ec_675_pha, ec_675_phb,
           ec_675_phc, ec_675, ec_692_t, ec_692_pha, ec_692_phb, ec_692_phc, ec_692, ec_611_t, ec_611_pha, ec_611_phb, ec_611_phc, ec_611, lsc_675_t1,
           lsc_675_t2, lsc_675_info, lsc_675, lsc_611_t1, lsc_611_t2, lsc_611_info, lsc_611, plsi_601, plsi_602, plsi_603, plsi_604, plsi_605, plsi_606,
           plsi_607]:
    network.add(io)

# Complete 630-632 regulator + line
network.connect_terminals(vr_650_632_t2, l_650_632_t1)

# Node 611
network.connect_terminals(l_684_611_t2, lsc_611_t1)
network.connect_terminals(lsc_611_t2, ec_611_t)

# Node 632
network.connect_terminals(l_650_632_t2, l_632_633_t1)
network.connect_terminals(l_650_632_t2, l_632_645_t1)
network.connect_terminals(l_650_632_t2, l_632_671_t1)

# Node 633
network.connect_terminals(l_632_633_t2, tx_633_634_t1)

# Node 634
network.connect_terminals(tx_633_634_t2, ec_634_t)

# Node 645
network.connect_terminals(l_632_645_t2, l_645_646_t1)
network.connect_terminals(l_632_645_t2, ec_645_t)

# Node 646
network.connect_terminals(l_645_646_t2, ec_646_t)

# Node 652
network.connect_terminals(l_684_652_t2, ec_652_t)

# Node 671
network.connect_terminals(l_632_671_t2, l_671_680_t1)
network.connect_terminals(l_632_671_t2, l_671_684_t1)
network.connect_terminals(l_632_671_t2, sw_671_692_t1)
network.connect_terminals(l_632_671_t2, ec_671_t)

# Node 675
network.connect_terminals(l_692_675_t2, lsc_611_t1)
network.connect_terminals(lsc_675_t2, ec_675_t)

# Node 684
network.connect_terminals(l_671_684_t2, l_684_611_t1)
network.connect_terminals(l_671_684_t2, l_684_652_t1)

# Node 692
network.connect_terminals(sw_671_692_t2, l_692_675_t1)
network.connect_terminals(sw_671_692_t2, ec_692_t)
