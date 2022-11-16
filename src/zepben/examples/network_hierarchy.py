#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from zepben.evolve import NetworkHierarchy, GeographicalRegion, SubGeographicalRegion, Feeder, Substation

# A network hierarchy describes the high-level hierarchy of the network.

fdr1, fdr2, fdr3, fdr4, fdr5, fdr6 = Feeder(), Feeder(), Feeder(), Feeder(), Feeder(), Feeder()

sub1 = Substation(normal_energized_feeders=[fdr1, fdr2])
sub2 = Substation(normal_energized_feeders=[fdr3])
sub3 = Substation(normal_energized_feeders=[fdr4, fdr5, fdr6])

sgr_sydney = SubGeographicalRegion(mrid="sgr_sydney", substations=[sub1, sub2])
sgr_newcastle = SubGeographicalRegion(mrid="sgr_newcastle", substations=[sub3])

gr_nsw = GeographicalRegion(mrid="gr_nsw", sub_geographical_regions=[sgr_sydney, sgr_newcastle])

nh = NetworkHierarchy(
    geographical_regions=[gr_nsw],
    sub_geographical_regions=[sgr_sydney, sgr_newcastle],
    substations=[sub1, sub2, sub3],
    feeders=[fdr1, fdr2, fdr3, fdr4, fdr5, fdr6],
)
