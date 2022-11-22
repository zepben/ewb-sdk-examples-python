#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from zepben.evolve import NetworkHierarchy, GeographicalRegion, SubGeographicalRegion, Feeder, Substation, Loop, Circuit

# A network hierarchy describes the high-level hierarchy of the network.

fdr1 = Feeder(name="Sydney feeder 1")
fdr2 = Feeder(name="Sydney feeder 2")
fdr3 = Feeder(name="Sydney feeder 3")
fdr4 = Feeder(name="Newcastle feeder 1")
fdr5 = Feeder(name="Newcastle feeder 2")
fdr6 = Feeder(name="Newcastle feeder 3")

sub1 = Substation(name="Sydney substation 1")
sub2 = Substation(name="Sydney substation 2", normal_energized_feeders=[fdr1, fdr2, fdr3])
sub3 = Substation(name="Newcastle substation", normal_energized_feeders=[fdr4, fdr5, fdr6])

circuit_sydney = Circuit(end_substations=[sub1, sub2])
loop_sydney = Loop(circuits=[circuit_sydney], substations=[sub1], energizing_substations=[sub2])
sgr_sydney = SubGeographicalRegion(name="Sydney", substations=[sub1, sub2])
sgr_newcastle = SubGeographicalRegion(name="Newcastle", substations=[sub3])

gr_nsw = GeographicalRegion(name="New South Wales", sub_geographical_regions=[sgr_sydney, sgr_newcastle])

network_hierarchy = NetworkHierarchy(
    geographical_regions={gr_nsw.mrid: gr_nsw},
    sub_geographical_regions={sgr.mrid: sgr for sgr in (sgr_sydney, sgr_newcastle)},
    substations={sub.mrid for sub in (sub1, sub2, sub3)},
    feeders={fdr.mrid: fdr for fdr in (fdr1, fdr2, fdr3, fdr4, fdr5, fdr6)},
    circuits={circuit_sydney.mrid: circuit_sydney},
    loops={loop_sydney.mrid: loop_sydney}
)

print("Network hierarchy:")
for gr in network_hierarchy.geographical_regions.values():
    print(f"- {gr.name}")
    for sgr in gr.sub_geographical_regions:
        print(f"  - {sgr.name}")
        for sub in sgr.substations:
            print(f"    - {sub.name}")
            for fdr in sub.feeders:
                print(f"      - {fdr.name}")