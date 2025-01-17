## [0.2.0] - UNRELEASED
### Breaking Changes
* None.

### New Features
* Added examples for the following:
  * Connecting to EWB's gRPC service
  * Fetching network models using the gRPC service
  * Building the IEEE 13 node test feeder
  * Modelling network hierarchies
  * Interacting with a network model (e.g. adding and removing objects)
  * Examining connectivity of cores on equipment and terminals
  * Running network traces
  * Creating and uploading studies
  * Translating a CIM network model into a pandapower model
  * Requesting a PowerFactory model through the SDK
  * Manipulating the current state of the network, including swapping a zone open point.

### Enhancements
* Limited power factory demo to 1 job at a time.
* Added model download function to power factory demo
* restrict installation to supported Python versions from 3.9 to 3.11

### Fixes
* None.

### Notes
* Support `zepben.eas` up to 0.16.0.
* Support `zepben.evolve` up to 0.43.0.
