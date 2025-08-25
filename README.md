# EWB Python SDK examples

This repo contains examples for utilising the [Python SDK](https://github.com/zepben/evolve-sdk-python). If you don't wish to use Python, you can alternatively use
the [JVM SDK](https://github.com/zepben/evolve-sdk-jvm), which is written in Kotlin and compatible with Java.

Some of the examples covering basic concepts are highlighted in this section, an [index of the examples](#examples-index) containing more advanced concepts can
be found at the end of the README.

## Adding the EWB SDK to your own project

Add the following items to your setup.py:

```
    install_requires=[
        "zepben.auth==<VERSION>",
        "zepben.ewb==<VERSION>"
    ],
```

The latest versions can be found on the Python package index:

* [zepben.ewb](https://pypi.org/project/zepben.ewb/)
* [zepben.auth](https://pypi.org/project/zepben.auth/)

> [!NOTE]  
> If you are a keen developer and want access to the latest updates, beta versions that have been released since the latest official release can be found in
> the "Release history" page for each dependency.

## Using the examples in your own project

> [!IMPORTANT]  
> Sometimes you may want to copy the code from the examples into your own project, rather than just running them from this repo. It is important to note that
> some of the examples may be using beta version features, and you should make sure to use the SDK version specified in [setup.py](setup.py), rather than the
> latest official release.

## Installation and setup of the examples

```
pip install -e .
```

## Basic usage with EWB server

The first step is to [connect to the EWB gRPC server](./src/zepben/examples/connecting_to_grpc_service.py). All other examples relating to using the EWB server
can use any of the connection methods shown, based on the configuration of the EWB server.

Once you are connected the most common use case is to [fetch a feeder](./src/zepben/examples/fetching_network_model.py). Once you have the feeder you can work
with it locally, without changing the objects on the server, or other users of the feeder. See further examples for things you may want to do ith the feeder.

## Building local models

Sometimes you do not want to pull a model from the EWB server, and instead want to build it locally. Depending on your use case, you may want
to [build your own network hierarchy](./src/zepben/examples/network_hierarchy.py). You can even build a full feeder, such as
the [IEEE 13 node test feeder](./src/zepben/examples/ieee_13_node_test_feeder.py)

# EAS Python examples

The EAS adds additional functionality to EWB.

## Adding the EAS client to your own project

In addition to the [EWB SDK requirements](#adding-the-ewb-sdk-to-your-own-project), add the following item to your setup.py:

```
    install_requires=[
        "zepben.eas==<VERSION>",
    ],
```

The latest version can be found on the Python package index:

* [zepben.eas](https://pypi.org/project/zepben.eas/)

> [!NOTE]  
> If you are a keen developer and want access to the latest updates, beta versions that have been released since the latest official release can be found in
> the "Release history" page.

## Creating and uploading studies

One feature added by the EAS is studies. You can [create you own study and upload it](./src/zepben/examples/studies/creating_and_uploading_study.py) to EAS,
which will then be available via the UI for visualisation.

# Examples Index

#### Reading from the server

* [Connecting to EWB's gRPC service](src/zepben/examples/connecting_to_grpc_service.py)
* [Fetching network models using the gRPC service](src/zepben/examples/fetching_network_model.py)

#### Creating local models

* [Interacting with a network model (e.g. adding and removing objects)](src/zepben/examples/network_service_interactions.py)
* [Building the IEEE 13 node test feeder](src/zepben/examples/ieee_13_node_test_feeder.py)
* [Modelling network hierarchies](src/zepben/examples/network_hierarchy.py)

#### Using the model

* [Examining connectivity of cores on equipment and terminals](src/zepben/examples/examining_connectivity.py)
* [Running network traces](src/zepben/examples/tracing.py)
* [Creating and uploading studies](src/zepben/examples/studies/creating_and_uploading_study.py)
* [Manipulating the current state of the network, including swapping a zone open point](src/zepben/examples/current_state_manipulations.py)

#### Power flow

* [Translating a CIM network model into a pandapower model](src/zepben/examples/translating_to_pandapower_model.py)
* [Requesting a PowerFactory model through the SDK](src/zepben/examples/request_power_factory_models.py)
