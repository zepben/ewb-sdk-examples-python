# EWB Python SDK examples

This repo contains examples for utilising the [Pytho SDK](https://github.com/zepben/evolve-sdk-python). If you don't wish us Python, you can alternatively use
the [JVM SDK](https://github.com/zepben/evolve-sdk-jvm), which is written in Kotlin and compatible with Java.

## Adding the EWB SDK to your own project

Add the following items to your setup.py:

```
    install_requires=[
        "zepben.auth==<VERSION>>",
        "zepben.evolve==<VERSION>>"
    ],
```

The latest versions can be found on the Python package index:

* [zepben.evolve](https://pypi.org/project/zepben.evolve/)
* [zepben.auth]([https://pypi.org/project/zepben.auth/)

> [!NOTE]  
> If you are a keen developer and want access to the latest updates, beta versions that have been released since the latest official release can be found in
> the "Release history" page for each dependency.

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

## Adding the SDK to your own project

In addition to the [EWB SDK requirements](#adding-the-ewb-sdk-to-your-own-project), add the following item to your setup.py:

```
    install_requires=[
        "zepben.eas==<VERSION>>",
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
