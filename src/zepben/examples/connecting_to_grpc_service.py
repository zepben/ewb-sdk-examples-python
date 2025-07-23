#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.


async def plaintext_connection():
    """
    Connects to an RPC server without TLS or authentication.
    This method should only be used in development and for demos.
    """
    from zepben.evolve import connect_insecure, NetworkConsumerClient
    async with connect_insecure("hostname", 1234) as insecure_channel:
        client = NetworkConsumerClient(insecure_channel)
        grpc_result = await client.get_network_hierarchy()
        print(grpc_result.result)


async def secure_connection():
    """
    Connects to an RPC server over TLS.
    No user/client credentials are used.
    """
    from zepben.evolve import connect_tls, NetworkConsumerClient
    async with connect_tls("hostname", 1234) as secure_channel:
        client = NetworkConsumerClient(secure_channel)
        grpc_result = await client.get_network_hierarchy()
        print(grpc_result.result)


async def secure_connection_with_user_credentials():
    """
    Connects to an RPC server over TLS with user credentials. The authentication config will be
    fetched from https://hostname/auth or https://hostname/ewb/auth by default, which includes
    the domain of the OAuth token provider.
    """
    from zepben.evolve import connect_with_password, NetworkConsumerClient
    async with connect_with_password(
        "client ID",
        "username",
        "password",
        "hostname",
        1234
    ) as secure_channel:
        client = NetworkConsumerClient(secure_channel)
        grpc_result = await client.get_network_hierarchy()
        print(grpc_result.result)

    # Specify authentication config explicitly
    from zepben.auth import AuthMethod
    async with connect_with_password(
        "client ID",
        "username",
        "password",
        "hostname",
        1234,
        audience="https://fake_audience/",
        issuer_domain="fake.issuer.domain",
        auth_method=AuthMethod.AUTH0
    ) as secure_channel:
        client = NetworkConsumerClient(secure_channel)
        grpc_result = await client.get_network_hierarchy()
        print(grpc_result.result)


async def secure_connection_with_client_credentials():
    """
    Connects to an RPC server over TLS with client credentials. The authentication config will be
    fetched from https://hostname/auth or https://hostname/ewb/auth by default, which includes the
    domain of the OAuth token provider.
    """
    from zepben.evolve import connect_with_secret, NetworkConsumerClient
    async with connect_with_secret(
        "client ID",
        "client secret",
        "hostname",
        1234
    ) as secure_channel:
        client = NetworkConsumerClient(secure_channel)
        grpc_result = await client.get_network_hierarchy()
        print(grpc_result.result)

    # Specify authentication config explicitly
    from zepben.auth import AuthMethod
    async with connect_with_secret(
        "client ID",
        "client secret",
        "hostname",
        1234,
        audience="https://fake_audience/",
        issuer_domain="fake.issuer.domain",
        auth_method=AuthMethod.AUTH0
    ) as secure_channel:
        client = NetworkConsumerClient(secure_channel)
        grpc_result = await client.get_network_hierarchy()
        print(grpc_result.result)


def connect_sync():
    """
    You may use `SyncNetworkConsumerClient` if you prefer not to use asyncio.
    The API calls are the same between `SyncNetworkConsumerClient` and `NetworkConsumerClient`.
    """
    from zepben.evolve import connect_insecure, SyncNetworkConsumerClient
    channel = connect_insecure("hostname", 1234)
    client = SyncNetworkConsumerClient(channel)
    grpc_result = client.get_network_hierarchy()
    print(grpc_result.result)


async def connect_using_token():
    import json
    from zepben.evolve import connect_with_token, NetworkConsumerClient

    with open("config.json") as f:
        c = json.load(f)

    print("Connecting to EWB..")
    channel = connect_with_token(
        host=c["host"],
        access_token=c["access_token"],
        rpc_port=c["rpc_port"]
    )
    client = NetworkConsumerClient(channel)
    print("Connection established..")
    print("Printing network hierarchy..")
    grpc_result = await client.get_network_hierarchy()
    print(grpc_result.result)


if __name__ == "__main__":
    import asyncio
    asyncio.run(connect_using_token())
