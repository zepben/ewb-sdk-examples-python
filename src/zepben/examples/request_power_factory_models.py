#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import json
from graphqlclient import GraphQLClient
from zepben.auth import ZepbenTokenFetcher, AuthMethod

# set of mRID of targets, leaving any target blank will exclude that level of hierarchy if it's the highest level
target_zone_substation = {"zonesub-mRID"}
target_feeder = {"feeder-mRID"}
target_lv = {"lvfeeder-mRID"}

# resulting file name
file_name = "test_file"

# graphQL endpoint access settings
network_endpoint = 'https://{url}/api/network/graphql'
api_endpoint = 'https://{url}/api/graphql'
audience = "https://{url}/"
issuer_domain = "issuer_domain"
auth_method = AuthMethod.AUTH0
client_id = 'client_id'
username = 'username'
password = 'password'

# full network hierarchy, simply remove levels that is not required
# Zone sub is the highest level supported in this example code
'''
query network {
    getNetworkHierarchy {
        geographicalRegions {
            mRID
            name
            subGeographicalRegions {
                mRID
                name
                substations {
                    mRID
                    name
                    feeders {
                        mRID
                        name
                        normalEnergizedLvFeeders {
                            mRID
                            name
                        }
                    }
                }
            }
        }
    }
}
'''


def request_pf_model_for_a_zone_with_hv_lv():
    token_fetcher = ZepbenTokenFetcher(audience=audience, issuer_domain=issuer_domain, auth_method=auth_method)
    tft = token_fetcher.fetch_graphql_token(client_id, username, password)
    target = []
    # Request for ZoneSub -> Feeder -> lvFeeder
    body = '''
    query network {
      getNetworkHierarchy {
            substations {
              mRID
              name
              feeders {
                mRID
                name
                normalEnergizedLvFeeders {
                  mRID
                  name
                }
              }
            }
          }
        }
            '''
    result = retrieve_network_hierarchy(body, tft)
    target = get_target(target, result)
    request_pf_model(target, file_name, tft)


def request_pf_model_for_a_zone_with_hv_only():
    token_fetcher = ZepbenTokenFetcher(audience=audience, issuer_domain=issuer_domain, auth_method=auth_method)
    tft = token_fetcher.fetch_graphql_token(client_id, username, password)
    target = []
    # Request for ZoneSub -> Feeder
    body = '''
    query network {
      getNetworkHierarchy {
            substations {
              mRID
              name
              feeders {
                mRID
                name
              }
            }
          }
        }
            '''
    result = retrieve_network_hierarchy(body, tft)
    target = get_target(target, result)
    request_pf_model(target, file_name, tft)


def retrieve_network_hierarchy(body, tft):
    client = GraphQLClient(network_endpoint)
    client.inject_token(tft)
    result = client.execute(body)
    return json.loads(result)


def get_target(target, result):
    if len(target_zone_substation) == 0:
        # No Zone sub was specified thus no zone sub will be added to target
        for zone_sub in result['data']['getNetworkHierarchy']["substations"]:
            target = get_feeder(target, zone_sub)
    else:
        queried_zone_sub = [x for x in result['data']['getNetworkHierarchy']["substations"] if x['mRID'] in target_zone_substation]
        for zone_sub in queried_zone_sub:
            target.append(zone_sub['mRID'])
            target = get_feeder(target, zone_sub)
    return target


def get_feeder(target, zone_sub):
    if 'feeders' in zone_sub.keys():
        if len(target_feeder) == 0:
            for feeder in zone_sub['feeders']:
                if len(target_zone_substation) != 0:
                    target.append(feeder['mRID'])
                target = get_lvfeeder(target, feeder)
            # Path to include only specific feeders
        else:
            queried_feeder = [x for x in zone_sub['feeders'] if x['mRID'] in target_feeder]
            for feeder in queried_feeder:
                target.append(feeder['mRID'])
                target = get_lvfeeder(target, feeder)
    return target


def get_lvfeeder(target, feeder):
    if 'normalEnergizedLvFeeders' in feeder.keys():
        # Path to include all lvFeeders
        if len(target_lv) == 0:
            for lv in feeder['normalEnergizedLvFeeders']:
                target.append(lv['mRID'])
        # Path to include only specific lvFeeders
        else:
            queried_lv = [x for x in feeder['normalEnergizedLvFeeders'] if x['mRID'] in target_lv]
            for lv in queried_lv:
                target.append(lv['mRID'])
    return target


def request_pf_model(equipment_container_list, filename, tft):
    client = GraphQLClient(api_endpoint)
    client.inject_token(tft)
    # Set isPublic to false if you only want the specific user to see the model
    body = '''
    mutation createNetModel($input: NetModelInput!) {
        createNetModel(input: $input)
    }
    '''
    variables = {'input': {
        'name': filename,
        'generationSpec': {'equipmentContainerMrids': equipment_container_list,
                           'distributionTransformerConfig': {
                               'rGround': 0.01,
                               'xGround': 0.01
                           }
                           },
        'isPublic': 'true'}}
    client.execute(body, variables)


if __name__ == "__main__":
    # Generate model with lv
    request_pf_model_for_a_zone_with_hv_lv()

    # Generate model without lv
    request_pf_model_for_a_zone_with_hv_only()
