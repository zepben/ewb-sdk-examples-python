#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import os
from typing import List

import requests
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from zepben.auth import get_token_fetcher

# This example utilises the EWB GraphQL APIs to fetch the network hierarchy from the server and
# then create a Powerfactory model by selecting components of the hierarchy to use.
# To use, populate the below variables with your desired targets plus the server and auth settings.

# Set of mRID/names of targets, leaving any target blank will exclude that level of hierarchy if it's the highest level
# Names are visible through the hierarchy viewer in the UI - or you can do a getNetworkHierarchy GraphQL query as per the below.
target_zone_substation = {"zonesub-mRID-or-name"}
target_feeder = {"feeder-mRID-or-name"}
target_lv = {"lvfeeder-mRID-or-name"}

# resulting PFD file name
file_name = "test_file"
output_dir = "path to output dir"

# use feeder max demand for load?
# False will create time series characteristic for load
feeder_max_demand = False

# graphQL endpoint access settings
network_endpoint = 'https://{url}/api/network/graphql'
api_endpoint = 'https://{url}/api/graphql'
audience = "https://{url}/"
issuer = "issuer_domain"
client_id = 'client_id'
username = 'username'
password = 'password'

### EXAMPLE QUERY ONLY ###
# This is an example GraphQL query for the full network hierarchy. This is not used as part of this code, and is purely illustrative.
# See below functions for actual queries used.
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
token_fetcher = get_token_fetcher(audience=audience, issuer=issuer, client_id=client_id, username=username, password=password)
tft = token_fetcher.fetch_token()
network_transport = RequestsHTTPTransport(url=network_endpoint, headers={'Authorization': tft})
api_transport = RequestsHTTPTransport(url=api_endpoint, headers={'Authorization': tft})

network_client = Client(transport=network_transport)
api_client = Client(transport=api_transport)

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
    # Set up auth
    target = []
    # Request for ZoneSub -> Feeder -> lvFeeder
    body = gql('''
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
    ''')
    if check_if_currently_generating_a_model():
        result = retrieve_network_hierarchy(body)
        target = get_target(target, result)
        model_id = request_pf_model(target, file_name, feeder_max_demand)
        print("Power factory model creation requested, model id: " + model_id)
    else:
        print("Warning: Still generating previous model, current model will not be generated.")


def request_pf_model_for_a_zone_with_hv_only():
    target = []
    # Request for ZoneSub -> Feeder
    body = gql('''
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
    ''')
    if check_if_currently_generating_a_model():
        result = retrieve_network_hierarchy(body)
        target = get_target(target, result)
        model_id = request_pf_model(target, file_name, feeder_max_demand)
        print("Power factory model creation requested, model id: " + model_id)
    else:
        print("Warning: Still generating previous model, current model will not be generated.")


def retrieve_network_hierarchy(body):
    return network_client.execute(body)


def get_target(target, result):
    if len(target_zone_substation) == 0:
        # No Zone sub was specified thus no zone sub will be added to target
        for zone_sub in result['getNetworkHierarchy']["substations"]:
            target = get_feeder(target, zone_sub)
    else:
        queried_zone_sub = [x for x in result['getNetworkHierarchy']["substations"] if
                            x['mRID'] in target_zone_substation or x['name'] in target_zone_substation]
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
            queried_feeder = [x for x in zone_sub['feeders'] if x['mRID'] in target_feeder or x['name'] in target_feeder]
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
            queried_lv = [x for x in feeder['normalEnergizedLvFeeders'] if x['mRID'] in target_lv or x['name'] in target_lv]
            for lv in queried_lv:
                target.append(lv['mRID'])
    return target


def request_pf_model(equipment_container_list: List[str], filename: str, spread_max_demand: bool = False):
    """
    Performs the GraphQL request to create the Powerfactory model for the provided list of equipment containers.

    :param equipment_container_list: List of EquipmentContainer mRIDs to include in the Powerfactory model.
    :param filename: Desired PFD filename
    :param spread_max_demand: Whether to spread max demand load across transformers/loads. False will instead configure the timeseries database.
    """
    # Set isPublic to false if you only want the specific user to see the model
    body = gql('''
    mutation createPowerFactoryModel($input: PowerFactoryModelInput!) {
        createPowerFactoryModel(input: $input)
    }
    ''')
    variables = {'input': {
        'name': filename,
        'generationSpec': {'equipmentContainerMrids': equipment_container_list,
                           'distributionTransformerConfig': {
                               'rGround': 0.01,
                               'xGround': 0.01
                           },
                           'loadConfig': {
                               'spreadMaxDemand': spread_max_demand
                           }
                           },
        'isPublic': 'true'}}
    result = api_client.execute(body, variable_values=variables)
    return result['createPowerFactoryModel']


def check_if_currently_generating_a_model():
    body = gql('''
    query pagedPowerFactoryModels(
      $limit: Int!
      $offset: Long!
      $filter: GetPowerFactoryModelsFilterInput
      $sort: GetPowerFactoryModelsSortCriteriaInput
    ) {
        pagedPowerFactoryModels(
            limit: $limit
            offset: $offset
            filter: $filter
            sort: $sort
        ) {
            totalCount
            offset
            powerFactoryModels {
                id
                name
                createdAt
                state
                errors
            }
        }
    }
    ''')
    variables = {
        "limit": 10,
        "offset": 0,
        "filter": {}
    }
    result = api_client.execute(body, variable_values=variables)
    for entry in result['pagedPowerFactoryModels']['powerFactoryModels']:
        if entry['state'] == 'CREATION':
            return False
    return True


def download_model(model_number):
    # Request model
    model_url = api_endpoint.replace("graphql", "power-factory-model/") + str(model_number)
    body = gql('''
    query powerFactoryModelById($modelId: ID!) {
      powerFactoryModelById(modelId: $modelId) {
        id
        name
        createdAt
        state
        generationSpec {
          equipmentContainerMrids
          distributionTransformerConfig {
            rGround
            xGround
          }
        }
        isPublic
        errors
      }
    }
    ''')
    variables = {
        "modelId": model_number,
    }
    result = api_client.execute(body, variable_values=variables)
    model_status = result['powerFactoryModelById']['state']
    if model_status == "COMPLETED":
        model = requests.get(model_url, headers={'Authorization': tft})
        open(os.path.join(output_dir, file_name) + ".pfd", 'wb').write(model.content)
        print(file_name + ".pfd saved at " + output_dir)
    elif model_status == "CREATION":
        print("Model is still being created, please download at a later time")
    elif model_status == "FAILED":
        print("Model creation error: " + str(result['powerFactoryModelById']['errors']))


if __name__ == "__main__":
    # Generate model with lv
    request_pf_model_for_a_zone_with_hv_lv()

    # Generate model without lv
    request_pf_model_for_a_zone_with_hv_only()

    # Download a model via model number
    download_model(123)
