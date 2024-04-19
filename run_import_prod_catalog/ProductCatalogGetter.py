import datetime
import json

import csv
import os
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# python 3.7+ affords us the use of nullcontext to do conditional "with ... as <x>:"" statements
# we use these for supporting optional 'direct to file' streaming
from contextlib import nullcontext  

from schema import config

import utils

class ProductCatalogGetter:

    environs = {
        'v1': {
            'NP_TEN':          'https://apigw-st.tsl.telus.com/marketsales/fifaproductcatalogmanagement/v1/',
            'NP_GKE_PRIVATE':  'https://apigw-private-yul-np-001.tsl.telus.com/marketsales/fifaproductcatalogmanagement/v1/',

            'PR_GKE_PRIVATE':  'https://apigw-private-yul-pr-001.tsl.telus.com/marketsales/fifaproductcatalogmanagement/v1/',
            'PR_AWS':          'https://gw-200-pr-001.api.cloudapps.telus.com/marketsales/fifaproductcatalogmanagement/v1/',
            'PR_TEN':          'https://apigw-pr.tsl.telus.com/marketsales/fifaproductcatalogmanagement/v1/'
        }
    }

    # tuple of valid endpoints, used for sanity checking in some methods
    _valid_endpoints = (
        'category',
        'productOffering',
        'productOfferingPrice'
    )

    def __init__(self,
                 env: str = 'NP_TEN', 
                 ignore_self_signed_certs: bool = False,
                 record_pull_size: int = 1000
    ):
        '''
        This object handles getting product offer catalog (JSON) from the API.

        Args:
        env (str) : Defaults to 'PR' (production), set to 'NP' for non-production testing

        ignore_self_signed_certs (bool): Defaults to False, may need to set to True if you cannot install the 
        TELUS Root CA certificates in whichever environment will be running this, as without them you will 
        get verification errors against the TMF620 API.
        Dictates whether requests have Verify=True/False.

        record_pull_size (int):  How many records should be pulled at once from the API when 'get_all_' 
        methods are called.  API has limitations so we iteratively get all records using offset/limit pairs
        as specified by record_pull_size.
        '''

        # we persist a requests.Session() so we can support robust retry handling, etc.
        self.http_session = requests.Session()

        retries = Retry(total = 3, 
                backoff_factor = 1,
                status_forcelist = [500,502,503,504])

        self.http_session.mount('https://',HTTPAdapter(max_retries = retries))

        # need to call get_access_token() to populate this
        # but will happen automatically if a request is made and it hasn't been called
        self.access_token = None 
        self.token_expiry = datetime.datetime.now()
        
        # SSL certificate error work-around (see docustring)
        self.ignore_self_signed_certs = ignore_self_signed_certs
        if self.ignore_self_signed_certs:
            # disables warnings.warn() messages that will otherwise pollute our logs
            # if we're using self-signed certificates that don't have a root CA cert installed
            # on our local environment
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
        

        # we only support an explicit list of values for 'env'        
        environ = self.environs['v1']
        if env in environ.keys():
            self.env = env
            print(f'Using environment "{env}"...')
        else:
            print(f'WARNING: unsupported env "{env}" specified. Using "NP" environment.')
            self.env = 'NP_TEN'
        
        # set the base_URL depending on the environment we're running from
        self.base_url = environ[self.env]

        # actual environment-specific settings/config stuff handled here
        if self.env[:2] == 'PR':
            self.env_dependant_http_headers = {}
        elif self.env[:2] == 'NP':
            self.env_dependant_http_headers = {'env': 'it01'}
        
        self.record_pull_size = record_pull_size   

        # Mapping of product offering to category
        self.product_offering_to_category = {}          
        self.product_offering_to_category_map_built_ind = False



    def check_and_refresh_access_token(self):
        '''
        Checks to make sure access token isn't expired, and exists.
        Gets a new one if needed.
        '''
        if not self.access_token or datetime.datetime.now() > self.token_expiry:
            self.get_access_token()



    def get_access_token(self) -> bool:
        '''
        Gets an access token, handle OAuth cycle, etc.
        Generally one should call check_and_refresh_access_token() unless you want to
        explicitly refresh the access token regardless of it's expiration status
        '''
        auth_grant_type='client_credentials'
        access_token_url = config["kong"]["token_url"]
        scope = config["kong"]["scope"]
        client_id = config["kong"]["client_id"]
        client_secret = utils.getSecret(config["kong"]["client_secret_id"])

        params = {
            'grant_type': auth_grant_type,
            'scope': scope
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8'
        }

        token_response = self.http_session.post(
                access_token_url,
                params = params,
                headers = headers,
                auth = (client_id, client_secret),
                verify = not self.ignore_self_signed_certs # TELUS uses self-signed certs that throw errors
        )
        
        try:
            json_object = json.loads(token_response.text)
            self.access_token = json_object['access_token']
            self.token_expiry = datetime.datetime.now() + datetime.timedelta(0,json_object['expires_in'])
            return True
        except json.decoder.JSONDecodeError:
            self.access_token = None
            print('Invalid JSON response when trying to get access token')
        except KeyError:
            self.access_token = None
            print('OAuth did not contain expected key "access_token" or "expires_in".  Check credentials.')

    def hit_endpoint_for_data(
        self, 
        endpoint: str, 
        offset: int = 0, 
        limit: int = 1000,
        filters: list[str] = [],
        id_list: list[str] = None
    ) -> requests.Response:
        '''
        Handles api calls against either the 'productOffering' or 'category' 
        API (as specified by the 'endpoint' parameter) and returns results between offsets.
        Broke out into it's own method, to be called by get_all_records_from_endpoint().
        
        Includes logic to handle retry on uncaught expired-authentication token
        environemnt-dependent headers, etc.

        Args:
        endpoint (str): 'productOffering' or 'category'.  If not one of these an exception will be raised.

        offset (int):  offset value to be passed to the API.  API has limitations of how 
        many records can be returned so we can use offset and limit to iteratively pull everything down.

        limit (int): limit value to be passed to the API.  See 'offset' parameter notes.
        '''

        if endpoint not in self._valid_endpoints:
            raise Exception('invalid "endpoint" passed in')


        headers = {
            'Accept':'application/json',
            'Accept-Encoding':'gzip, deflate, br',
            'Authorization': f'bearer {self.access_token}',
        }

        # v1 - {URL}?....&filters=+id,channel,description...
        if filters is not None and filters[0][:1] != '+': filters[0] = '+' + filters[0]
        params = {
            'offset': offset,
            'limit': limit,
            'fields': ','.join(filters),
        } 
        
        if endpoint == 'productOfferingPrice': 
            # this endpoint requires ids
            params['@type'] = 'TELUSProductOfferingPriceAlteration'
            params['discount.id'] = ','.join(id_list)
        elif id_list is not None: params['id'] = ','.join(id_list)

        # "NP" environment requires additional http headers.
        # env_dependant_http_headers is set up as part of
        # environment-specific setup in __init__
        headers.update(self.env_dependant_http_headers)

        # we wrap this in a a loop that conditionally repeats just one additional time
        # so that if by chance we hit a just-expired token and get an 'unauthorized'
        # response then we force-get a new token before a final attempt
        for attempt in range(0,2):
            
            target_url = self.base_url + endpoint

            print(f'GET request to "{target_url}"')
            response = self.http_session.get(
                            target_url,
                            params = params,
                            headers = headers,
                            verify = not self.ignore_self_signed_certs,
                            proxies = {
                                'http': '',
                                'https': ''
                            }
                        )
            # if our token expired before we expected it to, get new 
            # access token before trying a second and final time.
            if response.status_code == 401 and attempt == 0:
                self.get_access_token()
                headers.update({'Authorization': f'bearer {self.access_token}'})

            elif response.status_code == 422:
                # if we send an 'offset' that is greater than the available records, we will get
                # this status code. No need to retry, just return the response to be handled downstream.
                return response

            else:
                if response.status_code != 200 and attempt > 0:
                    print(f'(Attempt {attempt + 1})  Response code from TMF-620 - response code "{response.status_code}" - check OAuth credentials.')

                return response


    def load_select_product_offers(
        self,
        ncid_list: list[str],
        file_path = None,
        filters: list[str] = None,
        write_csv: bool = False,
        csv_columns: list[str] = None,
        batch_id: int = None,
    ):
        '''Given a list of ncids, construct a response of product offers from the two endpoints
        productOffering and productOfferingPrice'''
        if file_path is None:
            raise Exception('No file name provided')
        # ensure we have a good access token before making any requests
        self.check_and_refresh_access_token()

        with open(file_path, 'w', newline='', encoding='utf-8') as file_obj:
            csv_writer = csv.writer(file_obj, delimiter=',') if write_csv else None
            # First query productOffering
            # Not using offset/limit if we're giving a list of ncids, also limit doesn't work on v1
            prodOfferingResponse = self.hit_endpoint_for_data (
                'productOffering',
                filters = filters,
                id_list = ncid_list
            )
            if prodOfferingResponse.status_code == 200:
                # Write productOfferings
                prodOfferingJson = json.loads(prodOfferingResponse.text)
                # For NCIDs we didn't get a response for, set up query for
                # promotions
                missing_ids = [ response['id'] for response in prodOfferingJson ]
                missing_ids = [ ncid for ncid in ncid_list if ncid not in missing_ids]
                prodOfferingPriceResponse = self.hit_endpoint_for_data(
                    'productOfferingPrice',
                    filters = filters,
                    id_list = missing_ids
                )
                if prodOfferingPriceResponse.status_code == 200:
                    # Write productOfferings
                    prodOfferingJson = json.loads(prodOfferingResponse.text)
                    prodOfferingPriceJson = json.loads(prodOfferingPriceResponse.text)

                    # Check to see if ncids are accounted for, since both endpoints returned 200, assumption is
                    # if any ncids are not accounted for they have been deleted from product catalog - log them just in case
                    if (len(prodOfferingJson) + len(prodOfferingPriceJson)) != len(ncid_list):
                        found_ncids = [ response['id'] for response in prodOfferingJson if response['id'] in ncid_list ]
                        found_ncids.extend([ response['discount']['id'] for response in prodOfferingPriceJson if response['discount']['id'] in ncid_list])
                        print(f'Some NCIDs stored in our database did not have a response from FIFA PC V1 API:{[ncid for ncid in ncid_list if ncid not in found_ncids]}')
                        utils._sendGChatMessage(f'Some NCIDs stored in our database did not have a response from FIFA PC V1 API:{[ncid for ncid in ncid_list if ncid not in found_ncids]}')
                        # raise Exception('Did not get a response from TMF 620 API for every NCID in our database')
                    
                    if file_obj is not None:
                        if write_csv:
                            self.write_response_list_to_csv(
                                prodOfferingJson, 
                                csv_columns,
                                csv_writer,
                                batch_id = batch_id
                            )
                            self.write_response_list_to_csv(
                                prodOfferingPriceJson,
                                csv_columns,
                                csv_writer,
                                discount_ind = True,
                                batch_id = batch_id
                            )
                else:
                    raise Exception(f'Could not query both endpoints\n'
                                    f'productOffering status code {prodOfferingResponse.status_code}\n'
                                    f'productOfferingPrice status code {prodOfferingPriceResponse.status_code}')
            else:
                raise Exception(f'Could not query product offering endpoint\n'
                                f'productOffering status code {prodOfferingResponse.status_code}\n')

    def write_response_list_to_csv(self, resp_list, csv_columns, csv_writer, discount_ind = False, batch_id = None):
        #Read through every entry in resp_list, and for the csv_columns provided, write those values into a file
        for response in resp_list:
            # Unroll validFor into start and end date
            if 'startDateTime' in csv_columns and response.get('validFor', False):
                startDateTime = response.get('validFor', {}).get('startDateTime', '')
                response['startDateTime'] = startDateTime if startDateTime is not None else ''
            if 'endDateTime' in csv_columns and response.get('validFor', False):
                endDateTime = response.get('validFor', {}).get('endDateTime', '')
                response['endDateTime'] = endDateTime if endDateTime is not None else ''
            
            if discount_ind:
                # For discounts, use the discount id instead of parent id
                response['id'] = response['discount']['id']

            response['batch_id'] = str(batch_id) if batch_id is not None else ''

            csv_writer.writerow([
                response.get(key,'') if isinstance(response.get(key,''),str)
                else json.dumps(response.get(key,''))
                for key in csv_columns
            ])