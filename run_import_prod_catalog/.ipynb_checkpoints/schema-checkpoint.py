import dotenv
import os
import logging

logging.getLogger().setLevel(logging.INFO)
# # Uncomment if running locally, with local database
# if os.getenv('ENV','local') == 'local' and not dotenv.load_dotenv('local.env'):
#     logging.error('Cloud function is being run with ENV == "local", but local.env not found.')
#     raise IOError('Cloud function is being run with ENV == "local", but local.env not found.')

# Uncomment if running locally, with port forwarded np database - remember to update np.env with 
# the DATABASE_PORT you are forwarding to
if os.getenv('ENV','local') == 'local' and not dotenv.load_dotenv('np.env'):
    logging.error('Running locally with port-forwarded np database, but np.env not found.')
    raise IOError('Running locally with port-forwarded np database, but np.env not found.')

config = {
    'env': os.environ['ENV'],
    'gcp': {
        'project_id': os.environ['PROJECT_ID'],
        'bucket_name': os.environ['BUCKET_NAME'],
        'file_name': 'product_offering.csv',
        'secrets': {
            'server_ca': f'projects/{os.environ["PROJECT_ID"]}/secrets/dataflow-server-ca',
            'client_cert': f'projects/{os.environ["PROJECT_ID"]}/secrets/dataflow-client-cert',
            'client_key': f'projects/{os.environ["PROJECT_ID"]}/secrets/dataflow-client-key'
        },
        'endpoint': 'PR_GKE_PRIVATE' if os.environ['ENV'] == 'pr' else 'NP_GKE_PRIVATE'
    },
    'database': {
        'instance_id': os.environ['INSTANCE_ID'],
        'name': os.environ['DATABASE_NAME'],
        'ip': os.environ['DATABASE_IP'],
        'port': os.getenv('DATABASE_PORT', '5432'),
        'db_user': {
            'name': os.environ['PG_USER_NAME'],
            'secret_id': os.environ['PG_USER_SECRET_ID'],
        },
        'ssl_ind': False if os.getenv('SSL_IND') == 'False' else True,
        'schema': {
            't_pcpol': {
                'name': 'product_catalog_product_offering_loader',
                'columns': [
                    'id',
                    'ncid',                          
                    'name_str',
                    'description_str',
                    'attachment_json',
                    'channel_json',
                    'valid_start_ts',
                    'valid_end_ts',
                    'batch_id',
                ]
            }
        }
    },
    'file_system': {
        'name': 'product_offering.csv',
        'path': os.path.join('/tmp/', 'product_offering.csv')
    },
    'kong': {
        'token_url': os.environ['KONG_ACCESS_URL'],
        'client_id': os.environ['CLIENT_ID'],
        'client_secret_id': os.environ['CLIENT_SECRET_ID'],
        'scope': os.environ['SCOPE']
    }
}

# To be passed to FIFA PC API to filter out results to only relevant fields
product_offering_filters = [
    'id',
    'externalId',
    'name',
    'description',
    'category',
    'discount',
    'attachment',
    'channel',
    'validFor',
]

# Order in which the fields in the API will show up in .csv file
product_offering_csv_columns = [
    'id', # legacy from when we supported calling v2 API, and we stored NCID and Merlin ID separately
    'id',
    'name',
    'description',
    'attachment',
    'channel',
    'startDateTime',
    'endDateTime',
    'batch_id',
]

# To be passed to Google Cloud SQL import API so it knows mapping
# between csv columns and SQL table columns
product_offering_table_columns = [
    'id',
    'ncid',                          
    'name_str',
    'description_str',
    'attachment_json',
    'channel_json',
    'valid_start_ts',
    'valid_end_ts',
    'batch_id',
]

# List of NCIDs to ignore when calling TMF 620, as these are used only
# for internal testing purposes and are not actual product offerings.
ignore_ncid_list = [
    '1234567891234567891',
    '1234567891234567892',
    '1234567891234567893',
    '9159660387513725198', # Loaded into our db but not prod catalog
    'CONTROL' # Suffixed on all control offers, do not call TMF 620 API with this
]

# Control offers with their own unique NCID
control_offer_ncid_list = [
    # '9167013198434400000'
]