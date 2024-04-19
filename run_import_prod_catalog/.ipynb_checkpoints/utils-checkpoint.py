from datetime import datetime
import logging
import time

from google.cloud import storage, secretmanager
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

logging.basicConfig(level=logging.INFO)

def upload_file_to_gcs(bucket_name: str, gcs_file_name: str, file_path: str):
    '''
        Uploads a local file at file_path to a GCS bucket at bucket_name with the name gcs_file_name.
        Project ID is the default resolved by Application Default Credentials.
    '''
    try:
        logging.info("Writing file %s to %s/%s", file_path, bucket_name, gcs_file_name)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(gcs_file_name)
        blob.upload_from_filename(file_path)
        # return the file handler
        return f'gs://{bucket_name}/{gcs_file_name}'

    except Exception:
        logging.error("Could not upload %s to %s.", file_path, gcs_file_name)
        raise

def _waitForOperation(service, operationName, projectId):
  done = False

  while not done:
      request = service.operations().get(
        project=projectId,
        operation=operationName
      )
      response = request.execute()

      print(response)
      logging.info(response)

      done = (response['status'] == 'DONE')

      if not done:
          time.sleep(1)
      else:
          # If we are done, see if the operation finished with errors and raise those errors
          if 'error' in response:
              message = 'Some errors were detected while processing the operation:'
              
              for error in response['error']['errors']:
                  message = message + ' --- ' + error['message']

              raise Exception(message)

def _waitForPreviousOperation(service, projectId, instanceId):
  MAX_WAITS = 5
  # loop to ensure that if multiple jobs are waiting on the same
  # previous operation to finish, they don't all start at the same
  # time and instead will wait for each other
  for i in range(MAX_WAITS):
    # acquire most recent operation for this project & db instance
    request = service.operations().list(
      project = projectId,
      instance = instanceId,
      maxResults = 1
    )
    logging.info('Acquiring most recent operation')
    response = request.execute()

    logging.info(response)
    # if there have been no operations, return
    if len(response['items']) == 0:
      return

    # if previous operation has already completed, return
    status = response['items'][0]['status']
    if status not in ['PENDING', 'RUNNING']:
      return
    
    # otherwise, wait for it to finish
    logging.info('Waiting for previous operation')
    operationName = response['items'][0]['name']
    try:
      _waitForOperation(service, operationName, projectId)
    except Exception as e:
      logging.info('Previous SQL import failed.')
  raise Exception(f'Too many imports queued up and wait limit of {MAX_WAITS} exceeded.')

def insertCSVIntoCloudSQL(filePath, databaseName, table, columns, projectId, instanceId):

  credentials = GoogleCredentials.get_application_default()
  service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)
  
  _waitForPreviousOperation(service, projectId, instanceId)

  instances_import_request_body = {
      "importContext": {
          "kind": "sql#importContext",
          "fileType": "csv",
          "uri": filePath,
          "database": databaseName,
          "csvImportOptions": {
              "table": table,
              "columns": columns
          }
      }
  }

  logging.info(f'Performing import of {filePath} into cloud SQL ({instanceId}.offer_targeting)')
  
  request = service.instances().import_(project=projectId, instance=instanceId, body=instances_import_request_body)
  response = request.execute()

  logging.info(response)

  _waitForOperation(service, response['name'], projectId)

  logging.info('Finished the Cloud SQL import')

# Acquire a secret value from Google Secret Manager
def getSecret(secretId: str):
    try:
        logging.info(f'Attempting to acquire secret value at {secretId}...')
        secret_client = secretmanager.SecretManagerServiceClient()

        latest_version = f"{secretId}/versions/latest"

        response = secret_client.access_secret_version(request={"name": latest_version})
        payload = response.payload.data.decode("UTF-8")

        logging.info('Secret value successfully acquired.')
        return payload
    except Exception as e:
        logging.info(f'Could not acquire secret value at {secretId}')
        raise
    
# Write to GChat, currently using a throwaway gchat key just to ensure it works
def _sendGChatMessage(message):
  import json
  from httplib2 import Http
  # Throwaway gchat workspace right now, key will be replaced with secret key to actual workspace in future
  url = 'https://chat.googleapis.com/v1/spaces/AAAAeVf_TyQ/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=WGtQAiJsbK8GfLiR_tC3jWBg2bPeadZ73zROAAXRHmU%3D'
  now = datetime.now()
  bot_message = {
      'text' : f'The Offer Targeting Product Catalog cache process has thrown an error at {now}\n\n{message}'
  }

  message_headers = { 'Content-Type': 'application/json; charset=UTF-8'}

  http_obj = Http()

  response = http_obj.request(
      uri=url,
      method='POST',
      headers=message_headers,
      body=json.dumps(bot_message),
  )