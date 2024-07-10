# import required libraries
import kfp
from kfp import dsl
from kfp.v2.dsl import (Model, Input, Output, component)
from typing import NamedTuple

# Component for uploading model to Vertex Model Registry
@component(
# Uploads model
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="model-upload.yaml",
)

def upload_model(
    project_id: str,
    model: Input[Model],
    vertex_model: Output[Model],
    region: str,
    model_name: str,
    prediction_image: str,
    col_list: list, 
    model_uri: str
    )-> NamedTuple("output", [("model_uri", str)]):
    """
    Upload model to Vertex Model Registry.
    Args:
        project_id (str): project id for where this pipeline is being run
        model (Input[Model]): model passed in from training component. Must have path specified in model.uri
        region (str): region for where the query will be run
        model_name (str): name of model to be stored
        prediction_image (str): prediction image uri
        col_list (str): string of list of columns in serving data
    Returns:
        vertex_model (Output[Model]): Model saved in Vertex AI
    """

    from google.cloud import aiplatform
    from google.api_core.future.polling import DEFAULT_POLLING
    import os

    ### Set the default timeoutto 3600 seconds
    DEFAULT_POLLING._timeout = 3000
    
    aiplatform.init(project=project_id, location=region)

    ## check if prediction image is custom or not
    if prediction_image.startswith('northamerica-northeast1-docker'):
        # custom: must set ports
        health_route = "/ping"
        predict_route = "/predict"
        serving_container_ports = [7080]
    else:
        # Google pre-built
        health_route = None
        predict_route = None
        serving_container_ports = None

    ## check for existing models
    # if model exists, update the version
    try:
        model_uid = aiplatform.Model.list(
            filter=f'display_name={model_name}', 
            order_by="update_time",
            location=region)[-1].resource_name

        uploaded_model = aiplatform.Model.upload(
            display_name = model_name, 
            artifact_uri = os.path.dirname(model.uri),
            serving_container_image_uri = prediction_image,
            serving_container_predict_route=predict_route,
            serving_container_health_route=health_route,
            serving_container_ports=serving_container_ports,
            serving_container_environment_variables =  {"COL_LIST":str(col_list), "model_uri": model_uri},
            parent_model = model_uid,
            is_default_version = True, 
            upload_request_timeout = 3000
        )
    # if model does not already exist, create a new model
    except:
        uploaded_model = aiplatform.Model.upload(
            display_name = model_name,
            artifact_uri = os.path.dirname(model.uri),
            serving_container_image_uri=prediction_image,
            serving_container_predict_route=predict_route,
            serving_container_health_route=health_route,
            serving_container_ports=serving_container_ports,
            serving_container_environment_variables =  {"COL_LIST":str(col_list), "model_uri": model_uri},
            upload_request_timeout = 3000

        )

    vertex_model.uri = uploaded_model.resource_name
    
    return (vertex_model.uri,)
