
from kfp.v2.dsl import (Artifact, Output, Input, HTML, component) 
from typing import NamedTuple    

# Load Custom Model Component: load in most recent version of your model to run batch predictions with 
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-load-model-slim:1.0.0",
    output_component_file="load_ml_model.yaml"
)
def load_ml_model(
    project_id: str, 
    region: str, 
    model_name: str, 
    model: Output[Artifact]
) -> NamedTuple("output", [("model_uri", str)]):

    from google.cloud import aiplatform
    
    # List models with the given display name, order by update time
    models = aiplatform.Model.list(
        filter=f'display_name={model_name}', 
        order_by="update_time",
        location=region)
    
    if not models:
        raise ValueError(f"No model found with display name: {model_name}")
    
    # Get the latest model's resource name
    latest_model = models[-1]
    model_uri = latest_model.resource_name
    
    # Update the model URI and metadata
    model.uri = model_uri
    model.metadata['resourceName'] = model_uri
    env_var = latest_model.to_dict()['containerSpec']['env'][1]['value']

    # Return the model URI as part of the output
    return (env_var,)


# # this component returns a model artifact which will be passed to Batch Predictions 
# def load_ml_model(
#     project_id: str, 
#     region: str, 
#     model_name: str, 
#     model: Output[Artifact]
#     )-> NamedTuple("output", [("model_uri", str)]):
    
#     from google.cloud import aiplatform
    
#     model_uid = aiplatform.Model.list(
#         filter=f'display_name={model_name}', 
#         order_by="update_time",
#         location=region)[-1].resource_name
    
#     model.uri = model_uid
#     model.metadata['resourceName'] = model_uid

#     return (model.environment_variables["model_uri"])
    
# from typing import NamedTuple
# from google.cloud import aiplatform
# from pulumi import Output, Artifact
