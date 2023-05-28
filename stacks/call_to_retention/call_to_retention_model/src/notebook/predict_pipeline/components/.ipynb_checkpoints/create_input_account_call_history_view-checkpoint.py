from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="call_to_retention_model_account_call_history_view.yaml".format(SERVICE_TYPE),
)

def create_input_account_call_history_view(view_name: str,
                                        score_date: str,
                                        score_date_delta: str,
                                        project_id: str,
                                        dataset_id: str,
                                        region: str,
                                        resource_bucket: str,
                                        query_path: str,
                                        ):

    from google.cloud import bigquery
    from google.cloud import storage

    def if_tbl_exists(client, table_ref):
        from google.cloud.exceptions import NotFound
        try:
            client.get_table(table_ref)
            return True
        except NotFound:
            return False

    bq_client = bigquery.Client(project=project_id)
    dataset = bq_client.dataset(dataset_id)
    table_ref = dataset.table(view_name)

    # load query from .txt file
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(resource_bucket)
    blob = bucket.get_blob(query_path)
    content = blob.download_as_string()
    content = str(content, 'utf-8')

    if if_tbl_exists(bq_client, table_ref):
        bq_client.delete_table(table_ref)

    # content = open(query_path, 'r').read()

    create_base_feature_set_query = content.format(score_date=score_date,
                                                   score_date_delta=score_date_delta,
                                                   view_name=view_name,
                                                   dataset_id=dataset_id,
                                                   project_id=project_id,
                                                   )
    shared_dataset_ref = bq_client.dataset(dataset_id)
    base_feature_set_view_ref = shared_dataset_ref.table(view_name)
    base_feature_set_view = bigquery.Table(base_feature_set_view_ref)
    base_feature_set_view.view_query = create_base_feature_set_query.format(project_id)
    base_feature_set_view = bq_client.create_table(base_feature_set_view)
