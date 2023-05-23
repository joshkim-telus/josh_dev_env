from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="promo_expiry_list_model_promo_expiry_view.yaml".format(SERVICE_TYPE),
)
def create_input_account_promo_expiry_view(view_name: str,
                                           score_date: str,
                                           dataset_id: str,
                                           project_id: str,
                                           region: str,
                                           resource_bucket: str,
                                           query_path: str
                                           ):

    from google.cloud import bigquery
    from google.cloud import storage

    bq_client = bigquery.Client(project=project_id)
    dataset = bq_client.dataset(dataset_id)
    table_ref = dataset.table(view_name)

    # load query from .txt file
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(resource_bucket)
    blob = bucket.get_blob(query_path)
    content = blob.download_as_string()
    content = str(content, 'utf-8')

    def if_tbl_exists(client, table_ref):
        from google.cloud.exceptions import NotFound
        try:
            client.get_table(table_ref)
            return True
        except NotFound:
            return False

    if if_tbl_exists(bq_client, table_ref):
        bq_client.delete_table(table_ref)

    create_base_feature_set_query = content.format(score_date=score_date)

    create_base_feature_set_query = create_base_feature_set_query.replace('{', '{{')
    create_base_feature_set_query = create_base_feature_set_query.replace('}', '}}')

    shared_dataset_ref = bq_client.dataset(dataset_id)
    base_feature_set_view_ref = shared_dataset_ref.table(view_name)
    base_feature_set_view = bigquery.Table(base_feature_set_view_ref)
    base_feature_set_view.view_query = create_base_feature_set_query.format(project_id)
    base_feature_set_view = bq_client.create_table(base_feature_set_view)
