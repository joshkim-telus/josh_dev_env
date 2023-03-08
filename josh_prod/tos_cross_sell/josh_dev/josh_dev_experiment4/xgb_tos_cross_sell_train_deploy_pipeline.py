def main(mapping):
    print(mapping)
    from kfp import dsl
    from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output, OutputPath, ClassificationMetrics,
                            Metrics, component)
    import os
    import re
    import google
    from google.oauth2 import credentials
    from google.oauth2 import service_account
    from google.oauth2.service_account import Credentials
    from datetime import date
    from datetime import timedelta
    from dateutil.relativedelta import relativedelta

    SERVICE_TYPE = 'tos_cross_sell'
    DATASET_ID = '{}_dataset'.format(SERVICE_TYPE)
    PROJECT_ID = mapping['PROJECT_ID']
    RESOURCE_BUCKET = mapping['resources_bucket']
    FILE_BUCKET = mapping['gcs_csv_bucket']
    REGION = mapping['REGION']
    MODEL_ID = '5060'
    FOLDER_NAME = 'xgb_{}_{}_train_deploy'.format(SERVICE_TYPE, MODEL_ID)
    QUERIES_PATH = 'vertex_pipelines/' + FOLDER_NAME + '/queries/'

    scoringDate = date(2022, 9, 1)  # date.today() - relativedelta(days=2)- relativedelta(months=30)
    valScoringDate = date(2022, 10, 1)  # scoringDate - relativedelta(days=2)

    # training views
    CONSL_VIEW_NAME = '{}_pipeline_consl_data_training_bi_layer'.format(SERVICE_TYPE)  # done
    FFH_BILLING_VIEW_NAME = '{}_pipeline_ffh_billing_data_training_bi_layer'.format(SERVICE_TYPE)  # done
    HS_USAGE_VIEW_NAME = '{}_pipeline_hs_usage_data_training_bi_layer'.format(SERVICE_TYPE)  # done
    DEMO_INCOME_VIEW_NAME = '{}_pipeline_demo_income_data_training_bi_layer'.format(SERVICE_TYPE)  # done
    PROMO_EXPIRY_VIEW_NAME = '{}_pipeline_promo_expiry_data_training_bi_layer'.format(SERVICE_TYPE)  # done
    GPON_COPPER_VIEW_NAME = '{}_pipeline_gpon_copper_data_training_bi_layer'.format(SERVICE_TYPE)  # done
    CLCKSTRM_TELUS_VIEW_NAME = '{}_pipeline_clckstrm_telus_training_bi_layer'.format(SERVICE_TYPE)
    ALARMDOTCOM_APP_USAGE_VIEW_NAME = '{}_pipeline_alarmdotcom_app_usage_training_bi_layer'.format(SERVICE_TYPE)
    TOS_ACTIVE_BANS_VIEW_NAME = '{}_pipeline_tos_active_bans_training_bi_layer'.format(SERVICE_TYPE) 

    # validation views
    CONSL_VIEW_VALIDATION_NAME = '{}_pipeline_consl_data_validation_bi_layer'.format(SERVICE_TYPE)
    FFH_BILLING_VIEW_VALIDATION_NAME = '{}_pipeline_ffh_billing_data_validation_bi_layer'.format(SERVICE_TYPE)
    HS_USAGE_VIEW_VALIDATION_NAME = '{}_pipeline_hs_usage_data_validation_bi_layer'.format(SERVICE_TYPE)
    DEMO_INCOME_VIEW_VALIDATION_NAME = '{}_pipeline_demo_income_data_validation_bi_layer'.format(SERVICE_TYPE)
    PROMO_EXPIRY_VIEW_VALIDATION_NAME = '{}_pipeline_promo_expiry_data_validation_bi_layer'.format(SERVICE_TYPE)
    GPON_COPPER_VIEW_VALIDATION_NAME = '{}_pipeline_gpon_copper_data_validation_bi_layer'.format(SERVICE_TYPE)
    CLCKSTRM_TELUS_VIEW_VALIDATION_NAME = '{}_pipeline_clckstrm_telus_validation_bi_layer'.format(SERVICE_TYPE)
    ALARMDOTCOM_APP_USAGE_VIEW_VALIDATION_NAME = '{}_pipeline_alarmdotcom_app_usage_validation_bi_layer'.format(SERVICE_TYPE)
    TOS_ACTIVE_BANS_VALIDATION_VIEW_NAME = '{}_pipeline_tos_active_bans_validation_bi_layer'.format(SERVICE_TYPE) 

    # training dates
    SCORE_DATE = scoringDate.strftime('%Y%m%d')  # date.today().strftime('%Y%m%d')
    SCORE_DATE_DASH = scoringDate.strftime('%Y-%m-%d')
    SCORE_DATE_MINUS_6_MOS_DASH = ((scoringDate - relativedelta(months=6)).replace(day=1)).strftime('%Y-%m-%d')
    SCORE_DATE_LAST_MONTH_START_DASH = (scoringDate.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
    SCORE_DATE_LAST_MONTH_END_DASH = ((scoringDate.replace(day=1)) - timedelta(days=1)).strftime('%Y-%m-%d')
    SCORE_DATE_LAST_MONTH_YEAR = ((scoringDate.replace(day=1)) - timedelta(days=1)).year
    SCORE_DATE_LAST_MONTH_MONTH = ((scoringDate.replace(day=1)) - timedelta(days=1)).month

    # validation dates
    SCORE_DATE_VAL = valScoringDate.strftime('%Y%m%d')
    SCORE_DATE_VAL_DASH = valScoringDate.strftime('%Y-%m-%d')
    SCORE_DATE_VAL_MINUS_6_MOS_DASH = ((valScoringDate - relativedelta(months=6)).replace(day=1)).strftime('%Y-%m-%d')
    SCORE_DATE_VAL_LAST_MONTH_START_DASH = (valScoringDate.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
    SCORE_DATE_VAL_LAST_MONTH_END_DASH = ((valScoringDate.replace(day=1)) - timedelta(days=1)).strftime('%Y-%m-%d')
    SCORE_DATE_VAL_LAST_MONTH_YEAR = ((valScoringDate.replace(day=1)) - timedelta(days=1)).year
    SCORE_DATE_VAL_LAST_MONTH_MONTH = ((valScoringDate.replace(day=1)) - timedelta(days=1)).month

    SCORE_DATE_DELTA = 0
    SCORE_DATE_VAL_DELTA = 0
    TICKET_DATE_WINDOW = 30  # Days of ticket data to be queried

    ACCOUNT_CONSL_QUERY_PATH = QUERIES_PATH + 'create_input_account_consl_query.txt'
    ACCOUNT_GPON_COPPER_QUERY_PATH = QUERIES_PATH + 'create_input_account_gpon_copper_query.txt'
    ACCOUNT_PROMO_EXPIRY_QUERY_PATH = QUERIES_PATH + 'create_input_account_promo_expiry_query.txt'
    ACCOUNT_DEMO_INCOME_QUERY_PATH = QUERIES_PATH + 'create_input_account_demo_income_query.txt'
    ACCOUNT_HS_USAGE_QUERY_PATH = QUERIES_PATH + 'create_input_account_hs_usage_query.txt'
    ACCOUNT_FFH_BILLING_QUERY_PATH = QUERIES_PATH + 'create_input_account_ffh_billing_query.txt'
    ACCOUNT_CLCKSTRM_TELUS_QUERY_PATH = QUERIES_PATH + 'create_input_account_clckstrm_telus_query.txt'
    ACCOUNT_ALARMDOTCOM_APP_USAGE_QUERY_PATH = QUERIES_PATH + 'create_input_account_alarmdotcom_app_usage_query.txt'
    ACCOUNT_TOS_ACTIVE_BANS_QUERY_PATH = QUERIES_PATH + 'create_input_account_tos_active_bans_query.txt'

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
        output_component_file="{}_model_account_consl_view.yaml".format(SERVICE_TYPE),
    )
    def create_input_account_consl_view(view_name: str,
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

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
        output_component_file="{}_model_ffh_billing_view.yaml".format(SERVICE_TYPE),
    )
    def create_input_account_ffh_billing_view(view_name: str,
                                                  v_report_date: str,
                                                  v_start_date: str,
                                                  v_end_date: str,
                                                  v_bill_year: str,
                                                  v_bill_month: str,
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

        create_base_feature_set_query = content.format(v_report_date=v_report_date,
                                                       v_start_date=v_start_date,
                                                       v_end_date=v_end_date,
                                                       v_bill_year=v_bill_year,
                                                       v_bill_month=v_bill_month,
                                                       )

        shared_dataset_ref = bq_client.dataset(dataset_id)
        base_feature_set_view_ref = shared_dataset_ref.table(view_name)
        base_feature_set_view = bigquery.Table(base_feature_set_view_ref)
        base_feature_set_view.view_query = create_base_feature_set_query.format(project_id)
        base_feature_set_view = bq_client.create_table(base_feature_set_view)

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
        output_component_file="{}_model_hs_usage_view.yaml".format(SERVICE_TYPE),
    )
    def create_input_account_hs_usage_view(view_name: str,
                                           v_report_date: str,
                                           v_start_date: str,
                                           v_end_date: str,
                                           v_bill_year: str,
                                           v_bill_month: str,
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

        create_base_feature_set_query = content.format(v_report_date=v_report_date,
                                                       v_start_date=v_start_date,
                                                       v_end_date=v_end_date,
                                                       v_bill_year=v_bill_year,
                                                       v_bill_month=v_bill_month,
                                                       )

        shared_dataset_ref = bq_client.dataset(dataset_id)
        base_feature_set_view_ref = shared_dataset_ref.table(view_name)
        base_feature_set_view = bigquery.Table(base_feature_set_view_ref)
        base_feature_set_view.view_query = create_base_feature_set_query.format(project_id)
        base_feature_set_view = bq_client.create_table(base_feature_set_view)

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
        output_component_file="{}_model_demo_income_view.yaml".format(SERVICE_TYPE),
    )
    def create_input_account_demo_income_view(view_name: str,
                                              score_date: str,
                                              score_date_delta: str,
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

        create_base_feature_set_query = content.format(score_date=score_date,
                                                       score_date_delta=score_date_delta,
                                                       project_id=project_id,
                                                       dataset_id='common_dataset',
                                                       )

        shared_dataset_ref = bq_client.dataset(dataset_id)
        base_feature_set_view_ref = shared_dataset_ref.table(view_name)
        base_feature_set_view = bigquery.Table(base_feature_set_view_ref)
        base_feature_set_view.view_query = create_base_feature_set_query.format(project_id)
        base_feature_set_view = bq_client.create_table(base_feature_set_view)

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
        output_component_file="{}_model_promo_expiry_view.yaml".format(SERVICE_TYPE),
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

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
        output_component_file="{}_model_gpon_copper_view.yaml".format(SERVICE_TYPE),
    )
    def create_input_account_gpon_copper_view(view_name: str,
                                              score_date: str,
                                              score_date_delta: str,
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

        create_base_feature_set_query = content.format(score_date=score_date,
                                                       score_date_delta=score_date_delta,
                                                       )

        shared_dataset_ref = bq_client.dataset(dataset_id)
        base_feature_set_view_ref = shared_dataset_ref.table(view_name)
        base_feature_set_view = bigquery.Table(base_feature_set_view_ref)
        base_feature_set_view.view_query = create_base_feature_set_query.format(project_id)
        base_feature_set_view = bq_client.create_table(base_feature_set_view)

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
        output_component_file="{}_model_clckstrm_telus_view.yaml".format(SERVICE_TYPE),
    )
    def create_input_account_clckstrm_telus_view(view_name: str,
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

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
        output_component_file="{}_model_alarmdotcom_app_usage_view.yaml".format(SERVICE_TYPE),
    )
    def create_input_account_alarmdotcom_app_usage_view(view_name: str,
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

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
        output_component_file="{}_model_tos_active_bans_view.yaml".format(SERVICE_TYPE),
    )
    def create_input_account_tos_active_bans_view(view_name: str,
                                        score_date: str,
                                        score_date_delta: str,
                                        v_start_date: str,
                                        v_end_date: str,
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
                                                       v_start_date=v_start_date,
                                                       v_end_date=v_end_date,
                                                       view_name=view_name,
                                                       dataset_id=dataset_id,
                                                       project_id=project_id,
                                                       )
        shared_dataset_ref = bq_client.dataset(dataset_id)
        base_feature_set_view_ref = shared_dataset_ref.table(view_name)
        base_feature_set_view = bigquery.Table(base_feature_set_view_ref)
        base_feature_set_view.view_query = create_base_feature_set_query.format(project_id)
        base_feature_set_view = bq_client.create_table(base_feature_set_view)

    #---------------------------------------------------------------------------------------------------------------------------

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
        output_component_file="{}_model_preprocess.yaml".format(SERVICE_TYPE),
    )
    def preprocess(
            account_consl_view: str,
            account_bill_view: str,
            hs_usage_view: str,
            demo_income_view: str,
            promo_expiry_view: str,
            gpon_copper_view: str,
            clckstrm_telus_view: str, 
            alarmdotcom_app_usage_view: str, 
            tos_active_bans_view: str, 
            save_data_path: str,
            project_id: str,
            dataset_id: str
    ):
        
        from google.cloud import bigquery
        import pandas as pd
        import gc
        import time

        client = bigquery.Client(project=project_id)
        consl_data_set = f"{project_id}.{dataset_id}.{account_consl_view}" 

        build_df_consl = '''SELECT * FROM `{consl_data_set}`'''.format(consl_data_set=consl_data_set)
        df_consl = client.query(build_df_consl).to_dataframe()
        print('......base data done')

        # product mix
        df_mix = df_consl[[
            'ban',
            'product_mix_all',
            'sing_count',
            'hsic_count',
            'mob_count',
            'shs_count',
            'ttv_count',
            'stv_count',
            'diic_count',
            'new_c_ind',
            'new_sing_ind',
            'new_hsic_ind',
            'new_ttv_ind',
            'new_smhm_ind',
            'mnh_ind',
        ]]
        df_mix = df_mix.drop_duplicates(subset=['ban']).set_index('ban').add_prefix('productMix_')

        # df_join
        df_join = df_mix.fillna(0)

        del df_mix
        gc.collect()
        print('......product mix done')

        bill_data_set = f"{project_id}.{dataset_id}.{account_bill_view}" 
        build_df_bill = '''SELECT * FROM `{bill_data_set}`'''.format(bill_data_set=bill_data_set)

        client = bigquery.Client(project=project_id)
        df_bill = client.query(build_df_bill).to_dataframe() 

        df_bill = df_bill.set_index('ban').add_prefix('ffhBill_')

        # df_join
        df_join = df_join.join(df_bill).fillna(0) 
        del df_bill
        gc.collect()
        print('......account bill done')

        hs_usage_data_set = f"{project_id}.{dataset_id}.{hs_usage_view}" 
        build_df_hs_usage = '''SELECT * FROM `{hs_usage_data_set}`'''.format(hs_usage_data_set=hs_usage_data_set)

        client = bigquery.Client(project=project_id)
        df_hs_usage = client.query(build_df_hs_usage).to_dataframe() 

        df_hs_usage = df_hs_usage.set_index('ban').add_prefix('hsiaUsage_')

        # df_join
        df_join = df_join.join(df_hs_usage).fillna(0) 
        del df_hs_usage
        gc.collect()
        print('......hs usage done')

        demo_income_data_set = f"{project_id}.{dataset_id}.{demo_income_view}" 
        build_df_demo_income = '''SELECT * FROM `{demo_income_data_set}`'''.format(
            demo_income_data_set=demo_income_data_set)

        client = bigquery.Client(project=project_id)
        df_income = client.query(build_df_demo_income).to_dataframe()

        df_income = df_income.set_index('ban')
        df_income['demo_urban_flag'] = df_income.demo_sgname.str.lower().str.contains('urban').fillna(0).astype(int)
        df_income['demo_rural_flag'] = df_income.demo_sgname.str.lower().str.contains('rural').fillna(0).astype(int)
        df_income['demo_family_flag'] = df_income.demo_lsname.str.lower().str.contains('families').fillna(0).astype(int)
        df_income_dummies = pd.get_dummies(df_income[['demo_lsname']])
        df_income_dummies.columns = df_income_dummies.columns.str.replace('&', 'and')
        df_income_dummies.columns = df_income_dummies.columns.str.replace(' ', '_')
        df_income = df_income[['demo_avg_income', 'demo_urban_flag', 'demo_rural_flag', 'demo_family_flag']].join(
            df_income_dummies)
        df_income.demo_avg_income = df_income.demo_avg_income.astype(float)
        df_income.demo_avg_income = df_income.demo_avg_income.fillna(df_income.demo_avg_income.median())
        df_group_income = df_income.groupby('ban').agg('mean')
        df_group_income = df_group_income.add_prefix('demographics_')
        # df_join
        df_join = df_join.join(df_group_income.fillna(df_group_income.median()))

        del df_group_income
        del df_income
        gc.collect()
        print('......income done')

        promo_expiry_data_set = f"{project_id}.{dataset_id}.{promo_expiry_view}" 
        build_df_promo = '''SELECT * FROM `{promo_expiry_data_set}` '''.format(
            promo_expiry_data_set=promo_expiry_data_set)

        client = bigquery.Client(project=project_id)
        df_promo = client.query(build_df_promo).to_dataframe()  

        df_promo = df_promo.set_index('ban')
        disc_cols = [col for col in df_promo.columns if 'disc' in col]
        bill_cols = [col for col in df_promo.columns if 'disc' not in col]

        df_join = df_join.join(df_promo[disc_cols].add_prefix('promo_'))
        df_join = df_join.join(df_promo[bill_cols].add_prefix('ffhBill_')).fillna(0)

        del df_promo
        gc.collect()
        print('......promo expiry done')

        # gpon copper
        gpon_copper_data_set = f"{project_id}.{dataset_id}.{gpon_copper_view}"
        build_df_gpon_copper = '''
        SELECT * FROM `{gpon_copper_data_set}` 
        '''.format(gpon_copper_data_set=gpon_copper_data_set)

        client = bigquery.Client(project=project_id)
        df_gpon_copper = client.query(build_df_gpon_copper).to_dataframe()

        df_gpon_copper = df_gpon_copper.set_index('ban')
        df_join = df_join.join(df_gpon_copper.add_prefix('infra_')).fillna(0)
        del df_gpon_copper
        gc.collect()
        print('......gpon copper done')

        # clickstream data
        clckstrm_telus_data_set = f"{project_id}.{dataset_id}.{clckstrm_telus_view}" 
        build_df_clckstrm_telus = '''SELECT * FROM `{clckstrm_telus_data_set}`'''.format(clckstrm_telus_data_set=clckstrm_telus_data_set)

        client = bigquery.Client(project=project_id)
        df_clckstrm_telus = client.query(build_df_clckstrm_telus).to_dataframe() 

        df_clckstrm_telus = df_clckstrm_telus.set_index('ban').add_prefix('clckstrmData_')

        # df_join
        df_join = df_join.join(df_clckstrm_telus).fillna(0) 
        del df_clckstrm_telus
        gc.collect()
        print('......clcktsrm data done')

        # alarm.com data
        alarmdotcom_app_usage_data_set = f"{project_id}.{dataset_id}.{alarmdotcom_app_usage_view}" 
        build_df_alarmdotcom_app_usage = '''SELECT * FROM `{alarmdotcom_app_usage_data_set}`'''.format(alarmdotcom_app_usage_data_set=alarmdotcom_app_usage_data_set)

        client = bigquery.Client(project=project_id)
        df_alarmdotcom_app_usage = client.query(build_df_alarmdotcom_app_usage).to_dataframe() 

        df_alarmdotcom_app_usage = df_alarmdotcom_app_usage.set_index('ban').add_prefix('alarmdotcomAppUsage_')

        # df_join
        df_join = df_join.join(df_alarmdotcom_app_usage).fillna(0) 
        del df_alarmdotcom_app_usage
        gc.collect()
        print('......alarm.com app usage data done')

        # tos active bans
        tos_active_bans = f"{project_id}.{dataset_id}.{tos_active_bans_view}" 
        build_df_tos_active_data = '''SELECT * FROM `{tos_active_bans}`'''.format(tos_active_bans=tos_active_bans)

        client = bigquery.Client(project=project_id)
        df_tos_active_data = client.query(build_df_tos_active_data).to_dataframe()  # Make an API request.

        # df_join
        df_tos_active_data = df_tos_active_data.set_index('ban')
        df_join = df_join.join(df_tos_active_data).fillna(0)
        del df_tos_active_data
        gc.collect()
        print('......tos active data done')

        df_join.columns = df_join.columns.str.replace(' ', '_')
        df_join.columns = df_join.columns.str.replace('-', '_')

        df_final = df_join.copy()
        del df_join
        gc.collect()
        print('......df final done')

        for f in df_final.columns:
            df_final[f] = list(df_final[f])

        df_final = df_final.loc[(df_final['tos_ind'] == 0) & (df_final['productMix_new_smhm_ind'] == 0)].reset_index()
        df_final = df_final.drop(['tos_ind', 'productMix_new_smhm_ind'], axis=1) 

        df_final.to_csv(save_data_path, index=False, compression='gzip') 
        del df_final
        gc.collect()
        print(f'......csv saved in {save_data_path}')
        time.sleep(300)

    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-xgboost-slim:latest",
        output_component_file="xgb_train_model.yaml",
    )
    def train_and_save_model(
            file_bucket: str,
            service_type: str,
            score_date_dash: str,
            score_date_val_dash: str,
            project_id: str,
            dataset_id: str,
            metrics: Output[Metrics],
            metricsc: Output[ClassificationMetrics],
    ):

        import gc
        import time
        import pandas as pd
        import numpy as np
        import pickle
        from google.cloud import storage
        from google.cloud import bigquery
        from sklearn.model_selection import train_test_split

        def get_lift(prob, y_test, q):
            result = pd.DataFrame(columns=['Prob', 'CrossSell'])
            result['Prob'] = prob
            result['CrossSell'] = y_test
            result['Decile'] = pd.qcut(result['Prob'], q, labels=[i for i in range(q, 0, -1)])
            add = pd.DataFrame(result.groupby('Decile')['CrossSell'].mean()).reset_index()
            add.columns = ['Decile', 'avg_real_cross_sell_rate']
            result = result.merge(add, on='Decile', how='left')
            result.sort_values('Decile', ascending=True, inplace=True)
            lg = pd.DataFrame(result.groupby('Decile')['Prob'].mean()).reset_index()
            lg.columns = ['Decile', 'avg_model_pred_cross_sell_rate']
            lg.sort_values('Decile', ascending=False, inplace=True)
            lg['avg_cross_sell_rate_total'] = result['CrossSell'].mean()
            lg = lg.merge(add, on='Decile', how='left')
            lg['lift'] = lg['avg_real_cross_sell_rate'] / lg['avg_cross_sell_rate_total']

            return lg

        df_train = pd.read_csv('gs://{}/{}/{}_train.csv.gz'.format(file_bucket, service_type, service_type),
                               compression='gzip')  
        df_test = pd.read_csv('gs://{}/{}/{}_validation.csv.gz'.format(file_bucket, service_type, service_type),  
                              compression='gzip')

        #set up df_train
        client = bigquery.Client(project=project_id)
        sql_train = ''' SELECT * FROM `{}.{}.bq_tos_cross_sell_targets` '''.format(project_id, dataset_id) 
        df_target_train = client.query(sql_train).to_dataframe()
        df_target_train = df_target_train.loc[
            df_target_train['YEAR_MONTH'] == '-'.join(score_date_dash.split('-')[:2])]  # score_date_dash = '2022-08-31'

        df_target_train['ban'] = df_target_train['ban'].astype('int64')
        df_target_train = df_target_train.groupby('ban').tail(1)

        df_train = df_train.merge(df_target_train[['ban', 'product_acq_ind']], on='ban', how='left')
        df_train.rename(columns={'product_acq_ind': 'target'}, inplace=True)
        df_train.dropna(subset=['target'], inplace=True)
        df_train['target'] = df_train['target'].astype(int)

        #set up df_test
        client = get_gcp_bqclient(project_id)
        sql_test = ''' SELECT * FROM `{}.{}.bq_tos_cross_sell_targets` '''.format(project_id, dataset_id) 
        df_target_test = client.query(sql_test).to_dataframe()
        df_target_test = df_target_test.loc[
            df_target_test['YEAR_MONTH'] == '-'.join(score_date_val_dash.split('-')[:2])]  # score_date_dash = '2022-09-30'

        #set up df_train and df_test (add 'target')
        df_target_test['ban'] = df_target_test['ban'].astype('int64')
        df_target_test = df_target_test.groupby('ban').tail(1)

        df_test = df_test.merge(df_target_test[['ban', 'product_acq_ind']], on='ban', how='left')
        df_test.rename(columns={'product_acq_ind': 'target'}, inplace=True)
        df_test.dropna(subset=['target'], inplace=True)
        df_test['target'] = df_test['target'].astype(int)

        #set up features (list)
        cols_1 = df_train.columns.values
        cols_2 = df_test.columns.values
        cols = set(cols_1).intersection(set(cols_2))
        features = [f for f in cols if f not in ['ban', 'target']]

        #train test split
        df_train, df_val = train_test_split(df_train, shuffle=True, test_size=0.2, random_state=42,
                                            stratify=df_train['target']
                                            )

        ban_train = df_train['ban']
        X_train = df_train[features]
        y_train = np.squeeze(df_train['target'].values)

        ban_val = df_val['ban']
        X_val = df_val[features]
        y_val = np.squeeze(df_val['target'].values)

        ban_test = df_test['ban']
        X_test = df_test[features]
        y_test = np.squeeze(df_test['target'].values)

        del df_train, df_val, df_test
        gc.collect()

        # build model and fit in training data
        import xgboost as xgb
        from sklearn.metrics import roc_auc_score

        xgb_model = xgb.XGBClassifier(
            learning_rate=0.01,
            n_estimators=100,
            max_depth=8,
            min_child_weight=1,
            gamma=0,
            subsample=0.8,
            colsample_bytree=0.8,
            objective='binary:logistic',
            nthread=4,
            scale_pos_weight=1
            # seed=27
        )

        xgb_model.fit(X_train, y_train)
        print('xgb training done')

        from sklearn.preprocessing import normalize

        #predictions on X_val
        y_pred = xgb_model.predict_proba(X_val, ntree_limit=xgb_model.best_iteration)[:, 1]
        y_pred_label = (y_pred > 0.5).astype(int)
        auc = roc_auc_score(y_val, y_pred_label)
        metrics.log_metric("AUC", auc)

        pred_prb = xgb_model.predict_proba(X_test, ntree_limit=xgb_model.best_iteration)[:, 1]
        lg = get_lift(pred_prb, y_test, 10)
        lg.to_csv('gs://{}/{}/lift_on_scoring_data_{}.csv'.format(file_bucket, service_type, create_time, index=False))

        # save the model in GCS
        from datetime import datetime
        models_dict = {}
        create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        models_dict['create_time'] = create_time
        models_dict['model'] = xgb_model
        models_dict['features'] = features

        with open('model_dict.pkl', 'wb') as handle:
            pickle.dump(models_dict, handle)
        handle.close()

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(file_bucket)

        MODEL_PATH = '{}_xgb_models/'.format(service_type)
        blob = bucket.blob(MODEL_PATH)
        if not blob.exists(storage_client):
            blob.upload_from_string('')

        model_name_onbkt = '{}{}_models_xgb_{}'.format(MODEL_PATH, service_type, models_dict['create_time'])
        blob = bucket.blob(model_name_onbkt)
        blob.upload_from_filename('model_dict.pkl')

        print(f"....model loaded to GCS done at {str(create_time)}")

        time.sleep(300)
        
    #---------------------------------------------------------------------------------------------------------------------------

    @dsl.pipeline(
        # A name for the pipeline.
        name="{}-xgb-pipeline".format(SERVICE_TYPE),
        description=' pipeline for training {} model'.format(SERVICE_TYPE)
    )
    def pipeline(
            project_id: str = PROJECT_ID,
            region: str = REGION,
            resource_bucket: str = RESOURCE_BUCKET,
            file_bucket: str = FILE_BUCKET
    ):
        # ------------- train view ops ---------------
        create_input_account_consl_train_view_op = create_input_account_consl_view(
            view_name=CONSL_VIEW_NAME,
            score_date=SCORE_DATE,
            score_date_delta=SCORE_DATE_DELTA,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_CONSL_QUERY_PATH,
        )
        create_input_account_consl_train_view_op.set_memory_limit('16G')
        create_input_account_consl_train_view_op.set_cpu_limit('4')

        create_input_account_ffh_billing_train_view_op = create_input_account_ffh_billing_view(
            v_report_date=SCORE_DATE_DASH,
            v_start_date=SCORE_DATE_MINUS_6_MOS_DASH,
            v_end_date=SCORE_DATE_LAST_MONTH_END_DASH,
            v_bill_year=SCORE_DATE_LAST_MONTH_YEAR,
            v_bill_month=SCORE_DATE_LAST_MONTH_MONTH,
            view_name=FFH_BILLING_VIEW_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_FFH_BILLING_QUERY_PATH
        )

        create_input_account_ffh_billing_train_view_op.set_memory_limit('16G')
        create_input_account_ffh_billing_train_view_op.set_cpu_limit('4')

        create_input_account_hs_usage_train_view_op = create_input_account_hs_usage_view(
            v_report_date=SCORE_DATE_DASH,
            v_start_date=SCORE_DATE_MINUS_6_MOS_DASH,
            v_end_date=SCORE_DATE_LAST_MONTH_END_DASH,
            v_bill_year=SCORE_DATE_LAST_MONTH_YEAR,
            v_bill_month=SCORE_DATE_LAST_MONTH_MONTH,
            view_name=HS_USAGE_VIEW_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_HS_USAGE_QUERY_PATH
        )

        create_input_account_hs_usage_train_view_op.set_memory_limit('16G')
        create_input_account_hs_usage_train_view_op.set_cpu_limit('4')

        create_input_account_demo_income_train_view_op = create_input_account_demo_income_view(
            score_date=SCORE_DATE,
            score_date_delta=SCORE_DATE_DELTA,
            view_name=DEMO_INCOME_VIEW_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_DEMO_INCOME_QUERY_PATH
        )

        create_input_account_demo_income_train_view_op.set_memory_limit('16G')
        create_input_account_demo_income_train_view_op.set_cpu_limit('4')

        create_input_account_promo_expiry_train_view_op = create_input_account_promo_expiry_view(
            score_date=SCORE_DATE,
            view_name=PROMO_EXPIRY_VIEW_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_PROMO_EXPIRY_QUERY_PATH
        )

        create_input_account_promo_expiry_train_view_op.set_memory_limit('16G')
        create_input_account_promo_expiry_train_view_op.set_cpu_limit('4')

        create_input_account_gpon_copper_train_view_op = create_input_account_gpon_copper_view(
            score_date=SCORE_DATE,
            score_date_delta=SCORE_DATE_DELTA,
            view_name=GPON_COPPER_VIEW_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_GPON_COPPER_QUERY_PATH
        )

        create_input_account_gpon_copper_train_view_op.set_memory_limit('16G')
        create_input_account_gpon_copper_train_view_op.set_cpu_limit('4')

        create_input_account_clckstrm_telus_view_op = create_input_account_clckstrm_telus_view(
            score_date=SCORE_DATE,
            score_date_delta=SCORE_DATE_DELTA,
            view_name=CLCKSTRM_TELUS_VIEW_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_CLCKSTRM_TELUS_QUERY_PATH
        )

        create_input_account_clckstrm_telus_view_op.set_memory_limit('16G')
        create_input_account_clckstrm_telus_view_op.set_cpu_limit('4')

        create_input_account_alarmdotcom_app_usage_view_op = create_input_account_alarmdotcom_app_usage_view(
            score_date=SCORE_DATE,
            score_date_delta=SCORE_DATE_DELTA,
            view_name=ALARMDOTCOM_APP_USAGE_VIEW_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_ALARMDOTCOM_APP_USAGE_QUERY_PATH
        )

        create_input_account_alarmdotcom_app_usage_view_op.set_memory_limit('16G')
        create_input_account_alarmdotcom_app_usage_view_op.set_cpu_limit('4')

        create_input_account_tos_active_bans_view_op = create_input_account_tos_active_bans_view(
            score_date=SCORE_DATE,
            score_date_delta=SCORE_DATE_DELTA,
            v_start_date=SCORE_DATE_LAST_MONTH_START_DASH,
            v_end_date=SCORE_DATE_LAST_MONTH_END_DASH,
            view_name=TOS_ACTIVE_BANS_VIEW_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_TOS_ACTIVE_BANS_QUERY_PATH
        )

        create_input_account_tos_active_bans_view_op.set_memory_limit('16G')
        create_input_account_tos_active_bans_view_op.set_cpu_limit('4')

        # ----- preprocessing train data --------
        preprocess_train_op = preprocess(
            account_consl_view=CONSL_VIEW_NAME,
            account_bill_view=FFH_BILLING_VIEW_NAME,
            hs_usage_view=HS_USAGE_VIEW_NAME,
            demo_income_view=DEMO_INCOME_VIEW_NAME,
            promo_expiry_view=PROMO_EXPIRY_VIEW_NAME,
            gpon_copper_view=GPON_COPPER_VIEW_NAME,
            clckstrm_telus_view=CLCKSTRM_TELUS_VIEW_NAME, 
            alarmdotcom_app_usage_view=ALARMDOTCOM_APP_USAGE_VIEW_NAME, 
            tos_active_bans_view=TOS_ACTIVE_BANS_VIEW_NAME, 
            save_data_path='gs://{}/{}/{}_train.csv.gz'.format(FILE_BUCKET, SERVICE_TYPE, SERVICE_TYPE),
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID
        )

        preprocess_train_op.set_memory_limit('128G')
        preprocess_train_op.set_cpu_limit('32')

        preprocess_train_op.after(create_input_account_consl_train_view_op)
        preprocess_train_op.after(create_input_account_ffh_billing_train_view_op)
        preprocess_train_op.after(create_input_account_hs_usage_train_view_op)
        preprocess_train_op.after(create_input_account_demo_income_train_view_op)
        preprocess_train_op.after(create_input_account_promo_expiry_train_view_op)
        preprocess_train_op.after(create_input_account_gpon_copper_train_view_op)
        preprocess_train_op.after(create_input_account_clckstrm_telus_view_op)
        preprocess_train_op.after(create_input_account_alarmdotcom_app_usage_view_op)
        preprocess_train_op.after(create_input_account_tos_active_bans_view_op)

        # --------------- validation view ops ---------------
        create_input_account_consl_validation_view_op = create_input_account_consl_view(
            view_name=CONSL_VIEW_VALIDATION_NAME, 
            score_date=SCORE_DATE_VAL,
            score_date_delta=SCORE_DATE_VAL_DELTA,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_CONSL_QUERY_PATH,
        )

        create_input_account_consl_validation_view_op.set_memory_limit('16G')
        create_input_account_consl_validation_view_op.set_cpu_limit('4')

        create_input_account_ffh_billing_validation_view_op = create_input_account_ffh_billing_view(
            v_report_date=SCORE_DATE_VAL_DASH,
            v_start_date=SCORE_DATE_VAL_MINUS_6_MOS_DASH,
            v_end_date=SCORE_DATE_VAL_LAST_MONTH_END_DASH,
            v_bill_year=SCORE_DATE_VAL_LAST_MONTH_YEAR,
            v_bill_month=SCORE_DATE_VAL_LAST_MONTH_MONTH,
            view_name=FFH_BILLING_VIEW_VALIDATION_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_FFH_BILLING_QUERY_PATH
        )

        create_input_account_ffh_billing_validation_view_op.set_memory_limit('16G')
        create_input_account_ffh_billing_validation_view_op.set_cpu_limit('4')

        create_input_account_hs_usage_validation_view_op = create_input_account_hs_usage_view(
            v_report_date=SCORE_DATE_VAL_DASH,
            v_start_date=SCORE_DATE_VAL_MINUS_6_MOS_DASH,
            v_end_date=SCORE_DATE_VAL_LAST_MONTH_END_DASH,
            v_bill_year=SCORE_DATE_VAL_LAST_MONTH_YEAR,
            v_bill_month=SCORE_DATE_VAL_LAST_MONTH_MONTH,
            view_name=HS_USAGE_VIEW_VALIDATION_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_HS_USAGE_QUERY_PATH
        )

        create_input_account_hs_usage_validation_view_op.set_memory_limit('16G')
        create_input_account_hs_usage_validation_view_op.set_cpu_limit('4')

        create_input_account_demo_income_validation_view_op = create_input_account_demo_income_view(
            score_date=SCORE_DATE_VAL,
            score_date_delta=SCORE_DATE_VAL_DELTA,
            view_name=DEMO_INCOME_VIEW_VALIDATION_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_DEMO_INCOME_QUERY_PATH
        )

        create_input_account_demo_income_validation_view_op.set_memory_limit('16G')
        create_input_account_demo_income_validation_view_op.set_cpu_limit('4')

        create_input_account_promo_expiry_validation_view_op = create_input_account_promo_expiry_view(
            score_date=SCORE_DATE_VAL,
            view_name=PROMO_EXPIRY_VIEW_VALIDATION_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_PROMO_EXPIRY_QUERY_PATH
        )

        create_input_account_promo_expiry_validation_view_op.set_memory_limit('16G')
        create_input_account_promo_expiry_validation_view_op.set_cpu_limit('4')

        create_input_account_gpon_copper_validation_view_op = create_input_account_gpon_copper_view(
            score_date=SCORE_DATE_VAL,
            score_date_delta=SCORE_DATE_VAL_DELTA,
            view_name=GPON_COPPER_VIEW_VALIDATION_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_GPON_COPPER_QUERY_PATH
        )

        create_input_account_gpon_copper_validation_view_op.set_memory_limit('16G')
        create_input_account_gpon_copper_validation_view_op.set_cpu_limit('4')

        create_input_account_clckstrm_telus_validation_view_op = create_input_account_clckstrm_telus_view(
            score_date=SCORE_DATE_VAL,
            score_date_delta=SCORE_DATE_VAL_DELTA,
            view_name=CLCKSTRM_TELUS_VIEW_VALIDATION_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_CLCKSTRM_TELUS_QUERY_PATH
        )

        create_input_account_clckstrm_telus_validation_view_op.set_memory_limit('16G')
        create_input_account_clckstrm_telus_validation_view_op.set_cpu_limit('4')

        create_input_account_alarmdotcom_app_usage_validation_view_op = create_input_account_alarmdotcom_app_usage_view(
            score_date=SCORE_DATE_VAL,
            score_date_delta=SCORE_DATE_VAL_DELTA,
            view_name=ALARMDOTCOM_APP_USAGE_VIEW_VALIDATION_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_ALARMDOTCOM_APP_USAGE_QUERY_PATH
        )

        create_input_account_alarmdotcom_app_usage_validation_view_op.set_memory_limit('16G')
        create_input_account_alarmdotcom_app_usage_validation_view_op.set_cpu_limit('4')

        create_input_account_tos_active_bans_validation_view_op = create_input_account_tos_active_bans_view(
            score_date=SCORE_DATE_VAL,
            score_date_delta=SCORE_DATE_VAL_DELTA,
            v_start_date=SCORE_DATE_VAL_LAST_MONTH_START_DASH,
            v_end_date=SCORE_DATE_VAL_LAST_MONTH_END_DASH,
            view_name=TOS_ACTIVE_BANS_VALIDATION_VIEW_NAME,
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            region=REGION,
            resource_bucket=RESOURCE_BUCKET,
            query_path=ACCOUNT_TOS_ACTIVE_BANS_QUERY_PATH
        )

        create_input_account_tos_active_bans_validation_view_op.set_memory_limit('16G')
        create_input_account_tos_active_bans_validation_view_op.set_cpu_limit('4')

        # ----- preprocessing validation data --------
        preprocess_validation_op = preprocess(
            account_consl_view=CONSL_VIEW_VALIDATION_NAME,
            account_bill_view=FFH_BILLING_VIEW_VALIDATION_NAME,
            hs_usage_view=HS_USAGE_VIEW_VALIDATION_NAME,
            demo_income_view=DEMO_INCOME_VIEW_VALIDATION_NAME,
            promo_expiry_view=PROMO_EXPIRY_VIEW_VALIDATION_NAME,
            gpon_copper_view=GPON_COPPER_VIEW_VALIDATION_NAME,
            clckstrm_telus_view=CLCKSTRM_TELUS_VIEW_VALIDATION_NAME, 
            alarmdotcom_app_usage_view=ALARMDOTCOM_APP_USAGE_VIEW_VALIDATION_NAME, 
            tos_active_bans_view=TOS_ACTIVE_BANS_VALIDATION_VIEW_NAME, 
            save_data_path='gs://{}/{}/{}_validation.csv.gz'.format(FILE_BUCKET, SERVICE_TYPE, SERVICE_TYPE),
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
        )
        preprocess_validation_op.set_memory_limit('256G')
        preprocess_validation_op.set_cpu_limit('32')

        preprocess_validation_op.after(create_input_account_consl_validation_view_op)
        preprocess_validation_op.after(create_input_account_ffh_billing_validation_view_op)
        preprocess_validation_op.after(create_input_account_hs_usage_validation_view_op)
        preprocess_validation_op.after(create_input_account_demo_income_validation_view_op)
        preprocess_validation_op.after(create_input_account_promo_expiry_validation_view_op)
        preprocess_validation_op.after(create_input_account_gpon_copper_validation_view_op)
        preprocess_validation_op.after(create_input_account_clckstrm_telus_validation_view_op)
        preprocess_validation_op.after(create_input_account_alarmdotcom_app_usage_validation_view_op)
        preprocess_validation_op.after(create_input_account_tos_active_bans_validation_view_op)

        train_and_save_model_op = train_and_save_model(file_bucket=FILE_BUCKET,
                                                       service_type=SERVICE_TYPE,
                                                       score_date_dash=SCORE_DATE_DASH,
                                                       score_date_val_dash=SCORE_DATE_VAL_DASH,
                                                       project_id=PROJECT_ID,
                                                       dataset_id=DATASET_ID,
                                                       )
        train_and_save_model_op.set_memory_limit('256G')
        train_and_save_model_op.set_cpu_limit('32')

        train_and_save_model_op.after(preprocess_train_op)
        train_and_save_model_op.after(preprocess_validation_op)

    return pipeline
