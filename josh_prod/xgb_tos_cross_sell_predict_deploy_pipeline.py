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
    FOLDER_NAME = 'xgb_{}_{}_predict_deploy'.format(SERVICE_TYPE, MODEL_ID) #xgb_tos_cross_sell_5060_predict_deploy
    QUERIES_PATH = 'vertex_pipelines/' + FOLDER_NAME + '/queries/'

    scoringDate = date.today() - relativedelta(days=5)

    # current day views
    CONSL_VIEW_NAME = '{}_pipeline_consl_data_curr_bi_layer'.format(SERVICE_TYPE)  # done
    FFH_BILLING_VIEW_NAME = '{}_pipeline_ffh_billing_data_curr_bi_layer'.format(SERVICE_TYPE)  # done
    HS_USAGE_VIEW_NAME = '{}_pipeline_hs_usage_data_curr_bi_layer'.format(SERVICE_TYPE)  # done
    DEMO_INCOME_VIEW_NAME = '{}_pipeline_demo_income_data_curr_bi_layer'.format(SERVICE_TYPE)  # done
    PROMO_EXPIRY_VIEW_NAME = '{}_pipeline_promo_expiry_data_curr_bi_layer'.format(SERVICE_TYPE)  # done
    GPON_COPPER_VIEW_NAME = '{}_pipeline_gpon_copper_data_curr_bi_layer'.format(SERVICE_TYPE)  # done
    CLCKSTRM_TELUS_VIEW_NAME = '{}_pipeline_clckstrm_telus_curr_bi_layer'.format(SERVICE_TYPE)
    ALARMDOTCOM_APP_USAGE_VIEW_NAME = '{}_pipeline_alarmdotcom_app_usage_curr_bi_layer'.format(SERVICE_TYPE)
    TOS_ACTIVE_BANS_VIEW_NAME = '{}_pipeline_tos_active_bans_curr_bi_layer'.format(SERVICE_TYPE) 

    # dates
    SCORE_DATE = scoringDate.strftime('%Y%m%d')  # date.today().strftime('%Y%m%d')
    SCORE_DATE_DASH = scoringDate.strftime('%Y-%m-%d')
    SCORE_DATE_MINUS_6_MOS_DASH = ((scoringDate - relativedelta(months=6)).replace(day=1)).strftime('%Y-%m-%d')
    SCORE_DATE_LAST_MONTH_START_DASH = (scoringDate.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
    SCORE_DATE_LAST_MONTH_END_DASH = ((scoringDate.replace(day=1)) - timedelta(days=1)).strftime('%Y-%m-%d')
    SCORE_DATE_LAST_MONTH_YEAR = ((scoringDate.replace(day=1)) - timedelta(days=1)).year
    SCORE_DATE_LAST_MONTH_MONTH = ((scoringDate.replace(day=1)) - timedelta(days=1)).month

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
            'mnh_ind'
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
        output_component_file="xgb_batch_prediction.yaml",
    )
    def batch_prediction(
            project_id: str,
            dataset_id: str,
            file_bucket: str,
            service_type: str,
            score_table: str,
            score_date_dash: str,
            metrics: Output[Metrics],
            metricsc: Output[ClassificationMetrics],
    ):
        import time
        import pandas as pd
        import numpy as np
        import pickle
        from datetime import date
        from dateutil.relativedelta import relativedelta
        from google.cloud import bigquery
        from google.cloud import storage
        
        MODEL_ID = '5060'
        
        def if_tbl_exists(bq_client, table_ref):
            from google.cloud.exceptions import NotFound
            try:
                bq_client.get_table(table_ref)
                return True
            except NotFound:
                return False

        def upsert_table(project_id, dataset_id, table_id, sql, result):
            new_values = ',\n'.join(result.apply(lambda row: row_format(row), axis=1))
            new_sql = sql.format(proj_id=project_id, dataset_id=dataset_id, table_id=table_id,
                                 new_values=new_values)
            bq_client = bigquery.Client(project=project_id)
            code = bq_client.query(new_sql)
            time.sleep(5)

        def row_format(row):
            values = row.values
            new_values = ""
            v = str(values[0]) if not pd.isnull(values[0]) else 'NULL'
            if 'str' in str(type(values[0])):
                new_values += f"'{v}'"
            else:
                new_values += f"{v}"

            for i in range(1, len(values)):
                v = str(values[i]) if not pd.isnull(values[i]) else 'NULL'
                if 'str' in str(type(values[i])):
                    new_values += f",'{v}'"
                else:
                    new_values += f",{v}"
            return '(' + new_values + ')'

        def generate_sql_file(ll):
            s = 'MERGE INTO `{proj_id}.{dataset_id}.{table_id}` a'
            s += " USING UNNEST("
            s += "[struct<"
            for i in range(len(ll) - 1):
                v = ll[i]
                s += "{} {},".format(v[0], v[1])
            s += "{} {}".format(ll[-1][0], ll[-1][1])
            s += ">{new_values}]"
            s += ") b"
            s += " ON a.ban = b.ban and a.score_date = b.score_date"
            s += " WHEN MATCHED THEN"
            s += " UPDATE SET "
            s += "a.{}=b.{},".format(ll[0][0], ll[0][0])
            for i in range(1, len(ll) - 1):
                v = ll[i]
                s += "a.{}=b.{},".format(v[0], v[0])
            s += "a.{}=b.{}".format(ll[-1][0], ll[-1][0])
            s += " WHEN NOT MATCHED THEN"
            s += " INSERT("
            for i in range(len(ll) - 1):
                v = ll[i]
                s += "{},".format(v[0])
            s += "{})".format(ll[-1][0])
            s += " VALUES("
            for i in range(len(ll) - 1):
                s += "b.{},".format(ll[i][0])
            s += "b.{}".format(ll[-1][0])
            s += ")"

            return s

        MODEL_PATH = '{}_xgb_models/'.format(service_type)
        df_score = pd.read_csv('gs://{}/{}/{}_score.csv.gz'.format(file_bucket, service_type, service_type), compression='gzip')
        df_score.dropna(subset=['ban'], inplace=True)
        df_score.reset_index(drop=True, inplace=True)
        print('......scoring data loaded:{}'.format(df_score.shape))
        time.sleep(10)

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(file_bucket)
        blobs = storage_client.list_blobs(file_bucket, prefix='{}{}_models_xgb_'.format(MODEL_PATH, service_type))

        model_lists = []
        for blob in blobs:
            model_lists.append(blob.name)

        blob = bucket.blob(model_lists[-1])
        blob_in = blob.download_as_string()
        model_dict = pickle.loads(blob_in)
        model_xgb = model_dict['model']
        features = model_dict['features']
        print('...... model loaded')
        time.sleep(10)

        ll = [('ban', 'string'), ('score_date', 'string'), ('model_id', 'string'), ('score', 'float64')]
        sql = generate_sql_file(ll)

        df_score['ban'] = df_score['ban'].astype(int)
        print('.... scoring for {} tos cross sell bans base'.format(len(df_score)))

        # get full score to cave into bucket
        pred_prob = model_xgb.predict_proba(df_score[features], ntree_limit=model_xgb.best_iteration)[:, 1]
        result = pd.DataFrame(columns=['ban', 'score_date', 'model_id', 'score'])
        result['score'] = list(pred_prob)
        result['score'] = result['score'].fillna(0.0).astype('float64')
        result['ban'] = list(df_score['ban'])
        result['ban'] = result['ban'].astype('str')
        result['score_date'] = score_date_dash
        result['model_id'] = MODEL_ID

        result.to_csv('gs://{}/ucar/{}_prediction.csv.gz'.format(file_bucket, service_type), compression='gzip',
                      index=False)
        time.sleep(60)

        batch_size = 1000
        n_batchs = int(df_score.shape[0] / batch_size) + 1
        print('...... will upsert {} batches'.format(n_batchs))

        # start batch prediction
        all_scores = np.array(result['score'].values)
        for i in range(n_batchs):
        
            s, e = i * batch_size, (i + 1) * batch_size
            if e >= df_score.shape[0]:
                e = df_score.shape[0]

            df_temp = df_score.iloc[s:e]
            pred_prob = all_scores[s:e]
            batch_result = pd.DataFrame(columns=['ban', 'score_date', 'model_id', 'score'])
            batch_result['score'] = list(pred_prob)
            batch_result['score'] = batch_result['score'].fillna(0.0).astype('float64')
            batch_result['ban'] = list(df_temp['ban'])
            batch_result['ban'] = batch_result['ban'].astype('str')
            batch_result['score_date'] = score_date_dash
            batch_result['model_id'] = MODEL_ID

            upsert_table(project_id,
                         dataset_id,
                         score_table,
                         sql,
                         batch_result,
                         )
            if i % 20 == 0:
                print('predict for batch {} done'.format(i), end=' ')

        time.sleep(120)
        
        
        #-------------------------------------------------------complete upto here----------------------------------------------------------------#


    @component(
        base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-xgboost-slim:latest",
        output_component_file="xgb_postprocess.yaml",
    )
    def postprocess(
            project_id: str,
            file_bucket: str,
            service_type: str,
            score_date_dash: str,
    ):
        import time
        import pandas as pd
        from google.cloud import bigquery

        MODEL_ID = '5060'
        file_name = 'gs://{}/ucar/{}_prediction.csv.gz'.format(file_bucket, service_type)
        df_orig = pd.read_csv(file_name, compression='gzip')
        df_orig.dropna(subset=['ban'], inplace=True)
        df_orig.reset_index(drop=True, inplace=True)
        df_orig['scoring_date'] = score_date_dash
        df_orig.ban = df_orig.ban.astype(int)
        df_orig = df_orig.rename(columns={'ban': 'bus_bacct_num', 'score': 'score_num'})
        df_orig.score_num = df_orig.score_num.astype(float)
        df_orig['decile_grp_num'] = pd.qcut(df_orig['score_num'], q=10, labels=False)
        df_orig.decile_grp_num = df_orig.decile_grp_num + 1
        df_orig['percentile_pct'] = df_orig.score_num.rank(pct=True)
        df_orig['predict_model_nm'] = 'FFH TOS CROSS SELL Model - DIVG'
        df_orig['model_type_cd'] = 'FFH'
        df_orig['subscriber_no'] = ""
        df_orig['prod_instnc_resrc_str'] = ""
        df_orig['service_instnc_id'] = ""
        df_orig['segment_nm'] = ""
        df_orig['segment_id'] = ""
        df_orig['classn_nm'] = ""
        df_orig['predict_model_id'] = MODEL_ID
        df_orig.drop(columns=['model_id', 'score_date'], axis=1, inplace=True)

        get_cust_id = """
        WITH bq_snpsht_max_date AS(
        SELECT PARSE_DATE('%Y%m%d', MAX(partition_id)) AS max_date
            FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.INFORMATION_SCHEMA.PARTITIONS` 
        WHERE table_name = 'bq_prod_instnc_snpsht' 
            AND partition_id <> '__NULL__'
        ),
        -- BANs can have multiple Cust ID. Create rank by product type and status, prioritizing ban/cust id with active FFH products
        rank_prod_type AS (
        SELECT DISTINCT
            bacct_bus_bacct_num,
            consldt_cust_bus_cust_id AS cust_id,
            CASE WHEN pi_prod_instnc_resrc_typ_cd IN ('SING', 'HSIC', 'TTV', 'SMHM', 'STV', 'DIIC') AND pi_prod_instnc_stat_cd = 'A' THEN 1
                    WHEN pi_prod_instnc_resrc_typ_cd IN ('SING', 'HSIC', 'TTV', 'SMHM', 'STV', 'DIIC') THEN 2
                    WHEN pi_prod_instnc_stat_cd = 'A' THEN 3
                    ELSE 4
                    END AS prod_rank
        FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
        CROSS JOIN bq_snpsht_max_date
        WHERE CAST(prod_instnc_ts AS DATE)=bq_snpsht_max_date.max_date
        AND bus_prod_instnc_src_id = 1001
        ),
        --Rank Cust ID
        rank_cust_id AS (
        SELECT DISTINCT
            bacct_bus_bacct_num,
            cust_id,
            RANK() OVER(PARTITION BY bacct_bus_bacct_num
                            ORDER BY prod_rank,
                                        cust_id) AS cust_id_rank               
        FROM rank_prod_type
        )
        --Select best cust id
        SELECT bacct_bus_bacct_num,
            cust_id
        FROM rank_cust_id
        WHERE cust_id_rank = 1
        """

        client = bigquery.Client(project=project_id)
        df_cust = client.query(get_cust_id).to_dataframe()
        df_final = df_orig.set_index('bus_bacct_num').join(df_cust.set_index('bacct_bus_bacct_num')).reset_index()
        df_final = df_final.rename(columns={'index': 'bus_bacct_num', 'cust_bus_cust_id': 'cust_id'})
        df_final = df_final.sort_values(by=['score_num'], ascending=False)
        df_final.to_csv(file_name, compression='gzip', index=False)
        time.sleep(300)
        
    @dsl.pipeline(
        # A name for the pipeline.
        name="{}-xgb-predict-pipeline".format(SERVICE_TYPE),
        description=' pipeline for predict {} model'.format(SERVICE_TYPE)
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
        preprocess_op = preprocess(
            account_consl_view=CONSL_VIEW_NAME,
            account_bill_view=FFH_BILLING_VIEW_NAME,
            hs_usage_view=HS_USAGE_VIEW_NAME,
            demo_income_view=DEMO_INCOME_VIEW_NAME,
            promo_expiry_view=PROMO_EXPIRY_VIEW_NAME,
            gpon_copper_view=GPON_COPPER_VIEW_NAME,
            clckstrm_telus_view=CLCKSTRM_TELUS_VIEW_NAME, 
            alarmdotcom_app_usage_view=ALARMDOTCOM_APP_USAGE_VIEW_NAME, 
            tos_active_bans_view=TOS_ACTIVE_BANS_VIEW_NAME, 
            save_data_path='gs://{}/{}/{}_score.csv.gz'.format(FILE_BUCKET, SERVICE_TYPE, SERVICE_TYPE),
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID
        )

        preprocess_op.set_memory_limit('128G')
        preprocess_op.set_cpu_limit('32')

        preprocess_op.after(create_input_account_consl_train_view_op)
        preprocess_op.after(create_input_account_ffh_billing_train_view_op)
        preprocess_op.after(create_input_account_hs_usage_train_view_op)
        preprocess_op.after(create_input_account_demo_income_train_view_op)
        preprocess_op.after(create_input_account_promo_expiry_train_view_op)
        preprocess_op.after(create_input_account_gpon_copper_train_view_op)
        preprocess_op.after(create_input_account_clckstrm_telus_view_op)
        preprocess_op.after(create_input_account_alarmdotcom_app_usage_view_op)
        preprocess_op.after(create_input_account_tos_active_bans_view_op)

        batch_prediction_op = batch_prediction(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            file_bucket=FILE_BUCKET,
            service_type=SERVICE_TYPE,
            score_date_dash=SCORE_DATE_DASH,
            score_table='bq_tos_cross_sell_score',
        )
        batch_prediction_op.set_memory_limit('32G')
        batch_prediction_op.set_cpu_limit('4')

        batch_prediction_op.after(preprocess_op)

        postprocessing_op = postprocess(
            project_id=PROJECT_ID,
            file_bucket=FILE_BUCKET,
            service_type=SERVICE_TYPE,
            score_date_dash=SCORE_DATE_DASH,
        )
        postprocessing_op.set_memory_limit('16G')
        postprocessing_op.set_cpu_limit('4')

        postprocessing_op.after(batch_prediction_op)

    return pipeline
