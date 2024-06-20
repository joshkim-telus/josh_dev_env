from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

# Generate Stats Component: monitoring component for generating statistics on training, serving or predictions data
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-tfdv-slim:1.0.0",
    output_component_file="generate_data_stats.yaml"
)
def generate_data_stats(
    project_id: str,
    data_type: str,
    op_type: str,
    model_nm: str,
    update_ts: str,
    token: str, 
    statistics: Output[Artifact],
    bucket_nm: str = '',
    model_type: str = 'supervised',
    date_col: str = '',
    date_filter: str = '',
    table_block_sample: float = 1,
    row_sample: float = 1,
    in_bq_ind: bool = True,
    src_bq_path: str = '',
    src_csv_path: str = '',
    dest_stats_bq_dataset: str = '',
    dest_schema_path: str = '',
    dest_stats_gcs_path: str = '',
    pass_through_features: list = [],
    training_target_col: str = "",
    pred_cols: list = []
):
    '''
    Inputs:
        - project_id: project id
        - data_type: bigquery or csv
        - op_type: training or serving or predictions
        - model_nm: name of model
        - update_ts: time when pipeline was run
        - bucket_nm: name of bucket where pred schema is or will be stored (Optional: for predictions)
        - model_type: supervised or unsupervised. unsupervised models will create new schema as required.
        - date_col: (Optional: name of column for filtering data by date)
        - date_filter: YYYY-MM-DD (Optional: query only specific date for stats)
        - table_block_sample: sample of data blocks to be loaded (only if bq). Reduces query size for large datasets. 0.1 for 10% (default is 1)
        - row_sample: sample of rows to be loaded (only if bq). If using table_block_sample as well, this will be the % of rows from the selected blocks.
          0.1 for 10% (default is 1)
        - in_bq_ind: True or False (Optional: store stats in bigquery)
        - src_bq_path: bigquery path to data that will be used to generate stats (if data_type is bigquery)
        - src_csv_path: path to csv file that will be used to generate stats (if data_type is csv)
        - dest_stats_bq_dataset: bq dataset where monitoring stats will be stored (only if in_bq_path set to True)
        - dest_schema_path: gcs path to where schema will be stored (optional: only for training stats)
        - dest_stats_gcs_path: gcs path to where stats should be stored
        - pass_through_features: list of feature columns not used for training e.g. keys and ids 
        - training_target_col: target column name from training data (Optional: set to be excluded from serving data)
        - pred_cols: column names where predictions are stored
        
    Outputs:
        - statistics
    '''

    import tensorflow_data_validation as tfdv
    from apache_beam.options.pipeline_options import (
        PipelineOptions
    )
    from google.cloud import storage
    from google.cloud import bigquery
    from datetime import datetime
    import json
    import pandas as pd
    import numpy as np
    import google.oauth2.credentials

    print('msg1: all libraries imported')

    # convert timestamp to datetime
    update_ts = datetime.strptime(update_ts, '%Y-%m-%d %H:%M:%S')

    statistics.uri = dest_stats_gcs_path

    pipeline_options = PipelineOptions()
    stats_options = tfdv.StatsOptions()
    
    # import from csv in GCS
    if data_type == 'csv':
        df = pd.read_csv(src_csv_path)
        
        if op_type == 'predictions':
            df = df[pred_cols]

    # import from Big Query
    elif data_type == 'bigquery':
        
        percent_table_sample = table_block_sample * 100
        
        if op_type == 'predictions':
            # query data stored in BQ and load into pandas dataframe
            build_df = '''SELECT '''
            for pred_col in pred_cols:
                build_df = build_df + f"{pred_col}, "
            build_df = build_df + '''FROM `{bq_table}` TABLESAMPLE SYSTEM ({percent_table_sample} PERCENT)
                        WHERE rand() < {row_sample} 
                    '''.format(bq_table = src_bq_path,
                               percent_table_sample = percent_table_sample, 
                               row_sample = row_sample)
        else:
            # query data stored in BQ and load into pandas dataframe
            build_df = '''
            SELECT * FROM `{bq_table}` TABLESAMPLE SYSTEM ({percent_table_sample} PERCENT)
                        WHERE rand() < {row_sample} 
                    '''.format(bq_table = src_bq_path,
                               percent_table_sample = percent_table_sample, 
                               row_sample = row_sample)
        
        if len(date_filter) > 0:
            build_df = build_df + ''' AND {date_col}="{date_filter}"
            '''.format(date_col=date_col, date_filter=date_filter)
        
        job_config = bigquery.QueryJobConfig()
        df = client.query(build_df, job_config=job_config).to_dataframe()
        
        # check this: if dataframe is empty, return error
        if (df.size == 0):
            raise TypeError("Dataframe is empty, cannot generate statistics.")

    else:
        print("This data type is not supported. Please use a csv or Big Query table, otherwise create your own custom component")
        
    print('msg2: df created')
    
    # drop pass-through features
    if len(pass_through_features) > 0:
        df = df.drop(columns=pass_through_features)
        
    stats = tfdv.generate_statistics_from_dataframe(
        dataframe=df,
        stats_options=stats_options,
        n_jobs=1
    )
    tfdv.write_stats_text(
        stats=stats, 
        output_path=dest_stats_gcs_path
    )
    
    # generate schema for training data
    if op_type == 'training':
        schema = tfdv.infer_schema(stats)
        
        if 'TRAINING' not in schema.default_environment:
                schema.default_environment.append('TRAINING')
        
        # set training target column for supervised learning models (will not be in serving data)
        if model_type == 'supervised':
            if 'SERVING' not in schema.default_environment:
                schema.default_environment.append('SERVING')
            
            # check if training_target_col specified
            if len(training_target_col) > 0:
                # specify that target/label is not in SERVING environment
                if 'SERVING' not in tfdv.get_feature(schema, training_target_col).not_in_environment:
                    tfdv.get_feature(
                        schema, training_target_col).not_in_environment.append('SERVING') 
            else:
                print("No training target column specified")
        
        tfdv.write_schema_text(
            schema=schema, output_path=dest_schema_path
        )
        
    print('msg3: stats generated')
        
    if (op_type == 'predictions') | (model_type == 'unsupervised'):
        # if schema does not exist create new one for predictions or unsupervised model
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_nm)
        blob = bucket.blob(dest_schema_path.split(f'gs://{bucket_nm}/')[1])
        
        if not blob.exists():
            # generate schema for predictions data or unsupervised learning model
            schema = tfdv.infer_schema(stats)
            tfdv.write_schema_text(
                schema=schema, output_path=dest_schema_path
            )

    df_stats = pd.DataFrame(columns=['model_nm',
                                     'update_ts',
                                     'op_type',
                                     'feature_nm',
                                     'feature_type',
                                     'num_non_missing',
                                     'min_num_values',
                                     'max_num_values',
                                     'avg_num_values',
                                     'tot_num_values',
                                     'mean',
                                     'std_dev',
                                     'num_zeros',
                                     'min_val',
                                     'median',
                                     'max_val',
                                     'unique_values',
                                     'top_value_freq',
                                     'avg_length'])

    print('msg4: save stats in data monitoring table')

    # OPTIONAL: save stats in data monitoring table
    if in_bq_ind == True:

        for feature in stats.datasets[0].features:
            feature_nm = feature.path.step[0]
            if (feature.type == 0):
                feature_type = 'INT'
            elif (feature.type == 1):
                feature_type = 'FLOAT'
            elif (feature.type == 2):
                feature_type = 'STRING'
            else:
                featue_type = 'UNKNOWN'

            if (feature_type == 'INT') | (feature_type == 'FLOAT'):
                num_non_missing = feature.num_stats.common_stats.num_non_missing
                min_num_values = feature.num_stats.common_stats.min_num_values
                max_num_values = feature.num_stats.common_stats.max_num_values
                avg_num_values = feature.num_stats.common_stats.avg_num_values
                tot_num_values = feature.num_stats.common_stats.tot_num_values

                mean = feature.num_stats.mean
                std_dev = feature.num_stats.std_dev
                num_zeros = feature.num_stats.num_zeros
                min_val = feature.num_stats.min
                median = feature.num_stats.median
                max_val = feature.num_stats.max

                df_stats.loc[len(df_stats.index)] = pd.Series({
                    'model_nm': model_nm,
                    'update_ts': update_ts,
                    'op_type': op_type,
                    'feature_nm': feature_nm,
                    'feature_type': feature_type,
                    'num_non_missing': num_non_missing,
                    'min_num_values': min_num_values,
                    'max_num_values': max_num_values,
                    'avg_num_values': avg_num_values,
                    'tot_num_values': tot_num_values,
                    'mean': mean,
                    'std_dev': std_dev,
                    'num_zeros': num_zeros,
                    'min_val': min_val,
                    'median': median,
                    'max_val': max_val
                })
                
            elif feature_type == 'STRING':
                num_non_missing = feature.string_stats.common_stats.num_non_missing
                min_num_values = feature.string_stats.common_stats.min_num_values
                max_num_values = feature.string_stats.common_stats.max_num_values
                avg_num_values = feature.string_stats.common_stats.avg_num_values
                tot_num_values = feature.string_stats.common_stats.tot_num_values

                unique_values = feature.string_stats.unique

                # create dict of top values to be stored in BQ as record
                top_values = feature.string_stats.top_values
                top_value_freq_arr = []
                for i in range(len(top_values)):
                    top_value = top_values[i]
                    value = top_value.value
                    freq = top_value.frequency
                    top_value_dict = {'value': value, 'frequency': freq}
                    top_value_freq_arr.append(top_value_dict)

                avg_length = feature.string_stats.avg_length

                df_stats.loc[len(df_stats.index)] = pd.Series({
                    'model_nm': model_nm,
                    'update_ts': update_ts,
                    'op_type': op_type,
                    'feature_nm': feature_nm,
                    'feature_type': feature_type,
                    'num_non_missing': num_non_missing,
                    'min_num_values': min_num_values,
                    'max_num_values': max_num_values,
                    'avg_num_values': avg_num_values,
                    'tot_num_values': tot_num_values,
                    'unique_values': unique_values,
                    'top_value_freq': top_value_freq_arr,
                    'avg_length': avg_length
                })

        # set null records to None value
        df_stats['top_value_freq'] = df_stats['top_value_freq'].fillna(np.nan).replace([
            np.nan], [None])

#         #### For wb
#         CREDENTIALS = google.oauth2.credentials.Credentials(token)

#         client = bigquery.Client(project=project_id, credentials=CREDENTIALS)

        #### For prod 
        client = bigquery.Client(project=project_id)
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND",
                                            schema=[
                                                bigquery.SchemaField(
                                                    "model_nm", "STRING"),
                                                bigquery.SchemaField(
                                                    "update_ts", "TIMESTAMP"),
                                                bigquery.SchemaField(
                                                    "op_type", "STRING"),
                                                bigquery.SchemaField(
                                                    "feature_nm", "STRING"),
                                                bigquery.SchemaField(
                                                    "feature_type", "STRING"),
                                                bigquery.SchemaField(
                                                    "num_non_missing", "INTEGER"),
                                                bigquery.SchemaField(
                                                    "min_num_values", "INTEGER"),
                                                bigquery.SchemaField(
                                                    "max_num_values", "INTEGER"),
                                                bigquery.SchemaField(
                                                    "avg_num_values", "FLOAT"),
                                                bigquery.SchemaField(
                                                    "tot_num_values", "INTEGER"),
                                                bigquery.SchemaField(
                                                    "mean", "FLOAT"),
                                                bigquery.SchemaField(
                                                    "std_dev", "FLOAT"),
                                                bigquery.SchemaField(
                                                    "num_zeros", "INTEGER"),
                                                bigquery.SchemaField(
                                                    "min_val", "FLOAT"),
                                                bigquery.SchemaField(
                                                    "median", "FLOAT"),
                                                bigquery.SchemaField(
                                                    "max_val", "FLOAT"),
                                                bigquery.SchemaField(
                                                    "unique_values", "FLOAT"),
                                                bigquery.SchemaField("top_value_freq", "RECORD", mode="REPEATED", fields=[
                                                    bigquery.SchemaField("frequency", "FLOAT"), bigquery.SchemaField("value", "STRING")]),
                                                bigquery.SchemaField(
                                                    "avg_length", "FLOAT")
                                            ],)  # create new table or append if already exists
        
        data_stats_table = f"{project_id}.{dest_stats_bq_dataset}.bq_data_monitoring"
        
        print(f'msg5: {data_stats_table}')

        job = client.load_table_from_dataframe(# Make an API request.
            df_stats, data_stats_table, job_config=job_config
        )
        job.result()
        table = client.get_table(data_stats_table)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), data_stats_table
            )
        )
        
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-tfdv-slim:1.0.0",
    output_component_file="validate_stats.yaml"
)
def validate_stats( 
    project_id: str, 
    bucket_nm: str, 
    model_nm: str, 
    validation_type: str, 
    op_type: str, 
    statistics: Input[Artifact], 
    anomalies: Output[Artifact], 
    base_stats_path: str, 
    update_ts: str, 
    src_schema_path: str, 
    src_anomaly_thresholds_path: str, 
    dest_anomalies_gcs_path: str, 
    dest_anomalies_bq_dataset: str = '', 
    in_bq_ind: bool = True 
    ): 
    
    ''' 
    Inputs: 
        - project_id: project id 
        - bucket_nm: name of bucket where anomaly thresholds are stored 
        - model_nm: name of model 
        - validation_type: skew or drift 
        - op_type: serving or predictions 
        - statistics: path to statistics imported from generate stats component 
        - base_stats_path: path to statistics for comparison (training for skew, old serving stats for drift) 
        - update_ts: time pipeline is run. keep consistent across components 
        - src_schema_path: path to where schema is in GCS (for either serving stats or prediction stats) 
        - src_anomaly_thresholds_path: path to json file where skew/drift anomaly thresholds are specified 
        - dest_anomalies_gcs_path: path to where anomalies should be stored in GCS 
        - dest_anomalies_bq_dataset: dataset where anomalies will be stored in BQ 
        - in_bq_ind: indicate whether you want to save anomalies in BQ 
        
    Outputs: 
        - anomalies: path to anomalies file          
    ''' 
    
    import logging 
    import tensorflow_data_validation as tfdv 
    from google.cloud import storage 
    from google.cloud import bigquery 
    import json 
    import pandas as pd 
    from datetime import datetime 

    if (validation_type != "skew") and (validation_type != "drift"):
        raise ValueError("Error: the validation_type can only be one of skew or drift")
        
    if (op_type != "serving") and (op_type != "predictions"):
        raise ValueError("Error: the op_type can only be one of serving or predictions")
    
    # convert timestamp to datetime 
    update_ts = datetime.strptime(update_ts, '%Y-%m-%d %H:%M:%S') 
    
    # set uri of anomalies output 
    anomalies.uri = dest_anomalies_gcs_path 
    
    def load_stats(path): 
        print(f'loading stats from: {path}') 
        return tfdv.load_statistics(input_path=path) 
        
    base_stats = load_stats(base_stats_path) 
    stats = load_stats(statistics.uri) 
    
    # get schema 
    schema = tfdv.load_schema_text(
        input_path=src_schema_path
        ) 
        
    if op_type == 'serving': 
        # set anomaly check to features 
        anomaly_check = 'features' 
    
        # ensure that serving set as env 
        if 'SERVING' not in schema.default_environment: 
            schema.default_environment.append('SERVING') 
            
    if op_type == 'predictions': 
        # set anomaly check to predictions 
        anomaly_check = 'predictions' 
        
        #ensure that predictions set as env 
        if 'PREDICTIONS' not in schema.default_environment: 
            schema.default_environment.append('PREDICTIONS') 
        
    # get serving anomaly thresholds 
    storage_client = storage.Client() 
    bucket = storage_client.bucket(bucket_nm) 
    blob = bucket.blob(src_anomaly_thresholds_path) 
    blob.download_to_filename('anomaly_thresholds.json') 
    
    f = open('anomaly_thresholds.json') 
    anomaly_thresholds = json.load(f) 
    
    if validation_type == 'skew': 
        # set serving thresholds 
        for feature, threshold in anomaly_thresholds[anomaly_check]['numerical'].items(): 
            tfdv.get_feature(schema, feature).skew_comparator.jensen_shannon_divergence.threshold = threshold 
            
        for feature, threshold in anomaly_thresholds[anomaly_check]['categorical'].items(): 
            tfdv.get_feature(schema, feature).skew_comparator.infinity_norm.threshold = threshold
            
        # validating stats 
        detected_anomalies = tfdv.validate_statistics(
            statistics=stats, 
            schema=schema, 
            environment=op_type.upper(), 
            serving_statistics=base_stats
            ) 
        
    elif validation_type == 'drift': 
        # set serving thresholds 
        for feature, threshold in anomaly_thresholds[anomaly_check]['numerical'].items(): 
            tfdv.get_feature(schema, feature).drift_comparator.jensen_shannon_divergence.threshold = threshold 
            
        for feature, threshold in anomaly_thresholds[anomaly_check]['categorical'].items(): 
            tfdv.get_feature(schema, feature).drift_comparator.infinity_norm.threshold = threshold 
            
        # validating stats 
        detected_anomalies = tfdv.validate_statistics(
            statistics=stats, 
            schema=schema, 
            environment=op_type.upper(), 
            previous_statistics=base_stats 
            ) 
    
    else: 
        print("Please specify skew or drift") 
        
    # store updated schema in gcs
    tfdv.write_schema_text(schema=schema, output_path=src_schema_path) 
    
    logging.info(f'writing anomalies to: {dest_anomalies_gcs_path}') 
    tfdv.write_anomalies_text(detected_anomalies, dest_anomalies_gcs_path) 
    
    # OPTIONAL: save anomalies to BQ 
    if in_bq_ind == True: 
        print("yes there are anomalies")
        anomalies_dict = detected_anomalies.anomaly_info 
        skew_drift_dict = detected_anomalies.drift_skew_info

    df_anomalies = pd.DataFrame(columns=[
                                'model_nm', 'update_ts', 'feature_nm', 'short_description', 'long_description']) 
                                
    # check if there are anomalies (dict is not empty) 
    if bool(anomalies_dict): 
        for key in anomalies_dict: 
            feature = key 
            short_description = anomalies_dict[key].short_description 
            long_description = anomalies_dict[key].description 
            
            df_anomalies.loc[len(df_anomalies.index)] = pd.Series({ 
                'model_nm': model_nm, 
                'update_ts': update_ts, 
                'feature_nm': feature, 
                'short_description': short_description, 
                'long_description': long_description
                }) 

    # check for skew-drift
    df_sd = pd.DataFrame(columns=['feature_nm', 'skew_drift'])

    #check for skew-drift 
    if bool(skew_drift_dict): 
        for sd in skew_drift_dict: 
            
            feature = sd.path.step[0]
            
            if validation_type == 'skew': 
                value = sd.skew_measurements[0].value 
                threshold = sd.skew_measurements[0].threshold 
                val_type = 'skew' 
                skew_drift_type_num = sd.skew_measurements[0].type 
                
            elif validation_type == 'drift': 
                value = sd.drift_measurements[0].value 
                threshold = sd.drift_measurements[0].threshold 
                val_type = 'drift' 
                skew_drift_type_num = sd.drift_measurements[0].type 
            else: 
                print("Please specify skew or drift") 
                
            if skew_drift_type_num == 1: 
                skew_drift_type = 'L_INFTY' 
            elif skew_drift_type_num == 2: 
                skew_drift_type = 'JENSEN_SHANNON_DIVERGENCE' 
            elif skew_drift_type_num == 3: 
                skew_drift_type = 'NORMALIZED_ABSOLUTE_DIFFERENCE' 
            else: 
                skew_drift_type = 'UNKNOWN' 
                
            df_sd.loc[len(df_sd.index)] = pd.Series({ 
                'feature_nm': feature, 
                'skew_drift': {'type': skew_drift_type, 'validation_type': val_type, 'value': value, 'threshold': threshold}
                }) 
                
            print(feature) 
            
    df_anomalies = pd.merge(df_anomalies, df_sd, on='feature_nm', how='left') 
    
    # load data stats into BQ table 
    client = bigquery.Client(project=project_id) 
    
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND', 
                                        schema=[bigquery.SchemaField(
                                                    'model_nm', 'STRING'), 
                                                bigquery.SchemaField(
                                                    'update_ts', 'TIMESTAMP'), 
                                                bigquery.SchemaField(
                                                    'feature_nm', 'STRING'), 
                                                bigquery.SchemaField(
                                                    'short_description', 'STRING'), 
                                                bigquery.SchemaField(
                                                    'long_description', 'STRING'),
                                                bigquery.SchemaField('skew_drift', 'RECORD', 
                                                    fields=[bigquery.SchemaField('type', 'STRING'), 
                                                            bigquery.SchemaField('validation_type', 'STRING'), 
                                                            bigquery.SchemaField('value', 'FLOAT'), 
                                                            bigquery.SchemaField('threshold', 'FLOAT')]), 
                                                ],) # create new table or append if already exists 

    anomalies_table  = f'{project_id}.{dest_anomalies_bq_dataset}.bq_data_anomalies' 
    
    job = client.load_table_from_dataframe(
        df_anomalies, anomalies_table, job_config=job_config 
        ) 
    job.result() 
    table = client.get_table(anomalies_table) 
    print( 
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), anomalies_table
        )
    ) 
    
from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

# Visualize Stats Component: monitoring component for visualizing statistics created by generate_stats component
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-tfdv-slim:1.0.0",
    output_component_file="visualize_stats.yaml"
)
def visualize_stats(
    statistics: Input[Artifact],
    view: Output[HTML],
    op_type: str = "",
    stats_nm: str = "",
    base_stats_path: str = None,
    base_stats_nm: str = ""
):
    '''
    Inputs:
        - op_type: training or serving or predictions
        - stats_nm: name of new stats
        - base_stats_path: path to base stats in gcs (usually training)
        - base_stats_nm: base stats name
        - statistics: path to statistics imported from generate stats component
    
    Outputs:
        - html artifact
    '''

    import tensorflow_data_validation as tfdv
    from tensorflow_data_validation.utils.display_util import (
        get_statistics_html,
    )

    # load stats
    stats = tfdv.load_statistics(input_path=statistics.uri)
    print("statistics uri")
    print(statistics.uri)

    # create html content
    if base_stats_path is not None:
        base_stats = tfdv.load_statistics(input_path=base_stats_path)

        html = get_statistics_html(
            lhs_statistics=stats,
            lhs_name=stats_nm,
            rhs_statistics=base_stats,
            rhs_name=base_stats_nm,
        )
    
    else:
        html = get_statistics_html(
            lhs_statistics=stats,
            lhs_name=stats_nm,
        )

    # ensure view is stored as html (this will set content-type to text/html)
    if not view.path.endswith(".html"):
        view.path += ".html"

    print("view path")
    print(view.path)

    # write html to output file
    with open(view.path, "w") as f:
        f.write(html)