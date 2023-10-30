from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

# Validate Stats Component: monitoring component for validating serving or predictions stats against training data stats
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
    