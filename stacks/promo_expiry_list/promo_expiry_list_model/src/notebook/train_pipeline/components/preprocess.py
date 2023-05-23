from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="promo_expiry_list_model_preprocess.yaml".format(SERVICE_TYPE),
)
def preprocess(
        account_consl_view: str,
        account_bill_view: str,
        hs_usage_view: str,
        demo_income_view: str,
        promo_expiry_view: str,
        gpon_copper_view: str,
        clckstrm_telus_view: str, 
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
