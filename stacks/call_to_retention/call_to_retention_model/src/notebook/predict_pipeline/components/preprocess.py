from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="call_to_retention_model_preprocess.yaml".format(SERVICE_TYPE),
)
def preprocess(
        promo_expiry_list_view: str, 
        account_consl_view: str, 
        account_bill_view: str, 
        account_discounts_view: str, 
        hs_usage_view: str, 
        demo_income_view: str, 
        gpon_copper_view: str, 
        price_plan_view: str, 
        clckstrm_telus_view: str, 
        call_history_view: str, 
        save_data_path: str,
        project_id: str,
        dataset_id: str
):

    from google.cloud import bigquery
    import pandas as pd
    import gc
    import time

    client = bigquery.Client(project=project_id)
    
    #1.df_promo_expiry_list
    promo_expiry_list_set = f"{project_id}.{dataset_id}.{promo_expiry_list_view}" 
    build_df_promo_expiry_list = '''SELECT * FROM `{promo_expiry_list_set}`'''.format(promo_expiry_list_set=promo_expiry_list_set)
    df_promo_expiry_list = client.query(build_df_promo_expiry_list).to_dataframe()
    df_promo_expiry_list = df_promo_expiry_list.set_index('ban')
    df_join = df_promo_expiry_list.copy()
    print('......df_promo_expiry_list done')
    
    #2.df_consl
    consl_data_set = f"{project_id}.{dataset_id}.{account_consl_view}" 
    build_df_consl = '''SELECT * FROM `{consl_data_set}`'''.format(consl_data_set=consl_data_set)
    df_consl = client.query(build_df_consl).to_dataframe()
    df_mix = df_consl[[
        'ban',
        'customer_tenure', 
        'product_mix_all',
        'hsic_count',
        'ttv_count',
        'sing_count',
        'mob_count',
        'shs_count',
        'new_hsic_ind',
        'new_ttv_ind',
        'new_sing_ind',
        'new_c_ind',
        'new_smhm_ind',
        'mnh_ind'
    ]]
    df_mix = df_mix.drop_duplicates(subset=['ban']).set_index('ban').add_prefix('productMix_').fillna(0)
    df_join = df_join.join(df_mix)
    del df_mix
    gc.collect()
    print('......df_consl done')
    
    #3.df_bill
    bill_data_set = f"{project_id}.{dataset_id}.{account_bill_view}" 
    build_df_bill = '''SELECT * FROM `{bill_data_set}`'''.format(bill_data_set=bill_data_set)
    df_bill = client.query(build_df_bill).to_dataframe() 
    df_bill = df_bill.set_index('ban').add_prefix('ffhBill_')
    df_join = df_join.join(df_bill).fillna(0) 
    del df_bill
    gc.collect()
    print('......df_bill done')
    
    #4.df_discounts
    discounts_data_set = f"{project_id}.{dataset_id}.{account_discounts_view}" 
    build_df_discounts = '''SELECT * FROM `{discounts_data_set}`'''.format(discounts_data_set=discounts_data_set)
    df_discounts = client.query(build_df_discounts).to_dataframe() 
    df_discounts = df_discounts.set_index('ban').add_prefix('ffhDiscounts_')
    df_join = df_join.join(df_discounts).fillna(0) 
    del df_discounts
    gc.collect()
    print('......df_discounts done')

    #5.df_hs_usage
    hs_usage_data_set = f"{project_id}.{dataset_id}.{hs_usage_view}" 
    build_df_hs_usage = '''SELECT * FROM `{hs_usage_data_set}`'''.format(hs_usage_data_set=hs_usage_data_set)
    df_hs_usage = client.query(build_df_hs_usage).to_dataframe() 
    df_hs_usage = df_hs_usage.set_index('ban').add_prefix('hsiaUsage_')
    df_join = df_join.join(df_hs_usage).fillna(0) 
    del df_hs_usage
    gc.collect()
    print('......df_hs_usage done')

    #6.df_income
    demo_income_data_set = f"{project_id}.{dataset_id}.{demo_income_view}" 
    build_df_demo_income = '''SELECT * FROM `{demo_income_data_set}`'''.format(demo_income_data_set=demo_income_data_set)
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
    df_join = df_join.join(df_group_income.fillna(df_group_income.median()))
    del df_group_income
    del df_income
    gc.collect()
    print('......df_income done')

    #7.df_gpon_copper
    gpon_copper_data_set = f"{project_id}.{dataset_id}.{gpon_copper_view}"
    build_df_gpon_copper = '''SELECT * FROM `{gpon_copper_data_set}`'''.format(gpon_copper_data_set=gpon_copper_data_set)
    df_gpon_copper = client.query(build_df_gpon_copper).to_dataframe()
    df_gpon_copper = df_gpon_copper.set_index('ban')
    df_join = df_join.join(df_gpon_copper.add_prefix('infra_')).fillna(0)
    del df_gpon_copper
    gc.collect()
    print('......df_gpon_copper done')

    #8.df_price_plan
    price_plan_data_set = f"{project_id}.{dataset_id}.{price_plan_view}"
    build_df_price_plan = '''SELECT * FROM `{price_plan_data_set}`'''.format(price_plan_data_set=price_plan_data_set)
    df_price_plan = client.query(build_df_price_plan).to_dataframe()
    df_price_plan = df_price_plan.set_index('ban')
    df_pp_dummies = pd.get_dummies(df_price_plan[['price_plan']])
    df_pp_dummies.columns = df_pp_dummies.columns.str.replace('&', 'and')
    df_pp_dummies.columns = df_pp_dummies.columns.str.replace(' ', '_')
    df_price_plan = df_price_plan.join(df_pp_dummies)
    df_price_plan.drop(columns=['price_plan'], axis=1, inplace=True)
    print(df_price_plan.columns)
    df_join = df_join.join(df_price_plan.add_prefix('infra_')).fillna(0)
    del df_price_plan
    gc.collect()
    print('......df_price_plan done')

    #9.df_clckstrm_telus
    clckstrm_telus_data_set = f"{project_id}.{dataset_id}.{clckstrm_telus_view}" 
    build_df_clckstrm_telus = '''SELECT * FROM `{clckstrm_telus_data_set}`'''.format(clckstrm_telus_data_set=clckstrm_telus_data_set)
    df_clckstrm_telus = client.query(build_df_clckstrm_telus).to_dataframe() 
    df_clckstrm_telus = df_clckstrm_telus.set_index('ban').add_prefix('clckstrmData_')
    df_join = df_join.join(df_clckstrm_telus).fillna(0) 
    del df_clckstrm_telus
    gc.collect()
    print('......df_clckstrm_telus done')

    #10.df_call_history
    call_history_data_set = f"{project_id}.{dataset_id}.{call_history_view}" 
    build_df_call_history = '''SELECT * FROM `{call_history_data_set}`'''.format(call_history_data_set=call_history_data_set)
    df_call_history = client.query(build_df_call_history).to_dataframe() 
    df_call_history = df_call_history.set_index('ban').add_prefix('callHistory_')
    df_join = df_join.join(df_call_history)
    df_join[['callHistory_frequency', 'callHistory_have_called']] = df_join[['callHistory_frequency', 'callHistory_have_called']].fillna(0)
    df_join[['callHistory_recency']] = df_join[['callHistory_recency']].fillna(999)
    del df_call_history
    gc.collect()
    print('......df_call_history done')

    #column name clean-up
    df_join.columns = df_join.columns.str.replace(' ', '_')
    df_join.columns = df_join.columns.str.replace('-', '_')

    #df_final
    df_final = df_join.copy()
    del df_join
    gc.collect()
    print('......df_final done')

    for f in df_final.columns:
        df_final[f] = list(df_final[f])

    df_final.to_csv(save_data_path, index=True, compression='gzip') 
    del df_final
    gc.collect()
    print(f'......csv saved in {save_data_path}')
    time.sleep(120)
