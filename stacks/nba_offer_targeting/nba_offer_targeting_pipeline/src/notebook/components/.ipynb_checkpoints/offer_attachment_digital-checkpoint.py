import kfp
import pandas as pd
from kfp import dsl
# from kfp.v2.dsl import (Model, Input, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple
# Create Training Dataset for training pipeline

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="bq_import_df.yaml",
)

def offer_attachment_digital(irpc_base_csv: str
                   , irpc_offer_plans_csv: str 
                   , irpc_offer_prices_csv: str
                   , channel: str
                   , save_data_path: str
                  ): 
    import pandas as pd 
    import numpy as np 

    def convert_df_to_list_dict(df, 
                                channel: str = 'digital' # digital or casa
                                ): 

        import re
        import logging 
        from datetime import datetime

        print(df.head())

        # Change dataset / table + sp table name to version in bi-layer
        if channel == 'digital': 
            df = df
        elif channel == 'casa': 
            df = df.iloc[:4]
        else: 
            print("a parameter 'channel' can only be either 'digital' or 'casa'")

        # create list_hsia_speed = [250, 500, 1000, 1500, 3000]
        list_hsia_speed = df.columns[3:]

        print(list_hsia_speed)

        list_hsia_speed = [int(re.search(r'\d+', speed).group()) for speed in list_hsia_speed]

        # create dictionary of hsia_prices 
        # Convert DataFrame to a dictionary
        dict_hsia_details = df.to_dict(orient='list')

        return list_hsia_speed, dict_hsia_details

    def find_digital_irpc_offers(list_hsia_speed: str, # list_hsia_speed = [250, 500, 1000, 1500, 3000] as of Feb 2024
                             dict_hsia_plans_details: str, # e.g. internet_250: ['1P:Regular (Internet 250)', '1P:Tier 0 (Internet 250)', '1P:Tier 1 (Internet 250)']
                             dict_hsia_prices_details: str, # e.g. internet_250: [105, 100, 95, 85, 75]
                             row, 
                             offer_num: int
                             ):

        import pandas as pd
        import numpy as np 
        
        cust_id, ban, lpds_id = row['cust_id'], row['bacct_num'], row['lpds_id']
        
        if offer_num == 1: 
            provisioned_hs_speed_numeric, hs_max_speed_numeric, total_charges = row['provisioned_hs_speed_numeric'], row['hs_max_speed_numeric'], row['total_charges']
        elif offer_num == 2: 
            provisioned_hs_speed_numeric, hs_max_speed_numeric, total_charges = row['hsia_speed1'], row['hs_max_speed_numeric1'], row['hsia_price1']
        elif offer_num == 3: 
            provisioned_hs_speed_numeric, hs_max_speed_numeric, total_charges = row['hsia_speed2'], row['hs_max_speed_numeric2'], row['hsia_price2']
        
        if provisioned_hs_speed_numeric is None and hs_max_speed_numeric is None and total_charges is None: 
            return [None, None, None, None]

        ### 1. Find the smallest number in hsia_speed that is greater than provisioned_hs_speed_numeric

        # exception: if the current hsia_speed >= 150 AND hsia_speed < 500, then change the value to 499 so that it bypasses 250 
        ## Internet 150 customers should not be shown Internet 250 tier, as they are upsped in the backend to speeds of 300mbps ##
        if provisioned_hs_speed_numeric >= 150 and provisioned_hs_speed_numeric < 500: 
            provisioned_hs_speed_numeric = 499
            
        # It doesn't make sense when hs_max_speed_numeric < provisioned_hs_speed_numeric. 
        # Update hs_max_speed_numeric to provisioned_hs_speed_numeric when it's the case. 
        if hs_max_speed_numeric < provisioned_hs_speed_numeric: 
            hs_max_speed_numeric = provisioned_hs_speed_numeric

        # store minimum hsia speed offer available greater than (or equal to) the current speed in 'hsia_speed'
        # AND 
        # minimum speed offer available where the highest offer price in that speed is greater than or equal to 'total_charges'

        # requirements:
            # - list_hsia_speed
            # - np.max([spd for spd in dict_hsia_prices_details[f'internet_{hsia_speed}'] if not pd.isna(spd)]) --> max offer price in the internet speed
            # - compare max offer price vs total_charges 
            # - since internet_250 does not have offer tier equal to or greater than $125, we need to move on to the next speed, which is 500
            # - since internet_500 does not have offer tier equal to or greater than $125, we need to again move on to the next speed, which is 1000
            # - internet_1000 has an offer tier equal to or greater than $125, so hsia_speed = 1000

        try: 
            if offer_num == 1: 
                hsia_speed = np.min([spd for spd in list_hsia_speed if spd >= provisioned_hs_speed_numeric and spd <= hs_max_speed_numeric and not pd.isna(spd)]) # --> 250
            else: 
                hsia_speed = np.min([spd for spd in list_hsia_speed if spd > provisioned_hs_speed_numeric and spd <= hs_max_speed_numeric and not pd.isna(spd)])

            max_offer_price = np.max([spd for spd in dict_hsia_prices_details[f'internet_{hsia_speed}'] if not pd.isna(spd)])

        except ValueError: 
            return [None, None, None, None]

        try: 
            while max_offer_price < total_charges: 
                hsia_speed_idx = list_hsia_speed.index(hsia_speed)
                hsia_speed = list_hsia_speed[hsia_speed_idx + 1]
                max_offer_price = np.max([spd for spd in dict_hsia_prices_details[f'internet_{hsia_speed}'] if not pd.isna(spd)])

        except IndexError: 
            print(f"The customer {cust_id} {ban} {lpds_id} is currently paying > $145, thus not eligible for any offers")

        ### 2. Find the smallest number in dict_hsia_prices_details[hsia_speed] that is greater (or equal to) than total_charges --> 75

        list_hsia_price = dict_hsia_prices_details[f'internet_{hsia_speed}']

        if len([price for price in list_hsia_price if price >= total_charges]) > 0: 
            
            try: 
                if offer_num == 1: 
                    hsia_price = np.min([price for price in list_hsia_price if price >= total_charges])
                else: 
                    hsia_price = np.min([price for price in list_hsia_price if price > total_charges])
                    
            except ValueError: 
                return [None, None, None, None]

            try: 
                # scenario when the same price DOES EXIST in the next speed offer (e.g. if internet_500, check internet_1000 for $95 offer ==> YES): 
                hsia_speed_idx = list_hsia_speed.index(hsia_speed)
                next_hsia_speed = list_hsia_speed[hsia_speed_idx + 1]

                if hsia_price in dict_hsia_prices_details[f'internet_{next_hsia_speed}'] and hsia_speed == provisioned_hs_speed_numeric: 
                    list_hsia_price = dict_hsia_prices_details[f'internet_{next_hsia_speed}']
                    hsia_speed = next_hsia_speed 

            except IndexError: 
                print(f"For {cust_id} {ban} {lpds_id}, the hsia_speed {hsia_speed} is the fastest internet speed available.")

            except UnboundLocalError: 
                print(f"For {cust_id} {ban} {lpds_id}, the hsia_speed {hsia_speed} is the fastest internet speed available.")

            # scenario when the same price DOES NOT EXIST in the next speed offer (e.g. if internet_1000, check internet_1500 for $95 offer ==> NO): 

            if hsia_price in list_hsia_price:
                plan_idx = list_hsia_price.index(hsia_price)    

            ### 4. Call the plan name by hsia_plans[provisioned_hs_speed_numeric]==250][plan_idx==4] --> "1P: Tier 3 (Internet 250)"

            hsia_plan_name = dict_hsia_plans_details[f'internet_{hsia_speed}'][plan_idx]

            return [hsia_speed, hs_max_speed_numeric, hsia_price, hsia_plan_name] 

        else: 

            return [None, None, None, None]

    # read the above 3 csv's (bq_irpc_digital_1p_base.csv, irpc_offer_1p_plans.csv, irpc_offer_prices.csv
    # read bq_irpc_digital_1p_base and store in df
    df_base = pd.read_csv(irpc_base_csv)
    df_plans = pd.read_csv(irpc_offer_plans_csv)
    df_prices = pd.read_csv(irpc_offer_prices_csv)
    
    # convert irpc_offer_1p_plans and irpc_offer_prices to lists and dictionaries
    # - list_hsia_speed
    # - dict_hsia_details
    list_hsia_speed, dict_hsia_plans_details = convert_df_to_list_dict(df_plans, channel)
    list_hsia_speed, dict_hsia_prices_details = convert_df_to_list_dict(df_prices, channel)
    
    offer_1_list, offer_2_list, offer_3_list = [] , [], [] 

    df_base[['hsia_speed1', 'hs_max_speed_numeric1', 'hsia_price1', 'promo_seg1']]  = df_base.apply(lambda row: pd.Series(find_digital_irpc_offers(list_hsia_speed, dict_hsia_plans_details, dict_hsia_prices_details, row, 1)), axis=1)
    df_base[['hsia_speed2', 'hs_max_speed_numeric2', 'hsia_price2', 'promo_seg2']]  = df_base.apply(lambda row: pd.Series(find_digital_irpc_offers(list_hsia_speed, dict_hsia_plans_details, dict_hsia_prices_details, row, 2)), axis=1)
    df_base[['hsia_speed3', 'hs_max_speed_numeric3', 'hsia_price3', 'promo_seg3']]  = df_base.apply(lambda row: pd.Series(find_digital_irpc_offers(list_hsia_speed, dict_hsia_plans_details, dict_hsia_prices_details, row, 3)), axis=1)
    
    df_base = df_base[['cust_id', 'bacct_num', 'fms_address_id', 'lpds_id', 'candate', 'OPTIK_TV_IND', 'HSIA_IND', 'hs_max_speed_numeric', 'provisioned_hs_speed_numeric', 
                       'rpp_hsia_end_dt', 'rpp_ttv_end_dt', 'total_charges', 'promo_seg1', 'promo_seg2', 'promo_seg3']] 
    
    df_base.to_csv(save_data_path)