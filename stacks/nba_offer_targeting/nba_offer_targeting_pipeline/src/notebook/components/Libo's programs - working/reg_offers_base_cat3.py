
import kfp
from kfp import dsl
# from kfp.v2.dsl import (Model, Input, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple

# Create IRPC Digital 1P, Digital 2P, and Casa base tables
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="reg_offers_base_existing.yaml",
)
def reg_offers_base_cat3(project_id: str
                            , offer_parameter: str
                            , whsia_eligible_base: str
                            , qua_base: str
                            , token: str
                            ):
 
    import pandas as pd
    import sys
    import os
    import re
    import time
    from pathlib import Path
    import pdb
    from yaml import safe_load

    from google.cloud import bigquery
    import logging 
    from datetime import datetime
    
    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()

#     #### For prod 
#     client = bigquery.Client(project=project_id)
#     job_config = bigquery.QueryJobConfig()

    """
    This program creates eligible bases for three categories of customers
      - existing home solutions customers
      - naked mobility customers
      - cat3

    Initially drafted in Feb 2024
      - @author: T892899


    v0d4 @author: T892899; Feb 29, 2024
      - update ffh_bas query to welcome 9167815983798937909
      - double check pending orders


    v0d5 @author: T892899; Mar 4th, 2024
      - update a few tables
       - bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl
       - bi-srv-hsmdet-pr-7b9def.campaign_data.bq_fda_mob_mobility_base
       - bi-srv-divgdsa-pr-098bdd.common.bq_mobility_active_data


    v0d6 @author: T892899; Mar 6th, 2024
      - Add SHS Pro-install eligibility flag
        - this table is still a temp table, which requires more formal solution in the future
      - Adjust MOB offer filters


    v0d7 @author: T892899; Mar 8th, 2024
      - Update Offer parameter BQ table - formal source / daily update
      - New function for cr8bqt_sql_BI
      - Update demostats data source


    v0d8 @author: T892899; Mar 13th, 2024
      - Update SHS Pro-install eligibility BQ table - BI layer
      - Rename NCID to offer_code
      - Modify function last_dt_check


    v0d9 @author: T892899; Mar 22th, 2024
      - Update some project names to be a variable, for easy stg to srv update 
      - Add dwelling type to existing customers
      
    Notes: Feb 2024
     - SHS eligibility flag need to be added
     - prod_cd to be changed to bq table
     - TOS tier model need to be added
     - HSIC tier model need to be added
     - valid start/end date format
        - details to confirm with Weibo/Francis
        - 2022-01-01T00:00:00.000Z

    """

    # define some functions
    # please feel free to improve as fit
    def cr8bqt_sql_BI(
        clnt,
        sql_base,
        opt,
        est_num
        ): 

        sql_s1 =f"""

            TRUNCATE TABLE `{opt}_temp`;   
            INSERT INTO `{opt}_temp`
            {sql_base}            

            """ 

        crdt_s1 = clnt.query(sql_s1).to_dataframe()

        tableid = opt.split('.')

        sql_s2 =f"""

                SELECT
                  row_count
                FROM `{tableid[0]}.{tableid[1]}`.__TABLES__
                where table_id = '{tableid[2]}_temp'
                ;

            """ 

        crdt_s2 = clnt.query(sql_s2).to_dataframe()

        if crdt_s2['row_count'][0] > est_num:
            sql_s3 =f"""

                TRUNCATE TABLE `{opt}`;   
                INSERT INTO `{opt}`
                select * from `{opt}_temp`

            """ 
            crdt_s3 = clnt.query(sql_s3).to_dataframe()

        else:
            raise Exception(f"{opt}_temp has {crdt_s2['row_count'][0]} rows -- seems low. Update aborted.")

    def last_dt_check(
        clnt,
        ipt,
        part_dt,
        wd
        ): 

        sql_s1 =f"""

            with
                max_date1 as (
                    SELECT 
                        0 as a
                        , {part_dt} 
                        , count({part_dt}) as mxx
                    FROM `{ipt}`
                        WHERE {part_dt} >= DATE_SUB(CURRENT_DATE(), INTERVAL {wd} DAY)
                    group by a, {part_dt}
                    order by a, {part_dt} desc
                    )
                ,  max_date2 as (
                    select
                        0 as a
                        , avg(mxx) as mxx_avg
                    from max_date1
                        group by a
                    )

                select
                    cast(max(a.{part_dt}) AS STRING) as part_dt
                from max_date1 a left join max_date2 b
                on a.a = b.a
                where a.mxx >= b.mxx_avg * 0.8

        """ 

        crdt_s1 = clnt.query(sql_s1).to_dataframe()

        return(crdt_s1['part_dt'][0])

    # Beginning of Part 3

    # Creating eligible base for CSD channel technicians
    # for brand new customers with pending orders 


    # Pull Offer Info

    sq0l =f"""

       with
            max_dt as (    
              SELECT 
              max(part_dt) as part_dt
              FROM `{offer_parameter}` 
            )
        select
            Replace(a.Offer_Number, '-', '_') as Offer_Number2
            , a.* 
            , CAST(a.valid_start_ts AS DATE FORMAT 'MON DD, YYYY') AS valid_start_dt 
            , CAST(a.valid_end_ts AS DATE FORMAT 'MON DD, YYYY') AS valid_end_dt
        from `{offer_parameter}` a 
        inner join max_dt b
        on a.part_dt = b.part_dt
        where a.if_active = 1 and a.HS_filters is not null and if_cat3 = 1

    """ 

    offer_info = client.query(sq0l).to_dataframe()


    # Check latest snapshot date with reasonable counts


    last_dt_pid = last_dt_check(clnt = client,
                                ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details',
                                part_dt = 'part_load_dt',
                                wd = 14 )

    last_dt_pi = last_dt_check(clnt = client,
                                ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance',
                                part_dt = 'part_load_dt',
                                wd = 14 )

    last_dt_spd = last_dt_check(clnt = client,
                                ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_premise_universe',
                                part_dt = 'part_dt' ,
                                wd = 14)



    # Create cat3 customer profile - base

    sq0l =f"""

        with cat3_bas as (

                select  
                    cust_id
                    , bill_account_number as bacct_num
                    , lpds_id
                    , max(case when product_family = 'TOS' then 1 else 0 end) as TOS_IND
                    , max(case when product_family = 'HSIC' then 1 else 0 end) as HSIA_ind
                    , max(case when product_family = 'SMHM' then 1 else 0 end) as SHS_ind
                    , max(case when product_family = 'TTV' then 1 else 0 end) as OPTIK_TV_IND
                    , max(case when product_family = 'SING' then 1 else 0 end) as HP_IND
                    , max(case when product_family = 'WIFI' then 1 else 0 end) as WFP_ind
                    , max(case when product_family = 'WHSIA' then 1 else 0 end) as SMART_HUB_IND
                    , max(case when product_family = 'LWC' then 1 else 0 end) as LWC_ind
                    , max(case when product_family = 'SWS' then 1 else 0 end) as SWS_ind
                    , max(case when product_family = 'SOD' then 1 else 0 end) as SOD_ind
                    , max(case when product_family = 'HPRO' then 1 else 0 end) as HPRO_ind
                 from `bi-srv-hsmsd-3c-pr-ca2cd4.hsmsd_3c_rpt_dataset.bq_rpt_chnl_order_ffh_dtl_view`
                where is_current_order = 1 and current_yield_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 100 DAY)
                    and (current_yield_sub_status = 'Pending')
                    and current_order_status = 'Processing'
                    and soi_transaction_type = 'Enroll'
                    and is_existing_customer = 0
                    group by 1,2,3

        )

        , spd as (

            select distinct
                lpds_id
                , fms_address_id
                , postal_cd as SERV_POSTAL_CODE
                , hs_max_speed_bonded as HSIA_MAX_SPD
                , SYSTEM_PROVINCE_CD as SERV_PROV
                , snet_premise_type_cd
                , ttv_eligible_ind as OPTIK_ELIGIBLE
                , ttv_port_availability 
                , gpon_sellable_ind as TECH_GPON
            from `bi-srv-divgdsa-pr-098bdd.common.bq_premise_universe` 
            WHERE part_dt = '{last_dt_spd}'
        )


        , pid as (

                select 
                  cust_id
                  , max(case when access_technology = 'COPPER' then 1 else 0 end) as cpf_acctech_copper_ind
                  , max(case when access_technology = 'FIBRE' then 1 else 0 end) as cpf_acctech_fibre_ind
                  , max(case when access_technology = 'WIRELESS' then 1 else 0 end) as cpf_acctech_wls_ind
                  , max(case when access_technology = 'SATELLITE' then 1 else 0 end) as cpf_acctech_satellite_ind
                  , sum(case when service_instance_type_cd != 'DIIC' then 1 else 0 end) as cpf_prod_cnt
                  , max(case when service_instance_type_cd = 'DIIC' then 1 else 0 end) as cpf_diic_ind
                  , max(case when service_instance_type_cd = 'HSIC' then 1 else 0 end) as cpf_hsic_ind
                  , max(case when service_instance_type_cd = 'HSIC' then provisioned_hs else null end) as cpf_provisioned_hs
                  , max(case when service_instance_type_cd = 'LWC' then 1 else 0 end) as cpf_lwc_ind
                  , max(case when service_instance_type_cd = 'PIK' then 1 else 0 end) as cpf_pik_ind
                  , max(case when service_instance_type_cd = 'SHS' then 1 else 0 end) as cpf_shs_ind
                  , max(case when prod_intrnl_nm in ('Smart Automation Plus', 'Smart Automation Plus (V2)'
                          , 'Smart Camera (V2)', 'Secure Business: Smart Camera') then 1 else 0 end) as cpf_shs_ind2
                  , max(case when service_instance_type_cd = 'SING' then 1 else 0 end) as cpf_sing_ind
                  , max(case when service_instance_type_cd = 'STV' then 1 else 0 end) as cpf_stv_ind
                  , max(case when service_instance_type_cd = 'SWS' then 1 else 0 end) as cpf_sws_ind
                  , max(case when service_instance_type_cd = 'STMP' then 1 else 0 end) as cpf_stmp_ind
                  , max(case when service_instance_type_cd = 'TOS' then 1 else 0 end) as cpf_tos_ind
                  , max(case when service_instance_type_cd = 'TOS'
                          and prod_cd = '40983311' then 1 else 0 end) as cpf_tos_basic_ind
                  , max(case when service_instance_type_cd = 'TOS'
                          and prod_cd = '41079641' then 1 else 0 end) as cpf_tos_standard_ind
                  , max(case when service_instance_type_cd = 'TTV' then 1 else 0 end) as cpf_ttv_ind
                  , max(case when service_instance_type_cd = 'WFP' then 1 else 0 end) as cpf_wfp_ind
                  , max(case when service_instance_type_cd = 'WHSIA' then 1 else 0 end) as cpf_whsia_ind  
                  , max(case when service_instance_type_cd = 'HPRO' then 1 else 0 end) as cpf_HPRO_ind   

                  from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance`
                        where part_load_dt = '{last_dt_pi}'
                      and product_instance_status_cd = 'A' and current_ind = 1
                    group by cust_id

        )    

        , ffh_bas as (
            select 
                a.*
                , b.* except (lpds_id)
                , c.* except (cust_id)

                , 0 as EX_STANDARD_EX
                , 0 as std_exclud2
                , 'FIFA' as PROVISIONING_SYSTEM
                , 0 as MNH_MOB_BAN
                , '' as SHS_CONTRACT_END_DT
                , 0 as REWARDS_POINT_BALANCE
                , CAST(NULL AS TIMESTAMP) as ACCT_START_DT
                , 0 as OPTIK_PACKAGE_NUM
                , 0 as STV_IND
                , 0 as PIK_TV_IND

                , case when (( d.HTA1519_pct > 0.25)
                                or ( d.HTA2024_pct > 0.25)
                                or ( d.HTA2529_pct > 0.25)
                                or ( d.HTA3034_pct > 0.25)
                                or ( d.HTA3539_pct > 0.25)
                                or ( d.HTA4044_pct > 0.25)

                                or ( d.HMA1519_pct > 0.25)
                                or ( d.HMA2024_pct > 0.25)
                                or ( d.HMA2529_pct > 0.25)
                                or ( d.HMA3034_pct > 0.25)
                                or ( d.HMA3539_pct > 0.25)
                                or ( d.HMA4044_pct > 0.25)

                                or ( d.HFA1519_pct > 0.25)
                                or ( d.HFA2024_pct > 0.25)
                                or ( d.HFA2529_pct > 0.25)
                                or ( d.HFA3034_pct > 0.25)
                                or ( d.HFA3539_pct > 0.25)
                                or ( d.HFA4044_pct > 0.25)) then 1 else 0 end as demo_hs_189_ind

                , case when d.baskid > 50 then 1 else 0 end as demo_hs_188_ind

                , RAND() as rand_seed1

                , 0 as hs_202_ind

                , f.wHSIAQualTypeMarketing

                , case when g1.ACQ_DATE is not null
                            or g3.ACQ_DATE is not null
                            then 1 else 0 end as alarm_full_universe

                , 0 as hs_71_ind

            from cat3_bas a            
            left join spd b on a.lpds_id = cast(b.lpds_id as STRING)
            left join pid c on a.cust_id = cast(c.cust_id as STRING) 
            left join `bi-srv-divgdsa-pr-098bdd.environics_derived.bq_demostats_2023_features` d on b.SERV_POSTAL_CODE = d.code
            left join `{whsia_eligible_base}` f on a.LPDS_ID = cast(f.LPDSId as STRING)
            left join `bi-srv-hsmdet-pr-7b9def.hsmdet_public.bq_pub_fda_alarm_full_universe` g1
                on a.cust_id is not null and a.cust_id = cast(g1.cust_id as STRING)
            left join `bi-srv-hsmdet-pr-7b9def.hsmdet_public.bq_pub_fda_alarm_full_universe` g3
                on b.FMS_ADDRESS_ID is not null and b.FMS_ADDRESS_ID = g3.FMS_ADDRESS_ID        

            )


    """ 

    # Create eligible base for each offer

    sql_all = sq0l
    n_offer = offer_info.shape[0]
    for ii in range(n_offer):
        ii2 = ii + 1

        sql_b1 = (
                f""", {offer_info['Offer_Number2'][ii]} as (
                select distinct cast(cust_id as int64) as cust_id \n
                , cast(bacct_num as int64) as bacct_num \n
                , cast(lpds_id as int64) as lpds_id \n
                , cast(ACCT_START_DT as timestamp) as candate \n
                , '{offer_info['Category'][ii]}' as Category  \n
                , '{offer_info['Subcategory'][ii]}' as Subcategory  \n 
                , '' as digital_category
                , '{offer_info['promo_seg'][ii]}' as promo_seg  \n 
                , '{offer_info['NCID'][ii]}' as offer_code  \n 
                , cast('{offer_info['valid_start_dt'][ii]}' AS DATE) as ASSMT_VALID_START_TS  \n
                , cast('{offer_info['valid_end_dt'][ii]}' AS DATE) as ASSMT_VALID_END_TS  \n 
                , {str(offer_info['rk'][ii])} as rk  \n
                from ffh_bas \n where  {offer_info['HS_filters'][ii]} )  \n """
               )

        sql_all0 = sql_all + sql_b1

        sql_all = sql_all0


    # Union eligible bases

    sql_all0 = sql_all + f" select * from {offer_info['Offer_Number2'][0]} \n"
    sql_all = sql_all0
    n_offer = offer_info.shape[0] - 1
    for ii in range(n_offer):
        ii2 = ii + 1
        sql_b2 = f" union all select * from {offer_info['Offer_Number2'][ii2]}  \n"
        sql_all0 = sql_all + sql_b2
        sql_all = sql_all0


    # check base count before creating multiple eligible base
    # 

    sq0l ="""

            select count(distinct cust_id) as cnt 
             from `bi-srv-hsmsd-3c-pr-ca2cd4.hsmsd_3c_rpt_dataset.bq_rpt_chnl_order_ffh_dtl_view`
            where is_current_order = 1 
                and current_yield_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
                and (current_yield_sub_status = 'Pending')
                and current_order_status = 'Processing'
                and soi_transaction_type = 'Enroll'
                and is_existing_customer = 0

    """ 

    df_check = client.query(sq0l).to_dataframe()



    start_time = time.time()

    if df_check['cnt'][0] > 500:
        cr8bqt_sql_BI(
            clnt = client,
            sql_base = sql_all,
            opt = qua_base,
            est_num = 1500
        )

    else:
        raise Exception(f"Cat3 base has {df_check['cnt'][0]} rows -- seems low. Update aborted.")

    # print("This step took --- %s seconds ---" % (time.time() - start_time))


    # End of Part 3

    # End of this program