
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
def reg_offers_base_existing(project_id: str
                            , offer_parameter: str
                            , whsia_eligible_base: str
                            , shs_professional_install: str
                            , prod_cd2remove: str
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
            --INSERT INTO `{opt}_temp`
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

    # Beginning of Part 1
    # Creating eligible base for existing home solutions customers
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
        where a.if_active = 1 and a.HS_filters is not null
    """ 

    offer_info = client.query(sq0l).to_dataframe()

    # Check latest snapshot date with reasonable counts

    last_dt_spd = last_dt_check(clnt = client,
                                ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_premise_universe',
                                part_dt = 'part_dt',
                                wd = 14 )

    last_dt_pid = last_dt_check(clnt = client,
                                ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details',
                                part_dt = 'part_load_dt',
                                wd = 14 )

    last_dt_pi = last_dt_check(clnt = client,
                                ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance',
                                part_dt = 'part_load_dt',
                                wd = 14 )

    last_dt_gateway = last_dt_check(clnt = client,
                                ipt = 'cio-datahub-enterprise-pr-183a.ent_resrc_config.bq_product_instance_gateway_daily_snpsht',
                                part_dt = 'snapshot_load_dt',
                                wd = 60 )

    # last_dt_game = last_dt_check(clnt = client,
    #                             ipt = 'cio-datahub-enterprise-pr-183a.ent_resrc_performance_device_kpi.bq_cloudcheck_game_station',
    #                             part_dt = 'file_rcvd_dt' )

    # Create Bigquery for HS customer profile - base
    sq0l =f"""

        CREATE OR REPLACE TEMPORARY TABLE std1 AS 
        select distinct cust_id 
         from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details`
        where part_load_dt = '{last_dt_pid}'
            and effective_end_dt >= CURRENT_DATE()
            and (upper(prod_intrnl_nm) like '%TSD%'
                 or upper(prod_intrnl_nm) like '%CONNECTING FAMILIES%'
                 or upper(prod_intrnl_nm) like '%REALTOR%'
                 or upper(prod_intrnl_nm) like '%STRATA%' 
                )
        ;
        
        CREATE OR REPLACE TEMPORARY TABLE std2 AS 
        select distinct cust_id 
         from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details` a 
         inner join `{prod_cd2remove}` b
         on a.prod_cd = b.prod_cd
        where a.part_load_dt = '{last_dt_pid}'
            and a.effective_end_dt >= CURRENT_DATE()
            and b.standard_exclusions = 1
        ; 
        
        CREATE OR REPLACE TEMPORARY TABLE std AS 
        select * from std1
        union all
        select * from std2
        ; 

        CREATE OR REPLACE TEMPORARY TABLE spd AS 
        select distinct
            lpds_id
            , coid 
            , snet_premise_type_cd
        from `bi-srv-divgdsa-pr-098bdd.common.bq_premise_universe` 
        WHERE part_dt = '{last_dt_spd}'
        ; 
        
        CREATE OR REPLACE TEMPORARY TABLE pid AS 
        select 
          cust_id
          , bacct_num
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
            group by cust_id, bacct_num
        ; 

        CREATE OR REPLACE TEMPORARY TABLE pid4hproplus AS
        select distinct
          cust_id
          , bacct_num

          from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance`
                where part_load_dt = '{last_dt_pi}'
              and product_instance_status_cd = 'A' and current_ind = 1
              and service_instance_type_cd in ('HSIC','WHSIA','TTV','SHS','SWS')
                and si_start_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        ; 

        CREATE OR REPLACE TEMPORARY TABLE pid_all AS 
        select distinct
          cust_id
          , bacct_num
          , product_instance_id
          from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance`
                where part_load_dt = '{last_dt_pi}'
              and product_instance_status_cd = 'A' and current_ind = 1
        ; 

        CREATE OR REPLACE TEMPORARY TABLE custid_gaming AS
        select distinct cust_id
        , lpds_id
         FROM `cdo-dse-workspace-np-45d0d5.featurestore.daily_cp_prod_gaming` 
         where cpf_gaming_6m_ind > 0
        ; 
        
        CREATE OR REPLACE TEMPORARY TABLE pending_orders AS 
        select  
            cust_id
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
            group by 1,2
        ; 
        
        CREATE OR REPLACE TEMPORARY TABLE ffh_bas AS
        select 
            a.* except (OPTIK_TV_IND
                        , HSIA_IND
                        , HP_IND
                        , LWC_IND
                        , SHS_IND
                        , SWS_IND
                        , SMART_HUB_IND)

            , case when a.OPTIK_TV_IND > 0 or k.OPTIK_TV_IND > 0 then 1 else 0 end as OPTIK_TV_IND
            , case when a.HSIA_IND > 0 or k.HSIA_IND > 0 then 1 else 0 end as HSIA_IND
            , case when a.HP_IND > 0 or k.HP_IND > 0 then 1 else 0 end as HP_IND
            , case when a.SHS_IND > 0 or k.SHS_IND > 0 then 1 else 0 end as SHS_IND
            , case when a.LWC_IND > 0 or k.LWC_IND > 0 then 1 else 0 end as LWC_IND
            , case when a.SWS_IND > 0 or k.SWS_IND > 0 then 1 else 0 end as SWS_IND
            , case when a.SMART_HUB_IND > 0 or k.SMART_HUB_IND > 0 then 1 else 0 end as SMART_HUB_IND

            , case when b.cust_id is null then 0 else 1 end as std_exclud2

            , c.* except (cust_id, bacct_num, cpf_HPRO_ind, cpf_wfp_ind, cpf_whsia_ind)
            , case when c.cpf_whsia_ind > 0 or k.SMART_HUB_IND > 0 then 1 else 0 end as cpf_whsia_ind
            , case when c.cpf_wfp_ind > 0 or k.WFP_ind > 0 then 1 else 0 end as cpf_wfp_ind
            , case when c.cpf_HPRO_ind > 0 or k.HPRO_ind > 0 then 1 else 0 end as cpf_HPRO_ind

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

            , case when e.cust_id is not null then 1 else 0 end as hs_202_ind

            --, f.wHSIAQualTypeMarketing
            , null as wHSIAQualTypeMarketing

            , case when g1.ACQ_DATE is not null
                        or g2.ACQ_DATE is not null
                        or g3.ACQ_DATE is not null
                        then 1 else 0 end as alarm_full_universe

            , case when j.cust_id is not null then 1 else 0 end as hs_71_ind

            , case when a.serv_prov in ('AB', 'BC') 
                    or h.Coverage_Status like '%Professional%' 
                    then 1 else 0 end as shs_professional_install
                    
            , m.snet_premise_type_cd

        from `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl` a
        left join std b on a.cust_id = b.cust_id
        left join pid c on a.cust_id = c.cust_id and a.bacct_num = c.bacct_num 
        left join `bi-srv-divgdsa-pr-098bdd.environics_derived.bq_demostats_2023_features` d
            on a.SERV_POSTAL_CODE = d.code
        left join pid4hproplus e on a.cust_id = e.cust_id and a.bacct_num = e.bacct_num 
        --left join `{whsia_eligible_base}` f on a.LPDS_ID = f.LPDSId
        left join `bi-srv-hsmdet-pr-7b9def.hsmdet_public.bq_pub_fda_alarm_full_universe` g1
            on a.cust_id is not null and a.cust_id = g1.cust_id
        left join `bi-srv-hsmdet-pr-7b9def.hsmdet_public.bq_pub_fda_alarm_full_universe` g2
            on a.bacct_num is not null and a.bacct_num = g2.bacct_num
        left join `bi-srv-hsmdet-pr-7b9def.hsmdet_public.bq_pub_fda_alarm_full_universe` g3
            on a.FMS_ADDRESS_ID is not null and a.FMS_ADDRESS_ID = g3.FMS_ADDRESS_ID
        left join `{shs_professional_install}` h on substr(a.SERV_POSTAL_CODE, 1, 3) = h.FSA
        left join custid_gaming j on a.cust_id = j.cust_id and cast(a.lpds_id as int64) = cast(j.lpds_id as int64)
        left join pending_orders k on k.cust_id = cast(a.cust_id as STRING)
            and k.LPDS_ID = cast(a.LPDS_ID as STRING) 
        left join spd m on a.LPDS_ID = m.LPDS_Id
        where a.cust_id > 0
        ;

    INSERT INTO `{qua_base}_temp`
    
    WITH dummy_cte AS (
        select 1 as dummy_col
    )

    """ 

    # Create eligible base for each offer
    sql_all = sq0l
    n_offer = offer_info.shape[0]
    for ii in range(n_offer):
        ii2 = ii + 1

        sql_b1 = (
                f""", {offer_info['Offer_Number2'][ii]} as (
                select distinct safe_cast(cust_id as int64) as cust_id \n
                , safe_cast(bacct_num as int64) as bacct_num \n
                , safe_cast(lpds_id as int64) as lpds_id \n
                , ACCT_START_DT as candate \n
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
    sq0l ="""

        select
            count(distinct bacct_num) as cnt
        from `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`

    """ 

    df_check = client.query(sq0l).to_dataframe()

    # creating eligible base
    start_time = time.time()

    if df_check['cnt'][0] > 2_500_000:
        cr8bqt_sql_BI(
            clnt = client,
            sql_base = sql_all,
            opt = qua_base,
            est_num = 10_000_000
        )

    else:
        raise Exception(f"FFH base has {df_check['cnt'][0]} rows -- seems low. Update aborted.")

    # print("This step took --- %s seconds ---" % (time.time() - start_time))


    # End of Part 1
