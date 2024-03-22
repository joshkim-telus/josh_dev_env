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
  
  
Notes: Feb 2024
 - SHS eligibility flag need to be added
 - prod_cd to be changed to bq table
 - TOS tier model need to be added
 - HSIC tier model need to be added
 - valid start/end date format
    - details to confirm with Weibo/Francis
    - 2022-01-01T00:00:00.000Z

"""

import pandas as pd
import sys
import os
import re
import time
from pathlib import Path
import pdb
from yaml import safe_load
import google.oauth2.credentials
from google.cloud import bigquery
proj_id = 'divg-team-v03-pr-de558a'
client = bigquery.Client(project=proj_id)


# define some input BQ tables

offer_parameter = 'bi-stg-mobilityds-pr-db8ce2.nba_ot.bq_offering_target_params_upd'
#whsia_eligible_base = 'bi-srv-cpsbi-pr-a69cd8.hsce_hems_mdl_ds.bq_hh_mdl_whsiagtm_tbl'
whsia_eligible_base = 'divg-team-v03-pr-de558a.nba_offer_targeting.bq_whsiagtm4testing'
shs_professional_install = 'divg-team-v03-pr-de558a.OT.SHS_FSA_List_native'
prod_cd2remove = 'divg-team-v03-pr-de558a.nba_offer_targeting.prod_cd_exclusions'


# define output BQ table names for eligible base
qua_base_hs = 'divg-team-v03-pr-de558a.nba_offer_targeting.qua_base_hs'
qua_base_mob = 'divg-team-v03-pr-de558a.nba_offer_targeting.qua_base_mob'
qua_base_cat3 = 'divg-team-v03-pr-de558a.nba_offer_targeting.qua_base_cat3'


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
    part_dt
    ): 

    sql_s1 =f"""

        with
            max_date1 as (
                SELECT 
                    0 as a
                    , {part_dt} 
                    , count({part_dt}) as mxx
                FROM `{ipt}`
                    WHERE {part_dt} >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
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


last_dt_pid = last_dt_check(clnt = client,
                            ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details',
                            part_dt = 'part_load_dt' )

last_dt_pi = last_dt_check(clnt = client,
                            ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance',
                            part_dt = 'part_load_dt' )

last_dt_gateway = last_dt_check(clnt = client,
                            ipt = 'cio-datahub-enterprise-pr-183a.ent_resrc_config.bq_product_instance_gateway_daily_snpsht',
                            part_dt = 'snapshot_load_dt' )

last_dt_game = last_dt_check(clnt = client,
                            ipt = 'cio-datahub-enterprise-pr-183a.ent_resrc_performance_device_kpi.bq_cloudcheck_game_station',
                            part_dt = 'file_rcvd_dt' )




# Create Bigquery for HS customer profile - base

sq0l =f"""
    
    with std1 as (
        select distinct cust_id 
         from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details`
        where part_load_dt = '{last_dt_pid}'
            and effective_end_dt >= CURRENT_DATE()
            and (upper(prod_intrnl_nm) like '%TSD%'
                 or upper(prod_intrnl_nm) like '%CONNECTING FAMILIES%'
                 or upper(prod_intrnl_nm) like '%REALTOR%'
                 or upper(prod_intrnl_nm) like '%STRATA%' 
                )
    )
    
    ,std2 as (

        select distinct cust_id 
         from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details` a 
         inner join `{prod_cd2remove}` b
         on a.prod_cd = b.prod_cd
        where a.part_load_dt = '{last_dt_pid}'
            and a.effective_end_dt >= CURRENT_DATE()
            and b.standard_exclusions = 1

    )
    
    , std as (
        select * from std1
        union all
        select * from std2
    )
    
    , pid as (
    
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

    )
    
    , pid4hproplus as (
    
            select distinct
              cust_id
              , bacct_num

              from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance`
                    where part_load_dt = '{last_dt_pi}'
                  and product_instance_status_cd = 'A' and current_ind = 1
                  and service_instance_type_cd in ('HSIC','WHSIA','TTV','SHS','SWS')
                    and si_start_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

    )
    
    , pid_all as (
    
            select distinct
              cust_id
              , bacct_num
              , product_instance_id
              from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance`
                    where part_load_dt = '{last_dt_pi}'
                  and product_instance_status_cd = 'A' and current_ind = 1

    )
    
    , gtwy_ser_num as (
            SELECT distinct
                cust_id
                , prod_instnc_id
                , gtwy_ser_num
            FROM `cio-datahub-enterprise-pr-183a.ent_resrc_config.bq_product_instance_gateway_daily_snpsht` 
            WHERE snapshot_load_dt >= DATE_SUB('{last_dt_gateway}' , INTERVAL 30 DAY)
    )
    , last180gaming as (
          select distinct
            line_id
            from `cio-datahub-enterprise-pr-183a.ent_resrc_performance_device_kpi.bq_cloudcheck_game_station`
            where file_rcvd_dt >= DATE_SUB('{last_dt_game}', INTERVAL 180 DAY)

    )
    , custid_gaming as (
            select distinct a.cust_id, a.bacct_num
            from pid_all a
            left join gtwy_ser_num b on a.cust_id = cast(trim(b.cust_id) as INT64) and a.product_instance_id = b.prod_instnc_id
            left join last180gaming c on b.gtwy_ser_num = c.line_id
            where c.line_id is not null
    )
    
    , pending_orders as (

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

    )

    , ffh_bas as (
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
                
            , f.wHSIAQualTypeMarketing
            
            , case when g1.ACQ_DATE is not null
                        or g2.ACQ_DATE is not null
                        or g3.ACQ_DATE is not null
                        then 1 else 0 end as alarm_full_universe
            
            --, case when j.cust_id is not null then 1 else 0 end as hs_71_ind
            
            , 1 as hs_71_ind
            
            , case when a.serv_prov in ('AB', 'BC') 
                    or h.Coverage_Status like '%Professional%' 
                    then 1 else 0 end as shs_professional_install
            
        from `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl` a
        left join std b on a.cust_id = b.cust_id
        left join pid c on a.cust_id = c.cust_id and a.bacct_num = c.bacct_num 
        left join `bi-srv-divgdsa-pr-098bdd.environics_derived.bq_demostats_2023_features` d
            on a.SERV_POSTAL_CODE = d.code
        left join pid4hproplus e on a.cust_id = e.cust_id and a.bacct_num = e.bacct_num 
        left join `{whsia_eligible_base}` f on a.LPDS_ID = f.LPDSId
        left join `bi-srv-hsmdet-pr-7b9def.hsmdet_public.bq_pub_fda_alarm_full_universe` g1
            on a.cust_id is not null and a.cust_id = g1.cust_id
        left join `bi-srv-hsmdet-pr-7b9def.hsmdet_public.bq_pub_fda_alarm_full_universe` g2
            on a.bacct_num is not null and a.bacct_num = g2.bacct_num
        left join `bi-srv-hsmdet-pr-7b9def.hsmdet_public.bq_pub_fda_alarm_full_universe` g3
            on a.FMS_ADDRESS_ID is not null and a.FMS_ADDRESS_ID = g3.FMS_ADDRESS_ID
        left join `{shs_professional_install}` h on substr(a.SERV_POSTAL_CODE, 1, 3) = h.FSA
        -- left join custid_gaming j on a.cust_id = j.cust_id and a.bacct_num = j.bacct_num 
        left join pending_orders k on k.cust_id = cast(a.cust_id as STRING)
            and k.LPDS_ID = cast(a.LPDS_ID as STRING) 
        where a.cust_id > 0
        
        )
    
""" 

# Create eligible base for each offer

sql_all = sq0l
n_offer = offer_info.shape[0]
for ii in range(n_offer):
    ii2 = ii + 1
    
    sql_b1 = (
        f""", {offer_info['Offer_Number2'][ii]} as (
        select distinct cust_id, bacct_num, lpds_id
        , ACCT_START_DT as candate \n
        , '{offer_info['Category'][ii]}' as Category  \n
        , '{offer_info['Subcategory'][ii]}' as Subcategory  \n 
        , '{offer_info['promo_seg'][ii]}' as promo_seg  \n 
        , '{offer_info['line_of_business'][ii]}' as LOB  \n 
        , '{offer_info['NCID'][ii]}' as NCID  \n 
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
        opt = qua_base_hs,
        est_num = 10_000_000
    )

else:
    raise Exception(f"FFH base has {df_check['cnt'][0]} rows -- seems low. Update aborted.")

# print("This step took --- %s seconds ---" % (time.time() - start_time))


# End of Part 1




# Beginning of Part 2

# Creating eligible base for Naked mobility customers


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
    where a.if_active = 1 and a.MOB_filters is not null

""" 

offer_info = client.query(sq0l).to_dataframe()

# Check latest snapshot date with reasonable counts

last_dt_spd = last_dt_check(clnt = client,
                            ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_premise_universe',
                            part_dt = 'part_dt' )

last_dt_pi = last_dt_check(clnt = client,
                            ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance',
                            part_dt = 'part_load_dt' )

last_dt_mlpds = last_dt_check(clnt = client,
                            ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_mobility_active_data',
                            part_dt = 'part_dt' )

# Create Naked Mobility customer profile - base

sq0l =f"""
    
    with

    mob_lpds_id as (

        select *
        from `bi-srv-divgdsa-pr-098bdd.common.bq_mobility_active_data` 
        WHERE part_dt = '{last_dt_mlpds}'
    )
    
    , mob as (
        SELECT distinct 
            fmbase.BAN, 
            fmbase.INIT_ACTIVATION_DATE as mobdate,
            fmbase.PROVINCE,  
            fmbase.POSTCODE, 
            fmbase.DEVICE_NAME,
            fmbase.MNH_FFH_BAN,
            fmbase.LANG_PREF,
            CASE WHEN fmbase.LPDS_ID>0 then fmbase.LPDS_ID ELSE mad.LPDS_ID end as LPDS_ID

        FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_fda_mob_mobility_base`  fmbase
            left join mob_lpds_id mad
            on fmbase.ban = cast(mad.ban as int)
        WHERE fmbase.BRAND_ID =1
            and fmbase.product_type  in   ('C','I')
            and fmbase.account_type  in    ('I','C') 
            and fmbase.ACCOUNT_SUB_TYPE in ('I','R','E')
            and (fmbase.MNH_FFH_BAN = 0 or fmbase.MNH_FFH_BAN is null)
            and fmbase.PRIMARY_SUB = 1
            and fmbase.SUB_STATUS = 'A'
            and fmbase.STANDARD_EXCLUSIONS = 0 
            and fmbase.STOP_SELL = 0
    ),

    spd as (

        select 
            lpds_id
            , hs_max_speed 
            , hs_max_speed_bonded
            , obd_eligible_ind 
            , coid 
            , snet_premise_type_cd
            , ttv_port_availability 
        from `bi-srv-divgdsa-pr-098bdd.common.bq_premise_universe` 
        WHERE part_dt = '{last_dt_spd}'
    ),

    pid as (
    
            select 
              lpds_id 
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
              , max(case when service_instance_type_cd = 'TTV' then 1 else 0 end) as cpf_ttv_ind
              , max(case when service_instance_type_cd = 'WFP' then 1 else 0 end) as cpf_wfp_ind
              , max(case when service_instance_type_cd = 'WHSIA' then 1 else 0 end) as cpf_whsia_ind  
              , max(case when service_instance_type_cd = 'HPRO' then 1 else 0 end) as cpf_HPRO_ind  

              from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance`
                    where part_load_dt = '{last_dt_pi}'
                  and product_instance_status_cd = 'A' and current_ind = 1
                group by lpds_id

    )

    , pending_orders as (

            select  
                lpds_id
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
                group by 1

    )
    
    
    , pid_pending as (
    
            select distinct
              case when a.lpds_id is not null then a.lpds_id else b.lpds_id end as lpds_id  
              , case when a.cpf_hsic_ind > 0 or b.HSIA_IND > 0 then 1 else 0 end as cpf_hsic_ind
              , case when a.cpf_lwc_ind > 0 or b.LWC_ind > 0 then 1 else 0 end as cpf_lwc_ind
              , a.cpf_pik_ind
              , case when a.cpf_shs_ind > 0 or b.SHS_ind > 0 then 1 else 0 end as cpf_shs_ind
              , case when a.cpf_sing_ind > 0 or b.HP_IND > 0 then 1 else 0 end as cpf_sing_ind
              , case when a.cpf_sws_ind > 0 or b.SWS_ind > 0 then 1 else 0 end as cpf_sws_ind
              , case when a.cpf_ttv_ind > 0 or b.OPTIK_TV_IND > 0 then 1 else 0 end as cpf_ttv_ind
              , a.cpf_stv_ind
              , case when a.cpf_tos_ind > 0 or b.TOS_IND > 0 then 1 else 0 end as cpf_tos_ind
              , case when a.cpf_wfp_ind > 0 or b.WFP_ind > 0 then 1 else 0 end as cpf_wfp_ind
              , case when a.cpf_whsia_ind > 0 or b.SMART_HUB_IND > 0 then 1 else 0 end as cpf_whsia_ind
              , case when a.cpf_HPRO_ind > 0 or b.HPRO_ind > 0 then 1 else 0 end as cpf_HPRO_ind
              
              from pid a full join pending_orders b 
                  on b.LPDS_ID = a.LPDS_ID

    )
    
    , mob_base as (

        select distinct 
            a.*
            , p.* except (LPDS_ID)
            , c.* except (LPDS_ID)
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
                            
            , case when d.baskid > 50 
                        or upper(a.DEVICE_NAME) like '%APPLE%'
                        or upper(a.DEVICE_NAME) like '%IPAD%'
                        or upper(a.DEVICE_NAME) like '%IPHONE%'
                        then 1 else 0 end as demo_hs_188_ind
            , RAND() as rand_seed1
            , case when e.ban > 0 then 1 else 0 end as mob_shs
            , f.wHSIAQualTypeMarketing
            , case when a.PROVINCE in ('AB', 'BC') 
                    or b.Coverage_Status like '%Professional%' 
                then 1 else 0 end as shs_professional_install
            
        from mob a 
            left join spd p on a.lpds_id = p.lpds_id  
            left join `{shs_professional_install}` b on substr(a.POSTCODE, 1, 3) = b.FSA
            left join pid_pending c on cast(a.lpds_id as STRING) = c.LPDS_ID 
            left join `bi-srv-divgdsa-pr-098bdd.environics_derived.bq_demostats_2023_features` d
            on a.postcode = d.code
            left join `bi-srv-hsmdet-pr-7b9def.hsmdet_public.bq_pub_fda_alarm_full_universe` e 
            on (a.ban is not null and a.ban = e.ban)
            left join `{whsia_eligible_base}` f on a.LPDS_ID = f.LPDSId
        where a.ban > 0
   )
    
""" 

# Create eligible base for each offer

sql_all = sq0l
n_offer = offer_info.shape[0]
for ii in range(n_offer):
    ii2 = ii + 1
    
    sql_b1 = (
        f""", {offer_info['Offer_Number2'][ii]} as (
        select distinct ban, lpds_id, mobdate \n
        , '{offer_info['Category'][ii]}' as Category  \n
        , '{offer_info['Subcategory'][ii]}' as Subcategory  \n 
        , '{offer_info['promo_seg'][ii]}' as promo_seg  \n 
        , '{offer_info['line_of_business'][ii]}' as LOB  \n 
        , '{offer_info['NCID'][ii]}' as NCID  \n 
        , cast('{offer_info['valid_start_dt'][ii]}' AS DATE) as ASSMT_VALID_START_TS  \n
        , cast('{offer_info['valid_end_dt'][ii]}' AS DATE) as ASSMT_VALID_END_TS  \n 
        , {str(offer_info['rk'][ii])} as rk  \n
        from mob_base \n where  {offer_info['MOB_filters'][ii]} \n )"""
        
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

    select
        count(distinct fmbase.BAN) as cnt

    FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_fda_mob_mobility_base`  fmbase
    WHERE fmbase.BRAND_ID =1
        and fmbase.product_type  in   ('C','I')
        and fmbase.account_type  in    ('I','C') 
        and fmbase.ACCOUNT_SUB_TYPE in ('I','R','E')
        and (fmbase.MNH_FFH_BAN = 0 or fmbase.MNH_FFH_BAN is null)
        and fmbase.PRIMARY_SUB = 1
        and fmbase.SUB_STATUS = 'A'
        and fmbase.STANDARD_EXCLUSIONS = 0 
        and fmbase.STOP_SELL = 0

""" 

df_check = client.query(sq0l).to_dataframe()

# print(sql_all)

# creating eligible base

start_time = time.time()

if df_check['cnt'][0] > 800_000:
    cr8bqt_sql_BI(
        clnt = client,
        sql_base = sql_all,
        opt = qua_base_mob,
        est_num = 4_000_000
    )

else:
    raise Exception(f"Naked Mobility base has {df_check['cnt'][0]} rows -- seems low. Update aborted.")

# print("This step took --- %s seconds ---" % (time.time() - start_time))

# End of Part 2



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
                            part_dt = 'part_load_dt' )

last_dt_pi = last_dt_check(clnt = client,
                            ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance',
                            part_dt = 'part_load_dt' )

last_dt_spd = last_dt_check(clnt = client,
                            ipt = 'bi-srv-divgdsa-pr-098bdd.common.bq_premise_universe',
                            part_dt = 'part_dt' )


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
            , '' as ACCT_START_DT
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
        select distinct cust_id, bacct_num, lpds_id
        , ACCT_START_DT as candate \n
        , '{offer_info['Category'][ii]}' as Category  \n
        , '{offer_info['Subcategory'][ii]}' as Subcategory  \n 
        , '{offer_info['promo_seg'][ii]}' as promo_seg  \n 
        , '{offer_info['line_of_business'][ii]}' as LOB  \n 
        , '{offer_info['NCID'][ii]}' as NCID  \n 
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
        opt = qua_base_cat3,
        est_num = 1500
    )

else:
    raise Exception(f"Cat3 base has {df_check['cnt'][0]} rows -- seems low. Update aborted.")

# print("This step took --- %s seconds ---" % (time.time() - start_time))


# End of Part 3

# End of this program