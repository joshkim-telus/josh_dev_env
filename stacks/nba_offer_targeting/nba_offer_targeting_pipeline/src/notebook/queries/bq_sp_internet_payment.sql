

CREATE OR REPLACE PROCEDURE nba_offer_targeting.bq_sp_internet_payment()

BEGIN

  -- Internet Monthly Payment for IRPC
  CREATE OR REPLACE TABLE nba_offer_targeting.internet_payment as

  with hs_bills as (
        
    with bd as (
    select
      cust_id,
      billg_acct_num as ban,
      bill_dt,
      cast(format_date("%Y", bill_dt) as int64) as bill_year,
      format_date("%Y_%m", bill_dt) as bill_month,
      bill_doc_id,
      last_bill_due_amt as mth_last_bill_due_amt,
      bal_fwd_amt as mth_bal_fwd_amt,
      tot_inv_amt as mth_tot_inv_amt,
      tot_tax_inv_amt as mth_tot_tax_inv_amt,
      tot_due_amt as mth_tot_due_amt
    from
        `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_document_dtl`
    where
      bill_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 160 DAY) and
      bill_dt < CURRENT_DATE() and
      do_not_snd_bill_ind = "N" and
      del_rec_ind = "N" and
      bill_doc_run_typ_cd = "REG"
    ),

    bdl as (
    select
      bill_doc_id,
      case when regexp_contains(
            bill_itm_dsply_nm,
            "(?i)\\b(telus online security)|(tos)\\b"
            ) then 1
          else 0 end tos_charge_ind,
      net_chrg_amt,
      prc_plan_grp_cd,
      bill_srvc_instnc_id,
      bill_itm_dsply_nm,
      prc_plan_cd,
      chrg_rev_cd,
      chrg_typ_cd,
      chrg_cd,
      chrg_catgy_cd,
      max(case when srvc_resrc_typ_cd in ("HSSN") then 1 else 0 end) over (partition by bill_doc_id, bill_srvc_instnc_id) hsia_ind
    from `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_chrg_dtl`
    where
    bill_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 160 DAY) and
    bill_dt < CURRENT_DATE() and
    coalesce(bill_sect_cd, "") != "X" and
    coalesce(prc_plan_grp_cd, "") not in (
      "IGNORE",
      "",
      "OTSD",
      "OTSDX",
      "OTSD7"
      ) and
    sum_dtl_bill_itm_ind in ("D", "S") and
    bundle_disc_ind = "N" and
    net_chrg_amt != 0
    ),

    bsi as (
      select
        bill_srvc_instnc_id,
        srvc_instnc_key_id,
        prim_srvc_resrc_id_val as stn
      from `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_srvc_instnc`
      where
      bill_dt >= "2023-08-13" and
      bill_dt < CURRENT_DATE()
    ),

    pip as (
      select
        product_instance_id,
        srvc_instnc_key_id as srvc_instnc_key_id_,
        service_instance_type_cd,
        lpds_id
      from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance`
      where
        part_load_dt = (select max(part_load_dt) from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance`) and
        service_instance_type_cd in ("HSIC") and
        current_ind = 1 and
        product_instance_status_cd = "A"    
    ),

    bills as (
      select
        bd.*,
        bsi.srvc_instnc_key_id,
        bsi.stn,
        pip.product_instance_id,
        pip.service_instance_type_cd,
        pip.lpds_id,
        bdl.net_chrg_amt,
        bdl.prc_plan_grp_cd,
        bdl.bill_itm_dsply_nm,
        bdl.prc_plan_cd,
        bdl.chrg_rev_cd,
        bdl.chrg_typ_cd,
        bdl.chrg_cd,
        bdl.chrg_catgy_cd,
        row_number() over (partition by cust_id,ban,bill_month,bd.bill_doc_id order by bd.cust_id) as rn
      from
        bd inner join bdl
        on bd.bill_doc_id = bdl.bill_doc_id
        inner join bsi
        on bdl.bill_srvc_instnc_id = bsi.bill_srvc_instnc_id
        inner join pip
        on pip.srvc_instnc_key_id_ = bsi.srvc_instnc_key_id
      where
        not (
            (
              bdl.tos_charge_ind = 1 and
              pip.service_instance_type_cd = "HSIC"
            ) or
            (
              bdl.hsia_ind = 1 and
              bdl.tos_charge_ind != 1 and
              pip.service_instance_type_cd = "TOS"
            )
        )
    )

    select 
    cust_id,
    ban,
    bill_month,                
    sum(
      case
          when True and service_instance_type_cd = 'HSIC' then net_chrg_amt
          else 0
      end
      ) as hsic_tot_charges,
    row_number() over (
      partition by
      cust_id,
      ban
      order by
      bill_month desc
      ) as rn 

    from bills
    group by
      cust_id,
      ban,
      bill_month
    )

  select distinct
    a.cust_id,  
    a.ban,  
    b.hsic_avg_charges,
    c.hsic_tot_charges as hsic_tot_charges_1M,
    c.bill_month as bill_month_1M,
    d.hsic_tot_charges as hsic_tot_charges_2M,
    d.bill_month as bill_month_2M,
    e.hsic_tot_charges as hsic_tot_charges_3M,
    e.bill_month as bill_month_3M,
    f.hsic_tot_charges as hsic_tot_charges_4M,
    f.bill_month as bill_month_4M,
    g.hsic_tot_charges as hsic_tot_charges_5M,
    g.bill_month as bill_month_5M
  from
    hs_bills a
  left join (
    select distinct cust_id,  ban, cast(avg(
      case when hsic_tot_charges is not null and hsic_tot_charges >= 2 then hsic_tot_charges 
      else null
      end 
      ) as FLOAT64) as hsic_avg_charges
    from hs_bills
    where rn in (1,2,3,4,5)
      group by
      cust_id,
      ban
    ) b
    on a.cust_id = b.cust_id and a.ban =b.ban

  left join (select cust_id, ban, hsic_tot_charges,bill_month from hs_bills where rn=1) c
  on a.cust_id = c.cust_id and a.ban=c.ban

  left join (select cust_id, ban, hsic_tot_charges,bill_month from hs_bills where rn=2) d
  on a.cust_id = d.cust_id and a.ban=d.ban

  left join (select cust_id, ban, hsic_tot_charges,bill_month from hs_bills where rn=3) e
  on a.cust_id = e.cust_id and a.ban=e.ban

  left join (select cust_id, ban, hsic_tot_charges,bill_month from hs_bills where rn=4) f
  on a.cust_id = f.cust_id and a.ban=f.ban

  left join (select cust_id, ban, hsic_tot_charges,bill_month from hs_bills where rn=5) g
  on a.cust_id = g.cust_id and a.ban=g.ban

  order by
    cust_id,
    ban
; 

END 
; 

CALL nba_offer_targeting.bq_sp_internet_payment()