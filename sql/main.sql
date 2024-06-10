/*DIM_BUYER_CREDIT_LIMIT*/
create or replace table "BALANCE_V2"."STG"."DIM_BUYER_CREDIT_LIMIT" as
(
WITH CTE_MY_DATE AS
  (
    SELECT DATEADD(DAY, SEQ4(), '2000-01-01') AS MY_DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>10000))
  ), cte as
  (
  select created_at,entity_id,0 as credit_limit 
from BALANCE_V2.DBO.QUALIFICATION
where status = 'DECLINED'
union all
select created_at,entity_id,credit_limit 
from BALANCE_V2.DBO.CUSTOMER_CREDIT_LIMIT
order by entity_id, created_at desc
  ),cte2 as 
  (
   select entity_id,created_at as start_date,ifnull(lead(created_at,1) over(partition by entity_id order by created_at),CURRENT_TIMESTAMP) as end_date,credit_limit
  from cte
  )
select cte2.entity_id,b.merchant_id,my_date as balance_date,credit_limit
from cte2 left join cte_my_date 
on cte2.start_date < my_date and cte2.end_date >= my_date
left join BALANCE_V2.DBO.BUYER b on cte2.entity_id = b.public_id
where balance_date is not null
)
;


/*BALANCE_V2.DBO.DIM_ALL_LOANS*/
create or replace table BALANCE_V2.DBO.DIM_ALL_LOANS as 
(

with chargeable_sources as (
select
buyer_id,
min (paid_date) as first_pull_tx
from Balance_v2.dbo.charge
where charge_status='charged' and (payment_method_type in ('creditCard','achDebit') or (payment_method_type='bank' and payment_id not like 'src%'))
and transaction_id in (select id from balance_v2.dbo.transaction where is_financed='TRUE')
group by 1    
),charge_adjustment as 
(
select charge_id,null as adjustment_date,null as adjustment_reason,null as adjustment_amount,array_agg(distinct concat('adjustment_date: ',created_at,', reason: ',reason,', amount: ',adjusted_amount_cents/100)) as aggregated_adjustments,'1' as is_charge_adjusted
from BALANCE_V2.DBO.CHARGE_ADJUSTMENT
where CHARGE_ID in ( select CHARGE_ID
                     from BALANCE_V2.DBO.CHARGE_ADJUSTMENT
                     group by 1
                     having count(*) > 1
                      ) 
group by 1
union all
select charge_id,created_at as adjustment_date,reason as adjustment_reason,adjusted_amount_cents/100 as adjustment_amount, null as aggregated_adjustments,'1' as is_charge_adjusted
from BALANCE_V2.DBO.CHARGE_ADJUSTMENT
where CHARGE_ID in ( select CHARGE_ID
                     from BALANCE_V2.DBO.CHARGE_ADJUSTMENT
                     group by 1
                     having count(*) = 1
                      )
),

spv_payouts as (
select 
charge_id,
is_paid_out

from balance_v2.dbo.funding_charges_status
where is_paid_out=TRUE

),

split_payments as (
select charge_id from balance_v2.stg.dim_charges where split_type='PAY_NOW' and transaction_is_financed=TRUE
)

select 
    tx.id as tx_id,
    ch.id as charge_id,
    tx.buyer_id,
    b.funding_source as buyer_funding_source,
    tx.host_seller_id as merchant_id,
    tx.selected_payment_method,
    ch.payment_method_type,
    merchant_name,
    dim.smb_buyers,
    ch.created_at as revenue_date,
    ch.charge_date as due_date,
    paid_date,
    tx.source_platform as tx_creation_source,
    ch.source_platform as charge_creation_source,
    terms_net_days,
    ch.funding_source as charge_funding_source,
    ch.funding_source_tag_date as charge_funding_source_tag_date,
    spv.is_paid_out as spv_charge_payout,
    ch.charge_status,
    ch.amount_in_cents/100 as charge_amount,    
    case when paid_date is not null then 1 else 0 end as is_paid,
    case when paid_date is not null then TIMESTAMPDIFF('day',ch.charge_date,paid_date) else TIMESTAMPDIFF('day',ch.charge_date,current_date()) end as payment_days,  
    case when  paid_date<ch.charge_date or TIMESTAMPDIFF('day',ch.charge_date,current_date)<=0 then 1 else 0 end as early,
    case when cs.buyer_id is not null then 1 else 0 end as has_chargeable_source,
    case when  TIMESTAMPDIFF('day',ch.charge_date,paid_date) between 1 and 29 or (paid_date is null and TIMESTAMPDIFF('day',ch.charge_date,current_date) between 1 and 29) then 1 else 0 end as l_0_30,
    case when  TIMESTAMPDIFF('day',ch.charge_date,paid_date) between 30 and 59 or (paid_date is null and TIMESTAMPDIFF('day',ch.charge_date,current_date) between 30 and 59) then 1 else 0 end as l_30_60,
    case when  TIMESTAMPDIFF('day',ch.charge_date,paid_date) between 60 and 89 or (paid_date is null and TIMESTAMPDIFF('day',ch.charge_date,current_date) between 60 and 89) then 1 else 0 end as l_60_90,
    case when  TIMESTAMPDIFF('day',ch.charge_date,paid_date) between 90 and 119 or (paid_date is null and TIMESTAMPDIFF('day',ch.charge_date,current_date) between 90 and 119) then 1 else 0 end as l_90_120,
    case when  TIMESTAMPDIFF('day',ch.charge_date,paid_date) >=120 or (paid_date is null and TIMESTAMPDIFF('day',ch.charge_date,current_date) >=120) then 1 else 0 end as l_120,
     case when  TIMESTAMPDIFF('day',ch.charge_date,paid_date) >=365 or (paid_date is null and TIMESTAMPDIFF('day',ch.charge_date,current_date) >=365) then 1 else 0 end as l_365,
     case when  TIMESTAMPDIFF('day',ch.charge_date,paid_date) >=120  then 1 else 0 end as is_recovered,
     coalesce(ca.is_charge_adjusted,'0') as is_charge_adjusted,
     ca.adjustment_date,
     ca.adjustment_reason,
     ca.adjustment_amount,
     ca.aggregated_adjustments,
     brs.buyer_risk_score
         from BALANCE_V2.DBO.transaction tx
             join BALANCE_V2.DBO.charge ch
                    on tx.id=ch.transaction_id
                    and charge_status<>'canceled'
             left join BALANCE_V2.stg.dim_merchant dim
                     on tx.host_seller_id=dim.merchant_id
                left join chargeable_sources cs 
    on tx.buyer_id=cs.buyer_id and ch.created_at>cs.first_pull_tx 
    left join BALANCE_V2.DBO.BUYER b
    on tx.buyer_id=b.id
    left join charge_adjustment ca 
    on ch.id = ca.charge_id
    left join BALANCE_V2.STG.BUYER_RISK_SCORE brs
    on tx.buyer_id = brs.buyer_id and date(revenue_date) = brs.etl_date
    left join spv_payouts spv
    on ch.id=spv.charge_id

    where is_financed='TRUE' and ch.deleted_at is null and ch.id not in (select charge_id from split_payments)
    order by buyer_risk_score
);


/*"BALANCE_V2"."STG"."FACT_MERCHANT_REVENUE"*/
create or replace table "BALANCE_V2"."STG"."FACT_MERCHANT_REVENUE" as
(
 select
created_at,
accounting_date,
entity_ref_id,
entity_type,
case 
when type in ('fee_processing','fee_processing_adjustment','fee_card_authorization','canceled_fee_processing',
              'canceled_fee_processing_adjustment','canceled_fee_card_authorization') then 'processing_fees'
when type in ('fee_late','fee_financing','fee_financing_adjustment','canceled_fee_late','canceled_fee_financing','canceled_fee_financing_adjustment') then 'factoring_fees'
when type in ('fee_fx','fee_payout','canceled_fee_payout','canceled_fee_fx') then 'payout_fees'
when type in ('dispute','canceled_dispute') then 'dispute_fees' end as fee_category,
type as fee_type,
amount_in_cents/-100.0 as fee_amount
from BALANCE_V2.DBO.ACTIVITY_LOG al
where (type like ('fee%') or type like ('%canceled_fee%') or type like ('%canceled_fx_fee%'))
and entity_type in ('charge','payout')
and type<>'fee_surcharge' 
);


/*"BALANCE_V2"."STG"."DIM_MERCHANT_REVENUE"*/
create or replace table "BALANCE_V2"."STG"."DIM_MERCHANT_REVENUE" as
(
with cte as
(
select
entity_ref_id,
entity_type,
merchant_id,
sum(case when type in ('fee_processing','fee_processing_adjustment','fee_card_authorization') then amount_in_cents/-100.0 else 0 end) as processing_fee,
sum(case when type in ('canceled_fee_processing','canceled_fee_processing_adjustment','canceled_fee_card_authorization') then amount_in_cents/-100.0 else 0 end) as canceled_processing_fee,
sum(case when type in ('fee_late','fee_financing','fee_financing_adjustment') then amount_in_cents/-100.0 else 0 end) as factoring_fee,
sum(case when type in ('canceled_fee_late','canceled_fee_financing','canceled_fee_financing_adjustment') then amount_in_cents/-100.0 else 0 end) as canceled_factoring_fee,
sum(case when type in ('fee_fx','fee_payout') then amount_in_cents/-100.0 else 0 end) as payout_fee,
sum(case when type in ('canceled_fee_payout','canceled_fee_fx') then amount_in_cents/-100.0 else 0 end) as canceled_payout_fee,
sum(case when type in ('dispute') then amount_in_cents/-100.0 else 0 end) as dispute_fee,
sum(case when type in ('canceled_dispute') then amount_in_cents/-100.0 else 0 end) as canceled_dispute_fee,
from BALANCE_V2.DBO.ACTIVITY_LOG al
where (type like ('fee%') or type like ('%canceled_fee%') or type like ('%canceled_fx_fee%'))
and entity_type in ('charge','payout')
and type<>'fee_surcharge'
group by 1,2,3
)
select 
*,
processing_fee+canceled_processing_fee as net_processing_fee,
factoring_fee+canceled_factoring_fee as net_factoring_fee,
payout_fee+canceled_payout_fee as net_payout_fee,
dispute_fee+canceled_dispute_fee as net_dispute_fee
from cte
);

/*Dim_all_charges*/
/*create or replace table "BALANCE_V2"."STG"."DIM_CHARGES" as*/
/*Dim_all_charges*/
/*create or replace table "BALANCE_V2"."STG"."DIM_CHARGES" as*/
create or replace table "BALANCE_V2"."STG"."DIM_ALL_CHARGES" as  
(
with credit_bad as
(

with fraudulent_merchants as (
select
    id,
    merchant_id
    from BALANCE_V2.DBO.CHARGE
    
    where merchant_id in (
        691, /*Datavocity*/
        1391, /*CB imports*/
        1511, /*Myluan*/
        1524, /*Trueleads*/
        4634 /*Threadstudio*/
    )
    and charge_status not in ('charged','refunded','canceled')
)

select a.id as charge_id,
case when (c.charge_id is not null and is_paid=0) then 'Default'
when (c.charge_id is not null and is_paid=1) then 'Recovered' 
else null end as credit_bad,
case when a.id in (select id from fraudulent_merchants) then 'Merchant Fraud'
    else reason end as fraud_bad,
case when a.id in (select id from fraudulent_merchants) then revenue_date
    else b.created end as bad_creation_date,
status as fraud_status
from BALANCE_V2.DBO.CHARGE a
left join stripe_data_pipeline.stripe.disputes b
on a.external_charge_id=b.charge_id
left join BALANCE_V2.DBO.DIM_ALL_LOANS c
on a.id=c.charge_id
and payment_days>120
where to_char(created_at,'yyyy-mm-dd')>'2022-03-01'
and reason is not null or c.charge_id is not null
), failed_charges as
(
select distinct ENTITY_ID,

max(case when left(ENTITY_ID,3) = 'chg' and status = 'failed' then 1 else 0 end) as was_failed,
max(case when left(ENTITY_ID,3) = 'chg' and event_type like 'recharge%' then 1 else 0 end) as recharge_attempted,
max(case when left(ENTITY_ID,3) = 'chg' and event_type like 'recharge-success' then 1 else 0 end) as recharge_success,
max(case when event_type like 'recharge-success' and event_type like 'recharge%' and json_extract_path_text(metadata,'collectionRechargeType')='backup' then 1 else 0 end) as recharge_on_backup_payment_method,
max(case when event_type like 'recharge-success' and event_type like 'recharge%' and json_extract_path_text(metadata,'paymentMethodType')='achDebit' then 1 else 0 end) as backup_is_ach



from BALANCE_V2.DBO.PAYMENT_EVENT_LOG where left(ENTITY_ID,3) = 'chg' and (status = 'failed' or event_type like 'recharge%')
group by 1
),charge_adjustment as 
(
select charge_id,null as adjustment_date,null as adjustment_reason,null as adjustment_amount,array_agg(distinct concat('adjustment_date: ',created_at,', reason: ',reason,', amount: ',adjusted_amount_cents/100)) as aggregated_adjustments,'1' as is_charge_adjusted
from BALANCE_V2.DBO.CHARGE_ADJUSTMENT
where CHARGE_ID in ( select CHARGE_ID
                     from BALANCE_V2.DBO.CHARGE_ADJUSTMENT
                     group by 1
                     having count(*) > 1
                      ) 
group by 1
union all
select charge_id,created_at as adjustment_date,reason as adjustment_reason,adjusted_amount_cents/100 as adjustment_amount, null as aggregated_adjustments,'1' as is_charge_adjusted
from BALANCE_V2.DBO.CHARGE_ADJUSTMENT
where CHARGE_ID in ( select CHARGE_ID
                     from BALANCE_V2.DBO.CHARGE_ADJUSTMENT
                     group by 1
                     having count(*) = 1
                      )
),
stripe_costs as (
select
charge_id,
total_fee
from BALANCE_V2.STG.STRIPE_FEES

),

aditional_fees as (
select ENTITY_REF_ID,charge_id, sum(fe.AMOUNT_IN_CENTS/100) as processing_fee_usd 
from BALANCE_V2.DBO.FEES fe
join BALANCE_V2.DBO.core_payments cp 
on fe.entity_ref_id=cp.public_id
where fe.type = 'VENDOR_PROCESSING'
group by 1,2
),

aditional_fees_2 as (
select ENTITY_REF_ID,charge_id, sum(fe.AMOUNT_IN_CENTS/100) as late_fee_usd 
from BALANCE_V2.DBO.FEES fe
join BALANCE_V2.DBO.core_payments cp 
on fe.entity_ref_id=cp.public_id
where fe.type = 'LATE'
group by 1,2
),
is_split as
(
select cp.transaction_id
from balance_v2.dbo.core_payments as cp 
join balance_v2.dbo.charge as c on cp.charge_id = c.public_id
where cp.type in ('PAY_NOW', 'PAY_ON_TERMS')
group by 1
having count(distinct cp.type)>1 and cp.transaction_id is not null),

split_payments as
(
select cp.charge_id,type as split_type
from balance_v2.dbo.core_payments as cp 
where cp.type in ('PAY_NOW', 'PAY_ON_TERMS') and deleted_at is null and transaction_id in (select transaction_id from balance_v2.dbo.core_payments where type in ('PAY_NOW', 'PAY_ON_TERMS')
group by 1
having count(distinct type)>1 and transaction_id is not null)
),

ic_plus as (
select 
entity_ref_id as charge_id,
sum(net_processing_fee) as processing_revenue

from balance_v2.stg.dim_merchant_revenue
where entity_type='charge'
and  left(entity_ref_id,3) <> 'ven'
group by 1

)


    select 
       c.ID as charge_id
      ,c.TRANSACTION_ID 
      ,c.AMOUNT_IN_CENTS/100 as charge_amount_usd
      ,c.CURRENCY as charge_currency
      ,c.CHARGE_STATUS
      ,c.CREATED_AT
      ,c.CHARGE_DATE as due_date
      ,c.PAID_DATE
      ,c.deleted_at as is_deleted
      ,case when sc.total_fee is not null then sc.total_fee/100 else c.STRIPE_FEE_IN_CENTS/100 end as stripe_fee_usd
      ,(ic.processing_revenue)+zeroifnull(af.processing_fee_usd) as processing_fee_usd
      ,case 
        when c.payment_method_type = 'invoice' and c.CHARGE_STATUS = 'charged' then 'wire'
        else c.payment_method_type 
       end as charge_payment_method_type
      ,case 
        when t.IS_FINANCED = 'True' then c.CREATED_AT
        when t.IS_FINANCED = 'False'  and c.PAID_DATE is null then c.CHARGE_DATE
        else c.PAID_DATE 
       end as revenue_date
      ,t.PUBLIC_ID as transaction_public_id
      ,(zeroifnull(case when row_number() over (partition by c.transaction_id order by c.charge_date desc) =1 then t.FACTORING_FEE_IN_CENTS else 0 end)/100) + zeroifnull(af2.late_fee_usd) as transaction_factoring_fee_usd
      ,zeroifnull(af2.late_fee_usd) as late_fee_revenue
      ,(zeroifnull(ic.processing_revenue/100))+zeroifnull(case when row_number() over (partition by c.transaction_id order by c.charge_date desc) =1 then t.FACTORING_FEE_IN_CENTS else 0 end)/100 as charge_revenue
      ,t.IS_FINANCED as transaction_is_financed
      ,t.TERMS_NET_DAYS as transaction_terms_net_days
      ,t.is_auth as is_auth_capture
      ,round(t.AUTH_AMOUNT,2) as tx_auth_amount
      ,t.CAPTURED_AMOUNT as tx_captured_amount
      ,c.BUYER_ID
      ,b.PUBLIC_ID as buyer_public_id
      ,c.public_id as charge_public_id
      ,c.external_charge_id as external_charge_id
      ,b.EMAIL as buyer_email
      ,b.funding_source as buyer_funding_source
      ,c.merchant_id 
      ,c.source_platform as charge_source
      ,c.funding_source as charge_funding_source
      ,c.funding_source_tag_date as charge_funding_source_tag_date
      ,t.source_platform as transaction_source
      ,s.NAME as merchant_name
      ,s.merchant_token
     /* ,v.seller_id as vendor_id
      ,v.seller_name as vendor_name*/
      ,v2.seller_id as vendor_id
      ,v2.seller_name as vendor_name
      ,current_timestamp as _RIVERY_LAST_UPDATE
      ,credit_bad.credit_bad
      ,credit_bad.fraud_bad
      ,credit_bad.bad_creation_date
      ,credit_bad.fraud_status
      ,coalesce(fc.was_failed,0) as was_failed
      ,coalesce(fc.recharge_attempted,0) as recharge_attempted
      ,coalesce(fc.recharge_success,0) as recharge_success
       ,coalesce(fc.recharge_on_backup_payment_method,0) as recharge_on_backup_payment_method
        ,coalesce(fc.backup_is_ach,0) as backup_is_ach
      ,coalesce(ca.is_charge_adjusted,'0') as is_charge_adjusted
      ,ca.adjustment_date
      ,ca.adjustment_reason
      ,ca.adjustment_amount
      ,ca.aggregated_adjustments
      ,case when af2.charge_id is not null then 1 else 0 end as charged_late_fees
      ,case when spl.transaction_id is not null then 1 else 0 end as is_split
      ,split_type
from BALANCE_V2.DBO.CHARGE c left join BALANCE_V2.DBO.TRANSACTION t
on c.transaction_id = t.id
left join BALANCE_V2.DBO.BUYER b
on c.buyer_id = b.id
left join BALANCE_V2.DBO.SELLER s
on c.MERCHANT_ID = s.ID
left join credit_bad
on c.id = credit_bad.charge_id
left join failed_charges fc 
on c.PUBLIC_ID = fc.ENTITY_ID
left join charge_adjustment ca 
on c.id = ca.charge_id
left join stripe_costs sc 
on c.external_charge_id=sc.charge_id
left join aditional_fees af
on c.public_id=af.charge_id
left join aditional_fees_2 af2
on c.public_id=af2.charge_id
left join is_split spl 
on t.public_id=spl.transaction_id
left join split_payments spp
on c.public_id=spp.charge_id and type <>'VENDOR_PAYOUT'
left join ic_plus ic 
on c.id=ic.charge_id
/*left join (
select transaction_id,listagg(distinct seller_id,',') as SELLER_ID,listagg(distinct s.name,',') as SELLER_NAME 
from BALANCE_V2.DBO.LINE l
    left join BALANCE_V2.DBO.SELLER s
    on l.seller_id = s.id
    group by transaction_id
) v
on c.transaction_id = v.transaction_id  - replaced at 25/7/2023 for Sivan request https://balancepoc.monday.com/boards/2963149087/pulses/4717363337 */
left join (
select transaction_id,listagg(distinct p.VENDOR_ID,',') as SELLER_ID,listagg(distinct s.name,',') as SELLER_NAME 
from BALANCE_V2.DBO.PAYMENT p
    left join BALANCE_V2.DBO.SELLER s
    on p.VENDOR_ID = s.id
    group by transaction_id
) v2
on c.transaction_id = v2.transaction_id  
where c.id not in (67039,66860,66862,65862)
/*where
(t.IS_FINANCED='TRUE' and c.CHARGE_STATUS <>'canceled')
or
((t.IS_FINANCED='FALSE' or t.IS_FINANCED is NULL) and c.CHARGE_STATUS in ('charged','refunded') and payment_method_type !='outOfPlatform')*/
)
;

/*Dim_charges*/
create or replace table "BALANCE_V2"."STG"."DIM_CHARGES" as
(
select * from "BALANCE_V2"."STG"."DIM_ALL_CHARGES"
where
(transaction_is_financed='TRUE' and CHARGE_STATUS <>'canceled')
or
((transaction_is_financed='FALSE' or transaction_is_financed is NULL) and CHARGE_STATUS in ('charged','refunded','disputed','disputeLost') and charge_payment_method_type !='outOfPlatform')
);

/*Dim_cancelled_charges*/
create or replace table "BALANCE_V2"."STG"."DIM_CANCELLED_CHARGES" as 
(
select * from "BALANCE_V2"."STG"."DIM_ALL_CHARGES"
where transaction_is_financed='TRUE' and CHARGE_STATUS ='canceled'
);

/*Dim_buyer*/
create or replace table "BALANCE_V2"."STG"."DIM_BUYER" as
(
with stripe_cust_id as
(
select distinct buyer_id as buyer_id,
 token as stripe_buyer_id
from "BALANCE_V2"."DBO"."BUYER_THIRD_PARTY_TOKEN"
where subject='stripe-cid' or subject='stripe-cust'
),
buyer_qualification as
(
select distinct ENTITY_ID,
  first_value(status IGNORE NULLS) over (partition by ENTITY_ID order by CREATED_AT desc) as last_status,
    first_value(id IGNORE NULLS) over (partition by ENTITY_ID order by CREATED_AT desc) as last_qualification_id,
  count(case when status = 'APPROVED' or status ='DECLINED' then ID end) over (partition by ENTITY_ID) as num_of_qualifications
from "BALANCE_V2"."DBO"."QUALIFICATION"
),

terms_event_log_status as  (
select 
distinct parse_json(payload):buyerId::string as buyer_id 
,FIRST_VALUE(type) IGNORE NULLS OVER (PARTITION BY parse_json(payload):buyerId::string ORDER BY created_at desc ) as terms_activation_status 
,FIRST_VALUE(event_time) IGNORE NULLS OVER (PARTITION BY parse_json(payload):buyerId::string ORDER BY created_at desc ) as terms_activation_event_time
from BALANCE_V2.DBO.TERMS_EVENT_LOG
where type = 'activated_credit' or type = 'deactivated_credit'
),

num_suspensions as (
select entity_id, 
sum(case when c.status = 'SUSPENDED' then 1 else 0 end) as number_of_suspensions
from "BALANCE_V2"."DBO"."CUSTOMER_SUSPENSION" c
group by 1
), 

last_status as (
select entity_id, status, created_at, 
row_number() over (partition by entity_id order by created_at desc) as last_status
from "BALANCE_V2"."DBO"."CUSTOMER_SUSPENSION"
), 

buyer_status as (

select l.entity_id, l.status as suspension_status, l.created_at as last_suspension_status_update_date, 
number_of_suspensions 
from last_status l
join num_suspensions n on l.entity_id = n.entity_id
where last_status = 1
),

test_buyers as (
select
buyer_id
from "BALANCE_V2"."STG"."DIM_BUYER"
where total_tpv_usd<50 and (buyer_email like '%yopmail%' or buyer_email like '%mailinator' or buyer_name like '%test%')
),payment_dates as
(
select buyer_id
,min(date) as first_payment_date
,max(date) as last_payment_date
from BALANCE_V2.STG.DIM_PAYMENTS
group by buyer_id
),
buyer_charge_trx as 
(
select buyer_id,
sum( case when determining_charge_date is not null then tpv_usd else 0 end) as total_tpv_usd,
sum(case when determining_charge_date is not null then revenue_usd else 0 end) as total_revenue_usd,
min(transaction_created_date) as first_transaction_date,
max(transaction_created_date) as last_transaction_date,
sum(case when date_trunc('MONTH', determining_charge_date)=curr_year_month then tpv_usd else 0 end) as total_tpv_usd_mtd,
sum(case when date_trunc('QUARTER', determining_charge_date)=curr_year_quarter then tpv_usd else 0 end) as total_tpv_usd_qtd,
sum(case when date_trunc('MONTH', determining_charge_date)=curr_year_month then revenue_usd else 0 end) as total_revenue_usd_mtd,
sum(case when date_trunc('QUARTER', determining_charge_date)=curr_year_quarter then revenue_usd else 0 end) as total_revenue_usd_qtd,
count(distinct TRANSACTION_ID ) as total_num_of_transactions,
sum(days_late) as payment_days,
count(distinct(case when days_late > 1 then TRANSACTION_ID end)) as total_num_of_late_transactions
from
(
select c.buyer_id, round(AMOUNT_IN_CENTS/100,2) as tpv_usd,round((PROCESSING_FEE_IN_CENTS+t.factoring_fee_in_cents)/100,2) as revenue_usd,c.CHARGE_DATE,PAID_DATE,c.CREATED_AT,c.TRANSACTION_ID,CHARGE_STATUS,t.IS_FINANCED,
date_trunc('MONTH', current_date()) as curr_year_month,date_trunc('QUARTER', current_date()) as curr_year_quarter,t.status as transaction_status,t.CREATED_AT as transaction_created_date,
case when t.IS_FINANCED ='True' then datediff('day',c.CHARGE_DATE,coalesce(c.PAID_DATE,current_date())) end as days_late,
case
    when t.IS_FINANCED ='True' then c.CREATED_AT 
    else 
        case when CHARGE_STATUS = 'charged' then coalesce(PAID_DATE,c.CHARGE_DATE) 
        else null
        end
end as determining_charge_date
from "BALANCE_V2"."DBO"."CHARGE" c inner join "BALANCE_V2"."DBO"."TRANSACTION" t
on c.TRANSACTION_ID = t.id
where t.status = 'closed' or (status='auth' and captured_amount>0)
)  
trx_cte
group by buyer_id
)
select 
b.ID as buyer_id,
sci.stripe_buyer_id,
b.NAME as buyer_name,	
b.EMAIL as buyer_email,
b.FIRST_NAME as buyer_first_name,
b.LAST_NAME as buyer_last_name,
b.PHONE as buyer_phone,
b.CREATED_AT	as buyer_created_at,
current_date()-date(b.CREATED_AT) as days_since_created,
b.UPDATED_AT	as buyer_updated_at,
b.BUSINESS_ID	as buyer_business_id,
b.DELETED_AT	as buyer_deleted_at,
case when tst.buyer_id is not null then 1 else 0 end as is_test_buyer,
case when b.deleted_at is not null then 1 else 0 end as buyer_is_deleted,
b.PUBLIC_ID	as buyer_public_id,
b.MERCHANT_ID as buyer_merchant_id,
b.COMMUNICATION_CONFIG as buyer_communication_config,
b.funding_source as buyer_funding_source,
first_transaction_date,
last_transaction_date,
pd.first_payment_date,
pd.last_payment_date,
coalesce(total_tpv_usd,0) as total_tpv_usd,
coalesce(total_revenue_usd,0) as total_revenue_usd,
coalesce(total_tpv_usd_mtd,0) as total_tpv_usd_mtd,
coalesce(total_tpv_usd_qtd,0) as total_tpv_usd_qtd,
coalesce(total_revenue_usd_mtd,0) as total_revenue_usd_mtd,
coalesce(total_revenue_usd_qtd,0) as total_revenue_usd_qtd,
coalesce(total_num_of_transactions,0) as total_num_of_transactions,
coalesce(round(total_tpv_usd/total_num_of_transactions,2),0) as avg_transaction_amount_usd,
coalesce(total_num_of_late_transactions,0) as total_num_of_late_transactions,
coalesce(round(total_num_of_late_transactions/total_num_of_transactions,2),0) as late_percentage,
coalesce(round(payment_days/total_num_of_transactions,2),0) as avg_payment_days,
coalesce(last_status,'never had terms') as current_terms_status,
cl.CREDIT_LIMIT/100 as credit_limit_buyer_usd,
coalesce(num_of_qualifications,0) as num_of_qualifications,
case when s.suspension_status = 'SUSPENDED' then 'Yes' else 'No' end as is_currently_suspended, 
s.last_suspension_status_update_date, 
s.number_of_suspensions,
coalesce(tel.terms_activation_status,'none') as terms_activation_status,
tel.terms_activation_event_time,
current_timestamp as _RIVERY_LAST_UPDATE
from "BALANCE_V2"."DBO"."BUYER" b 
left join buyer_qualification q
on b.PUBLIC_ID = q.ENTITY_ID
left join buyer_charge_trx c
on b.id = c.buyer_id
left join "BALANCE_V2"."DBO".CUSTOMER_CREDIT_LIMIT cl 
on q.last_qualification_id = cl.qualification_id 
left join buyer_status s 
on b.public_id = s.entity_id
left join terms_event_log_status tel 
on b.id = tel.buyer_id
left join test_buyers tst
on b.id=tst.buyer_id
left join stripe_cust_id sci
on b.id=sci.buyer_id
left join payment_dates pd 
on b.public_id = pd.buyer_id
);




/*Dim_merchant*/
create or replace table "BALANCE_V2"."STG"."DIM_MERCHANT" as
(
with merchant_max_month as(
select
merchant_id,
to_char (revenue_date,'yyyy-mm') as month_tpv,
sum(charge_amount) as max_month
from BALANCE_V2.DBO.ACCOUNTING_REPORT_ANALYTICS_WITH_PAID_DATE
group by 1,2
qualify row_number() over (partition by merchant_id order by max_month desc) = 1
),merchant_balance as
(
SELECT merchant_id, ARRAY_AGG(OBJECT_CONSTRUCT('amount', TO_NUMBER(amount, 10,2) , 'currency', currency)) as merchant_balance
from "BALANCE_V2"."DBO"."MERCHANT_BALANCE"
GROUP BY merchant_id
),configuration_table as
(
select distinct entity_id
,round(first_value(case when name = 'PARENT_MAX_QUALIFICATION_SUM_LIMIT' then value/100 else null end ignore nulls) over (partition by entity_id order by created_at desc),2) as max_qualification_limit
,round(first_value(case when name = 'PARENT_MAX_OUTSTANDING_SUM_LIMIT' then value/100 else null end ignore nulls) over (partition by entity_id order by created_at desc),2) as max_outstanding_limit
from BALANCE_V2.DBO.CONFIGURATION
),merchant_payment_dates as
(
select merchant_id
,min(date) as first_payment_date
,max(date) as last_payment_date
from BALANCE_V2.STG.DIM_PAYMENTS
group by merchant_id
),payout_calc as 
(
select MERCHANT_ID
,round(sum(AMOUNT_REQUESTED),2) as total_amount_requested 
,round(sum(case when merchant_id = vendor_id then AMOUNT_REQUESTED end),2) as self_payout
,round(sum(case when merchant_id != vendor_id then AMOUNT_REQUESTED end),2) as vendor_payout
from 
BALANCE_V2.DBO.PAYOUT
where STATUS = 'paid'
group by 1
),byr as 
(
select
count(buyer_id) as num_of_buyers,
sum (case when total_tpv_usd>0 then 1 else 0 end) as num_of_transacted_buyers,
buyer_merchant_id
from BALANCE_V2.stg.dim_buyer where buyer_deleted_at is null
group by 3
), vndr as
(
select 
count(vendor_id) as num_of_vendors,
sum (case when total_payout>0 then 1 else 0 end) as num_of_transacted_vendors,
vendor_parent_id

from BALANCE_V2.stg.dim_vendor where vendor_deleted_at is null
group by 3

),topup_calc as
(
select MERCHANT_ID,round(sum(AMOUNT),2) as total_amount
from BALANCE_V2.DBO.TOPUP
group by 1
),loss_calc as
(
select MERCHANT_ID
,round(sum(case when CREDIT_BAD is not null then CHARGE_AMOUNT_USD else 0 end),2) as total_bad_loss
,round(sum(case when FRAUD_BAD is not null then CHARGE_AMOUNT_USD else 0 end),2) as total_fraud_loss
from BALANCE_V2.STG.DIM_CHARGES
group by 1
), dal as
(
select merchant_id,
round(sum(case when l_365 = 1 then charge_amount else 0 end),2) as total_writeoffs,
round(sum(case when is_recovered = 1 then charge_amount else 0 end),2) as total_recovery
from BALANCE_V2.DBO.DIM_ALL_LOANS 
group by 1
),seller_credit_limit as
(
select MERCHANT_ID,round(sum(credit_limit)/100) as credit_limit_usd
from "BALANCE_V2"."DBO"."BUYER" b left join "TERMS"."DBO"."CUSTOMER_CREDIT_LIMIT" c
on b.public_id = c.entity_id
group by MERCHANT_ID
),merchant_cl as
(
select merchant_id,sum(credit_limit) as current_total_credit_limit
from BALANCE_V2.STG.DIM_BUYER_CREDIT_LIMIT
where balance_date = current_date()
group by merchant_id
),hubspot_deal as
(
select DEALID,SE_OWNER_VALUE,CS_OWNER_VALUE,HUBSPOT_OWNER_ID_VALUE,INDUSTRY_DEAL__VALUE,BUSINESS_TYPE_NEW_VALUE,round(ANNUAL_TPV_VALUE,2) as ANNUAL_TPV_VALUE ,ecommerce_platform_value,CONTRACT_SIGN_DATE_VALUE,MOVE_TO_LIVE_STAG_VALUE,fully_ramped_date_value,
round(ANNUAL_TPV_VALUE/12,2) as MONTHLY_TPV_VALUE,round(AMOUNT_VALUE/12,2) as MONTHLY_REVENUE_VALUE,DEALSTAGE_VALUE,DB_MERCHANT_ID_VALUE,S_ID_VALUE,SMB_BUYERS_VALUE,PRODUCT_VALUE,TRY_TO_NUMBER(MONTHLY_MINIMUM_VALUE) as MONTHLY_MINIMUM_VALUE,
associations_associatedcompanyids[0] as deal_company_id
from "BALANCE_V2"."DBO"."HUBSPOT_DEAL_CUSTOM"
),seller_charge_trx as 
(
select MERCHANT_ID,
sum( case when determining_charge_date is not null then tpv_usd else 0 end) as total_tpv_usd,
sum(case when determining_charge_date is not null then revenue_usd else 0 end) as total_revenue_usd,
min(transaction_created_date) as first_transaction_date,
max(transaction_created_date) as last_transaction_date,
sum(case when date_trunc('MONTH', determining_charge_date)=curr_year_month then tpv_usd else 0 end) as total_tpv_usd_mtd,
sum(case when date_trunc('QUARTER', determining_charge_date)=curr_year_quarter then tpv_usd else 0 end) as total_tpv_usd_qtd,
sum(case when date_trunc('MONTH', determining_charge_date)=curr_year_month then revenue_usd else 0 end) as total_revenue_usd_mtd,
sum(case when date_trunc('QUARTER', determining_charge_date)=curr_year_quarter then revenue_usd else 0 end) as total_revenue_usd_qtd,
count(distinct TRANSACTION_ID ) as total_num_of_transactions,
round(sum(case when determining_charge_date is not null then tpv_usd else 0 end)/nullifzero(count(distinct date_trunc(month,determining_charge_date))),2) as average_monthly_tpv
from
(
select c.MERCHANT_ID, round(AMOUNT_IN_CENTS/100,2) as tpv_usd,round((PROCESSING_FEE_IN_CENTS+t.factoring_fee_in_cents)/100,2) as revenue_usd,
  c.CHARGE_DATE,PAID_DATE,c.CREATED_AT,c.TRANSACTION_ID,CHARGE_STATUS,t.IS_FINANCED,
date_trunc('MONTH', current_date()) as curr_year_month,date_trunc('QUARTER', current_date()) as curr_year_quarter,t.status as transaction_status,t.CREATED_AT as transaction_created_date,
case
    when t.IS_FINANCED ='True' then c.CREATED_AT 
    else 
        case when CHARGE_STATUS = 'charged' then coalesce(PAID_DATE,c.CHARGE_DATE) 
        else null
        end
end as determining_charge_date
from "BALANCE_V2"."DBO"."CHARGE" c inner join "BALANCE_V2"."DBO"."TRANSACTION" t
on c.TRANSACTION_ID = t.id
where t.status = 'closed'
)  
trx_cte
group by MERCHANT_ID
)
select 
s.ID as merchant_id,
s.NAME as merchant_name,	
s.DOMAIN as merchant_domain,
s.CREATED_AT	as merchant_created_at,
current_date()-date(s.CREATED_AT) as days_since_created,
s.UPDATED_AT	as merchant_updated_at,
s.PARENT_ID		as merchant_parent_id,
s.DELETED_AT	as merchant_deleted_at,
case when s.DELETED_AT is not null then 1 else 0 end as merchant_is_deleted,
s.MERCHANT_TOKEN	as merchant_token,
s.BUSINESS_ID as merchant_business_id,
s.CONTACT_USER_ID as merchant_contact_id,
/*s.STATUS as merchant_status,*/
/*case
when datediff ('month',TRY_TO_DATE(h.CONTRACT_SIGN_DATE_VALUE),current_date)<6 and total_tpv_usd<1000 then 'Recently signed-not active'
when (s.STATUS<>'active' and coalesce(total_tpv_usd,0)<50) or s.id in ('590') or ((h.DEALSTAGE_VALUE not in('appointmentscheduled','presentationscheduled','9105133','42432330','closedlost','64638760') or h.DEALSTAGE_VALUE is null) and last_transaction_date is null) then 'test/irrelevant'
when (h.DEALSTAGE_VALUE<>'closedlost' or h.DEALSTAGE_VALUE is null) and datediff ('day',last_transaction_date,current_date)>180 and total_tpv_usd>=1000 then 'dormant'
when (h.DEALSTAGE_VALUE= 'closedlost' or h.DEALSTAGE_VALUE is null) and total_tpv_usd<1000 then 'Never Active'
when h.DEALSTAGE_VALUE= 'closedlost' and total_tpv_usd>=1000 then 'Offboarded'
when s.id in (691,1391,4634,1511,1524) then 'Suspected as Fraud'
else 'active' end as merchant_status,*/
case when hc.properties_company_status_value is null and (s.STATUS<>'active' and coalesce(total_tpv_usd,0)<50) then 'Test/Irrelevant'
when hc.properties_company_status_value='Live' and datediff(day,last_transaction_date,current_date) >180 then 'Dormant'
else hc.properties_company_status_value end as merchant_status,ma.businessaddress_address1 as merchant_address,
ma.businessaddress_city as merchant_city,
ma.state as merchant_state,
ma.businessaddress_country as merchant_country,
s.CAPABLITIES as merchant_capabilities,
s.CHARGES_ENABLED	 as merchant_charges_enabled,
s.PAYOUTS_ENABLED as merchant_payouts_enabled,
s.ALLOWED_PAYMENT_METHODS as merchant_allowed_payment_methods,
s.PUBLIC_ID as merchant_public_id,
s.CURRENCY as merchant_currency,
first_transaction_date,
last_transaction_date,
mpd.first_payment_date,
mpd.last_payment_date,
coalesce(total_tpv_usd,0) as total_tpv_usd,
coalesce(total_revenue_usd,0) as total_revenue_usd,
coalesce(total_tpv_usd_mtd,0) as total_tpv_usd_mtd,
coalesce(total_tpv_usd_qtd,0) as total_tpv_usd_qtd,
coalesce(total_revenue_usd_mtd,0) as total_revenue_usd_mtd,
coalesce(total_revenue_usd_qtd,0) as total_revenue_usd_qtd,
coalesce(total_num_of_transactions,0) as total_num_of_transactions,
coalesce(credit_limit_usd,0) as total_approved_credit_limit_usd,
coalesce(round(total_tpv_usd/total_num_of_transactions,2),0) as avg_transaction_amount_usd,
coalesce(average_monthly_tpv,0) as average_monthly_tpv,
ifnull(MONTHLY_MINIMUM_VALUE,0) as monthly_minimum_fees,
h.DEALID as deal_id,
//h.SE_OWNER_VALUE as se_owner_code,
concat(ose.FIRSTNAME,' ',ose.LASTNAME) as se_deal_owner_name,
//h.CS_OWNER_VALUE as cs_owner_code,
concat(ocs.FIRSTNAME,' ',ocs.LASTNAME) as cs_deal_owner_name,
concat(oh.FIRSTNAME,' ',oh.LASTNAME) as hubspot_deal_owner_name,
h.INDUSTRY_DEAL__VALUE as industry,
h.BUSINESS_TYPE_NEW_VALUE as business_type,
h.PRODUCT_VALUE as product,
h.ecommerce_platform_value as ecom_platform,
TRY_TO_DATE(h.CONTRACT_SIGN_DATE_VALUE) as contract_sign_timestamp,
TRY_TO_DATE(h.MOVE_TO_LIVE_STAG_VALUE) as go_live_timestamp,
TRY_TO_DATE(h.fully_ramped_date_value) as fully_ramped_date,

h.SMB_BUYERS_VALUE as smb_buyers,
coalesce(h.ANNUAL_TPV_VALUE,0) as expected_annual_tpv,
coalesce(h.MONTHLY_TPV_VALUE,0) as expected_monthly_tpv,
coalesce(h.MONTHLY_REVENUE_VALUE,0) as expected_monthly_revenue,
case when (mmm.max_month/nullifzero(coalesce(h.MONTHLY_TPV_VALUE,0)))>=0.5 then 1 else 0 end as is_ramped_customer,
/*case
   when h.DEALSTAGE_VALUE = 'appointmentscheduled' then 'Discovery' 
   when h.DEALSTAGE_VALUE = 'presentationscheduled' then 'POC'
   when h.DEALSTAGE_VALUE = '64638760' then 'Qualify'
   when h.DEALSTAGE_VALUE = '9105133' then 'Negotiation'
   when h.DEALSTAGE_VALUE = '42432330' then 'Closed Won'
   when h.DEALSTAGE_VALUE = 'closedlost' then 'Closed lost'
   else null end as deal_stage,*/
hc.properties_company_status_value as deal_stage,
mb.merchant_balance as merchant_current_balance,
round(mcl.current_total_credit_limit/100,2) as current_total_credit_limit,
coalesce(lc.total_bad_loss,0) as total_bad_loss,
coalesce(lc.total_fraud_loss,0) as total_fraud_loss,
coalesce(rr.REPAYMENT_RATE_USD,0) as repayment_rate_usd,
coalesce(total_writeoffs,0) as total_writeoffs,
coalesce(total_recovery,0) as total_recovery,
coalesce(total_amount_requested,0) as total_payout,
coalesce(self_payout,0) as self_payout,
coalesce(vendor_payout,0) as vendor_payout,
coalesce(tpc.total_amount,0) as total_topup,
coalesce(byr.num_of_buyers,0) as number_of_buyers,
coalesce(byr.num_of_transacted_buyers,0) as number_of_transacted_buyers,
coalesce(vndr.num_of_vendors,0) as number_of_vendors,
coalesce(vndr.num_of_transacted_vendors,0) as number_of_transacted_vendors,
ct.max_qualification_limit,
ct.max_outstanding_limit,
current_timestamp as _RIVERY_LAST_UPDATE
from "BALANCE_V2"."DBO"."SELLER" s
left join seller_credit_limit cl
on S.ID = cl.MERCHANT_ID
left join seller_charge_trx c
on S.ID = c.merchant_id
left join hubspot_deal h
on s.merchant_token = h.S_ID_VALUE
left join BALANCE_V2.DBO.HUBSPOT_COMPANIES_CUSTOM hc
on h.deal_company_id = hc.companyid
left join "BALANCE_V2"."DBO"."HUBSPOT_OWNER_CUSTOM" ose
on h.SE_OWNER_VALUE = ose.OWNERID
left join "BALANCE_V2"."DBO"."HUBSPOT_OWNER_CUSTOM" ocs
on h.CS_OWNER_VALUE = ocs.OWNERID
left join "BALANCE_V2"."DBO"."HUBSPOT_OWNER_CUSTOM" oh
on h.HUBSPOT_OWNER_ID_VALUE = oh.OWNERID
left join merchant_balance mb
on S.ID = mb.MERCHANT_ID
left join merchant_cl mcl 
on s.id = mcl.merchant_id
left join merchant_max_month mmm
on s.id=mmm.merchant_id
left join BALANCE_V2.DBO.MERCHANT_ADDRESSES ma
on s.ID = ma.merchant_id
left join loss_calc lc
on s.id = lc.merchant_id
left join BALANCE_V2.DBO.MERCHANT_REPAYMENT_RATES rr
on s.id = rr.merchant_id
left join dal 
on s.id = dal.merchant_id
left join payout_calc p
on s.id = p.merchant_id
left join topup_calc tpc
on s.id = tpc.merchant_id
left join configuration_table ct
on s.merchant_token = ct.entity_id
left join byr
on s.ID=byr.buyer_merchant_id
left join vndr
on s.ID=vndr.vendor_parent_id
left join merchant_payment_dates mpd
on s.id = mpd.merchant_id
where PARENT_ID is null
);


/*Dim_vendor*/
create or replace table "BALANCE_V2"."STG"."DIM_VENDOR" as
(
with seller_credit_limit as
(
select MERCHANT_ID,round(sum(credit_limit)/100) as credit_limit_usd
from "BALANCE_V2"."DBO"."BUYER" b left join "TERMS"."DBO"."CUSTOMER_CREDIT_LIMIT" c
on b.public_id = c.entity_id
group by MERCHANT_ID
),payment as 
(
select distinct transaction_id,vendor_id
from BALANCE_V2.DBO.PAYMENT
where vendor_id != merchant_id
),payout_calc as 
(
select VENDOR_ID
,sum(AMOUNT_REQUESTED) as total_amount_requested 
from 
BALANCE_V2.DBO.PAYOUT
where STATUS = 'paid' and merchant_id != vendor_id
group by 1
),seller_charge_trx2 as 
(
select vendor_id,
sum( case when determining_charge_date is not null then tpv_usd else 0 end) as total_tpv_usd,
/*sum(case when determining_charge_date is not null then revenue_usd else 0 end) as total_revenue_usd,*/
min(transaction_created_date) as first_transaction_date,
max(transaction_created_date) as last_transaction_date,
sum(case when date_trunc('MONTH', determining_charge_date)=curr_year_month then tpv_usd else 0 end) as total_tpv_usd_mtd,
sum(case when date_trunc('QUARTER', determining_charge_date)=curr_year_quarter then tpv_usd else 0 end) as total_tpv_usd_qtd,
/*sum(case when date_trunc('MONTH', determining_charge_date)=curr_year_month then revenue_usd else 0 end) as total_revenue_usd_mtd,
sum(case when date_trunc('QUARTER', determining_charge_date)=curr_year_quarter then revenue_usd else 0 end) as total_revenue_usd_qtd,*/
count(distinct TRANSACTION_ID ) as total_num_of_transactions
from
(
select payment.VENDOR_ID, round(AMOUNT_IN_CENTS/100,2) as tpv_usd,round((PROCESSING_FEE_IN_CENTS+t.factoring_fee_in_cents)/100,2) as revenue_usd,
  c.CHARGE_DATE,PAID_DATE,c.CREATED_AT,c.TRANSACTION_ID,CHARGE_STATUS,t.IS_FINANCED,
date_trunc('MONTH', current_date()) as curr_year_month,date_trunc('QUARTER', current_date()) as curr_year_quarter,t.status as transaction_status,t.CREATED_AT as transaction_created_date,
case
    when t.IS_FINANCED ='True' then c.CREATED_AT 
    else 
        case when CHARGE_STATUS = 'charged' then coalesce(PAID_DATE,c.CHARGE_DATE) 
        else null
        end
end as determining_charge_date
from "BALANCE_V2"."DBO"."CHARGE" c inner join "BALANCE_V2"."DBO"."TRANSACTION" t
on c.TRANSACTION_ID = t.id inner join payment on t.id = payment.transaction_id
where t.status = 'closed'
)  
trx_cte
group by VENDOR_ID
)
select 
s.ID as vendor_id,
s.NAME as vendor_name,	
s.DOMAIN as vendor_domain,
s.CREATED_AT	as vendor_created_at,
current_date()-date(s.CREATED_AT) as days_since_created,
s.UPDATED_AT	as vendor_updated_at,
s.PARENT_ID		as vendor_parent_id,
s.DELETED_AT	as vendor_deleted_at,
case when s.DELETED_AT is not null then 1 else 0 end as vendor_is_deleted,
s.MERCHANT_TOKEN	as vendor_token,
s.BUSINESS_ID as vendor_business_id,
s.CONTACT_USER_ID as vendor_contact_id,
s.STATUS as vendor_status,
s.CAPABLITIES as vendor_capabilities,
s.CHARGES_ENABLED	 as vendor_charges_enabled,
s.PAYOUTS_ENABLED as vendor_payouts_enabled,
s.ALLOWED_PAYMENT_METHODS as vendor_allowed_payment_methods,
s.PUBLIC_ID as vendor_public_id,
s.CURRENCY as vendor_currency,
c2.first_transaction_date,
c2.last_transaction_date,
/*coalesce(c.total_tpv_usd,0) as total_tpv_usd,
coalesce(c.total_tpv_usd_mtd,0) as total_tpv_usd_mtd,
coalesce(c.total_tpv_usd_qtd,0) as total_tpv_usd_qtd,
coalesce(c.total_num_of_transactions,0) as total_num_of_transactions,
coalesce(round(c.total_tpv_usd/c.total_num_of_transactions,2),0) as avg_transaction_amount_usd,*/
coalesce(p.total_amount_requested,0) as total_payout,
coalesce(c2.total_tpv_usd,0) as total_tpv_usd2,
coalesce(c2.total_tpv_usd_mtd,0) as total_tpv_usd_mtd2,
coalesce(c2.total_tpv_usd_qtd,0) as total_tpv_usd_qtd2,
coalesce(c2.total_num_of_transactions,0) as total_num_of_transactions2,
coalesce(round(c2.total_tpv_usd/c2.total_num_of_transactions,2),0) as avg_transaction_amount_usd2,
current_timestamp as _RIVERY_LAST_UPDATE
from "BALANCE_V2"."DBO"."SELLER" s
left join seller_credit_limit cl
on S.ID = cl.MERCHANT_ID
/*left join seller_charge_trx c
on S.ID = c.merchant_id replaced at 25/7/2023 fro Sivan's request https://balancepoc.monday.com/boards/2963149087/pulses/4389522340 */
left join seller_charge_trx2 c2
on S.ID = c2.vendor_id
left join "BALANCE_V2"."DBO"."MERCHANT_BALANCE" mb
on S.ID = mb.MERCHANT_ID
left join payout_calc p 
on s.id = p.vendor_id
where PARENT_ID is not null
);

/*PAYOUT_INITIATION_FAILS*/
create or replace table  "BALANCE_V2"."STG"."PAYOUT_INITIATION_FAILS" as (
    SELECT distinct
    id,
    created_at,
    merchant_id,
    to_account as seller_id,
    entity_id as entity_id_number,
    event_type,
    error_code,
    error_message,
    source
    
    from 
    "BALANCE_V2"."DBO"."PAYMENT_EVENT_LOG" 

   /* where type='payout' and subtype='payout-initiate-failed'*/ 
    where EVENT_TYPE='payout-initiate-failed' 
    );
    
 /*PAYOUT_ISSUE*/   
create or replace table "BALANCE_V2"."STG"."PAYOUT_ISSUE" as (
    SELECT distinct 
    id,
    created_at,
    merchant_id,
    to_account as seller_id,
    entity_id as entity_id_number,
    event_type,
    error_code,
    error_message,
    source
    from  
    "BALANCE_V2"."DBO"."PAYMENT_EVENT_LOG"

 /*   where type='payout' and subtype in ('payout-failed','transfer-failed')*/    
     where EVENT_TYPE in ('payout-failed','transfer-failed')                                
                            
    );
    
/*SUCCESFUL_PAYOUTS*/    
create or replace table "BALANCE_V2"."STG"."SUCCESFUL_PAYOUTS" as (
    SELECT distinct
    id,
    created_at,
    merchant_id,
    to_account as seller_id,
    entity_id as entity_id_number,
    event_type,
    error_code,
    error_message,
    source
    from 
    "BALANCE_V2"."DBO"."PAYMENT_EVENT_LOG" 


  /* where type='payout' and subtype='payout-completed'*/
    where EVENT_TYPE = 'payout-completed'
    union all 
    select  *
    from "BALANCE_V2"."STG"."PAYOUT_INITIATION_FAILS"                               
    );


/*FINAL_PAYOUTS_DISTRIBUTION*/
create or replace table "BALANCE_V2"."STG"."FINAL_PAYOUTS_DISTRIBUTION" as (
 with payout_source as (
    select 
    entity_id,
    max(source) as source,
    max(origin) as origin
    from balance_v2.dbo.payment_event_log
    where entity_type='payout' and event_type='payout-initiate-completed'
    group by 1
    
    )

    SELECT distinct  
a.*,
case when b.entity_id_number is not null then b.event_type
     when a.entity_id_number is null then 'payout-initiate-failed'
     else 'Successful Payout' end as payout_status_distribution,
case when a.entity_id_number is null then (row_number() over (partition by a.merchant_id,a.seller_id,to_char(a.created_at,'yyyy-mm-dd'),(case when a.entity_id_number is null then 1 else 0 end)
                     order by b.created_at desc))  else null end as distinct_attempts,   
b.error_code as payout_error,
b.error_message as payout_error_message,
c.source as initiation_source,
c.origin as initiation_origin
from "BALANCE_V2"."STG"."SUCCESFUL_PAYOUTS" a
left join "BALANCE_V2"."STG"."PAYOUT_ISSUE" b
on a.entity_id_number=b.entity_id_number
left join payout_source c 
on a.entity_id_number=c.entity_id
);


/*GOOGLE_ADS_FUNNEL*/
create or replace table "BALANCE_V2"."STG"."GOOGLE_ADS_FUNNEL" as
(
with cte as
(
select 
    CAMPAIGN_ID,
    CAMPAIGN_NAME,
    SEGMENTS_DATE,
    sum(METRICS_IMPRESSIONS) as impressions,
    sum(METRICS_CLICKS) as clicks,
    sum(METRICS_CONVERSIONS) as conversions,
    sum(METRICS_COSTMICROS) as cost
from BALANCE_V2.DBO.GOOGLE_ADS_AD_METRICS
group by 1,2,3
)
select 
CAMPAIGN_ID,
CAMPAIGN_NAME,
SEGMENTS_DATE,
impressions,
clicks,
conversions,
round(clicks/nullifzero(impressions),3) as ctr,
round(conversions/nullifzero(clicks),3) as conversion_rate,
round(cost/nullifzero(conversions),3) as cost_per_conversion,
round(cost/nullifzero(clicks),3) as cost_per_click
from cte
)
;


create or replace table BALANCE_V2.DBO.MERCHANT_REPAYMENT_RATES as (
select
    merchant_id,
    cast( AVG(case when is_paid=1 then (PAYMENT_DAYS) else null end) as decimal (15,2)) AS DSO, /* only for paid amounts */
    sum( case when is_paid=1 then charge_amount else 0 end) as total_paid_usd,
    sum( case when is_paid=0 then charge_amount else 0 end) as total_outstanding_usd,
    sum( case when is_paid=0 and early<>1 then charge_amount else 0 end) as total_due_unpaid_usd,
    sum( case when is_paid=1 then 1 else 0 end) as total_paid_count,
    sum( case when is_paid=0 then 1 else 0 end) as total_outstanding_count,
        cast 
            (sum( case when is_paid=1 then charge_amount else 0 end)/
             nullifzero (sum( case when is_paid=1 then charge_amount else 0 end)+sum( case when is_paid=0 and early<>1 and payment_days>5 then charge_amount else 0         end)) 
             as decimal (15,2)) as repayment_rate_usd,
        cast
            (sum( case when is_paid=1 then 1 else 0 end)/
            nullifzero (sum( case when is_paid=1 then 1 else 0 end)+sum( case when is_paid=0 and early<>1 and payment_days>5 then 1 else 0 end)) 
            as decimal (15,2)) as repayment_rate_count
    from BALANCE_V2.DBO.dim_all_loans
    group by 1
    );



create or replace table BALANCE_V2.DBO.BUYER_REPAYMENT_RATES as (

with first_invoice as (
select
buyer_id,
charge_id, 
payment_days,
due_date,
row_number () OVER (PARTITION BY buyer_id ORDER BY due_date asc NULLS LAST) as due_date_rank


from balance_v2.dbo.dim_all_loans

)


select
    dal.buyer_id,
    max(merchant_name) as merchant_name,
    cast( AVG(case when is_paid=1 then (dal.PAYMENT_DAYS) else null end) as decimal (15,2)) AS DSO, /* only for paid amounts */
    sum(charge_amount) as total_tpv,
    sum( case when is_paid=1 then charge_amount else 0 end) as total_paid_usd,
    sum( case when is_paid=0 then charge_amount else 0 end) as total_outstanding_usd,
    sum( case when is_paid=0 and early= 1 then charge_amount else 0 end) as total_early_unpaid_usd,
    sum( case when is_paid=0 and early<>1 then charge_amount else 0 end) as total_due_unpaid_usd,
    count(*) as total_count,
    sum( case when is_paid=1 then 1 else 0 end) as total_paid_count,
    sum( case when is_paid=0 then 1 else 0 end) as total_outstanding_count,
    max(dal.payment_days) as max_late_repayment,
    max(case when is_paid=0 then dal.payment_days end) as max_current_late,

    max(dal.paid_date) as last_paid_date,
    avg(fi.payment_days) as first_repayment_dpd,
        cast 
            (sum( case when is_paid=1 then charge_amount else 0 end)/
             nullifzero (sum( case when is_paid=1 then charge_amount else 0 end)+sum( case when is_paid=0 and early<>1 and dal.payment_days>5 then charge_amount else 0 end)) 
             as decimal (15,2)) as repayment_rate_usd,
        cast
            (sum( case when is_paid=1 then 1 else 0 end)/
            nullifzero (sum( case when is_paid=1 then 1 else 0 end)+sum( case when is_paid=0 and early<>1 and dal.payment_days>5 then 1 else 0 end)) 
            as decimal (15,2)) as repayment_rate_count
    
    
    
    from balance_v2.dbo.dim_all_loans dal 
    left join first_invoice fi 
    on dal.buyer_id=fi.buyer_id and due_date_rank=1
    
    group by 1
    );



create or replace table BALANCE_V2.DBO.LOAN_PROJECTED_LOSSES as (
    select
    a.*,
    b.dso as merchant_dso,
    c.dso as buyer_dso,
    b.repayment_rate_usd as merchant_repayment_rate,
    c.repayment_rate_usd as buyer_repayment_rate,
    case when is_paid=0 then (charge_amount*(1-b.repayment_rate_usd))  else 0 end as expected_credit_loss,
    case when is_paid=0 then dateadd(day, 120, to_date(due_date)) else null end as expected_loss_date
    from BALANCE_V2.DBO.dim_all_loans a
    left join BALANCE_V2.DBO.merchant_repayment_rates b 
    on a.merchant_id=b.merchant_id and is_paid=0
    left join BALANCE_V2.DBO.buyer_repayment_rates c
    on a.buyer_id=c.buyer_id and is_paid=0   
);

create or replace table BALANCE_V2.DBO.first_transfer as (
SELECT 
payout_id,
min(created_at) as first_transfer
from BALANCE_V2.DBO.PAYOUT_STATUS_EVENT
where status='transfer/completed'
group by 1 
    );
    
    
create or replace table BALANCE_V2.DBO.first_paid as (  
    SELECT 
payout_id,
min(created_at) as first_paid
from BALANCE_V2.DBO.PAYOUT_STATUS_EVENT
where status='payout/paid'
group by 1
    );
    
    
create or replace table "BALANCE_V2"."DBO"."first_payout" as (
    SELECT 
payout_id,
min(created_at) as first_payout
from BALANCE_V2.DBO.PAYOUT_STATUS_EVENT
where status='payout/established'
group by 1  
    );
    
    

create or replace table "BALANCE_V2"."DBO"."payout_status_event_pivot" as (
select 
    a.payout_id,
    min(first_transfer) as first_tran,
    min(first_payout) as first_payo,
    min(first_paid) as first_pai
    from BALANCE_V2.DBO.payout_status_event a
    left join BALANCE_V2.DBO.first_transfer b
    on a.payout_id=b.payout_id
    left join BALANCE_V2.DBO.first_paid c
    on a.payout_id=c.payout_id
    left join BALANCE_V2.DBO.first_payout d
    on a.payout_id=d.payout_id
    group by 1
    );


/*dim_all_qualifications*/
create or replace table "BALANCE_V2"."STG"."DIM_ALL_QUALIFICATIONS" as (
with credit_limit_increase as
(
select distinct parse_json(payload):customerId::string  as buyer_id 
,first_value(type) over(partition by parse_json(payload):qualificationId::string order by created_at desc) as current_type
,first_value(split_part(type, '_', 4)) over(partition by parse_json(payload):qualificationId::string order by created_at desc) as credit_limit_increase_status
,first_value(created_at) over(partition by parse_json(payload):qualificationId::string order by created_at desc) as credit_limit_increase_status_date
from BALANCE_V2.DBO.TERMS_EVENT_LOG
where type in ('credit_limit_increase_requested','credit_limit_increase_declined','credit_limit_increase_approved')
order by 1
),activation_status as
(
select distinct 
    parse_json(payload):buyerId::string as buyer_id
    ,first_value(type) over (partition by buyer_id order by event_time desc) as activation_status
    ,first_value(parse_json(payload):source::string) ignore nulls over (partition by buyer_id order by event_time desc) as activation_method
    ,case when activation_status in ('activated_credit','deactivated_credit') then first_value(event_time) ignore nulls over (partition by buyer_id order by event_time desc) else null end as activation_status_time
from BALANCE_V2.DBO.TERMS_EVENT_LOG
where type like '%activ%'
order by 1
),
buyer_risk_level as
(
select distinct 
    parse_json(payload):customerId::string as buyer_id
    , parse_json(payload):qualificationId::string  as qualification_id 
    ,first_value(type) over (partition by buyer_id order by event_time desc) as risk_level_status
    ,first_value(parse_json(payload):source::string) ignore nulls over (partition by buyer_id order by event_time desc) as risk_level_source
    ,first_value(parse_json(payload):riskLevel::string) ignore nulls over (partition by buyer_id order by event_time desc) as risk_level_value
    ,case when risk_level_status in ('risk_level_update') then first_value(event_time) ignore nulls over (partition by buyer_id order by event_time desc) else null end as risk_level_status_time
from BALANCE_V2.DBO.TERMS_EVENT_LOG
where type = 'risk_level_update'
order by 1
),
connect_bank as (
select distinct parse_json(payload):customerId::string  as buyer_id 
, parse_json(payload):qualificationId::string  as qualification_id 
,first_value(parse_json(payload):reason::string) ignore nulls over (partition by qualification_id order by event_time desc) as connect_bank_reason
,first_value(created_at) over(partition by parse_json(payload):qualificationId::string order by created_at desc) as connect_bank_request_date
from BALANCE_V2.DBO.TERMS_EVENT_LOG
where type ='connect_bank_account'
order by 1
),
backup_payment_method as (
select distinct parse_json(payload):customerId::string  as buyer_id 
, parse_json(payload):qualificationId::string  as qualification_id 
,first_value(parse_json(payload):paymentMethodType::string) ignore nulls over (partition by qualification_id order by event_time desc) as backup_payment_method_type
,first_value(created_at) over(partition by parse_json(payload):qualificationId::string order by created_at desc) as backup_payment_method_date
from BALANCE_V2.DBO.TERMS_EVENT_LOG
where type ='backup_payment_method'
order by 1
),

pre_approved as (

select buyer_id, date(created_at) as qual_date
from balance_v2.dbo.buyer_qualification
where merchant_id = 20032
and parse_json(app_interaction):"welcome" is null
and status in ('pendingActivation', 'declined', 'approved')
and qualification_origin = 'pre_assessment'
group by 1,2

),

qualification_data as (
select
buyer_id,
incorporation_type,
year_of_establishment

from balance_v2.stg.user_json_flt
),

cte as
(
select distinct 
        case when type = 'qualification_created' then qe.id else null end as qc_id
        ,case when type = 'qualification_approved' then qe.id else null end as qa_id
        ,case when type = 'qualification_declined' then qe.id else null end as qd_id
        ,case when type = 'qualification_in_review' then qe.id else null end as qir_id
        ,case when type = 'qualification_created' then qe.created_at else null end as qc_created_at
        ,case when type = 'qualification_approved' then qe.created_at else null end as qa_created_at
        ,case when type = 'qualification_declined' then qe.created_at else null end as qd_created_at
        ,case when type = 'qualification_in_review' then qe.created_at else null end as qir_created_at
    /*    ,first_value(type) over(partition by case when type = 'qualification_created' then parse_json(payload):qualificationId::string else parent_qualification_id end order by qe.id desc) as current_status* dropped on 18/1/23 by Or*/
        ,first_value(type) over(partition by parse_json(payload):qualificationId::string order by qe.id desc) as current_status
   /*     ,qe.updated_at*/
   /*     ,qe.type as qualification_event_type*/
        ,qe.merchant_id
        ,s.name as merchant_name
   /*     ,qe.payload*/
   /*     ,parse_json(payload):qualificationId::string as qualificationId dropped on 18/1/23 by Or*/
   /*     ,case when type = 'qualification_created' then parse_json(payload):qualificationId::string else parent_qualification_id end as parent_qualification_id dropped on 18/1/23 by Or*/
        ,parse_json(payload):qualificationId::string as parent_qualification_id
        ,case when type = 'qualification_created' then parse_json(payload):origin::string else null end as origin
        ,case when type = 'qualification_created' then parse_json(payload):endpoint::string else null end as endpoint
        ,case when type = 'qualification_approved' then parse_json(payload):type::string else null end as qa_payload_type
        ,case when type = 'qualification_declined' then parse_json(payload):type::string else null end as qd_payload_type
        ,case when type = 'qualification_in_review' then parse_json(payload):type::string else null end as qir_payload_type
        /*,parse_json(payload):eventTime::timestamp  as eventTime*/
        ,case when type = 'qualification_approved' then parse_json(payload):source::string else null end as qa_payload_source
        ,case when type = 'qualification_declined' then parse_json(payload):source::string else null end as qd_payload_source
        ,case when type = 'qualification_in_review' then parse_json(payload):source::string else null end as qir_payload_source
     /*   ,parse_json(payload):type::string  as type*/
     /*   ,parse_json(payload):source::string  as source*/
        ,case when type = 'qualification_approved' then parse_json(payload):creditLimit::int else null end as creditLimit
        ,qp.request_amount 
        ,case when type = 'qualification_in_review' then parse_json(payload):reason::string else null end as in_review_reason
        ,case when type = 'qualification_declined' then parse_json(payload):reason::string else null end as decline_reason
        ,parse_json(payload):customerId::string  as buyer_id
        ,parse_json(payload):merchantId::string  as merchant_token
        ,coalesce(parse_json(payload):qualificationVersion ::string,'legacy') as qualification_version
        ,b.email as buyer_email
        ,b.name as buyer_name
 /*       ,s.id as merchant_id*/
from BALANCE_V2.DBO.TERMS_EVENT_LOG qe 
left join BALANCE_V2.DBO.SELLER s
on qe.merchant_id = s.id
left join BALANCE_V2.DBO.BUYER b 
on buyer_id = b.public_id
/*left join BALANCE_V2.DBO.QUALIFICATION q
on qualificationId = q.id dropped on 18/1/23 by Or*/
left join BALANCE_V2.DBO.QUALIFICATION_PENDING qp
on parent_qualification_id = qp.qualification_id
where parent_qualification_id like  'qual%' and type not in ('backup_payment_method','risk_level_update') /*second condition added on 1/4/24 for task "Final status in dim_all_qualifications"*/
/*where parent_qualification_id= 'qual_34dab337f6e04004963deab5e7430ddc' or qualificationId = 'qual_34dab337f6e04004963deab5e7430ddc'*/
/*order by parent_qualification_id*/ 
/*limit 50*/
)
select
       array_agg(parent_qualification_id)[0]::string as parent_qualification_id
 /*      ,current_status dropped on 18/1/23 by Or*/
       ,array_agg(current_status)[0]::string as current_status
       ,array_agg(qc_id)[0] as qc_id
       ,array_agg(qa_id)[0] as qa_id
       ,array_agg(qd_id)[0] as qd_id
       ,array_agg(qir_id)[0] as qir_id
       ,array_agg(qc_created_at)[0]::timestamp as qc_created_at
       ,array_agg(qa_created_at)[0]::timestamp as qa_created_at
       ,array_agg(qd_created_at)[0]::timestamp as qd_created_at
       ,array_agg(qir_created_at)[0]::timestamp as qir_created_at
       ,array_agg(cte.merchant_id)[0] as merchant_id
       ,array_agg(merchant_name)[0]::string as merchant_name
       ,array_agg(merchant_token)[0]::string as merchant_token
/*       ,array_agg(qualificationId)[0]::string as qualificationId*/
       ,array_agg(origin)[0]::string as origin
       ,array_agg(endpoint)[0]::string as endpoint
       ,array_agg(qa_payload_type)[0]::string as qa_payload_type
       ,array_agg(qd_payload_type)[0]::string as qd_payload_type
       ,array_agg(qir_payload_type)[0]::string as qir_payload_type
       ,array_agg(qa_payload_source)[0]::string as qa_payload_source
       ,array_agg(qd_payload_source)[0]::string as qd_payload_source
       ,array_agg(qir_payload_source)[0]::string as qir_payload_source
       ,array_agg(request_amount)[0]::string/100 as request_amount
       ,array_agg(creditLimit)[0]::string as creditLimit
       ,array_agg(in_review_reason)[0]::string as in_review_reason
       ,array_agg(decline_reason)[0]::string as decline_reason
       ,array_agg(cte.buyer_id)[0]::string as buyer_id
   /*    ,array_agg(merchant_id)[0]::string as merchant_id*/
       ,array_agg(buyer_email)[0]::string as buyer_email
       ,array_agg(buyer_name)[0]::string as buyer_name
       ,qd.incorporation_type
       ,qd.year_of_establishment
       ,array_agg(qualification_version)[0]::string as qualification_version
       ,max(crh.rules) as qualification_rules_hits
       ,array_agg(distinct crh.sub_rule) within group (order by crh.sub_rule) as qualification_sub_rules_hits
       ,cli.credit_limit_increase_status
       ,cli.credit_limit_increase_status_date
       ,case when abs(datediff(day,array_agg(qa_created_at)[0]::timestamp,array_agg(qd_created_at)[0]::timestamp))>7 then 1 else 0 end as is_decision_overriden
       ,ast.activation_status 
       ,ast.activation_method
       ,activation_status_time
       ,brl.risk_level_value as buyer_risk_tier
       ,brl.risk_level_source as buyer_risk_tier_source
       ,brl.risk_level_status_time as buyer_risk_tier_ts
       ,cb.connect_bank_reason as connect_bank_reason
       ,cb.connect_bank_request_date as connect_bank_date
       ,bupm.backup_payment_method_type as backup_payment_method_type
       ,bupm.backup_payment_method_date as backup_payment_method_date
       ,case when fpa.public_id is not null then 1 
       when bq.qualification_origin = 'pre_assessment' then 1 
            else max(case when (cte.merchant_id=4481 and date(cte.qc_created_at)='2023-01-27') or (cte.merchant_id=4434 and date(cte.qc_created_at)='2023-10-25') or (cte.merchant_id=1030 and date(cte.qc_created_at)='2023-09-27') then 1 else 0 end) end as is_pre_assessed,
            case when pa.buyer_id is not null then 1 else 0 end as is_pre_approved



from cte
left join BALANCE_V2.DBO.CLASSIFICATION_RULES_HITS crh
on cte.parent_qualification_id = crh.qualification_id
left join credit_limit_increase cli 
on cte.buyer_id = cli.buyer_id
left join BALANCE_V2.DBO.BUYER b
on cte.buyer_id = b.public_id
left join activation_status ast 
on  b.id= ast.buyer_id
left join buyer_risk_level brl 
on  cte.parent_qualification_id = brl.qualification_id
left join connect_bank cb 
on  cte.parent_qualification_id = cb.qualification_id
left join backup_payment_method bupm 
on  cte.parent_qualification_id = bupm.qualification_id
left join BALANCE_V2.DBO.BUYER_QUALIFICATION bq
on cte.parent_qualification_id = bq.QUALIFICATION_CASE_ID
left join BALANCE_V2.DBO.fg_pre_assessment fpa
on cte.buyer_id=fpa.public_id and fpa.public_id is not null
left join pre_approved pa on b.id=pa.buyer_id
left join qualification_data qd on b.public_id=qd.buyer_id

group by parent_qualification_id,cli.credit_limit_increase_status,credit_limit_increase_status_date,ast.activation_status 
       ,ast.activation_method,activation_status_time /*,current_status dropped on 18/1/23 by Or*/
       ,brl.risk_level_value,brl.risk_level_source,brl.risk_level_status_time,cb.connect_bank_reason ,cb.connect_bank_request_date,bupm.backup_payment_method_type,bupm.backup_payment_method_date,bq.qualification_origin,fpa.public_id,pa.buyer_id, incorporation_type,year_of_establishment
);

/*collection_charges*/
create or replace table BALANCE_V2.DBO.COLLECTION_CHARGES as (
select
a.customer_id,
c.buyer_email,
b.buyer_id,
sum(amount/100) as total_collection_amount_charged
from stripe_data_pipeline.stripe.charges a
left join BALANCE_V2.dbo.buyer_third_party_token b
on a.customer_id=b.token
left join BALANCE_V2.stg.dim_buyer c on
b.buyer_id=c.buyer_id
where description like any ('Repayment','repayment','collection','COLLECTION','Collection','Payment for Invoice','Repayment plan')
and status='succeeded'
group by 1,2,3
);

/*dim_loan_tape*/
create or replace table BALANCE_V2.STG.DIM_LOAN_TAPE as
(
with industry as
(
select distinct a.buyer_id,buyer_merchant_id,first_value(industrycodes_description ignore nulls) over (partition by a.buyer_id order by duns) as industry_code_desc
from BALANCE_V2.STG.DNB_IDENTITY_CMTPS a 
left join balance_v2.stg.dim_buyer b 
on a.buyer_id=b.buyer_public_id
),

industry_agg as (
select 
i.buyer_merchant_id as merchant_id,
industry_code_desc as industry_code_agg,
count(*) as cnt
from industry i 
left join balance_v2.stg.dim_buyer db 
on i.buyer_id=db.buyer_public_id
group by 1,2
QUALIFY ROW_NUMBER() OVER (PARTITION BY i.buyer_merchant_id ORDER BY cnt desc) = 1
),

addresses as (
select 
entity_id, 
row_number () over (partition by entity_id order by created_at desc) as row_num,
t0.value: "address1"::string as address_line1, 
t0.value: "city"::string as city,
t0.value: "country"::string as country, 
t0.value: "state"::string as state, 
t0.value: "zipCode"::string as zipcode
from balance_v2.dbo.qualification_data d,
lateral flatten( input => PARSE_JSON(data)) as t0
where type = 'PreApproval'
and key = 'businessAddress'
qualify row_num=1
),
risk_score as
(
select buyer_public_id,buyer_risk_score
from BALANCE_V2.STG.BUYER_RISK_SCORE
where etl_date = (select max(etl_date) from BALANCE_V2.STG.BUYER_RISK_SCORE)
),

spv_loans as 
(
select charge_id
from BALANCE_V2.STG.DIM_CHARGES
where charge_funding_source='spv'
)


    select
 l.tx_id as trxn_id
 ,l.charge_id
 ,c.charge_currency as currency 
,b.public_id as buyer_public_id
,b.name as buyer_name
,case when adr.address_line1 is not null then adr.address_line1 else a.street_address1 end as buyer_address
,case when adr.city is not null then adr.city else a.city end as buyer_city
,case when adr.state is not null then adr.state else a.state end as buyer_state
,case when adr.country in ('USA','US','United States','us','usa','Us','United States of America','1','+1') then 'US' 
      when  a.country_code in ('USA','US','United States','us','usa','Us','United States of America','1','+1') then 'US'  else adr.country end as buyer_country
,case when adr.zipcode is not null then adr.zipcode else a.zip_code end as buyer_zip
,round(l.buyer_risk_score,0) as buyer_transaction_risk_score
,round(rs.buyer_risk_score,0)as buyer_current_risk_score
,s.public_id as merchant_id
,l.merchant_name


,to_char(date(l.revenue_date),'DD-MM-YYYY') as created_date
,CASE
WHEN l.due_date IS NULL THEN to_char(date(dateadd(day,l.terms_net_days,l.revenue_date)),'DD-MM-YYYY')
ELSE to_char(date(l.due_date),'DD-MM-YYYY')
END AS due_date
,CASE
WHEN l.due_date IS NULL THEN date(dateadd(day,l.terms_net_days,l.revenue_date))
ELSE date(l.due_date)
END AS due_date_original_format
,CASE
WHEN l.charge_status='failed' THEN NULL
ELSE to_char(date(l.paid_date),'DD-MM-YYYY')
END AS paid_date
,CASE
WHEN l.charge_status='charged' and l.paid_date IS NOT NULL THEN 'Paid' 
WHEN l.charge_status in ('refunded','canceled') THEN 'Dilution'
WHEN l.charge_status<>'charged' and l.paid_date IS NULL AND  current_date()<=l.due_date then 'Current'
WHEN l.charge_status<>'charged' and l.paid_date IS NULL AND datediff(day,l.due_date,current_date())<=30 THEN '1-29D'
WHEN l.charge_status<>'charged' and l.paid_date IS NULL AND datediff(day,l.due_date,current_date())<=60 THEN '30-59D'
WHEN l.charge_status<>'charged' and l.paid_date IS NULL AND datediff(day,l.due_date,current_date())<=90 THEN '60-89D'
WHEN l.charge_status<>'charged' and l.paid_date IS NULL AND datediff(day,l.due_date,current_date())<=120 THEN '90-120D'
WHEN l.charge_status<>'charged' and l.paid_date IS NULL AND datediff(day,l.due_date,current_date())>120 THEN '120D+'
ELSE 'Other'
END AS status
,l.terms_net_days
,CASE
when early=1 then 0 else payment_days
END AS DPD
,round(l.charge_amount,2) AS original_amount
,case when l.charge_status='charged' and round(l.charge_amount,2)<=round(c.charge_amount_usd,2) then charge_amount else 0 end as principal_paid
,round(l.charge_amount,2)-(case when l.charge_status='charged' and round(l.charge_amount,2)<=round(c.charge_amount_usd,2) then charge_amount else 0 end) as outstanding_principal
,round(l.charge_amount,2) AS amount_in_arrears
,CONCAT('https://pay.getbalance.com/invoice?cid=',l.merchant_id ,'-', tr.token,'&chargeId=', l.charge_id) as invoice_lnk


,CASE
WHEN tr.is_auth=true THEN ROUND(tr.factoring_fee_in_cents/nullif(tr.captured_amount*100,0),4)
ELSE ROUND(tr.factoring_fee_in_cents/nullif(tr.total_price*100,0),4)
END AS factoring_rate /*rate is still at best an approximation and a few transactions do not match accounting table*/
,factoring_rate/nullif(l.terms_net_days,0)*360 as APR
,factoring_rate*charge_amount as revenue
,APR*charge_amount as apr_x_amount
,cast(year(l.revenue_date)*100+month(l.revenue_date)as string) as vintage
,l.payment_days as DSO
,l.l_120*charge_amount as charge_off_amount
,case when l_120=1 then to_char(dateadd(day,120,l.due_date),'DD-MM-YYYY') end as charge_off_date
,l.is_recovered*charge_amount as recovered_amount
,case when i.industry_code_desc is not null then i.industry_code_desc 
      when i.industry_code_desc is null then ia.industry_code_agg 
              when i.industry_code_desc is null and s.public_id ='ven_ec0f4e6499db05b7d697e834' then 'All Other Miscellaneous Retailers'
              when i.industry_code_desc is null and s.public_id ='ven_344c45d69cc69cf01b437bb8' then 'Freight Transportation Arrangement'
              when i.industry_code_desc is null and s.public_id ='ven_c4174ae88388d63d4442c7a9' then 'Metal Service Centers and Other Metal Merchant Wholesalers'
      else 'Unclassified Establishments' end as industry_code_desc
,case when (l.charge_funding_source='spv' and year(l.due_date)=2024) then '2024-01-29'
      when  (l.charge_funding_source='spv' and year(l.due_date)=2023) then '2024-09-18'
      else date(l.CHARGE_FUNDING_SOURCE_TAG_DATE) end as Sell_to_SPV_date


from BALANCE_V2.DBO.DIM_ALL_LOANS l
left join BALANCE_V2.STG.DIM_CHARGES c
on l.charge_id = c.charge_id
left join BALANCE_V2.DBO.BUYER b
on l.buyer_id = b.id
left join BALANCE_V2.DBO.SELLER s
on l.merchant_id = s.id

left join BALANCE_V2.DBO.TRANSACTION tr
on l.tx_id = tr.id
left join BALANCE_V2.DBO.BUSINESS bs
on b.business_id = bs.id 
left join BALANCE_V2.DBO.ADDRESS a
on bs.address_id = a.id 
left join industry i 
on b.public_id = i.buyer_id
left join industry_agg ia 
on s.id = ia.merchant_id
left join addresses adr 
on b.public_id=adr.entity_id
left join risk_score rs 
on b.public_id=rs.buyer_public_id


where 
tr.status IN ('closed', 'auth') AND
l.charge_status IN ('charged', 'waitingForPayment', 'pending', 'failed', 'processing','refunded','canceled') AND
tr.deleted_at IS NULL AND
tr.is_financed = TRUE
and l.due_date > '2023-07-01'
and (l.charge_id in (select charge_id from balance_v2.DBO.LOAN_TAPE_ELIGIBLE_BUYERS) or l.charge_id in (select charge_id from spv_loans))

and l.charge_id not in ('129589')

/*and  l.tx_id = '1101'*/
);

/*loan_repayment_rate*/
create or replace table BALANCE_V2.STG.loan_repayment_rate as (select 
to_char (revenue_date,'yyyy-mm-dd') as loan_date,
merchant_name,
case when terms_net_days between 1 and 16 then '15'
     when terms_net_days between 17 and 31 then '30'
     when terms_net_days between 32 and 46 then '45'
     when terms_net_days between 47 and 61 then '60'
     when terms_net_days between 62 and 91 then '90'
     else 'other' end as terms_days,
sum (charge_amount) as total_loaned,
sum(case when is_paid=1 and early=1 then charge_amount else 0 end) as repayment_usd_at_day_0,
sum(case when is_paid=1 and (l_0_30=1 or early=1) then charge_amount else 0 end) as repayment_usd_at_d30,
sum(case when is_paid=1 and (l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as repayment_usd_at_d60,
sum(case when is_paid=1 and (l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as repayment_usd_at_d90,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as repayment_usd_at_d120,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1 or payment_days between 121 and 366) then charge_amount else 0 end) as repayment_usd_at_d365,
sum(case when is_paid=1 and early=1 then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_day_0,
sum(case when is_paid=1 and (l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_d30,
sum(case when is_paid=1 and (l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_d60,
sum(case when is_paid=1 and (l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_d90,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_d120,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1 or payment_days between 121 and 365) then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_d365
/*
sum(case when is_paid=1 and (l_0_30=1 or early=1) then charge_amount else 0 end) as unpaid_usd_at_d30,
sum(case when is_paid=1 and (l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as unpaid_usd_at_d60,
sum(case when is_paid=1 and (l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as unpaid_usd_at_d90,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as unpaid_usd_at_d120,
sum(case when is_paid=1 and (l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as unpaid_rate_at_d30,
sum(case when is_paid=1 and (l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as unpaid_rate_at_d60,
sum(case when is_paid=1 and (l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as unpaid_rate_at_d90,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as unpaid_rate_at_d120    
*/       
from BALANCE_V2.DBO.DIM_ALL_LOANS
group by 1,2,3
);

/*loan_repayment_rate_by_due_date*/
create or replace table BALANCE_V2.STG.loan_repayment_rate_by_due_date as
(
  select 
to_char (due_date,'yyyy-mm-dd') as due_date,
merchant_name,
case when terms_net_days between 1 and 16 then '15'
    when terms_net_days between 17 and 31 then '30'
    when terms_net_days between 32 and 46 then '45'
    when terms_net_days between 47 and 61 then '60'
    when terms_net_days between 62 and 91 then '90'
    else 'other' end as terms_days,
sum (charge_amount) as total_due,
sum(case when is_paid=1 and early=1 then charge_amount else 0 end) as repayment_usd_at_due_date,
sum(case when is_paid=1 and (l_0_30=1 or early=1) then charge_amount else 0 end) as repayment_usd_at_d30,
sum(case when is_paid=1 and (l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as repayment_usd_at_d60,
sum(case when is_paid=1 and (l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as repayment_usd_at_d90,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as repayment_usd_at_d120,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1 or payment_days between 121 and 366) then charge_amount else 0 end) as repayment_usd_at_d365,
sum(case when is_paid=1 and early=1 then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_day_0,
sum(case when is_paid=1 and (l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_d30,
sum(case when is_paid=1 and (l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_d60,
sum(case when is_paid=1 and (l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_d90,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_d120,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1 or payment_days between 121 and 365) then charge_amount else 0 end) /sum(charge_amount) as repayment_rate_at_d365
/*
sum(case when is_paid=1 and (l_0_30=1 or early=1) then charge_amount else 0 end) as unpaid_usd_at_d30,
sum(case when is_paid=1 and (l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as unpaid_usd_at_d60,
sum(case when is_paid=1 and (l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as unpaid_usd_at_d90,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) as unpaid_usd_at_d120,
sum(case when is_paid=1 and (l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as unpaid_rate_at_d30,
sum(case when is_paid=1 and (l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as unpaid_rate_at_d60,
sum(case when is_paid=1 and (l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as unpaid_rate_at_d90,
sum(case when is_paid=1 and (l_90_120=1 or l_60_90=1 or l_30_60=1 or l_0_30=1 or early=1) then charge_amount else 0 end) /sum(charge_amount) as unpaid_rate_at_d120    
*/     
from BALANCE_V2.DBO.DIM_ALL_LOANS
group by 1,2,3  
);

/*SALES_FUNNEL_PIT*/
create or replace table BALANCE_V2.STG.SALES_FUNNEL_PIT as
(
WITH CTE_MY_DATE AS
  (
    SELECT DATEADD(DAY, SEQ4(), '2000-01-01') AS MY_DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>10000))
  ), cte as
(
select 
DEALID
,try_to_date(move_to_qualified_stage_date_value) as move_to_date
,'qualified' as stage
,AMOUNT_VALUE
from BALANCE_V2.DBO.HUBSPOT_DEAL_CUSTOM 
where len(move_to_qualified_stage_date_value)>1 
   /* and MOVE_TO_CLOSE_LOST_STAGE_VALUE is not null*/
union all

    select 
DEALID
,try_to_date(move_to_engaged_stage_date_value) as move_to_date
,'engaged' as stage
,AMOUNT_VALUE
from BALANCE_V2.DBO.HUBSPOT_DEAL_CUSTOM 
where len(MOVE_TO_ENGAGED_STAGE_DATE_VALUE)>1
   /* and MOVE_TO_CLOSE_LOST_STAGE_VALUE is not null*/
union all

    select 
DEALID
,try_to_date(move_to_proposal_stage_date_value) as move_to_date
,'proposal' as stage
,AMOUNT_VALUE
from BALANCE_V2.DBO.HUBSPOT_DEAL_CUSTOM 
where len(move_to_proposal_stage_date_value)>1
   /* and MOVE_TO_CLOSE_LOST_STAGE_VALUE is not null*/
union all

    select 
DEALID
,try_to_date(move_to_close_lost_stage_value) as move_to_date
,'closed lost' as stage
,AMOUNT_VALUE
from BALANCE_V2.DBO.HUBSPOT_DEAL_CUSTOM 
where len(move_to_close_lost_stage_value)>1
   /* and MOVE_TO_CLOSE_LOST_STAGE_VALUE is not null*/
union all    
    
    
    select 
DEALID
,least(ifnull(try_to_date(move_to_implementation_stage_date_value),'9999-01-01'), ifnull(try_to_date(move_to_live_stage_date_value),'9999-01-01')) as move_to_date
,'live' as stage
,AMOUNT_VALUE
from BALANCE_V2.DBO.HUBSPOT_DEAL_CUSTOM 
where len(move_to_live_stage_date_value)>1  or len(move_to_implementation_stage_date_value)>1
   /* and MOVE_TO_CLOSE_LOST_STAGE_VALUE is not null*/
),
    cte2 as 
(
select DEALID,move_to_date as start_date,ifnull(lead(move_to_date,1) over(partition by DEALID order by move_to_date),CURRENT_TIMESTAMP) as end_date,stage
    ,AMOUNT_VALUE
from cte
)
select cte2.DEALID,my_date as stage_date,stage,AMOUNT_VALUE
from cte2 left join cte_my_date 
on cte2.start_date <= my_date and cte2.end_date > my_date
order by 1,2
);

/*MARKETING_FUNNEL_PIT*/
create or replace table BALANCE_V2.STG.MARKETING_FUNNEL_PIT as
(
WITH CTE_MY_DATE AS
  (
    SELECT DATEADD(DAY, SEQ4(), '2000-01-01') AS MY_DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>10000))
  ), cte as
(
select 
    COMPANYID
,try_to_timestamp(properties_became_a_lead_date__custom_value) as Lead
,try_to_timestamp(properties_became_an_mql_date__custom_value) as MQL
,try_to_timestamp(properties_became_an_opportunity_date__custom_value) as Opportunity
,try_to_timestamp(properties_became_a_customer_date__custom_value) as Customer
,try_to_timestamp(properties_closedate_value) as Closed
from BALANCE_V2.DBO.HUBSPOT_COMPANIES_CUSTOM 
),cte_2 as
(
select 
* from cte
unpivot(start_date for stage in (Lead, MQL, Opportunity, Customer, Closed))
),cte_3 as
(
select *,lead(start_date , 1,current_date) over (partition by companyid order by start_date) as end_date
from cte_2
)
select companyid,date(my_date) as stage_date,stage
from cte_3 left join cte_my_date 
on cte_3.start_date <= my_date and cte_3.end_date > my_date
/*where stage != 'CLOSED' */      
);

/*MARKETING_FUNNEL*/
create or replace table BALANCE_V2.STG.MARKETING_FUNNEL as (
    
with deals as
(
select  date(createdate_value) as deal_date,count(DEALID) as deals
from BALANCE_V2.DBO.HUBSPOT_DEAL_CUSTOM where pipeline_value<>'401421'
/*where and MOVE_TO_CLOSE_LOST_STAGE_VALUE is not null*/
group by 1
)
select  date(l.PROPERTIES_BECAME_A_LEAD_DATE__CUSTOM_VALUE) as PROPERTIES_BECAME_A_LEAD_DATE__CUSTOM_VALUE
    ,count(distinct l.PROPERTIES_ASSOCIATEDCOMPANYID_VALUE) as leads
    ,count(distinct sql.PROPERTIES_ASSOCIATEDCOMPANYID_VALUE) as SQLs
    ,count(distinct o.PROPERTIES_ASSOCIATEDCOMPANYID_VALUE) as opportunities
    ,count(distinct mql.PROPERTIES_ASSOCIATEDCOMPANYID_VALUE) as MQLs
    ,count(distinct c.PROPERTIES_ASSOCIATEDCOMPANYID_VALUE) as customers
    ,d.deals
    ,leads+SQLs+MQLs+opportunities+customers as Leads_agg
    ,SQLs+MQLs+opportunities+customers as SQL_agg 
    ,MQLs+opportunities+customers as MQL_agg 
    ,opportunities+customers as OPP_agg 
from "BALANCE_V2"."DBO"."HUBSPOT_CONTACTS_CUSTOM" l
left join "BALANCE_V2"."DBO"."HUBSPOT_CONTACTS_CUSTOM" sql
on l.PROPERTIES_BECAME_A_LEAD_DATE__CUSTOM_VALUE = sql.PROPERTIES_BECAME_AN_SQL_DATE__CUSTOM_VALUE
left join "BALANCE_V2"."DBO"."HUBSPOT_CONTACTS_CUSTOM" o
on l.PROPERTIES_BECAME_A_LEAD_DATE__CUSTOM_VALUE = o.PROPERTIES_BECAME_AN_OPPORTUNITY_DATE__CUSTOM_VALUE
left join "BALANCE_V2"."DBO"."HUBSPOT_CONTACTS_CUSTOM" mql
on l.PROPERTIES_BECAME_A_LEAD_DATE__CUSTOM_VALUE = mql.PROPERTIES_BECAME_AN_MQL_DATE__CUSTOM_VALUE
left join "BALANCE_V2"."DBO"."HUBSPOT_CONTACTS_CUSTOM" c
on l.PROPERTIES_BECAME_A_LEAD_DATE__CUSTOM_VALUE = c.PROPERTIES_BECAME_A_CUSTOMER_DATE__CUSTOM_VALUE
left join deals d
on l.PROPERTIES_BECAME_A_LEAD_DATE__CUSTOM_VALUE = d.deal_date
group by 1,deals
);

/*MERCHANT_REPAYMENT_RATES_PIT*/
create or replace table BALANCE_V2.STG.MERCHANT_REPAYMENT_RATES_PIT as 
(
WITH CTE_MY_DATE AS
  (
    SELECT DATEADD(DAY, SEQ4(), '2000-01-01') AS MY_DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>10000))
  ),
  cte as
  (
  select merchant_id,tx_id,charge_status,charge_amount,REVENUE_DATE, PAID_DATE,DUE_DATE
from BALANCE_V2.DBO.DIM_ALL_LOANS
/*where merchant_id = 122*/
  ),cte_2 as
  (
    select cte.*
      ,case when  paid_date<DUE_DATE or TIMESTAMPDIFF('day',DUE_DATE,my_date)<=0 then 1 else 0 end as early
      ,case when  paid_date<my_date then 1 else 0 end as is_paid
      ,case when paid_date is not null then TIMESTAMPDIFF('day',DUE_DATE,paid_date) else TIMESTAMPDIFF('day',DUE_DATE,current_date()) end as payment_days
      ,MY_DATE from cte left join CTE_MY_DATE
on cte.REVENUE_DATE <= my_date and current_date > my_date
  )
  select 
   merchant_id
   ,my_date
   ,cast( AVG(case when is_paid=1 then (PAYMENT_DAYS) else null end) as decimal (15,2)) AS DSO /* only for paid amounts */
   ,sum(charge_amount) as total_tpv
   , sum( case when is_paid=1 then charge_amount else 0 end) as total_paid_usd
   ,sum( case when is_paid=0 then charge_amount else 0 end) as total_outstanding_usd
   ,sum( case when is_paid=0 and early<>1 then charge_amount else 0 end) as total_due_unpaid_usd
   ,count(*) as total_count
   ,sum( case when is_paid=1 then 1 else 0 end) as total_paid_count
   ,sum( case when is_paid=0 then 1 else 0 end) as total_outstanding_count
   ,cast 
        (sum( case when is_paid=1 then charge_amount else 0 end)/
        nullifzero (sum( case when is_paid=1 then charge_amount else 0 end)+sum( case when is_paid=0 and early<>1 and payment_days>30 then charge_amount else 0 end)) 
             as decimal (15,2)) as repayment_rate_usd
    ,cast 
        (sum( case when is_paid=1 and TIMESTAMPDIFF('day',REVENUE_DATE,my_date) <= 90 then charge_amount else 0 end)/
        nullifzero (sum( case when is_paid=1 and TIMESTAMPDIFF('day',REVENUE_DATE,my_date) <= 90 then charge_amount else 0 end)+sum( case when is_paid=0 and early<>1 and payment_days>30 then charge_amount else 0 end)) 
             as decimal (15,2)) as repayment_rate_usd_sliding_90
   ,cast
        (sum( case when is_paid=1 then 1 else 0 end)/
        nullifzero (sum( case when is_paid=1 then 1 else 0 end)+sum( case when is_paid=0 and early<>1 and payment_days>30 then 1 else 0 end)) 
            as decimal (15,2)) as repayment_rate_count
   ,cast
        (sum( case when is_paid=1 and TIMESTAMPDIFF('day',REVENUE_DATE,my_date) <= 90 then 1 else 0 end)/
        nullifzero (sum( case when is_paid=1 and TIMESTAMPDIFF('day',REVENUE_DATE,my_date) <= 90 then 1 else 0 end)+sum( case when is_paid=0 and early<>1 and payment_days>30 then 1 else 0 end)) 
            as decimal (15,2)) as repayment_rate_count_sliding_90
  from cte_2
  group by 1,2
  order by 1,2 
);

/*MERCHANT_REPAYMENT_RATES_PIT_BY_DUE_DATE*/
create or replace table BALANCE_V2.STG.MERCHANT_REPAYMENT_RATES_PIT_BY_DUE_DATE as 
(
WITH CTE_MY_DATE AS
  (
    SELECT DATEADD(DAY, SEQ4(), '2000-01-01') AS MY_DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>10000))
  ),
  cte as
  (
  select merchant_id,tx_id,charge_status,charge_amount,REVENUE_DATE, PAID_DATE,DUE_DATE
from BALANCE_V2.DBO.DIM_ALL_LOANS
/*where merchant_id = 122*/
  ),cte_2 as
  (
    select cte.*
      ,case when  paid_date<DUE_DATE or TIMESTAMPDIFF('day',DUE_DATE,my_date)<=0 then 1 else 0 end as early
      ,case when  paid_date<my_date then 1 else 0 end as is_paid
      ,case when paid_date is not null then TIMESTAMPDIFF('day',DUE_DATE,paid_date) else TIMESTAMPDIFF('day',DUE_DATE,current_date()) end as payment_days
      ,MY_DATE from cte left join CTE_MY_DATE
on cte.DUE_DATE <= my_date and current_date > my_date
  )
  select 
   merchant_id
   ,my_date
   ,cast( AVG(case when is_paid=1 then (PAYMENT_DAYS) else null end) as decimal (15,2)) AS DSO /* only for paid amounts */
   ,sum(charge_amount) as total_tpv
   , sum( case when is_paid=1 then charge_amount else 0 end) as total_paid_usd
   ,sum( case when is_paid=0 then charge_amount else 0 end) as total_outstanding_usd
   ,sum( case when is_paid=0 and early<>1 then charge_amount else 0 end) as total_due_unpaid_usd
   ,count(*) as total_count
   ,sum( case when is_paid=1 then 1 else 0 end) as total_paid_count
   ,sum( case when is_paid=0 then 1 else 0 end) as total_outstanding_count
   ,cast 
        (sum( case when is_paid=1 then charge_amount else 0 end)/
        nullifzero (sum( case when is_paid=1 then charge_amount else 0 end)+sum( case when is_paid=0 and early<>1 and payment_days>30 then charge_amount else 0 end)) 
             as decimal (15,2)) as repayment_rate_usd
    ,cast 
        (sum( case when is_paid=1 and TIMESTAMPDIFF('day',DUE_DATE,my_date) <= 90 then charge_amount else 0 end)/
        nullifzero (sum( case when is_paid=1 and TIMESTAMPDIFF('day',DUE_DATE,my_date) <= 90 then charge_amount else 0 end)+sum( case when is_paid=0 and early<>1 and payment_days>30 then charge_amount else 0 end)) 
             as decimal (15,2)) as repayment_rate_usd_sliding_90
   ,cast
        (sum( case when is_paid=1 then 1 else 0 end)/
        nullifzero (sum( case when is_paid=1 then 1 else 0 end)+sum( case when is_paid=0 and early<>1 and payment_days>30 then 1 else 0 end)) 
            as decimal (15,2)) as repayment_rate_count
   ,cast
        (sum( case when is_paid=1 and TIMESTAMPDIFF('day',DUE_DATE,my_date) <= 90 then 1 else 0 end)/
        nullifzero (sum( case when is_paid=1 and TIMESTAMPDIFF('day',DUE_DATE,my_date) <= 90 then 1 else 0 end)+sum( case when is_paid=0 and early<>1 and payment_days>30 then 1 else 0 end)) 
            as decimal (15,2)) as repayment_rate_count_sliding_90
  from cte_2
  group by 1,2
  order by 1,2 
);


/*DIM_PAYMENTS*/
create or replace table BALANCE_V2.STG.DIM_PAYMENTS as 
(
with fees as
(
select payout_id, sum(fe.AMOUNT_IN_CENTS/100) as payout_fee_usd 
from BALANCE_V2.DBO.FEES fe
join BALANCE_V2.DBO.core_payments cp 
on fe.entity_ref_id=cp.public_id
where fe.type = 'PAYOUT' and fe.deleted_at is null
group by 1
),financed_trx as
(
select distinct public_id,is_financed
from BALANCE_V2.DBO.TRANSACTION
),po_source as 
(
select distinct entity_id,origin as source
from BALANCE_V2.DBO.PAYMENT_EVENT_LOG
where ENTITY_TYPE = 'payout' and status = 'initiated'
),trx_origin as 
(
select distinct entity_id,FIRST_VALUE(origin) IGNORE  NULLS OVER (PARTITION BY entity_id  ORDER BY created_at desc) as transaction_origin 
from BALANCE_V2.DBO.PAYMENT_EVENT_LOG
where ENTITY_TYPE = 'transaction'
),chg_origin as 
(
select distinct entity_id,
case when event_type='recharge-success' then 'Recharge' else FIRST_VALUE(origin) IGNORE  NULLS OVER (PARTITION BY entity_id  ORDER BY created_at desc) end as charge_origin 
from BALANCE_V2.DBO.PAYMENT_EVENT_LOG
where ENTITY_TYPE = 'charge'
)
select 
'payment' as type
,ft.is_financed
,c.charge_id as entity_id
,case when c.charge_id in (select charge_id from BALANCE_V2.DBO.MIN_FEES) then mf.billing_month else c.revenue_date end as date
,c.charge_amount_usd as amount
,upper(c.charge_currency) as currency 
,c.buyer_public_id as buyer_id
,c.charge_public_id as public_id
,case when c.charge_id in (select charge_id from BALANCE_V2.DBO.MIN_FEES) then m.merchant_id else c.merchant_id end as merchant_id
,case when c.charge_id in (select charge_id from BALANCE_V2.DBO.MIN_FEES) then m.merchant_name else c.merchant_name end as merchant_name
,c.merchant_token
,to_varchar(c.vendor_id) as vendor_id
,c.processing_fee_usd
,c.transaction_factoring_fee_usd
,null as payout_fee_usd 
,c.processing_fee_usd+c.transaction_factoring_fee_usd as total_revenue
,c.charge_payment_method_type as payment_method
,c.charge_source
,c.transaction_source
,null as payout_source
,case when c.charge_id in (select charge_id from BALANCE_V2.DBO.MIN_FEES) then 1 else 0 end as is_min_fee
,charge_status as status
,'dim_charges' as source
,cho.charge_origin
,tro.transaction_origin
from BALANCE_V2.STG.DIM_CHARGES c
left join BALANCE_V2.STG.DIM_MERCHANT m 
on c.merchant_token = m.merchant_token
left join financed_trx ft
on c.transaction_public_id = ft.public_id
left join chg_origin cho
on c.charge_public_id = cho.entity_id
left join trx_origin tro
on c.transaction_public_id = tro.entity_id
left join BALANCE_V2.DBO.MIN_FEES mf
on c.charge_id = mf.charge_id and billing_month>='2023-01-01'
union all
select 
'vendor_payout' as type
,null as is_financed
,p.id as entity_id
,p.created_at as date
,p.amount_requested as amount
,upper(p.currency) as currency
,null as buyer_id
,p.public_id
,p.merchant_id
,m.merchant_name
,m.merchant_token
,to_varchar(p.vendor_id) as vendor_id
,null as processing_fee_usd
,null as transaction_factoring_fee_usd
,payout_fee_usd 
,payout_fee_usd as total_revenue
,null as payment_method
,null as charge_source
,null as transaction_source
,ps.source as payout_source
,0  as is_min_fee
,status
,'payout' as source
,null as charge_origin
,null as transaction_origin
from BALANCE_V2.DBO.PAYOUT p 
left join fees f
on p.public_id = f.payout_id
left join BALANCE_V2.STG.DIM_MERCHANT m
on p.merchant_id = m.merchant_id
left join po_source ps
on p.public_id = ps.entity_id

where status = 'paid' 
and vendor_id<>p.merchant_id
);






/*CREDIT_BALANCE*/
create or replace table BALANCE_V2.STG.CREDIT_BALANCE as 
(
WITH CTE_MY_DATE AS
(
SELECT DATEADD(DAY, SEQ4(), '2022-01-01') AS MY_DATE
FROM TABLE(GENERATOR(ROWCOUNT=>10000))
),loans_extended as
(
select
to_char (revenue_date, 'yyyy-mm-dd') as loan_date
,sum(charge_amount)*-1 as loans_extended
,'created' as loan_status 
from BALANCE_V2.DBO.DIM_ALL_LOANS
where date(revenue_date)>='2022-01-01' and not (is_paid=0 and payment_days>120)
group by 1
),loans_paid as
(
select
to_char (paid_date, 'yyyy-mm-dd') as loan_date
,sum(charge_amount) as loans_paid
,'paid' as loan_status
from BALANCE_V2.DBO.DIM_ALL_LOANS
where date(revenue_date)>='2022-01-01' and not (is_paid=0 and payment_days>120)
group by 1
),credit_losses_daily as 
(
select  
to_char(revenue_date,'yyyy-mm-dd') as loan_date
,sum(case when credit_bad='Default' then charge_amount_usd else 0 end) as net_credit_bad
,sum(case when credit_bad is not null then charge_amount_usd else 0 end) as gross_credit_bad
from BALANCE_V2.STG.DIM_CHARGES 
where merchant_id not in (1107,588,1391,691,1524) and charge_id not in (select charge_id from BALANCE_V2.STG.DIM_CHARGES where merchant_id=4634 and buyer_id<>46337 )
group by 1
),cte_credit_balance as
(
select 
to_timestamp(coalesce(a.loan_date,b.loan_date)) as cons_loan_date
,lead(to_timestamp(cons_loan_date),1) over (order by cons_loan_date) as end_date
,a.loans_extended
,Sum(a.loans_extended) OVER( partition BY NULL ORDER BY cons_loan_date ASC rows UNBOUNDED PRECEDING ) as "Cumulative Sum Loaned"
,b.loans_paid
,Sum(b.loans_paid) OVER( partition BY NULL ORDER BY cons_loan_date ASC rows UNBOUNDED PRECEDING ) as "Cumulative Sum Paid"
,Sum(a.loans_extended) OVER( partition BY NULL ORDER BY cons_loan_date ASC rows UNBOUNDED PRECEDING ) +
 Sum(b.loans_paid) OVER( partition BY NULL ORDER BY cons_loan_date ASC rows UNBOUNDED PRECEDING ) as current_balance
from loans_extended a 
FULL OUTER JOIN loans_paid b 
on a.loan_date=b.loan_date
)
select 
MY_DATE as date
,le.loans_extended
,lp.loans_paid
,"Cumulative Sum Loaned"
,"Cumulative Sum Paid"
,CURRENT_BALANCE
,cld.net_credit_bad
,cld.gross_credit_bad
,avg(current_balance) OVER (ORDER BY MY_DATE
                                  ROWS BETWEEN 90 PRECEDING AND 0 following) as average_balance_90_days
,sum(cld.net_credit_bad) OVER (ORDER BY MY_DATE
                                  ROWS BETWEEN 90 PRECEDING AND 0 following) as loss_90_days       
,sum(cld.gross_credit_bad) OVER (ORDER BY MY_DATE
                                  ROWS BETWEEN 90 PRECEDING AND 0 following) as gross_loss_90_days     
                                  
from CTE_MY_DATE d left join cte_credit_balance cb
on d.MY_DATE >= cb.cons_loan_date and d.MY_DATE< end_date
left join credit_losses_daily cld 
on my_date = cld.loan_date
left join loans_paid lp 
on MY_DATE = lp.loan_date
left join loans_extended le 
on MY_DATE = le.loan_date
order by MY_DATE   
);

/*CREDIT_BALANCE_SINCE_2021*/
create or replace table BALANCE_V2.STG.CREDIT_BALANCE_SINCE_2021 as 
(
WITH CTE_MY_DATE AS
(
SELECT DATEADD(DAY, SEQ4(), '2021-01-01') AS MY_DATE
FROM TABLE(GENERATOR(ROWCOUNT=>10000))
),loans_extended as
(
select
to_char (revenue_date, 'yyyy-mm-dd') as loan_date
,sum(charge_amount)*-1 as loans_extended
,'created' as loan_status 
from BALANCE_V2.DBO.DIM_ALL_LOANS
where date(revenue_date)>='2021-01-01' 
group by 1
),loans_paid as
(
select
to_char (paid_date, 'yyyy-mm-dd') as loan_date
,sum(charge_amount) as loans_paid
,'paid' as loan_status
from BALANCE_V2.DBO.DIM_ALL_LOANS
where date(revenue_date)>='2021-01-01'
group by 1
),credit_losses_daily as 
(
select  
to_char(revenue_date,'yyyy-mm-dd') as loan_date
,sum(case when credit_bad='Default' then charge_amount_usd else 0 end) as net_credit_bad
,sum(case when credit_bad is not null then charge_amount_usd else 0 end) as gross_credit_bad
from BALANCE_V2.STG.DIM_CHARGES 
where merchant_id not in (1107,588,1391,691)
group by 1
),cte_credit_balance as
(
select 
to_timestamp(coalesce(a.loan_date,b.loan_date)) as cons_loan_date
,lead(to_timestamp(cons_loan_date),1) over (order by cons_loan_date) as end_date
,a.loans_extended
,Sum(a.loans_extended) OVER( partition BY NULL ORDER BY cons_loan_date ASC rows UNBOUNDED PRECEDING ) as "Cumulative Sum Loaned"
,b.loans_paid
,Sum(b.loans_paid) OVER( partition BY NULL ORDER BY cons_loan_date ASC rows UNBOUNDED PRECEDING ) as "Cumulative Sum Paid"
,Sum(a.loans_extended) OVER( partition BY NULL ORDER BY cons_loan_date ASC rows UNBOUNDED PRECEDING ) +
 Sum(b.loans_paid) OVER( partition BY NULL ORDER BY cons_loan_date ASC rows UNBOUNDED PRECEDING ) as current_balance
from loans_extended a 
FULL OUTER JOIN loans_paid b 
on a.loan_date=b.loan_date
)
select 
MY_DATE as date
,le.loans_extended
,lp.loans_paid
,"Cumulative Sum Loaned"
,"Cumulative Sum Paid"
,CURRENT_BALANCE
,cld.net_credit_bad
,cld.gross_credit_bad
,avg(current_balance) OVER (ORDER BY MY_DATE
                                  ROWS BETWEEN 90 PRECEDING AND 0 following) as average_balance_90_days
,sum(cld.net_credit_bad) OVER (ORDER BY MY_DATE
                                  ROWS BETWEEN 90 PRECEDING AND 0 following) as loss_90_days       
from CTE_MY_DATE d left join cte_credit_balance cb
on d.MY_DATE >= cb.cons_loan_date and d.MY_DATE< end_date
left join credit_losses_daily cld 
on my_date = cld.loan_date
left join loans_paid lp 
on MY_DATE = lp.loan_date
left join loans_extended le 
on MY_DATE = le.loan_date
order by MY_DATE 
);

/*BALANCE_V2.STG.RISK_RULE_HITS*/
create or replace table BALANCE_V2.STG.RISK_RULE_HITS as 
(
with rules as
(
select 
ID
,CHECKPOINT_ID
,event_time 
,parse_json(CONTEXT):name::string as rule_name
,case when rule_name = 'merchant__high_dispute_rate' then parse_json(CONTEXT):outcomes[0]:value::string
                                                     else parse_json(CONTEXT):outcomes[0]:value:decision::string end as rule_outcome
,case when parse_json(CONTEXT):silent::string = 'true' then 1 else 0 end as is_silent
,case when parse_json(CONTEXT):error::string = 'true' then 1 else 0 end as is_error
from BALANCE_V2.DBO.CHECKPOINT_EVENT_LOG
where EVENT_NAME = 'RULE_EVALUATED'
),sessions_end as 
(
select distinct
CHECKPOINT_ID
,parse_json(CONTEXT):outcomes[0]:decision::string as checkpoint_outcome
from BALANCE_V2.DBO.CHECKPOINT_EVENT_LOG
where EVENT_NAME = 'SESSION_ENDED'
),events as
(
select distinct 
CHECKPOINT_NAME
,CHECKPOINT_ID
,first_value(ENTITY_ID) ignore nulls over(partition by CHECKPOINT_ID order by 1) as ENTITY_ID
,first_value(event_time) ignore nulls over(partition by CHECKPOINT_ID order by 1) as event_time
,ENTITY_TYPE
,first_value(parse_json(CONTEXT):input:buyerId::string) ignore nulls over(partition by CHECKPOINT_ID order by 1) as buyer_id
,first_value(parse_json(CONTEXT):input:merchantId::string) ignore nulls over(partition by CHECKPOINT_ID order by 1) as merchant_id
,first_value(parse_json(CONTEXT):input:destinationId::string) ignore nulls over(partition by CHECKPOINT_ID order by 1) as vendor_id
from BALANCE_V2.DBO.CHECKPOINT_EVENT_LOG 
where EVENT_NAME not in ('RULE_EVALUATED','SESSION_ENDED')
)
select 
e.CHECKPOINT_NAME
,e.CHECKPOINT_ID
,e.ENTITY_ID
,e.ENTITY_TYPE
,e.BUYER_ID
,e.MERCHANT_ID
,e.vendor_id
,r.rule_name
,r.rule_outcome
,r.is_silent
,r.is_error
,coalesce(r.event_time,e.event_time) as event_time
,s.checkpoint_outcome
from events e full outer join rules r 
on e.checkpoint_id = r.checkpoint_id
full outer join sessions_end s 
on e.checkpoint_id = s.checkpoint_id
);

/*BALANCE_V2.STG.MARKETING_FUNNEL_COMPANY*/
create or replace table BALANCE_V2.STG.MARKETING_FUNNEL_COMPANY as
(    
with deals as
(
select  date(createdate_value) as deal_date,count(DEALID) as deals
from BALANCE_V2.DBO.HUBSPOT_DEAL_CUSTOM where pipeline_value<>'401421'
/*where and MOVE_TO_CLOSE_LOST_STAGE_VALUE is not null*/
group by 1
),stg_1 as
(
select 
companyid,
try_to_date(properties_became_a_customer_date__custom_value) as customer,
case when customer is not null or properties_became_an_opportunity_date__custom_value is not null then coalesce(try_to_date(properties_became_an_opportunity_date__custom_value),customer) else null end as opp,
case when opp is not null or properties_became_an_sql_date__custom_value is not null then coalesce(try_to_date(properties_became_an_sql_date__custom_value),opp) else null end as sql,
case when sql is not null or properties_became_an_mql_date__custom_value is not null then coalesce(try_to_date(properties_became_an_mql_date__custom_value),sql) else null end as mql,
case when mql is not null or properties_became_a_lead_date__custom_value is not null then coalesce(try_to_date(properties_became_a_lead_date__custom_value),mql) else null end as lead,
try_to_date(properties_first_contact_createdate_value) as contact
from BALANCE_V2.DBO.HUBSPOT_COMPANIES_CUSTOM
)
select 
fc.contact as first_contact_createdate
,count(distinct fc.COMPANYID) as first_contacts
,count(distinct l.COMPANYID) as leads
,count(distinct sql.COMPANYID) as SQLs
,count(distinct o.COMPANYID) as opportunities
,count(distinct mql.COMPANYID) as MQLs
,count(distinct c.COMPANYID) as customers
,coalesce(d.deals,0) as deals
,first_contacts+leads+SQLs+MQLs+opportunities+customers as First_contacts_agg
,leads+SQLs+MQLs+opportunities+customers as Leads_agg
,SQLs+MQLs+opportunities+customers as SQL_agg 
,MQLs+opportunities+customers as MQL_agg 
,opportunities+customers as OPP_agg 
from stg_1 fc
left join stg_1 l
on fc.contact = l.lead
left join stg_1 sql
on fc.contact = sql.sql
left join stg_1 o
on fc.contact = o.opp
left join stg_1 mql
on fc.contact = mql.mql
left join stg_1 c
on fc.contact = c.customer
left join deals d
on fc.contact = d.deal_date
group by 1,deals
order by 1
);

/*REVENUE_FACTORING*/
create or replace table "BALANCE_V2"."STG"."REVENUE_FACTORING" as
(
WITH CTE_MY_DATE AS
  (
    SELECT date(DATEADD(month, SEQ4(), '2022-01-01')) AS MY_DATE,last_day(my_date) as eom
      FROM TABLE(GENERATOR(ROWCOUNT=>100))
  ), all_loans as 
  (
select TRANSACTION_ID,CHARGE_ID,REVENUE_DATE,DUE_DATE,PAID_DATE,least(coalesce(paid_date,due_date),due_date) as end_date,TRANSACTION_TERMS_NET_DAYS,TRANSACTION_FACTORING_FEE_USD,CHARGE_STATUS,0 as is_cancelled,null as cancellation_date
from BALANCE_V2.STG.DIM_CHARGES
where TRANSACTION_FACTORING_FEE_USD >0 and revenue_date >= '2022-01-01'
union all
select TRANSACTION_ID,CHARGE_ID,REVENUE_DATE,DUE_DATE,PAID_DATE,least(coalesce(paid_date,due_date),due_date) as end_date,TRANSACTION_TERMS_NET_DAYS,TRANSACTION_FACTORING_FEE_USD,CHARGE_STATUS,1 as is_cancelled,parse_json(CANCEL_INFO):"cancelTime"::date as cancellation_date
from BALANCE_V2.STG.DIM_CANCELLED_CHARGES cc
left join BALANCE_V2.DBO.TRANSACTION t
on cc.transaction_id = t.id
where TRANSACTION_FACTORING_FEE_USD >0 and revenue_date >= '2022-01-01'
  ),loans_flt as
  (
    select my_date,eom,datediff(day,GREATEST(dateadd(day,-1,my_date),dateadd(day,-1,REVENUE_DATE)),least(eom,end_date)) as monthly_share
  ,datediff(day,dateadd(day,-1,REVENUE_DATE),end_date) as total_share,sum(monthly_share) over (partition by transaction_id order by my_date) as monthly_share_cu
  ,monthly_share/nullif(total_share,0)*transaction_factoring_fee_usd as monthly_revenue
  ,transaction_factoring_fee_usd-sum(monthly_revenue) over (partition by transaction_id order by my_date) as monthly_deferred_revenue,a.*
  from all_loans a left join CTE_MY_DATE d
  on d.my_date >= DATE_TRUNC( month,REVENUE_DATE) and d.my_date<end_date 
  )
select transaction_id,charge_id,my_date as "MONTH",monthly_share,total_share,monthly_revenue
,monthly_deferred_revenue,CHARGE_STATUS,TRANSACTION_FACTORING_FEE_USD as total_revenue
,revenue_date,paid_date,due_date,is_cancelled,cancellation_date
from loans_flt
union all
select transaction_id,charge_id,/*DATE_TRUNC( month,cancellation_date)*/ my_date as "MONTH",sum(monthly_share) as monthly_share,total_share,-1*sum(monthly_revenue) as monthly_revenue,
sum(case when my_date = DATE_TRUNC( month,cancellation_date) then -1*monthly_deferred_revenue else 0 end) as monthly_deferred_revenue,
charge_status,-1*TRANSACTION_FACTORING_FEE_USD as total_revenue,revenue_date,paid_date,due_date,is_cancelled,cancellation_date
from loans_flt where  is_cancelled = 1 /*transaction_id = '64986'*/ and my_date <= DATE_TRUNC( month,cancellation_date)
group by 1,2,3,5,8,9,10,11,12,13,14
union all
select transaction_id,charge_id,/*DATE_TRUNC( month,cancellation_date)*/ my_date as "MONTH",monthly_share,total_share,-1*monthly_revenue as monthly_revenue,
0 as monthly_deferred_revenue,
charge_status,-1*TRANSACTION_FACTORING_FEE_USD as total_revenue,revenue_date,paid_date,due_date,is_cancelled,cancellation_date
from loans_flt where  is_cancelled = 1  /*transaction_id = '64986'*/ and my_date > DATE_TRUNC( month,cancellation_date)
order by 1,3
  );

/*BALANCE_V2.STG.ZENDESK_TICKET_STATUS_EVENTS*/
create or replace table "BALANCE_V2"."STG"."ZENDESK_TICKET_STATUS_EVENTS" as
(
  WITH CTE_MY_DATE AS
  (
    SELECT DATEADD(DAY, SEQ4(), '2000-01-01') AS MY_DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>10000))
  ), cte as
  (
  select  
e.ticket_id
,e.created_at as start_date
,ifnull(lead(e.created_at,1) over(partition by ticket_id order by e.created_at),CURRENT_TIMESTAMP) as end_date
,e.UPDATER_ID
,u.name as updator_name
,u.email as updator_email
,u.role as updator_role
,t0.value:status::string as new_status
,t0.value:via::string as via
from BALANCE_V2.DBO.ZENDESK_TICKET_EVENTS e
left join BALANCE_V2.DBO.ZENDESK_USERS u
on e.updater_id = u.id,
lateral flatten( input => PARSE_JSON(child_events)) as t0
where 
t0.value:status is not null
order by 1,2
)
select 
cte.ticket_id
,MY_DATE as status_date
,cte.UPDATER_ID
,cte.updator_name
,cte.updator_email
,cte.updator_role
,cte.new_status as status
,cte.via
from cte left join cte_my_date 
on cte.start_date < my_date and cte.end_date >= my_date
);

/*BALANCE_V2.STG.ZENDESK_TICKETS_ENRICHED*/
create or replace table "BALANCE_V2"."STG"."ZENDESK_TICKETS_ENRICHED" as
(
select 
T.ID AS TICKET_ID,
T.CREATED_AT,
T.UPDATED_AT,
T.TYPE,
T.SUBJECT,
T.RAW_SUBJECT,
T.DESCRIPTION,
T.PRIORITY,
T.STATUS,
T.RECIPIENT,
T.ORGANIZATION_ID,
T.GROUP_ID,
T.FORUM_TOPIC_ID,
T.PROBLEM_ID,
T.HAS_INCIDENTS,
T.IS_PUBLIC,
T.DUE_AT,
T.ALLOW_CHANNELBACK,
T.ALLOW_ATTACHMENTS,
T.RIVERY_TYPE,
T.DATA_SOURCE,
T.GIT_ZEN_DATA,
T.COLLABORATOR_IDS,
T.REQUESTER_ID,
UR.NAME AS REQUESTER_NAME, 
UR.EMAIL AS REQUESTER_EMAIL, 
UR.TIME_ZONE AS REQUESTER_TIMEZONE,
T.SUBMITTER_ID,
US.NAME AS SUBMITTER_NAME, 
US.EMAIL AS SUBMITTER_EMAIL, 
US.TIME_ZONE AS SUBMITTER_TIMEZONE,
T.ASSIGNEE_ID,
UA.NAME AS ASSIGNEE_NAME, 
UA.EMAIL AS ASSIGNEE_EMAIL, 
UA.TIME_ZONE AS ASSIGNEE_TIMEZONE,
T.BRAND_ID
FROM BALANCE_V2.DBO.ZENDESK_TICKETS T
LEFT JOIN BALANCE_V2.DBO.ZENDESK_TICKET_METRICS M
    ON T.ID = M.TICKET_ID
LEFT JOIN BALANCE_V2.DBO.ZENDESK_USERS UR
    ON T.REQUESTER_ID = UR.ID
LEFT JOIN BALANCE_V2.DBO.ZENDESK_USERS US
    ON T.SUBMITTER_ID = US.ID
LEFT JOIN BALANCE_V2.DBO.ZENDESK_USERS UA
    ON T.ASSIGNEE_ID = UA.ID
);

/*BALANCE_V2.STG.MERCHANT_BALANCE_HIST*/

insert into "BALANCE_V2"."STG"."MERCHANT_BALANCE_HIST"
(
with cte as
(
select distinct
current_date() as balance_date,
merchant_id,
merchant_name,
merchant_status,
cb.value:amount::integer as merchant_current_balance_amount,
cb.value:currency::string as merchant_current_balance_currency,
null as last_updated
from "BALANCE_V2"."STG"."DIM_MERCHANT"
, LATERAL FLATTEN(INPUT => MERCHANT_CURRENT_BALANCE) cb
)
select * from cte
where concat(merchant_id,'|',balance_date) not in (select concat(merchant_id,'|',balance_date) from "BALANCE_V2"."STG"."MERCHANT_BALANCE_HIST")
);

UPDATE BALANCE_V2.STG.MERCHANT_BALANCE_HIST set last_updated = current_timestamp ;


/*BALANCE_V2.STG.BUYER_RISK_SCORE*/

insert into "BALANCE_V2"."STG"."BUYER_RISK_SCORE" 
(
with buyer_data1 as 
(
select buyer_id, charge_id, charge_amount, merchant_id,
case when is_paid = 0 and charge_status <> 'refunded' then charge_amount else 0 end as total_balance_now,
case when is_paid = 0 and charge_status <> 'refunded' then charge_amount else 0 end as total_non_diluted_tpv_now,
case when is_paid = 0 and payment_days > 1 then charge_amount else 0 end as past_due_amount, 
case when is_paid = 0 and payment_days > 60 then charge_amount else 0 end as amount_60d
from "BALANCE_V2"."DBO".dim_all_loans l
),buyer_data as 
(
select d.buyer_id, merchant_id, sum(charge_amount) as buyer_tpv, 
sum(total_balance_now) as total_balance_now, sum(total_non_diluted_tpv_now) as total_non_diluted_tpv_due, 
sum(past_due_amount) as past_due_now, sum(amount_60d) as amount_due_60d
from buyer_data1 d
group by 1,2
),buyer_number_due as 
(
select buyer_id, count(charge_id) as number_invoices_due 
from "BALANCE_V2"."DBO".dim_all_loans l
where is_paid = 0
group by 1
),max_dso as 
(
select buyer_id, max(payment_days) as buyer_max_dso 
from "BALANCE_V2"."DBO".dim_all_loans
group by 1
),buyer_failed_charges as 
(
select buyer_id, count(charge_id) as buyer_number_failed_charges 
from "BALANCE_V2"."DBO".dim_all_loans l
where charge_status = 'failed'
group by 1
),buyer_paid as (
select buyer_id, count(charge_id) as buyer_paid_count
from "BALANCE_V2"."DBO".dim_all_loans l
where l.is_paid = 1
group by 1
)
,buyer_summary as 
(
select d.buyer_id, buyer_public_id, merchant_id, 
ifnull(number_invoices_due,0) as number_invoices_due,
past_due_now, total_non_diluted_tpv_due, 
case when total_non_diluted_tpv_due = 0 then 0 else 
past_due_now/total_non_diluted_tpv_due end as buyer_past_due_percent,
amount_due_60d, total_balance_now, 
case when total_balance_now = 0 then 0 else amount_due_60d/total_balance_now end as buyer_60d_percent,
ifnull(buyer_max_dso,0) as buyer_max_dso, ifnull(buyer_number_failed_charges,0) as buyer_number_failed_charges, 
case when buyer_paid_count is null then 'No'
when buyer_paid_count < 5 then 'No'
else 'Yes' end as existing_buyer
from buyer_data d
left join buyer_failed_charges c on d.buyer_id = c.buyer_id
left join max_dso m on d.buyer_id = m.buyer_id
left join buyer_number_due b on b.buyer_id = d.buyer_id
join "BALANCE_V2"."STG".dim_buyer db on db.buyer_id = d.buyer_id
left join buyer_paid bp on d.buyer_id = bp.buyer_id
),merchant_paid as 
(
select merchant_id, sum(charge_amount) as merchant_paid_amount, 
count(charge_id) as merchant_paid_count
from "BALANCE_V2"."DBO".dim_all_loans l
where is_paid = 1
group by 1
),merchant_diluted as 
(
select merchant_id, 
sum(charge_amount) as all_merchant_tpv,
sum(case when a.charge_status in ('refunded','canceled') then charge_amount else 0 end) as merchant_diluted_tpv, 
sum(charge_amount) - sum(case when a.charge_status in ('refunded','canceled') then charge_amount else 0 end) as merchant_non_diluted_tpv
from "BALANCE_V2"."DBO".accounting_report_analytics_with_paid_date a
where is_financed = 'TRUE'
group by 1
),merchant_summary as 
(
select d.merchant_id, all_merchant_tpv, 
case when merchant_paid_count is null then 'No'
when merchant_paid_count < 5 then 'No'
else 'Yes' end as existing_merchant,
ifnull(merchant_diluted_tpv,0) as merchant_diluted_tpv, 
ifnull(merchant_non_diluted_tpv,0) as merchant_non_diluted_tpv, 
ifnull(merchant_paid_amount, 0) as merchant_paid_amount, 
case when merchant_non_diluted_tpv = 0 then 0
else ifnull(merchant_paid_amount, 0)/ merchant_non_diluted_tpv end as merchant_repayment_percent, 
ifnull(merchant_diluted_tpv,0) / all_merchant_tpv as merchant_dilution_percent
from merchant_diluted d
left join merchant_paid p on d.merchant_id = p.merchant_id
),dnb_scores as 
(
select distinct b.buyer_id, ifnull(number_of_employees,0) as number_of_employees, ifnull(businesstrading_totalexperiencescount,0) as trades, ifnull(businesstrading_maximumhighcreditamount,0) as max_credit_amount, failure_class_raw_score as fss, 
case when failure_score_date is null then 'fail'
when datediff(day,failure_score_date,current_date) > 365 then 'fail'
when datediff(day,failure_score_date,current_date) > 180 then 'discount'
else 'ok' end as fss_discount, 
delinquency_raw_score as css, 
case when delinquency_score_date is null then 'fail'
when datediff(day, delinquency_score_date, current_date) > 365 then 'fail'
when datediff(day, delinquency_score_date, current_date) > 180 then 'discount'
else 'ok' end as css_discount, 
case when experian_match_score > 85 then 'Yes'
when dnb_bm_l1_matchqualityinformation_namematchscore > 85 then 'Yes'
else 'No' end as match_score_sufficient, c.legalevents_hasbankruptcy as bankruptcy
from "BALANCE_V2".stg.dnb_identity_cmtps c
join "BALANCE_V2".stg.dim_buyer b on c.buyer_id = b.buyer_public_id
left join "BALANCE_V2".stg.lendflow_json_business_credit_flt e on c.buyer_id = e.entity_id
left join "BALANCE_V2".stg.lendflow_json_commercial_data_flt f on e.uuid = f.uuid
),weights_raw_data as 
(
select b.buyer_id, buyer_public_id, number_invoices_due, existing_buyer, existing_merchant,
buyer_past_due_percent, 
buyer_60d_percent, 
buyer_max_dso, 
buyer_number_failed_charges, 
ifnull(merchant_repayment_percent,0) as merchant_repayment_percent, 
ifnull(merchant_dilution_percent,0) as merchant_dilution_percent, 
number_of_employees, 
trades, 
max_credit_amount,
fss, fss_discount, 
css, css_discount, 
case when css is null then 'No'
else match_score_sufficient end as bureau_data_sufficient, 
case when bankruptcy is null then 'false' else bankruptcy end as bankruptcy
/*ntile(9) over (order by buyer_max_dso) as dso_ntile*/
from buyer_summary b
left join merchant_summary m on b.merchant_id = m.merchant_id
left join dnb_scores d on b.buyer_id = d.buyer_id
),
weights1 as (
select buyer_id, buyer_public_id, number_invoices_due, existing_buyer, existing_merchant, 
case when buyer_past_due_percent < 0.01 then 1
when buyer_past_due_percent < 0.025 then 2
when buyer_past_due_percent < 0.1 then 3
when buyer_past_due_percent < 0.2 then 4
when buyer_past_due_percent < 0.3 then 5
when buyer_past_due_percent < 0.5 then 6
when buyer_past_due_percent < 0.75 then 7
when buyer_past_due_percent < 1 then 8
else 9 end as buyer_past_due_percent,

case when buyer_60d_percent < 0.003 then 1
when buyer_60d_percent < 0.005 then 2
when buyer_60d_percent < 0.01 then 3
when buyer_60d_percent < 0.05 then 4
when buyer_60d_percent < 0.1 then 5
when buyer_60d_percent < 0.25 then 6
when buyer_60d_percent < 0.5 then 7
when buyer_60d_percent < 1 then 8
else 9 end as buyer_60d_percent, 

case when buyer_max_dso < -15 then 1
when buyer_max_dso < -7 then 2
when buyer_max_dso < 0 then 3
when buyer_max_dso < 7 then 4
when buyer_max_dso < 15 then 5
when buyer_max_dso < 30 then 6
when buyer_max_dso < 60 then 7
when buyer_max_dso < 90 then 8
else 9 end as buyer_max_dso, 

case when buyer_number_failed_charges < 1 then 2
when buyer_number_failed_charges < 2 then 4
when buyer_number_failed_charges < 5 then 5
when buyer_number_failed_charges < 10 then 6
when buyer_number_failed_charges < 15 then 7
when buyer_number_failed_charges < 20 then 8
else 9 end as buyer_number_failed_charges, 

case when merchant_repayment_percent < 0.7 then 9
when merchant_repayment_percent < 0.8 then 8
when merchant_repayment_percent < 0.9 then 7
when merchant_repayment_percent < 0.95 then 6
when merchant_repayment_percent < 0.995 then 5
else 1 end as merchant_repayment_percent, 

case when merchant_dilution_percent < 0.01 then 1
when merchant_dilution_percent < 0.02 then 2
when merchant_dilution_percent < 0.03 then 3
when merchant_dilution_percent < 0.05 then 4
when merchant_dilution_percent < 0.06 then 5
when merchant_dilution_percent < 0.07 then 6
when merchant_dilution_percent < 0.08 then 7
when merchant_dilution_percent < 0.09 then 8
else 9 end as merchant_dilution_percent, 

case when trades >= 100 then 1
when trades >= 50 then 2
when trades >= 20 then 3
when trades >= 10 then 4
when trades >= 5 then 5
when trades >= 1 then 6
when trades >= 0 then 7
when trades >= -5 then 8
else 0 end as trades, 

case when max_credit_amount is null then 9
when max_credit_amount < 500 then 9
when max_credit_amount < 1000 then 8
when max_credit_amount < 5000 then 7
when max_credit_amount < 10000 then 6
when max_credit_amount < 100000 then 5
when max_credit_amount < 500000 then 4
when max_credit_amount < 1000000 then 3
when max_credit_amount < 10000000 then 2
else 1 end as max_credit_amount, 

case when fss is null then 9
when fss >= 95 then 1 
when fss >= 70 then 2
when fss >= 50 then 3
when fss >= 30 then 4 
when fss >= 20 then 5
when fss >= 10 then 6
when fss >= 5 then 7
when fss >= 1 then 8
else 9 end as fss, 

case when css is null then 9
when css >= 95 then 1
when css >= 70 then 2
when css >= 50 then 3
when css >= 30 then 4
when css >= 20 then 5
when css >= 10 then 6
when css >= 5 then 7
when css >= 1 then 8
else 9 end as css, 
case when bankruptcy = 'false' then 1 else 9 end as bankruptcy,

number_of_employees,  css_discount, fss_discount, bureau_data_sufficient
from weights_raw_data w

),
weights as (
select w.buyer_id, buyer_public_id, number_invoices_due, bureau_data_sufficient, existing_buyer, existing_merchant, 
buyer_past_due_percent*0.3 as weighted_past_due_percent, 
buyer_60d_percent*0.3 as weighted_60d_percent, 
buyer_max_dso*0.1 as weighted_max_dso, 
buyer_number_failed_charges*0.3 as weighted_buyer_number_failed_charges, 
merchant_repayment_percent*0.8 as weighted_merchant_repayment_percent, 
merchant_dilution_percent*0.2 as weighted_merchant_dilution_percent, 
case when css_discount = 'fail' then 9*0.3
when css_discount = 'discount' and css*2 >=9 then 9*0.3
when css_discount = 'discount' and css*2 <9 then css*2*0.3
else css*0.3 end as weighted_css, 
case when fss_discount = 'fail' then 9*0.3
when fss_discount = 'discount' and fss*2 >= 9 then 9*0.3
when fss_discount = 'discount' and fss < 9 then fss*2*0.3
else fss*0.3 end as weighted_fss, 
trades*0.15 as weighted_trades, 
max_credit_amount*0.05 as weighted_max_amount, 
bankruptcy*0.1 as weighted_bankruptcy
from weights1 w
),final as 
(
select *, 
weighted_past_due_percent + weighted_60d_percent + weighted_max_dso + weighted_buyer_number_failed_charges as buyer_weighted, 
weighted_merchant_repayment_percent + weighted_merchant_dilution_percent as merchant_weighted, 
case when bureau_data_sufficient = 'No' then 5.5
else weighted_css + weighted_fss + weighted_trades + weighted_max_amount + weighted_bankruptcy end as bureau_weighted
from weights w
),lastone as 
(
select *, 
case when existing_merchant = 'Yes' and existing_buyer = 'Yes' then (buyer_weighted*0.6) + (merchant_weighted*0.3) + (bureau_weighted*0.1)
when existing_merchant = 'Yes' and existing_buyer = 'No' then (merchant_weighted*0.5) + (bureau_weighted*0.5)
when existing_merchant = 'No' and existing_buyer = 'Yes' then (buyer_weighted*0.7) + (bureau_weighted*0.3)
when existing_merchant = 'No' and existing_buyer = 'No' then (bureau_weighted*1) 
end as buyer_risk_score
/* case when number_invoices_due > 0 then (buyer_weighted*0.7) + (merchant_weighted*0.1) + (bureau_weighted*0.2)
 else (merchant_weighted*0.3) + (bureau_weighted*0.7) end as buyer_risk_score*/
from final f
)
select concat(buyer_id,'|',current_date()) as buyer_risk_score_key, buyer_id, buyer_public_id, buyer_risk_score, current_date() as etl_date 
from lastone l
where concat(buyer_id,'|',current_date()) not in (select buyer_risk_score_key from "BALANCE_V2"."STG"."BUYER_RISK_SCORE")
);

/*buyer_loan_tape*/
create or replace table BALANCE_V2.STG.BUYER_LOAN_TAPE as (
with risk_score as
(
select buyer_public_id,buyer_risk_score
from BALANCE_V2.STG.BUYER_RISK_SCORE
where etl_date = (select max(etl_date) from BALANCE_V2.STG.BUYER_RISK_SCORE)
),
industry as
(
select distinct buyer_id,first_value(industrycodes_description ignore nulls) over (partition by buyer_id order by duns) as industry_code_desc
from BALANCE_V2.STG.DNB_IDENTITY_CMTPS
)
select 
dlt.buyer_public_id
,sum(case when status='Paid' then original_amount else 0 end) as "Paid"
,sum(case when status = 'Current' then original_amount else 0 end) as "Current"
,sum(case when status = '1-29D' then original_amount else 0 end) as "1-29D"
,sum(case when status = '30-59D' then original_amount else 0 end) as "30-59D"
,sum(case when status = '60-89D' then original_amount else 0 end) as "60-89D"
,sum(case when status = '90-120D' then original_amount else 0 end) as "90-120D"
,sum(case when status = '120D+' then original_amount else 0 end) as "120D+"
,sum(case when status = 'Dilution' then original_amount else 0 end) as "Dilution"
,sum(original_amount) as grand_total
,round(buyer_risk_score,0) as rating
,i.industry_code_desc
,("Current"+"1-29D"+"30-59D"+"60-89D"+"90-120D"+"120D+"+"Dilution") as Balance

from BALANCE_V2.STG.DIM_LOAN_TAPE dlt
left join risk_score rs
on dlt.buyer_public_id = rs.buyer_public_id
left join industry i 
on dlt.buyer_public_id = i.buyer_id
group by 1,11,12
order by balance desc
);

/*STRIPE_FEES*/


create or replace table BALANCE_V2.STG.STRIPE_FEES as (
with stripe_fees_temp as (
select
min(date(incurred_at)) as charge_date,
charge_id,
card_brand,
card_funding,
card_country,
max (case when event_type='charge_failed' then 1 else 0 end) as is_failed,
max (case when refund_id is not null then 1 else 0 end) as is_refund,
max (case when dispute_id is not null then 1 else 0 end) as is_disputed,
max (case when fee_name in ('interchange','discount') then plan_name end) as plan,
avg (case when fee_name='volume_fee' then variable_volume_amount end) as charge_amount,
avg(case when fee_name in ('interchange','discount') then variable_rate end ) as variable_rate,
sum(case when (fee_category='network_cost' and fee_name='card_scheme') then total_amount else 0 end) as network_fee_card_scheme,
sum(case when (fee_category='network_cost' and fee_name in ('interchange','discount')) then total_amount else 0 end) as network_fee_interchange,
sum(case when fee_name='volume_fee' then total_amount else 0 end) as volume_fee,
sum(case when fee_name='per_auth_fee' then total_amount else 0 end) as per_auth_fee,
sum(total_amount) as total_fee
from stripe_data_pipeline.stripe.icplus_fees
where charge_id is not null
group by 2,3,4,5
),
card_category_mapping as (
select 
distinct(a.plan_name) as plan_name,
card_category

from STRIPE_DATA_PIPELINE.STRIPE.network_cost_insights_report a
join stripe_data_pipeline.stripe.icplus_fees b on a.charge_id=b.charge_id 
),

refunds_aggregated as (
select
charge_id,
date_trunc ('month', created) as refund_month,
sum(amount) as refund_amount
 
from stripe_data_pipeline.stripe.refunds
group by 1,2

)

select
a.*,
c.processing_fee_usd as balance_processing_fee,
/*zeroifnull(a.charge_amount)-zeroifnull(case when date_trunc('month',a.charge_date)=d.refund_month then a.charge_amount else null end)) as net_charge_amount,*/
zeroifnull(a.charge_amount)-zeroifnull(d.refund_amount)as net_charge_amount,


div0(balance_processing_fee,a.charge_amount) as balance_processing_fee_rate,
volume_fee+per_auth_fee as stripe_fees,
div0 ((volume_fee+per_auth_fee),charge_amount) as stripe_fee_rate,
network_fee_card_scheme+network_fee_interchange as network_cost,
div0 ((network_fee_card_scheme+network_fee_interchange),charge_amount) as network_cost_rate,
div0 ((volume_fee+per_auth_fee+network_fee_card_scheme+network_fee_interchange),charge_amount) as total_fee_rate
from stripe_fees_temp a
left join balance_v2.stg.dim_charges c on a.charge_id=c.external_charge_id
left join refunds_aggregated d on a.charge_id=d.charge_id  and date_trunc('month',a.charge_date)=d.refund_month

);

/*"BALANCE_V2"."STG"."BUYER_RISK_SCORE_FEATURES"*/
create or replace table "BALANCE_V2"."STG"."BUYER_RISK_SCORE_FEATURES" as
(
with buyer_data1 as 
(
select buyer_id, charge_id, charge_amount, merchant_id,
case when is_paid = 0 and charge_status <> 'refunded' then charge_amount else 0 end as total_balance_now,
case when is_paid = 0 and charge_status <> 'refunded' then charge_amount else 0 end as total_non_diluted_tpv_now,
case when is_paid = 0 and payment_days > 1 then charge_amount else 0 end as past_due_amount, 
case when is_paid = 0 and payment_days > 60 then charge_amount else 0 end as amount_60d
from "BALANCE_V2"."DBO".dim_all_loans l
),buyer_data as 
(
select d.buyer_id, merchant_id, sum(charge_amount) as buyer_tpv, 
sum(total_balance_now) as total_balance_now, sum(total_non_diluted_tpv_now) as total_non_diluted_tpv_due, 
sum(past_due_amount) as past_due_now, sum(amount_60d) as amount_due_60d
from buyer_data1 d
group by 1,2
),buyer_number_due as 
(
select buyer_id, count(charge_id) as number_invoices_due 
from "BALANCE_V2"."DBO".dim_all_loans l
where is_paid = 0
group by 1
),max_dso as 
(
select buyer_id, max(payment_days) as buyer_max_dso 
from "BALANCE_V2"."DBO".dim_all_loans
group by 1
),buyer_failed_charges as 
(
select buyer_id, count(charge_id) as buyer_number_failed_charges 
from "BALANCE_V2"."DBO".dim_all_loans l
where charge_status = 'failed'
group by 1
),buyer_paid as (
select buyer_id, count(charge_id) as buyer_paid_count
from "BALANCE_V2"."DBO".dim_all_loans l
where l.is_paid = 1
group by 1
)
,buyer_summary as 
(
select d.buyer_id, buyer_public_id, merchant_id, 
ifnull(number_invoices_due,0) as number_invoices_due,
past_due_now, total_non_diluted_tpv_due, 
case when total_non_diluted_tpv_due = 0 then 0 else 
past_due_now/total_non_diluted_tpv_due end as buyer_past_due_percent,
amount_due_60d, total_balance_now, 
case when total_balance_now = 0 then 0 else amount_due_60d/total_balance_now end as buyer_60d_percent,
ifnull(buyer_max_dso,0) as buyer_max_dso, ifnull(buyer_number_failed_charges,0) as buyer_number_failed_charges, 
case when buyer_paid_count is null then 'No'
when buyer_paid_count < 5 then 'No'
else 'Yes' end as existing_buyer
from buyer_data d
left join buyer_failed_charges c on d.buyer_id = c.buyer_id
left join max_dso m on d.buyer_id = m.buyer_id
left join buyer_number_due b on b.buyer_id = d.buyer_id
join "BALANCE_V2"."STG".dim_buyer db on db.buyer_id = d.buyer_id
left join buyer_paid bp on d.buyer_id = bp.buyer_id
),merchant_paid as 
(
select merchant_id, sum(charge_amount) as merchant_paid_amount, 
count(charge_id) as merchant_paid_count
from "BALANCE_V2"."DBO".dim_all_loans l
where is_paid = 1
group by 1
),merchant_diluted as 
(
select merchant_id, 
sum(charge_amount) as all_merchant_tpv,
sum(case when a.charge_status in ('refunded','canceled') then charge_amount else 0 end) as merchant_diluted_tpv, 
sum(charge_amount) - sum(case when a.charge_status in ('refunded','canceled') then charge_amount else 0 end) as merchant_non_diluted_tpv
from "BALANCE_V2"."DBO".accounting_report_analytics_with_paid_date a
where is_financed = 'TRUE'
group by 1
),merchant_summary as 
(
select d.merchant_id, all_merchant_tpv, 
case when merchant_paid_count is null then 'No'
when merchant_paid_count < 5 then 'No'
else 'Yes' end as existing_merchant,
ifnull(merchant_diluted_tpv,0) as merchant_diluted_tpv, 
ifnull(merchant_non_diluted_tpv,0) as merchant_non_diluted_tpv, 
ifnull(merchant_paid_amount, 0) as merchant_paid_amount, 
case when merchant_non_diluted_tpv = 0 then 0
else ifnull(merchant_paid_amount, 0)/ merchant_non_diluted_tpv end as merchant_repayment_percent, 
ifnull(merchant_diluted_tpv,0) / all_merchant_tpv as merchant_dilution_percent
from merchant_diluted d
left join merchant_paid p on d.merchant_id = p.merchant_id
),dnb_scores as 
(
select distinct b.buyer_id, ifnull(number_of_employees,0) as number_of_employees, ifnull(businesstrading_totalexperiencescount,0) as trades, ifnull(businesstrading_maximumhighcreditamount,0) as max_credit_amount, failure_class_raw_score as fss, 
case when failure_score_date is null then 'fail'
when datediff(day,failure_score_date,current_date) > 365 then 'fail'
when datediff(day,failure_score_date,current_date) > 180 then 'discount'
else 'ok' end as fss_discount, 
delinquency_raw_score as css, 
case when delinquency_score_date is null then 'fail'
when datediff(day, delinquency_score_date, current_date) > 365 then 'fail'
when datediff(day, delinquency_score_date, current_date) > 180 then 'discount'
else 'ok' end as css_discount, 
case when experian_match_score > 85 then 'Yes'
when dnb_bm_l1_matchqualityinformation_namematchscore > 85 then 'Yes'
else 'No' end as match_score_sufficient, c.legalevents_hasbankruptcy as bankruptcy
from "BALANCE_V2".stg.dnb_identity_cmtps c
join "BALANCE_V2".stg.dim_buyer b on c.buyer_id = b.buyer_public_id
left join "BALANCE_V2".stg.lendflow_json_business_credit_flt e on c.buyer_id = e.entity_id
left join "BALANCE_V2".stg.lendflow_json_commercial_data_flt f on e.uuid = f.uuid
)
select b.buyer_id, buyer_public_id, number_invoices_due, existing_buyer, existing_merchant,
buyer_past_due_percent, 
buyer_60d_percent, 
buyer_max_dso, 
buyer_number_failed_charges, 
ifnull(merchant_repayment_percent,0) as merchant_repayment_percent, 
ifnull(merchant_dilution_percent,0) as merchant_dilution_percent, 
number_of_employees, 
trades, 
max_credit_amount,
fss, fss_discount, 
css, css_discount, 
case when css is null then 'No'
else match_score_sufficient end as bureau_data_sufficient, 
case when bankruptcy is null then 'false' else bankruptcy end as bankruptcy
/*ntile(9) over (order by buyer_max_dso) as dso_ntile*/
from buyer_summary b
left join merchant_summary m on b.merchant_id = m.merchant_id
left join dnb_scores d on b.buyer_id = d.buyer_id
);

/*"BALANCE_V2"."STG"."ACCOUNTING_REPORT"*/
create or replace table "BALANCE_V2"."STG"."ACCOUNTING_REPORT" as
(
SELECT 
    'payment' AS type,
    t.host_seller_id AS merchant_id,
    s.name AS merchant_name, 
    t.created_at,
    t.id AS payment_id,
    t.buyer_id,
    b.email AS buyer_email,
    t.total_price AS payment_amount,
    t.currency AS payment_currency,
    c.id AS charge_id,
    CAST(c.amount_in_cents AS FLOAT)/100 AS charge_amount,
    c.currency AS charge_currency,
    c.type AS event_type,
    c.name AS event_name,
    CASE
        WHEN t.is_financed THEN c.created_at
        WHEN t.is_financed = false AND c.paid_date IS NULL THEN c.charge_date
        ELSE c.paid_date
    END::DATE AS revenue_date,
    c.charge_date AS due_date,
    CASE
        WHEN c.payment_method_type = 'invoice' AND c.charge_status = 'charged' THEN 'wire'
        ELSE c.payment_method_type
    END AS payment_method,
    t.plan_type,
    c.charge_status,
    CAST(c.stripe_fee_in_cents AS FLOAT)/100 AS stripe_fee,
    CAST(c.processing_fee_in_cents AS FLOAT)/100 AS processing_fee,
    (CAST(c.processing_fee_in_cents AS FLOAT) - CAST(c.stripe_fee_in_cents AS FLOAT))/100 AS charge_revenue,
    t.is_financed,
    t.id AS transaction_id,
    settle.fee_amount AS settle_fee_amount,
    settle.approval_amount AS settle_approval_amount,
    ROUND(((CAST(c.amount_in_cents AS DOUBLE) * (CAST(t.factoring_fee_in_cents AS DOUBLE) / 
        (CASE
            WHEN t.is_auth = true THEN t.captured_amount
            ELSE t.total_price
        END * 100)))/100)::NUMERIC, 2) AS factoring_fee,
    (CAST(t.factoring_fee_in_cents AS FLOAT)/100) - settle.fee_amount AS payment_revenue,
    NULL AS payout_id,
    NULL AS payout_amount_requested,
    NULL AS payout_amount_transferred,
    NULL AS payout_created_at,
    NULL AS payout_trigger,
    NULL AS refund_id,
    NULL AS refund_amount,
    NULL AS refund_notes,
    NULL AS refund_reason
FROM BALANCE_V2.DBO.TRANSACTION t
JOIN BALANCE_V2.DBO.SELLER s 
ON t.host_seller_id = s.id
JOIN BALANCE_V2.DBO.BUYER b ON t.buyer_id = b.id
JOIN BALANCE_V2.DBO.CHARGE c ON t.id = c.transaction_id
LEFT JOIN (
    SELECT 
        st.transaction_id,
        SUM(st.approval_amount) AS approval_amount,
        SUM(st.fee_amount) AS fee_amount
    FROM BALANCE_V2.DBO.SETTLE_TRANSACTION st 
    WHERE st.is_transmitted = true
    GROUP BY st.transaction_id
) settle ON settle.transaction_id = t.id
WHERE 
    c.amount_in_cents > 500 
    AND (
        (t.is_financed = false OR t.is_financed IS NULL) 
        AND c.charge_status = 'charged' 
        OR c.charge_status = 'refunded' 
        OR t.is_financed = true
    )
    );
create or replace table "BALANCE_V2"."STG"."FACT_ALL_REVENUE" as 
(

with fx_fees as (
select ENTITY_REF_ID,payout_id, sum(fe.AMOUNT_IN_CENTS/100) as fx_fee_usd 
from BALANCE_V2.DBO.FEES fe
join BALANCE_V2.DBO.core_payments cp 
on fe.entity_ref_id=cp.public_id
where fe.type = 'FX'
group by 1,2)

select
charge_public_id as public_id,
merchant_id,
merchant_name,
buyer_public_id,
date (created_at) as accounting_date,
charge_status as status,
CHARGE_PAYMENT_METHOD_TYPE as PAYMENT_METHOD,
transaction_factoring_fee_usd as revenue,
'factoring revenue' as revenue_type



from BALANCE_V2.STG.DIM_CHARGES
where transaction_is_financed=TRUE
and charge_status in ('charged','refunded','pending','waitingForPayment')

union all


select
charge_public_id as public_id,
merchant_id,
merchant_name,
buyer_public_id,
date (paid_date) as accounting_date,
charge_status as status,
CHARGE_PAYMENT_METHOD_TYPE as PAYMENT_METHOD,
processing_fee_usd as revenue,
'terms processing revenue' as revenue_type



from BALANCE_V2.STG.DIM_CHARGES
where transaction_is_financed=TRUE
and charge_status in ('charged','refunded')

union all



select
charge_public_id as public_id,
merchant_id,
merchant_name,
buyer_public_id,
date (paid_date) as accounting_date,
charge_status as status,
CHARGE_PAYMENT_METHOD_TYPE as PAYMENT_METHOD,
processing_fee_usd as revenue,
'Spot processing revenue' as revenue_type


from BALANCE_V2.STG.DIM_CHARGES
where (transaction_is_financed=FALSE or transaction_is_financed is null)
and charge_status in ('charged','refunded')

union all

select
public_id as public_id,
merchant_id,
merchant_name,
null as buyer_public_id,
date (date) as accounting_date,
status,
'payout' as PAYMENT_METHOD,
payout_fee_usd as revenue,
'Payout revenue' as revenue_type

from BALANCE_V2.STG.DIM_PAYMENTS
where type='vendor_payout'

union all 

select
public_id as public_id,
merchant_id,
merchant_name,
null as buyer_public_id,
date (date) as accounting_date,
status,
PAYMENT_METHOD,
amount as revenue,
'Platform revenue' as revenue_type

from BALANCE_V2.STG.DIM_PAYMENTS
where is_min_fee=1

union all

select
public_id as public_id,
merchant_id,
merchant_name,
null as buyer_public_id,
date (date) as accounting_date,
status,
'payout' as PAYMENT_METHOD,
fx_fee_usd as revenue,
'Payout FX revenue' as revenue_type

from BALANCE_V2.STG.DIM_PAYMENTS dp
join fx_fees fx
on dp.public_id=fx.payout_id 
)
;

/*"BALANCE_V2"."STG"."FACT_ALL_COSTS"*/

create or replace table BALANCE_V2.STG.FACT_ALL_COSTS as 
(

/*#######################
  CC network costs
#######################*/

select
charge_public_id as public_id,
merchant_id,
merchant_name,
buyer_public_id,
date (sf.charge_date) as accounting_date,
charge_payment_method_type as payment_method,
charge_status as status,
network_cost/100 as cost,
'network cost' as cost_type



from balance_v2.stg.dim_charges dc
left join balance_v2.stg.stripe_fees sf
on dc.external_charge_id=sf.charge_id 

where charge_status in ('charged','refunded','canceled') and dc.charge_payment_method_type ='creditCard'

union all

/*#######################
  Non CC processing costs
#######################*/

select
charge_public_id as public_id,
merchant_id,
merchant_name,
buyer_public_id,
date (paid_date) as accounting_date,
charge_payment_method_type as payment_method,
charge_status as status,
stripe_fee_usd as cost,
'Non CC processing cost' as cost_type



from balance_v2.stg.dim_charges dc

where charge_status in ('charged','refunded','canceled') and dc.charge_payment_method_type <>'creditCard'

union all

/*#######################
  CC stripe costs
#######################*/

select
charge_public_id as public_id,
merchant_id,
merchant_name,
buyer_public_id,
date (sf.charge_date) as accounting_date,
charge_payment_method_type as payment_method,
charge_status as status,
stripe_fees/100 as cost,
'stripe cost' as cost_type



from balance_v2.stg.dim_charges dc
left join balance_v2.stg.stripe_fees sf
on dc.external_charge_id=sf.charge_id 

where charge_status in ('charged','refunded','canceled') and dc.charge_payment_method_type ='creditCard'

union all

/*#######################
  Withdrawal costs
#######################*/

select
public_id as public_id,
a.merchant_id,
b.merchant_name,
null as buyer_public_id,
date (a.created_at) as accounting_date,
'payout' as payment_method,
status as status,
case when year(date(a.created_at))<=2023 then 2 else 0.5 end as cost,
'withdrawal payout cost' as cost_type

from balance_v2.dbo.payout a
left join balance_v2.stg.dim_merchant b
on a.merchant_id=b.merchant_id
where a.merchant_id=a.vendor_id


union all


/*#######################
  Vendor payout costs
#######################*/

select
public_id as public_id,
a.merchant_id,
b.merchant_name,
null as buyer_public_id,
date (a.created_at) as accounting_date,
'payout' as payment_method,
status as status,
case when year(date(a.created_at))<=2023 then 2 else 0.5 end as cost,
'vendor payout cost' as cost_type

from balance_v2.dbo.payout a
left join balance_v2.stg.dim_merchant b
on a.merchant_id=b.merchant_id
where a.merchant_id<>a.vendor_id

union all 

/*#######################
  Stripe Other network fees
#######################*/

select 
null as public_id,
108 as merchant_id,
'Balance Payments Inc' as merchant_name,
null as buyer_public_id,
date(created) as accounting_date,
'stripe fee' as payment_method,
'charged' as status,
amount/100*-1 as cost,
'Stripe Other Costs' as cost_type
from stripe_data_pipeline.stripe.balance_transactions
where reporting_category = 'fee'
and description not like '%Card payments%'
and description not like '%Payout Fee%'


);

/*MERCHANT_REVENUE_DAILY*/
create or replace table "BALANCE_V2"."STG"."MERCHANT_REVENUE_DAILY" as
(
 select 
p.merchant_id
,m.merchant_name
,m.merchant_token
,date_trunc(p.day,date) as revenue_date
,sum(total_revenue) as total_daily_revenue
from BALANCE_V2.STG.DIM_PAYMENTS p
left join "BALANCE_V2"."STG"."DIM_MERCHANT" m
on p.merchant_id = m.merchant_id
group by 1,2,3,4 
);

/*FG Pre assessment*/
create or replace table "BALANCE_V2"."DBO"."fg_pre_assessment" as
(

with unions as (
select * 
from balance_v2.dbo."fg_pre_assessment_first_batch"

union

select * 
from balance_v2.dbo."fg_pre_assessment_april_2.5k_batch"

union

select * 
from balance_v2.dbo."fg_pre_assessment_40k_batch"
),

batches as (

select *, 
case when batch_name = 'First 9k' then 1
when batch_name = 'April 2.5k' then 2
else 3 end as batch_rank, 
row_number() over (partition by retailerguid order by batch_name) as in_batch
from unions
), 

final as (
select *, 
row_number() over (partition by retailerguid order by batch_rank) as highest_batch
from batches b
qualify in_batch = 1 and highest_batch = 1
), 

lastone as (

select  batch_name, retailerguid, public_id, companyname, u.email, bad_history, billingaddress, website, on_offline,  
billing_country, fg_latest_tier, num_failed_transactions, num_dispute_transactions, total_txn, total_vol, one_m_volume, six_m_volume, txn_3m, one_to_six, 
num_months_with_vol, num_years_with_vol, num_years_with_txn, vol_last_month, num_txn_last_month,
num_disputes_last_month, last_month_failure_rate, first_volume_year, avg_yearly_vol, avg_monthly_vol, avg_monthly_vol_no_extreme, avg_monthly_txn, avg_monthly_disputes,
avg_monthly_failures, current_failure_rate, six_m_consistency, three_to_six_trend_vol, 
three_to_six_trend_txn, dispute_rate,
volume_decline, vol_ratios, 
current_to_yearly_vol, consistency_subscore, vol_trend_subscore, vol_ratios_subscore, 
current_to_yearly_vol_subscore, txn_trend_subscore, frequency_subscore, success_rate_subscore, operational_subscore, enrichment_subscore, total_subscores,
weighted_consistency_subscore, weighted_frequency_subscore, weighted_success_rate_subscore, weighted_vol_trend_subscore, 
weighted_combined_vol_subscre, weighted_combined_vol_subscore2, total_weighted_subscore2,
buyer_risk_tier, recommended_cl, pre_approved,
aggressive_cl, moderate_cl, conservative_cl, pre_approved_credit_limit, 
row_number() over (partition by retailerguid order by b.created_at desc) as last_buyer
from final u
left join balance_v2.dbo.buyer b on u.retailerguid = b.external_reference_id
where b.deleted_at is null
qualify last_buyer = 1
)

select  batch_name, retailerguid, public_id, companyname, email, bad_history, billingaddress, website, on_offline,  
billing_country, fg_latest_tier, num_failed_transactions, num_dispute_transactions, total_txn, total_vol, one_m_volume, six_m_volume, txn_3m, one_to_six, 
num_months_with_vol, num_years_with_vol, num_years_with_txn, vol_last_month, num_txn_last_month,
num_disputes_last_month, last_month_failure_rate, first_volume_year, avg_yearly_vol, avg_monthly_vol, avg_monthly_vol_no_extreme, avg_monthly_txn, avg_monthly_disputes,
avg_monthly_failures, current_failure_rate, six_m_consistency, three_to_six_trend_vol, 
three_to_six_trend_txn, dispute_rate,
volume_decline, vol_ratios, 
current_to_yearly_vol, consistency_subscore, vol_trend_subscore, vol_ratios_subscore, 
current_to_yearly_vol_subscore, txn_trend_subscore, frequency_subscore, success_rate_subscore, operational_subscore, enrichment_subscore, total_subscores,
weighted_consistency_subscore, weighted_frequency_subscore, weighted_success_rate_subscore, weighted_vol_trend_subscore, 
weighted_combined_vol_subscre, weighted_combined_vol_subscore2, total_weighted_subscore2,
buyer_risk_tier, recommended_cl, pre_approved,
aggressive_cl, moderate_cl, conservative_cl, pre_approved_credit_limit

from lastone

);



create or replace table BALANCE_V2.DBO.buyer_utilization_rates as
(

with dates as
(
    select '2022-01-01'::date+x as date
    from 
    (
        select row_number() over(order by 0) x 
        from table(generator(rowcount => 1460))
    )),

loans as (
    select to_date(c.created_at) as charge_date, 
    c.merchant_id,
    c.merchant_name,
    c.buyer_id,
    c.charge_source,
    c.charge_payment_method_type,
    c.transaction_is_financed,
    c.charge_id,
    c.paid_date,
    c.due_date,
    c.TRANSACTION_TERMS_NET_DAYS, 
    c.charge_amount_usd as tpv,
    c.charge_revenue as revenue,
    ifnull(c.stripe_fee_usd,0) as cost
    from balance_v2.stg.dim_charges as c 
    join balance_v2.dbo.core_payments as cp on c.charge_public_id = cp.charge_id
    where 1=1
    and cp.type = 'PAY_ON_TERMS'
    and cp.deleted_at is null
    ),

allocated as (
    select d.date, buyer_id, merchant_id, 
    sum(case when paid_date is null and datediff('day',due_date, date) > 120 then 0 else tpv end) as outstanding_usd
    from dates as d
    left join loans as l
    on case when paid_date is null and d.date>=charge_date then 1 
    when paid_date is not null and d.date>=charge_date and d.date<paid_date then 1 else 0 end = 1
    group by 1,2,3
    ),
granted as
    (
    select date(balance_date) as credit_date, entity_id, sum(credit_limit/100) as granted_credit
    from balance_v2.stg.dim_buyer_credit_limit as c
    
    group by 1,2
    ),

data as
    (
    select g.credit_date, g.entity_id, sum(ifnull(outstanding_usd,0)) as outstanding_usd,
    sum(granted_credit) as granted_credit
    from granted as g
    join balance_v2.dbo.buyer b on g.entity_id = b.public_id
    left join allocated as a on g.credit_date = a.date and b.id = a.buyer_id
    group by 1,2
    )

select d.entity_id as buyer_id, credit_date, outstanding_usd, granted_credit, 
case when granted_credit = 0 then 0
else outstanding_usd/granted_credit end as utilization_rate
from data d
where credit_date >= '2023-01-01'

);
