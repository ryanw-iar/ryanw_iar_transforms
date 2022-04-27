{{ config(
    materialized="view"
) }}

SELECT 
GENERATE_UUID() AS ACTIVITY_ID,
CAST(effectiveStartDate AS TIMESTAMP) TS,
tailNumber AS CUSTOMER,
'i5_active_status' AS ACTIVITY,
'sdt_' || productOfferingInstanceId || '_' || sigmaOrderNumber ANONYMOUS_CUSTOMER_ID, --not sure if this is unique
NULL FEATURE_1,
NULL FEATURE_2,
NULL FEATURE_3,
NULL REVENUE_IMPACT,
NULL LINK,
CAST(NULL AS INT) ACTIVITY_OCCURENCE,
CAST(NULL AS TIMESTAMP) ACTIVITY_REPEATED_AT,
'sdt_active_status' _ACTIVITY_SOURCE,
CAST(CURRENT_DATETIME() AS TIMESTAMP) AS _INSERT_DATE
FROM `inmarsat-datalake-prod.sdt_dwh.rp_factCibSubscriptionsHistory`
WHERE status = 'Active' --only active status lines
and tailNumber is not null --only return rows that have a tail number, not sure if all rows are populated with tail number for abu
