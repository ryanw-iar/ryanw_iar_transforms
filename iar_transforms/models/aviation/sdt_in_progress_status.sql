{{ config(
    materialized="view"
) }}

SELECT 
GENERATE_UUID() AS ACTIVITY_ID,
CAST(effectiveStartDate AS TIMESTAMP) TS,
tailNumber AS CUSTOMER,
'i5_status_change' AS ACTIVITY,
'sdt_' || productOfferingInstanceId || '_' || sigmaOrderNumber ANONYMOUS_CUSTOMER_ID, --not sure if this is unique
CAST('in_progress' AS STRING) FEATURE_1,
CAST(substatus AS STRING) FEATURE_2,
CAST(gxTerminalDid AS STRING) FEATURE_3,
NULL REVENUE_IMPACT,
NULL LINK,
CAST(NULL AS INT) ACTIVITY_OCCURENCE,
CAST(NULL AS TIMESTAMP) ACTIVITY_REPEATED_AT,
'sdt_in_progress_status' _ACTIVITY_SOURCE,
CAST(CURRENT_DATETIME() AS TIMESTAMP) AS _INSERT_DATE
FROM `inmarsat-datalake-prod.sdt_dwh.rp_factCibSubscriptionsHistory`
WHERE status = 'In Progress' --only active status lines
and tailNumber is not null --only return rows that have a tail number, not sure if all rows are populated with tail number for abu
