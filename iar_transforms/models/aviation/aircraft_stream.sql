{{ config(
    materialized="incremental"
) }}

WITH COMBINED_DATA AS (

    -- incremental load from the cirium_brought_into_service view
    SELECT *
    FROM {{ref('cirium_brought_into_service')}}
    -- code within this block is only executed on an incremental run
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE CAST(TS AS TIMESTAMP) > (SELECT MAX(TS) FROM {{ this }} WHERE ACTIVITY = 'brought_into_service')
    {% endif %}

    UNION ALL

    -- incremental load from the cirium_delivered_to_operator view
    SELECT *
    FROM {{ref('cirium_delivered_to_operator')}}
    -- code within this block is only executed on an incremental run
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE CAST(TS AS TIMESTAMP) > (SELECT MAX(TS) FROM {{ this }} WHERE ACTIVITY = 'delivered_to_operator')
    {% endif %}

    UNION ALL

    -- incremental load from the cirium_delivered_to_operator view
    SELECT *
    FROM {{ref('sdt_active_status')}}
    -- code within this block is only executed on an incremental run
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE CAST(TS AS TIMESTAMP) > (SELECT MAX(TS) FROM {{ this }} WHERE ACTIVITY = 'sdt_active_status')
    {% endif %}

    UNION ALL

    -- incremental load from the cirium_delivered_to_operator view
    SELECT *
    FROM {{ref('sdt_terminated_status')}}
    -- code within this block is only executed on an incremental run
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE CAST(TS AS TIMESTAMP) > (SELECT MAX(TS) FROM {{ this }} WHERE ACTIVITY = 'sdt_terminated_status')
    {% endif %}

    UNION ALL

    -- incremental load from the cirium_delivered_to_operator view
    SELECT *
    FROM {{ref('sdt_in_progress_status')}}
    -- code within this block is only executed on an incremental run
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE CAST(TS AS TIMESTAMP) > (SELECT MAX(TS) FROM {{ this }} WHERE ACTIVITY = 'sdt_in_progress_status')
    {% endif %}

    UNION ALL

    -- incremental load from the cirium_delivered_to_operator view
    SELECT *
    FROM {{ref('sdt_suspended_status')}}
    -- code within this block is only executed on an incremental run
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE CAST(TS AS TIMESTAMP) > (SELECT MAX(TS) FROM {{ this }} WHERE ACTIVITY = 'sdt_suspended_status')
    {% endif %}

)
SELECT 
ACTIVITY_ID,
TS,
CUSTOMER,
ACTIVITY,
ANONYMOUS_CUSTOMER_ID,
FEATURE_1,
FEATURE_2,
FEATURE_3,
REVENUE_IMPACT,
LINK,
ROW_NUMBER() OVER(PARTITION BY CUSTOMER, ACTIVITY ORDER BY TS) ACTIVITY_OCCURENCE,
LEAD(TS,1) OVER(PARTITION BY CUSTOMER, ACTIVITY ORDER BY TS)  ACTIVITY_REPEATED_AT,
_ACTIVITY_SOURCE,
CAST(CURRENT_DATETIME() AS TIMESTAMP) AS _INSERT_DATE
FROM COMBINED_DATA
ORDER BY CUSTOMER, TS