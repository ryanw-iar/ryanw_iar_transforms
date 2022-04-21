{{ config(
    materialized="incremental"
) }}

WITH NEW_DATA AS (

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

),

COMBINED_DATA AS (

    SELECT *
    FROM NEW_DATA

    {% if is_incremental() %}

        -- On an incremental run, combine existing data in the model with new data
        UNION ALL
        SELECT * FROM {{ this }}

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