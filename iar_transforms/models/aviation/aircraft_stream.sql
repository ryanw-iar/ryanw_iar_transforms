{{ config(
    materialized="incremental"
) }}

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