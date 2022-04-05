{{ config(
    materialized="incremental"
) }}

SELECT *
FROM {{ref('cirium_brought_into_service')}}
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE CAST(In_Service_Date AS TIMESTAMP) > (SELECT MAX(TS) FROM {{ref('cirium_brought_into_service')}} WHERE ACTIVITY_TYPE = 'brought_into_service')
{% endif %}
UNION ALL
SELECT *
FROM {{ref('cirium_delivered_to_operator')}}
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE CAST(Operator_Delivery_Date AS TIMESTAMP) > (SELECT MAX(TS) FROM {{ref('cirium_delivered_to_operator')}} WHERE ACTIVITY_TYPE = 'delivered_to_operator')
{% endif %}