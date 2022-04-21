{{ config(
    materialized="view"
) }}


WITH AIRCRAFT_TAIL_RANKING AS (
	SELECT *,
	RANK() OVER (PARTITION BY Registration_Clean ORDER BY In_Service_Date DESC) AS rn --used to create a unique value against rows with the same tail_number
	FROM `inmarsat-datalake-prod.aviation_sandbox_eu.cirium_aircraft_database`
    WHERE Operator_Delivery_Date IS NOT NULL --removing aircraft that have a no in_service_date as they're not needed for this activity
)
SELECT 
GENERATE_UUID() AS ACTIVITY_ID,
CAST(Operator_Delivery_Date AS TIMESTAMP) TS,
Registration_Clean AS CUSTOMER,
'delivered_to_operator' AS ACTIVITY,
'cirium_' || serial_number || '_' || type ANONYMOUS_CUSTOMER_ID,
NULL FEATURE_1,
NULL FEATURE_2,
NULL FEATURE_3,
NULL REVENUE_IMPACT,
NULL LINK,
CAST(NULL AS INT) ACTIVITY_OCCURENCE,
CAST(NULL AS TIMESTAMP) ACTIVITY_REPEATED_AT,
'cirium_delivered_to_operator' _ACTIVITY_SOURCE,
CAST(CURRENT_DATETIME() AS TIMESTAMP) AS _INSERT_DATE
FROM AIRCRAFT_TAIL_RANKING 
WHERE rn = 1 --only bring back the tail number most recently brought into service
AND status != 'Cancelled' --removing any cancelled aircraft