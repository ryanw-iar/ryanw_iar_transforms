{{ config(
    materialized="view"
) }}

WITH AIRCRAFT_TAIL_RANKING AS (
	SELECT *,
	RANK() OVER (PARTITION BY Registration_Clean ORDER BY In_Service_Date DESC) AS rn --used to create a unique value against rows with the same tail_number
	FROM `inmarsat-datalake-prod.aviation_sandbox_eu.cirium_aircraft_database`
    WHERE In_Service_Date IS NOT NULL --removing aircraft that have a no in_service_date as they're not needed for this activity
)
SELECT 
GENERATE_UUID() AS ACTIVITY_ID,
CAST(In_Service_Date AS TIMESTAMP) TS,
Registration_Clean AS CUSTOMER,
'brought_into_service' AS ACTIVITY,
'cirium_' || serial_number || '_' || type ANONYMOUS_CUSTOMER_ID,
NULL FEATURE_1,
NULL FEATURE_2,
NULL FEATURE_3,
NULL REVENUE_IMPACT,
NULL LINK,
NULL ACTIVITY_OCCURENCE,
NULL ACTIVITY_REPEATED_AT,
'cirium_brought_into_service' _ACTIVITY_SOURCE
FROM AIRCRAFT_TAIL_RANKING 
WHERE rn = 1 --only bring back the tail number most recently brought into service
AND status != 'Cancelled' --removing any cancelled aircraft