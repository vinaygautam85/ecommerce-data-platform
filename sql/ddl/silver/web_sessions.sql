CREATE OR REPLACE TABLE SILVER.WEB_SESSIONS AS
SELECT
    SESSION_ID::NUMBER                                           AS SESSION_ID,
    CUSTOMER_ID::NUMBER                                          AS CUSTOMER_ID,
    SESSION_START::TIMESTAMP_NTZ                                 AS SESSION_START,
    SESSION_END::TIMESTAMP_NTZ                                   AS SESSION_END,
    DATEDIFF(SECOND, SESSION_START, SESSION_END)                 AS SESSION_DURATION_SEC,
    LOWER(TRIM(SOURCE))                                          AS SOURCE,
    LOWER(TRIM(MEDIUM))                                          AS MEDIUM,
    CAMPAIGN_ID::NUMBER                                          AS CAMPAIGN_ID,
    INITCAP(TRIM(DEVICE))                                        AS DEVICE,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                                          AS _SILVER_RUN_ID
FROM BRONZE.WEB_SESSIONS
WHERE SESSION_END IS NULL OR SESSION_END >= SESSION_START
QUALIFY ROW_NUMBER() OVER (PARTITION BY SESSION_ID ORDER BY _LOADED_AT DESC) = 1;
