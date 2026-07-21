CREATE OR REPLACE TABLE SILVER.CUSTOMERS AS
SELECT
    CUSTOMER_ID::NUMBER                                          AS CUSTOMER_ID,
    INITCAP(TRIM(FIRST_NAME))                                    AS FIRST_NAME,
    INITCAP(TRIM(LAST_NAME))                                     AS LAST_NAME,
    LOWER(TRIM(EMAIL))                                           AS EMAIL,
    SIGNUP_DATE::DATE                                            AS SIGNUP_DATE,
    INITCAP(TRIM(COUNTRY))                                       AS COUNTRY,
    INITCAP(TRIM(STATE))                                         AS STATE,
    INITCAP(TRIM(CITY))                                          AS CITY,
    CASE WHEN AGE BETWEEN 13 AND 120 THEN AGE END                AS AGE,
    UPPER(TRIM(GENDER))                                          AS GENDER,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                                          AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                                          AS _SILVER_RUN_ID
FROM BRONZE.CUSTOMERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY _LOADED_AT DESC) = 1;
