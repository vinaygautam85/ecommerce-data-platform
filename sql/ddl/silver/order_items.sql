CREATE OR REPLACE TABLE SILVER.ORDER_ITEMS AS
SELECT
    ORDER_ITEM_ID::NUMBER                                       AS ORDER_ITEM_ID,
    ORDER_ID::NUMBER                                            AS ORDER_ID,
    PRODUCT_ID::NUMBER                                          AS PRODUCT_ID,
    GREATEST(QUANTITY, 0)::NUMBER                               AS QUANTITY,
    UNIT_PRICE::NUMBER(10,2)                                    AS UNIT_PRICE,
    COALESCE(DISCOUNT, 0)::NUMBER(10,2)                         AS DISCOUNT,
    COALESCE(TAX, 0)::NUMBER(10,2)                              AS TAX,
    ((QUANTITY * UNIT_PRICE) - COALESCE(DISCOUNT, 0)
       + COALESCE(TAX, 0))::NUMBER(12,2)                        AS LINE_TOTAL,
    _LOADED_AT, _INGESTION_ID, _SOURCE_SYSTEM,
    CURRENT_TIMESTAMP()                                         AS _PROCESSED_AT,
    {{ SILVER_RUN_ID }}                                         AS _SILVER_RUN_ID
FROM BRONZE.ORDER_ITEMS
WHERE QUANTITY > 0 AND UNIT_PRICE >= 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ITEM_ID ORDER BY _LOADED_AT DESC) = 1;
