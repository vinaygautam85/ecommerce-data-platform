CREATE OR REPLACE TABLE GOLD.DIM_DATE AS
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2022-01-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 2557))
)
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))   AS DATE_KEY,
    d                                   AS FULL_DATE,
    YEAR(d)                             AS YEAR,
    QUARTER(d)                          AS QUARTER,
    MONTH(d)                            AS MONTH_NUM,
    MONTHNAME(d)::VARCHAR(20)           AS MONTH_NAME,
    DAY(d)                              AS DAY_OF_MONTH,
    DAYOFWEEKISO(d)                     AS DAY_OF_WEEK,
    DAYNAME(d)::VARCHAR(20)             AS DAY_NAME,
    WEEKOFYEAR(d)                       AS WEEK_OF_YEAR,
    (DAYOFWEEKISO(d) >= 6)              AS IS_WEEKEND,
    (d = LAST_DAY(d))                   AS IS_MONTH_END,
    TO_CHAR(d, 'YYYY-MM')::VARCHAR(20)  AS YEAR_MONTH
FROM date_spine
WHERE d <= '2028-12-31'::DATE;

INSERT INTO GOLD.DIM_DATE
(DATE_KEY, FULL_DATE, YEAR, QUARTER, MONTH_NUM, MONTH_NAME, DAY_OF_MONTH,
 DAY_OF_WEEK, DAY_NAME, WEEK_OF_YEAR, IS_WEEKEND, IS_MONTH_END, YEAR_MONTH)
VALUES (-1, NULL, NULL, NULL, NULL, 'Unknown', NULL, NULL, 'Unknown', NULL, FALSE, FALSE, 'Unknown');