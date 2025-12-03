
--STAGING TABLE (loaded from GCS)

CREATE TABLE IF NOT EXISTS `nyc-electric-project.nyc_electric_dw.electric_staging` (
  BillDate_Std DATE,
  `Service End Date` DATE,
  Borough STRING,
  `Development Name` STRING,
  `TDS #` STRING,
  `Consumption _KWH_` FLOAT64,
  `Current Charges` FLOAT64

);

 --DIMENSION TABLES


CREATE TABLE IF NOT EXISTS `nyc-electric-project.nyc_electric_dw.dim_date` (
  bill_date DATE,
  bill_year INT64,
  bill_quarter INT64,
  bill_month INT64,
  bill_day INT64
);

INSERT INTO `nyc-electric-project.nyc_electric_dw.dim_date` (
  bill_date,
  bill_year,
  bill_quarter,
  bill_month,
  bill_day
)
SELECT DISTINCT
  s.BillDate_Std AS bill_date,
  EXTRACT(YEAR    FROM s.BillDate_Std) AS bill_year,
  EXTRACT(QUARTER FROM s.BillDate_Std) AS bill_quarter,
  EXTRACT(MONTH   FROM s.BillDate_Std) AS bill_month,
  EXTRACT(DAY     FROM s.BillDate_Std) AS bill_day
FROM `nyc-electric-project.nyc_electric_dw.electric_staging` AS s
WHERE s.BillDate_Std IS NOT NULL;

CREATE TABLE IF NOT EXISTS `nyc-electric-project.nyc_electric_dw.dim_development` (
  development_name STRING,
  borough STRING,
  tds_number STRING
);

INSERT INTO `nyc-electric-project.nyc_electric_dw.dim_development` (
  development_name,
  borough,
  tds_number
)
SELECT DISTINCT
  s.`Development Name` AS development_name,
  s.Borough            AS borough,
  s.`TDS #`            AS tds_number
FROM `nyc-electric-project.nyc_electric_dw.electric_staging` AS s
WHERE s.`Development Name` IS NOT NULL;



-- FACT TABLES

CREATE TABLE IF NOT EXISTS `nyc-electric-project.nyc_electric_dw.fact_borough_year_usage` (
  bill_year INT64,
  borough STRING,
  total_kwh NUMERIC,
  total_current_charges NUMERIC
);

INSERT INTO `nyc-electric-project.nyc_electric_dw.fact_borough_year_usage` (
  bill_year,
  borough,
  total_kwh,
  total_current_charges
)
SELECT
  CAST(d.bill_year AS INT64) AS bill_year,
  dev.borough                 AS borough,
  CAST(SUM(s.`Consumption _KWH_`) AS NUMERIC) AS total_kwh,
  CAST(SUM(s.`Current Charges`)   AS NUMERIC) AS total_current_charges
FROM `nyc-electric-project.nyc_electric_dw.electric_staging` AS s
LEFT JOIN `nyc-electric-project.nyc_electric_dw.dim_date` AS d
  ON d.bill_date = s.BillDate_Std
LEFT JOIN `nyc-electric-project.nyc_electric_dw.dim_development` AS dev
  ON dev.development_name = s.`Development Name`
 AND dev.borough          = s.Borough
 AND dev.tds_number       = s.`TDS #`
WHERE CAST(d.bill_year AS INT64) >= 2020
GROUP BY
  bill_year,
  dev.borough
ORDER BY
  bill_year,
  dev.borough;
