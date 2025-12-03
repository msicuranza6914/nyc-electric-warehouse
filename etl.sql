-- Create dim_date
CREATE OR REPLACE TABLE `nyc-electric-project.nyc_electric_dw.dim_date` AS
SELECT DISTINCT
  CAST(FORMAT_DATE('%Y%m%d', BillDate_Std) AS INT64) AS date_key,  -- 20250115
  BillDate_Std AS bill_date,
  Bill_Year,
  Bill_Quarter,
  Bill_Month,
  Bill_Day
FROM `nyc-electric-project.nyc_electric_dw.electric_staging`
WHERE BillDate_Std IS NOT NULL;
-- Check the DimDate table
SELECT *
FROM `nyc-electric-project.nyc_electric_dw.dim_date`
ORDER BY bill_date
LIMIT 20;

-- Create dim_development
CREATE OR REPLACE TABLE `nyc-electric-project.nyc_electric_dw.dim_development` AS
SELECT DISTINCT
  ROW_NUMBER() OVER (
    ORDER BY `Development Name`, Borough, `TDS #`
  ) AS development_key,  -- surrogate key

  `Development Name` AS development_name,
  Borough,
  `TDS #` AS tds_number,
  `AMP #` AS amp_number,
  `Funding Source` AS funding_source,
  `RC Code` AS rc_code

FROM `nyc-electric-project.nyc_electric_dw.electric_staging`
WHERE `Development Name` IS NOT NULL;

-- Check the dim_developmet table
SELECT *
FROM `nyc-electric-project.nyc_electric_dw.dim_development`
ORDER BY development_key
LIMIT 20;

-- Create dim_meter
CREATE OR REPLACE TABLE `nyc-electric-project.nyc_electric_dw.dim_meter` AS
SELECT DISTINCT
  ROW_NUMBER() OVER (
    ORDER BY `Meter Number`, `Meter AMR`, `Meter Scope`, Location
  ) AS meter_key,   

  `Meter Number` AS meter_number,
  `Meter AMR` AS meter_amr_type,
  `Meter Scope` AS meter_scope,
  Location

FROM `nyc-electric-project.nyc_electric_dw.electric_staging`
WHERE `Meter Number` IS NOT NULL;

-- Check dim_meter table
SELECT *
FROM `nyc-electric-project.nyc_electric_dw.dim_meter`
ORDER BY meter_key
LIMIT 20;

-- Create dim_vendor
CREATE OR REPLACE TABLE `nyc-electric-project.nyc_electric_dw.dim_vendor` AS
SELECT DISTINCT
  ROW_NUMBER() OVER (
    ORDER BY `Vendor Name`
  ) AS vendor_key,   -- surrogate key

  `Vendor Name` AS vendor_name

FROM `nyc-electric-project.nyc_electric_dw.electric_staging`
WHERE `Vendor Name` IS NOT NULL;

-- Check dim_vendor
SELECT *
FROM `nyc-electric-project.nyc_electric_dw.dim_vendor`
ORDER BY vendor_key
LIMIT 20;

-- Creat fact_borough_year_usage
CREATE OR REPLACE TABLE `nyc-electric-project.nyc_electric_dw.fact_borough_year_usage` AS
WITH joined AS (
  SELECT
    d.Bill_Year AS bill_year,
    dev.Borough AS borough,
    s.`Consumption _KWH_` AS kwh,
    s.`Current Charges` AS current_charges
  FROM `nyc-electric-project.nyc_electric_dw.electric_staging` AS s
  LEFT JOIN `nyc-electric-project.nyc_electric_dw.dim_date` AS d
    ON d.bill_date = s.BillDate_Std
  LEFT JOIN `nyc-electric-project.nyc_electric_dw.dim_development` AS dev
    ON dev.development_name = s.`Development Name`
   AND dev.Borough = s.Borough
   AND dev.tds_number = s.`TDS #`
  WHERE d.Bill_Year IS NOT NULL
)
SELECT
  bill_year,
  borough,
  SUM(kwh) AS total_kwh,
  SUM(current_charges) AS total_current_charges
FROM joined
GROUP BY bill_year, borough
ORDER BY bill_year, borough;

-- Create eletric_staging
CREATE OR REPLACE TABLE `nyc-electric-project.nyc_electric_dw.fact_borough_year_usage` AS
SELECT
  CAST(Bill_Year AS INT64)      AS bill_year,
  Borough                       AS borough,
  SUM(`Consumption _KWH_`)      AS total_kwh,
  SUM(`Current Charges`)        AS total_current_charges
FROM `nyc-electric-project.nyc_electric_dw.electric_staging`
WHERE Bill_Year IS NOT NULL
GROUP BY
  bill_year,
  borough
ORDER BY
  bill_year,
  borough;

