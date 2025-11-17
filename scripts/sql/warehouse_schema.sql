CREATE SCHEMA IF NOT EXISTS nyc_electric_dw;

SET search_path TO nyc_electric_dw;

-----------------------------------------------------------
-- Dimension: dim_date
-----------------------------------------------------------
CREATE TABLE nyc_electric_dw.dim_date (
    date_key            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    calendar_date       DATE,
    year                INTEGER,
    quarter             INTEGER,
    month               INTEGER,
    month_name          VARCHAR(15),
    day                 INTEGER,
    weekday             VARCHAR(10),
    is_weekend          BOOLEAN
);

-----------------------------------------------------------
-- Dimension: dim_time
-----------------------------------------------------------
CREATE TABLE nyc_electric_dw.dim_time (
    time_key        INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hour            INTEGER,
    minute          INTEGER,
    clock_time      VARCHAR(10)
);

-----------------------------------------------------------
-- Dimension: dim_location
-----------------------------------------------------------
CREATE TABLE nyc_electric_dw.dim_location (
    location_key    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    borough         VARCHAR(50),
    zip_code        VARCHAR(10)
);

-----------------------------------------------------------
-- Dimension: dim_meter_type
-----------------------------------------------------------
CREATE TABLE nyc_electric_dw.dim_meter_type (
    meter_type_key      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    meter_type_name     VARCHAR(50)
);

-----------------------------------------------------------
-- Dimension: dim_service_class
-----------------------------------------------------------
CREATE TABLE nyc_electric_dw.dim_service_class (
    service_key     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    service_class   VARCHAR(50),
    rate_code       VARCHAR(20),
    description     VARCHAR(200)
);

-----------------------------------------------------------
-- Dimension: dim_customer (optional)
-----------------------------------------------------------
CREATE TABLE nyc_electric_dw.dim_customer (
    customer_key    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id     VARCHAR(50),
    customer_type   VARCHAR(50),
    borough         VARCHAR(50),
    zip_code        VARCHAR(10),
    account_status  VARCHAR(20)
);

-----------------------------------------------------------
-- Fact: fact_electric_consumption
-----------------------------------------------------------
CREATE TABLE nyc_electric_dw.fact_electric_consumption (
    fact_key            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_key            INTEGER NOT NULL,
    time_key            INTEGER,
    location_key        INTEGER,
    meter_type_key      INTEGER,
    service_key         INTEGER,
    kwh_consumed        NUMERIC(18,2),
    total_cost_usd      NUMERIC(18,2),
    record_source       VARCHAR(50),
    ingestion_timestamp TIMESTAMP
);

-----------------------------------------------------------
-- Foreign Key Constraints
-----------------------------------------------------------
ALTER TABLE nyc_electric_dw.fact_electric_consumption
    ADD CONSTRAINT fk_fact_date
        FOREIGN KEY (date_key)
        REFERENCES nyc_electric_dw.dim_date (date_key),

    ADD CONSTRAINT fk_fact_time
        FOREIGN KEY (time_key)
        REFERENCES nyc_electric_dw.dim_time (time_key),

    ADD CONSTRAINT fk_fact_location
        FOREIGN KEY (location_key)
        REFERENCES nyc_electric_dw.dim_location (location_key),

    ADD CONSTRAINT fk_fact_meter_type
        FOREIGN KEY (meter_type_key)
        REFERENCES nyc_electric_dw.dim_meter_type (meter_type_key),

    ADD CONSTRAINT fk_fact_service_class
        FOREIGN KEY (service_key)
        REFERENCES nyc_electric_dw.dim_service_class (service_key);
