-- Activer l'extension dblink dans la base de données
CREATE EXTENSION IF NOT EXISTS dblink;

-- Se connecter à la base de données distante data-warehouse
SELECT dblink_connect('dw_link', 'host=data-warehouse port=5432 dbname=datawarehouse user=postgres password=admin');

-- Insérer les données dans dim_time
INSERT INTO dim_time (pickup_datetime, dropoff_datetime)
SELECT DISTINCT "tpep_pickup_datetime", "tpep_dropoff_datetime"
FROM dblink('dw_link', 'SELECT "tpep_pickup_datetime", "tpep_dropoff_datetime" FROM yellow_tripdata')
         AS t("tpep_pickup_datetime" TIMESTAMP, "tpep_dropoff_datetime" TIMESTAMP);

-- Insérer les données dans dim_passenger
INSERT INTO dim_passenger (passenger_count)
SELECT DISTINCT "passenger_count"
FROM dblink('dw_link', 'SELECT "passenger_count" FROM yellow_tripdata')
         AS t("passenger_count" BIGINT);

-- Insérer les données dans dim_rate_code
INSERT INTO dim_rate_code (rate_code)
SELECT DISTINCT "RatecodeID"
FROM dblink('dw_link', 'SELECT "RatecodeID" FROM yellow_tripdata')
         AS t("RatecodeID" BIGINT);

-- Insérer les données dans dim_store_and_fwd_flag
INSERT INTO dim_store_and_fwd_flag (flag)
SELECT DISTINCT "store_and_fwd_flag"
FROM dblink('dw_link', 'SELECT "store_and_fwd_flag" FROM yellow_tripdata')
         AS t("store_and_fwd_flag" TEXT);

-- Insérer les données dans dim_location
INSERT INTO dim_location (pu_location_id, do_location_id)
SELECT DISTINCT "PULocationID", "DOLocationID"
FROM dblink('dw_link', 'SELECT "PULocationID", "DOLocationID" FROM yellow_tripdata')
         AS t("PULocationID" INT, "DOLocationID" INT);

-- Insérer les données dans dim_payment_type
INSERT INTO dim_payment_type (payment_type)
SELECT DISTINCT "payment_type"
FROM dblink('dw_link', 'SELECT "payment_type" FROM yellow_tripdata')
         AS t("payment_type" BIGINT);

-- Insérer les données dans dim_vendor
INSERT INTO dim_vendor (vendor_id)
SELECT DISTINCT "VendorID"
FROM dblink('dw_link', 'SELECT "VendorID" FROM yellow_tripdata')
         AS t("VendorID" INT);

-- Insérer les données dans la table fact_trip (table de faits)
INSERT INTO fact_trip (
    vendor_id, time_id, passenger_id, rate_code_id, store_forward_id, location_id, payment_type_id,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
)
SELECT
    dv.id, dt.id, dp.id, dr.id, ds.id, dl.id, dpay.id,
    yt.fare_amount, yt.extra, yt.mta_tax, yt.tip_amount, yt.tolls_amount,
    yt.improvement_surcharge, yt.total_amount, yt.congestion_surcharge, yt."Airport_fee"  -- Correction de la casse ici
FROM dblink('dw_link', 'SELECT "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee" FROM yellow_tripdata')
         AS yt("VendorID" INT, "tpep_pickup_datetime" TIMESTAMP, "tpep_dropoff_datetime" TIMESTAMP, "passenger_count" BIGINT, "RatecodeID" BIGINT, "store_and_fwd_flag" TEXT, "PULocationID" INT, "DOLocationID" INT, "payment_type" BIGINT, "fare_amount" DOUBLE PRECISION, "extra" DOUBLE PRECISION, "mta_tax" DOUBLE PRECISION, "tip_amount" DOUBLE PRECISION, "tolls_amount" DOUBLE PRECISION, "improvement_surcharge" DOUBLE PRECISION, "total_amount" DOUBLE PRECISION, "congestion_surcharge" DOUBLE PRECISION, "Airport_fee" DOUBLE PRECISION)
         JOIN dim_vendor dv ON yt."VendorID" = dv.vendor_id
         JOIN dim_time dt ON DATE_TRUNC('minute', yt."tpep_pickup_datetime") = DATE_TRUNC('minute', dt.pickup_datetime)
         JOIN dim_passenger dp ON yt."passenger_count" = dp.passenger_count
         JOIN dim_rate_code dr ON yt."RatecodeID" = dr.rate_code
         JOIN dim_store_and_fwd_flag ds ON yt."store_and_fwd_flag" = ds.flag
         JOIN dim_location dl ON yt."PULocationID" = dl.pu_location_id AND yt."DOLocationID" = dl.do_location_id
         JOIN dim_payment_type dpay ON yt."payment_type" = dpay.payment_type;


-- Fermer la connexion dblink
SELECT dblink_disconnect('dw_link');
