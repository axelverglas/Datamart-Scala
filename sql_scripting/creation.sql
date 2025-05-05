-- Dimension du temps
CREATE TABLE dim_time (
                          id SERIAL PRIMARY KEY,
                          pickup_datetime TIMESTAMP,
                          dropoff_datetime TIMESTAMP
);

-- Dimension des passagers
CREATE TABLE dim_passenger (
                               id SERIAL PRIMARY KEY,
                               passenger_count INT
);

-- Dimension des codes tarifaires
CREATE TABLE dim_rate_code (
                               id SERIAL PRIMARY KEY,
                               rate_code INT
);

-- Dimension de l'indicateur store_and_fwd
CREATE TABLE dim_store_and_fwd_flag (
                                        id SERIAL PRIMARY KEY,
                                        flag CHAR(1)
);

-- Dimension des localisations
CREATE TABLE dim_location (
                              id SERIAL PRIMARY KEY,
                              pu_location_id INT,
                              do_location_id INT
);

-- Dimension des types de paiement
CREATE TABLE dim_payment_type (
                                  id SERIAL PRIMARY KEY,
                                  payment_type INT
);

-- Dimension des fournisseurs
CREATE TABLE dim_vendor (
                            id SERIAL PRIMARY KEY,
                            vendor_id INT
);

CREATE TABLE fact_trip (
                           id SERIAL PRIMARY KEY,
                           vendor_id INT,
                           time_id INT,
                           passenger_id INT,
                           rate_code_id INT,
                           store_forward_id INT,
                           location_id INT,
                           payment_type_id INT,
                           fare_amount DOUBLE PRECISION,
                           extra DOUBLE PRECISION,
                           mta_tax DOUBLE PRECISION,
                           tip_amount DOUBLE PRECISION,
                           tolls_amount DOUBLE PRECISION,
                           improvement_surcharge DOUBLE PRECISION,
                           total_amount DOUBLE PRECISION,
                           congestion_surcharge DOUBLE PRECISION,
                           airport_fee DOUBLE PRECISION
);
