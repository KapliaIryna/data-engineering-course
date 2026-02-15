-- Redshift Setup: зовнішня схема silver (S3/Glue) + внутрішня gold
-- {IAM_ROLE_ARN} -> arn:aws:iam::730335603706:role/iryna-data-platform-redshift-service-role

CREATE EXTERNAL SCHEMA IF NOT EXISTS silver
FROM
    DATA CATALOG DATABASE 'iryna_data_platform_database' IAM_ROLE '{IAM_ROLE_ARN}' CREATE EXTERNAL DATABASE IF NOT EXISTS;

CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.user_profiles_enriched;

CREATE TABLE gold.user_profiles_enriched (
    client_id INTEGER NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) NOT NULL,
    registration_date DATE,
    state VARCHAR(50),
    birth_date DATE,
    phone_number VARCHAR(50),
    created_at TIMESTAMP DEFAULT GETDATE (),
    updated_at TIMESTAMP DEFAULT GETDATE (),
    PRIMARY KEY (client_id)
) DISTSTYLE KEY DISTKEY (client_id) SORTKEY (client_id);