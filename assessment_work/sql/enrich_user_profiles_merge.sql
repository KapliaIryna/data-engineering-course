-- MERGE: збагачення customers даними з user_profiles
-- COALESCE заповнює пропущені first_name, last_name, state
-- Додає birth_date, phone_number

MERGE INTO gold.user_profiles_enriched
USING (
    SELECT
        c.client_id,
        COALESCE(c.first_name, up.first_name) AS first_name,
        COALESCE(c.last_name, up.last_name) AS last_name,
        c.email,
        c.registration_date,
        COALESCE(c.state, up.state) AS state,
        up.birth_date,
        up.phone_number
    FROM silver.customers c
    LEFT JOIN silver.user_profiles up ON LOWER(c.email) = LOWER(up.email)
) AS source
ON gold.user_profiles_enriched.client_id = source.client_id
WHEN MATCHED THEN UPDATE SET
    first_name = source.first_name,
    last_name = source.last_name,
    email = source.email,
    registration_date = source.registration_date,
    state = source.state,
    birth_date = source.birth_date,
    phone_number = source.phone_number,
    updated_at = GETDATE()
WHEN NOT MATCHED THEN INSERT (
    client_id, first_name, last_name, email, registration_date,
    state, birth_date, phone_number, created_at, updated_at
) VALUES (
    source.client_id, source.first_name, source.last_name, source.email,
    source.registration_date, source.state, source.birth_date,
    source.phone_number, GETDATE(), GETDATE()
);

-- Перевірка
SELECT COUNT(*) AS total FROM gold.user_profiles_enriched;
