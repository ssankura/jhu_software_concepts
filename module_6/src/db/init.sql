-- Schema for Module 6 (derived from your Module 5 load_data.py)

CREATE TABLE IF NOT EXISTS applicants (
    p_id SERIAL PRIMARY KEY,
    program TEXT,
    comments TEXT,
    date_added DATE,
    url TEXT,
    status TEXT,
    term TEXT,
    us_or_international TEXT,
    gpa DOUBLE PRECISION,
    gre DOUBLE PRECISION,
    gre_v DOUBLE PRECISION,
    gre_aw DOUBLE PRECISION,
    degree TEXT,
    llm_generated_program TEXT,
    llm_generated_university TEXT
);

-- Idempotency for inserts (matches your loader's ON CONFLICT(url) DO NOTHING)
CREATE UNIQUE INDEX IF NOT EXISTS applicants_url_uniq ON applicants(url);

-- Watermark table required by Module 6 worker idempotence
CREATE TABLE IF NOT EXISTS ingestion_watermarks (
    source TEXT PRIMARY KEY,
    last_seen TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);
