-- Idempotent: safe to run on every cold start.

CREATE TABLE IF NOT EXISTS uploaded_files (
    id          UUID PRIMARY KEY,
    filename    TEXT NOT NULL,
    file_size   INT  NOT NULL,
    page_count  INT  NOT NULL,
    pdf_data    BYTEA NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_uploaded_files_expires_at ON uploaded_files (expires_at);

CREATE TABLE IF NOT EXISTS merged_files (
    id           UUID PRIMARY KEY,
    source_hash  TEXT UNIQUE NOT NULL,
    filename     TEXT NOT NULL,
    file_size    INT  NOT NULL,
    page_count   INT  NOT NULL,
    pdf_data     BYTEA NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_merged_files_expires_at ON merged_files (expires_at);
