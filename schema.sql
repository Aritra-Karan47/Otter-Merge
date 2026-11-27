-- Idempotent: safe to run on every cold start.

CREATE TABLE IF NOT EXISTS uploaded_files (
    id          UUID PRIMARY KEY,
    filename    TEXT NOT NULL,
    file_size   INT  NOT NULL,
    page_count  INT  NOT NULL,
    s3_key      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL
);
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'uploaded_files' AND column_name = 's3_key'
    ) THEN
        ALTER TABLE uploaded_files ADD COLUMN s3_key TEXT;
        DELETE FROM uploaded_files WHERE s3_key IS NULL;
        ALTER TABLE uploaded_files ALTER COLUMN s3_key SET NOT NULL;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'uploaded_files' AND column_name = 'pdf_data'
    ) THEN
        ALTER TABLE uploaded_files DROP COLUMN pdf_data;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_uploaded_files_expires_at ON uploaded_files (expires_at);

CREATE TABLE IF NOT EXISTS merged_files (
    id           UUID PRIMARY KEY,
    source_hash  TEXT UNIQUE NOT NULL,
    filename     TEXT NOT NULL,
    file_size    INT  NOT NULL,
    page_count   INT  NOT NULL,
    s3_key       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL
);
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'merged_files' AND column_name = 's3_key'
    ) THEN
        ALTER TABLE merged_files ADD COLUMN s3_key TEXT;
        DELETE FROM merged_files WHERE s3_key IS NULL;
        ALTER TABLE merged_files ALTER COLUMN s3_key SET NOT NULL;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'merged_files' AND column_name = 'pdf_data'
    ) THEN
        ALTER TABLE merged_files DROP COLUMN pdf_data;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_merged_files_expires_at ON merged_files (expires_at);
