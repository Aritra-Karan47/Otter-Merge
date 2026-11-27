import os

import psycopg
import pytest

POSTGRES_URL = os.environ.get("CI_POSTGRES_URL", "")


@pytest.mark.integration
@pytest.mark.skipif(not POSTGRES_URL, reason="CI_POSTGRES_URL not configured")
def test_schema_idempotency_and_legacy_migration():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "schema.sql"), "r", encoding="utf-8") as f:
        ddl = f.read()

    with psycopg.connect(POSTGRES_URL) as conn:
        conn.execute("DROP TABLE IF EXISTS uploaded_files, merged_files CASCADE")

        # 1. Fresh install + repeat execution (idempotency)
        conn.execute(ddl)
        conn.execute(ddl)
        cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'uploaded_files'"
            ).fetchall()
        }
        assert "s3_key" in cols
        assert "pdf_data" not in cols

        # 2. Legacy schema migration (pdf_data BYTEA -> s3_key TEXT NOT NULL)
        conn.execute("DROP TABLE IF EXISTS uploaded_files, merged_files CASCADE")
        conn.execute(
            """
            CREATE TABLE uploaded_files (
                id UUID PRIMARY KEY,
                filename TEXT NOT NULL,
                file_size INT NOT NULL,
                page_count INT NOT NULL,
                pdf_data BYTEA NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE merged_files (
                id UUID PRIMARY KEY,
                source_hash TEXT UNIQUE NOT NULL,
                filename TEXT NOT NULL,
                file_size INT NOT NULL,
                page_count INT NOT NULL,
                pdf_data BYTEA NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL
            );
            INSERT INTO uploaded_files (id, filename, file_size, page_count, pdf_data, expires_at)
            VALUES (gen_random_uuid(), 'legacy.pdf', 4, 1, '\\x25504446'::bytea, NOW() + interval '1 hour');
            """
        )
        conn.execute(ddl)
        for tbl in ("uploaded_files", "merged_files"):
            info = dict(
                conn.execute(
                    "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = %s",
                    (tbl,),
                ).fetchall()
            )
            assert "pdf_data" not in info
            assert info.get("s3_key") == "NO"
