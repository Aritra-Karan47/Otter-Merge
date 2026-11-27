import io
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import redis
from botocore.exceptions import ClientError
from pypdf import PdfReader, PdfWriter

import app as otter_app
from tests.test_app import FakeConn, FakeDBState, FakeRedis, FakeS3, make_pdf_bytes


def make_sized_pdf(num_pages, width):
    writer = PdfWriter()
    for _ in range(num_pages):
        writer.add_blank_page(width=width, height=200)
    buf = io.BytesIO()
    writer.write(buf)
    writer.close()
    return buf.getvalue()


@pytest.fixture
def env(monkeypatch):
    fake_redis = FakeRedis()
    fake_s3 = FakeS3()
    fake_db = FakeDBState()

    monkeypatch.setattr(otter_app, "AWS_BUCKET", "test-bucket")
    monkeypatch.setattr(otter_app, "AWS_ENDPOINT_URL_S3", "https://s3.example.test")
    monkeypatch.setattr(otter_app, "AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setattr(otter_app, "AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setattr(otter_app, "CRON_SECRET", "cron-secret-123")
    monkeypatch.setattr(otter_app, "_schema_ready", True)
    monkeypatch.setattr(otter_app, "CLEANUP_PROBABILITY", 0.0)
    monkeypatch.setattr(otter_app, "cache", lambda: fake_redis)
    monkeypatch.setattr(otter_app, "s3", lambda: fake_s3)
    monkeypatch.setattr(otter_app, "db", lambda: FakeConn(fake_db))

    client = otter_app.app.test_client()
    return client, fake_redis, fake_s3, fake_db


def test_rate_limiting_429_and_fail_open(env, monkeypatch):
    client, fake_redis, _, _ = env
    pdf = make_pdf_bytes(1)

    limit, _ = otter_app.RATE_LIMITS["upload"]
    for i in range(limit):
        res = client.post(
            "/api/upload",
            data={"file": (io.BytesIO(pdf), f"f{i}.pdf", "application/pdf")},
            content_type="multipart/form-data",
        )
        assert res.status_code == 201

    blocked = client.post(
        "/api/upload",
        data={"file": (io.BytesIO(pdf), "over.pdf", "application/pdf")},
        content_type="multipart/form-data",
    )
    assert blocked.status_code == 429
    assert int(blocked.headers["Retry-After"]) >= 1

    # Fail-open when Redis pipeline raises RedisError
    def broken_pipeline():
        raise redis.RedisError("redis down")

    monkeypatch.setattr(fake_redis, "pipeline", broken_pipeline)
    ok_again = client.post(
        "/api/upload",
        data={"file": (io.BytesIO(pdf), "open.pdf", "application/pdf")},
        content_type="multipart/form-data",
    )
    assert ok_again.status_code == 201


def test_pdf_validation_edge_cases(env, monkeypatch):
    client, _, fake_s3, fake_db = env

    # Empty 0-byte file
    assert (
        client.post(
            "/api/upload",
            data={"file": (io.BytesIO(b""), "empty.pdf", "application/pdf")},
            content_type="multipart/form-data",
        ).status_code
        == 400
    )

    # Corrupt payload starting with %PDF-
    assert (
        client.post(
            "/api/upload",
            data={"file": (io.BytesIO(b"%PDF-1.4\ngarbage"), "corrupt.pdf", "application/pdf")},
            content_type="multipart/form-data",
        ).status_code
        == 400
    )

    # 0-page PDF
    empty_writer = PdfWriter()
    zero_buf = io.BytesIO()
    empty_writer.write(zero_buf)
    empty_writer.close()
    assert (
        client.post(
            "/api/upload",
            data={"file": (io.BytesIO(zero_buf.getvalue()), "zero.pdf", "application/pdf")},
            content_type="multipart/form-data",
        ).status_code
        == 400
    )

    # Encrypted PDF
    enc_writer = PdfWriter()
    enc_writer.add_blank_page(width=200, height=200)
    enc_writer.encrypt("secret")
    enc_buf = io.BytesIO()
    enc_writer.write(enc_buf)
    enc_writer.close()
    assert (
        client.post(
            "/api/upload",
            data={"file": (io.BytesIO(enc_buf.getvalue()), "locked.pdf", "application/pdf")},
            content_type="multipart/form-data",
        ).status_code
        == 400
    )

    # Oversized >4MB upload
    monkeypatch.setattr(otter_app, "MAX_UPLOAD_BYTES", 64)
    big_pdf = make_pdf_bytes(1)
    assert (
        client.post(
            "/api/upload",
            data={"file": (io.BytesIO(big_pdf), "big.pdf", "application/pdf")},
            content_type="multipart/form-data",
        ).status_code
        == 413
    )

    # Corrupt PDF during merge -> 422
    monkeypatch.setattr(otter_app, "MAX_UPLOAD_BYTES", 4 * 1024 * 1024)
    id1 = str(uuid.uuid4())
    id2 = str(uuid.uuid4())
    future = datetime.now(timezone.utc) + timedelta(minutes=30)
    fake_db.uploaded[id1] = {
        "id": id1,
        "filename": "a.pdf",
        "file_size": 100,
        "page_count": 1,
        "s3_key": f"uploads/{id1}.pdf",
        "expires_at": future,
    }
    fake_db.uploaded[id2] = {
        "id": id2,
        "filename": "b.pdf",
        "file_size": 100,
        "page_count": 1,
        "s3_key": f"uploads/{id2}.pdf",
        "expires_at": future,
    }
    fake_s3.objects[("test-bucket", f"uploads/{id1}.pdf")] = make_pdf_bytes(1)
    fake_s3.objects[("test-bucket", f"uploads/{id2}.pdf")] = b"%PDF-1.4\nbroken"
    assert client.post("/api/merge", json={"file_ids": [id1, id2]}).status_code == 422


def test_orphan_rollback_race_and_dedupe_fallback(env, monkeypatch):
    client, fake_redis, fake_s3, fake_db = env
    pdf1 = make_pdf_bytes(1)
    pdf2 = make_pdf_bytes(1)

    # 1. DB error during upload rolls back S3 object
    class FailingDBConn(FakeConn):
        def execute(self, sql, params=None):
            if "INSERT INTO uploaded_files" in sql:
                raise RuntimeError("db insert error")
            return super().execute(sql, params)

    monkeypatch.setattr(otter_app, "db", lambda: FailingDBConn(fake_db))
    fail_up = client.post("/api/upload", data={"file": (io.BytesIO(pdf1), "a.pdf")})
    assert fail_up.status_code == 500
    assert fake_s3.objects == {}

    # Restore normal DB and upload 2 valid files
    monkeypatch.setattr(otter_app, "db", lambda: FakeConn(fake_db))
    id1 = client.post("/api/upload", data={"file": (io.BytesIO(pdf1), "a.pdf")}).get_json()["file_id"]
    id2 = client.post("/api/upload", data={"file": (io.BytesIO(pdf2), "b.pdf")}).get_json()["file_id"]

    # 2. DB error during merge rolls back merged S3 object
    class FailingMergeDBConn(FakeConn):
        def execute(self, sql, params=None):
            if "INSERT INTO merged_files" in sql:
                raise RuntimeError("merge insert error")
            return super().execute(sql, params)

    monkeypatch.setattr(otter_app, "db", lambda: FailingMergeDBConn(fake_db))
    before_keys = set(fake_s3.objects.keys())
    assert client.post("/api/merge", json={"file_ids": [id1, id2]}).status_code == 500
    assert set(fake_s3.objects.keys()) == before_keys

    # 3. Concurrent race: ON CONFLICT DO NOTHING cleans loser S3 key and returns winner
    monkeypatch.setattr(otter_app, "db", lambda: FakeConn(fake_db))
    import hashlib

    source_hash = hashlib.sha256(f"{id1},{id2}".encode()).hexdigest()
    winner_id = str(uuid.uuid4())
    fake_db.merged[winner_id] = {
        "id": winner_id,
        "source_hash": source_hash,
        "filename": "winner.pdf",
        "file_size": 100,
        "page_count": 2,
        "s3_key": f"merged/{winner_id}.pdf",
        "expires_at": datetime.now(timezone.utc) - timedelta(seconds=1),
    }

    # Simulate winner already present at INSERT conflict time
    class RaceDBConn(FakeConn):
        def execute(self, sql, params=None):
            norm = " ".join(sql.split())
            if norm.startswith("SELECT id, page_count FROM merged_files WHERE source_hash=%s AND expires_at > NOW()"):
                return super().execute(sql, params)
            if norm.startswith("DELETE FROM merged_files WHERE source_hash=%s AND expires_at <= NOW()"):
                res = super().execute(sql, params)
                # Another worker inserted winner right after expired check
                fake_db.merged[winner_id] = {
                    "id": winner_id,
                    "source_hash": source_hash,
                    "filename": "winner.pdf",
                    "file_size": 100,
                    "page_count": 2,
                    "s3_key": f"merged/{winner_id}.pdf",
                    "expires_at": datetime.now(timezone.utc) + timedelta(minutes=30),
                }
                return res
            return super().execute(sql, params)

    monkeypatch.setattr(otter_app, "db", lambda: RaceDBConn(fake_db))
    race_res = client.post("/api/merge", json={"file_ids": [id1, id2]})
    assert race_res.status_code == 200
    assert fake_redis.get(f"dl:{race_res.get_json()['download_token']}") == winner_id
    # Only uploads + winner key (no orphan loser key)
    assert all(k[1] in {f"uploads/{id1}.pdf", f"uploads/{id2}.pdf"} for k in fake_s3.objects)

    # 4. Layerbase cache miss falls back to Postgres dedupe without re-merging
    monkeypatch.setattr(otter_app, "db", lambda: FakeConn(fake_db))
    fake_redis.delete(f"merge:{source_hash}")
    dedupe_res = client.post("/api/merge", json={"file_ids": [id1, id2]})
    assert dedupe_res.status_code == 200
    assert dedupe_res.get_json()["cached"] is False
    tok1 = dedupe_res.get_json()["download_token"]
    assert fake_redis.get(f"dl:{tok1}") == winner_id

    # 5. Cache hit with expired dl:<token> mints a fresh token
    fake_redis.delete(f"dl:{tok1}")
    refresh_res = client.post("/api/merge", json={"file_ids": [id1, id2]})
    assert refresh_res.status_code == 200
    assert refresh_res.get_json()["cached"] is True
    tok2 = refresh_res.get_json()["download_token"]
    assert tok2 != tok1
    assert json.loads(fake_redis.get(f"merge:{source_hash}"))["token"] == tok2


def test_s3_helpers_and_503_unconfigured(env, monkeypatch):
    _, _, fake_s3, _ = env

    # s3_delete_keys batches >1000 keys and deduplicates
    calls = []
    orig_delete = fake_s3.delete_objects

    def spy_delete(Bucket, Delete):
        calls.append(len(Delete["Objects"]))
        return orig_delete(Bucket=Bucket, Delete=Delete)

    monkeypatch.setattr(fake_s3, "delete_objects", spy_delete)
    keys = [f"k/{i}.pdf" for i in range(2500)] + ["k/0.pdf", "", None]
    otter_app.s3_delete_keys(keys)
    assert calls == [1000, 1000, 500]

    # s3_get_bytes returns None on NoSuchKey and re-raises AccessDenied
    assert otter_app.s3_get_bytes("missing.pdf") is None

    def denied_get(Bucket, Key):
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "GetObject")

    monkeypatch.setattr(fake_s3, "get_object", denied_get)
    with pytest.raises(ClientError):
        otter_app.s3_get_bytes("secret.pdf")

    # Missing config -> 503 ApiError
    monkeypatch.undo()
    monkeypatch.setattr(otter_app, "DATABASE_URL", "")
    with pytest.raises(otter_app.ApiError) as db_err:
        otter_app.db()
    assert db_err.value.status == 503

    monkeypatch.setattr(otter_app, "_redis", None)
    monkeypatch.setattr(otter_app, "LAYERBASE_URL", "")
    with pytest.raises(otter_app.ApiError) as redis_err:
        otter_app.cache()
    assert redis_err.value.status == 503

    monkeypatch.setattr(otter_app, "_s3", None)
    monkeypatch.setattr(otter_app, "AWS_BUCKET", "")
    with pytest.raises(otter_app.ApiError) as s3_err:
        otter_app.s3()
    assert s3_err.value.status == 503


def test_merge_preserves_exact_page_order_and_duplicates(env):
    client, fake_redis, fake_s3, _ = env
    pdf_a = make_sized_pdf(1, 200)
    pdf_b = make_sized_pdf(2, 400)

    id_a = client.post("/api/upload", data={"file": (io.BytesIO(pdf_a), "a.pdf")}).get_json()["file_id"]
    id_b = client.post("/api/upload", data={"file": (io.BytesIO(pdf_b), "b.pdf")}).get_json()["file_id"]

    # [A, B] -> widths [200, 400, 400]
    res_ab = client.post("/api/merge", json={"file_ids": [id_a, id_b]})
    assert res_ab.status_code == 200
    mid_ab = fake_redis.get(f"dl:{res_ab.get_json()['download_token']}")
    pages_ab = PdfReader(io.BytesIO(fake_s3.objects[("test-bucket", f"merged/{mid_ab}.pdf")])).pages
    assert [float(p.mediabox.width) for p in pages_ab] == [200, 400, 400]

    # [B, A] -> widths [400, 400, 200]
    res_ba = client.post("/api/merge", json={"file_ids": [id_b, id_a]})
    assert res_ba.status_code == 200
    mid_ba = fake_redis.get(f"dl:{res_ba.get_json()['download_token']}")
    pages_ba = PdfReader(io.BytesIO(fake_s3.objects[("test-bucket", f"merged/{mid_ba}.pdf")])).pages
    assert [float(p.mediabox.width) for p in pages_ba] == [400, 400, 200]

    # Duplicate [A, A] -> widths [200, 200]
    res_aa = client.post("/api/merge", json={"file_ids": [id_a, id_a]})
    assert res_aa.status_code == 200
    mid_aa = fake_redis.get(f"dl:{res_aa.get_json()['download_token']}")
    pages_aa = PdfReader(io.BytesIO(fake_s3.objects[("test-bucket", f"merged/{mid_aa}.pdf")])).pages
    assert [float(p.mediabox.width) for p in pages_aa] == [200, 200]
