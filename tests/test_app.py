import io
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError
from pypdf import PdfReader, PdfWriter

import app as otter_app


def make_pdf_bytes(num_pages=1, text_hint=None):
    writer = PdfWriter()
    for _ in range(num_pages):
        writer.add_blank_page(width=200, height=200)
    if text_hint:
        writer.add_metadata({"/Title": text_hint})
    buf = io.BytesIO()
    writer.write(buf)
    writer.close()
    return buf.getvalue()


class FakePipeline:
    def __init__(self, store):
        self.store = store
        self.ops = []

    def incr(self, key):
        self.ops.append(("incr", key))

    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl))

    def execute(self):
        results = []
        for op in self.ops:
            if op[0] == "incr":
                val = int(self.store.data.get(op[1], 0)) + 1
                self.store.data[op[1]] = str(val)
                results.append(val)
            elif op[0] == "expire":
                results.append(True)
        return results


class FakeRedis:
    def __init__(self):
        self.data = {}

    def ping(self):
        return True

    def get(self, key):
        return self.data.get(key)

    def setex(self, key, _ttl, value):
        self.data[key] = value
        return True

    def exists(self, key):
        return 1 if key in self.data else 0

    def delete(self, *keys):
        for k in keys:
            self.data.pop(k, None)

    def pipeline(self):
        return FakePipeline(self)


class FakeBody:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return self._payload


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **_kwargs):
        self.objects[(Bucket, Key)] = bytes(Body)

    def get_object(self, Bucket, Key):
        data = self.objects.get((Bucket, Key))
        if data is None:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject")
        return {"Body": FakeBody(data)}

    def delete_objects(self, Bucket, Delete):
        for item in Delete.get("Objects", []):
            self.objects.pop((Bucket, item["Key"]), None)
        return {"Deleted": Delete.get("Objects", [])}

    def generate_presigned_url(self, _op, Params, ExpiresIn):
        return f"https://s3.example.test/{Params['Bucket']}/{Params['Key']}?expires={ExpiresIn}"


class FakeResult:
    def __init__(self, rows=None, rowcount=0):
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class FakeDBState:
    def __init__(self):
        self.uploaded = {}
        self.merged = {}
        self.force_race_source_hash = None


class FakeConn:
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        norm = " ".join(sql.split())
        now = datetime.now(timezone.utc)

        if norm.startswith("INSERT INTO uploaded_files"):
            fid, filename, size, pages, s3_key, ttl = params
            self.state.uploaded[fid] = {
                "id": fid,
                "filename": filename,
                "file_size": size,
                "page_count": pages,
                "s3_key": s3_key,
                "expires_at": now + timedelta(seconds=ttl),
            }
            return FakeResult(rowcount=1)

        if norm.startswith("SELECT id, page_count FROM merged_files WHERE source_hash=%s AND expires_at > NOW()"):
            sh = params[0]
            for row in self.state.merged.values():
                if row["source_hash"] == sh and row["expires_at"] > now:
                    return FakeResult([(row["id"], row["page_count"])])
            return FakeResult([])

        if norm.startswith("SELECT id::text, file_size, s3_key FROM uploaded_files WHERE id = ANY"):
            wanted = set(params[0])
            rows = [
                (r["id"], r["file_size"], r["s3_key"])
                for r in self.state.uploaded.values()
                if r["id"] in wanted and r["expires_at"] > now
            ]
            return FakeResult(rows)

        if norm.startswith("DELETE FROM merged_files WHERE source_hash=%s AND expires_at <= NOW() RETURNING s3_key"):
            sh = params[0]
            for mid, row in list(self.state.merged.items()):
                if row["source_hash"] == sh and row["expires_at"] <= now:
                    del self.state.merged[mid]
                    return FakeResult([(row["s3_key"],)], rowcount=1)
            return FakeResult([])

        if norm.startswith("INSERT INTO merged_files"):
            mid, sh, filename, size, pages, s3_key, ttl = params
            if self.state.force_race_source_hash == sh or any(
                r["source_hash"] == sh for r in self.state.merged.values()
            ):
                return FakeResult([])
            self.state.merged[mid] = {
                "id": mid,
                "source_hash": sh,
                "filename": filename,
                "file_size": size,
                "page_count": pages,
                "s3_key": s3_key,
                "expires_at": now + timedelta(seconds=ttl),
            }
            return FakeResult([(mid,)], rowcount=1)

        if norm.startswith("SELECT id, page_count FROM merged_files WHERE source_hash=%s"):
            sh = params[0]
            for row in self.state.merged.values():
                if row["source_hash"] == sh:
                    return FakeResult([(row["id"], row["page_count"])])
            return FakeResult([])

        if norm.startswith("SELECT filename, s3_key FROM merged_files WHERE id=%s AND expires_at > NOW()"):
            mid = params[0]
            row = self.state.merged.get(mid)
            if row and row["expires_at"] > now:
                return FakeResult([(row["filename"], row["s3_key"])])
            return FakeResult([])

        if norm.startswith("SELECT id, s3_key FROM uploaded_files WHERE expires_at < NOW()"):
            rows = [(r["id"], r["s3_key"]) for r in self.state.uploaded.values() if r["expires_at"] < now]
            return FakeResult(rows)

        if norm.startswith("SELECT id, s3_key FROM merged_files WHERE expires_at < NOW()"):
            rows = [(r["id"], r["s3_key"]) for r in self.state.merged.values() if r["expires_at"] < now]
            return FakeResult(rows)

        if norm.startswith("DELETE FROM uploaded_files WHERE id = ANY"):
            ids = set(params[0])
            count = sum(
                1 for k in list(self.state.uploaded) if k in ids and self.state.uploaded.pop(k, None) is not None
            )
            return FakeResult(rowcount=count)

        if norm.startswith("DELETE FROM merged_files WHERE id = ANY"):
            ids = set(params[0])
            count = sum(1 for k in list(self.state.merged) if k in ids and self.state.merged.pop(k, None) is not None)
            return FakeResult(rowcount=count)

        raise AssertionError(f"Unhandled SQL in FakeConn: {norm}")


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


def test_smoke_routes(env):
    client, *_ = env
    home = client.get("/")
    assert home.status_code == 200
    html = home.get_data(as_text=True)
    assert "otter-merge" in html
    assert "<style>" in html and "<script>" in html

    health = client.get("/healthz")
    assert health.status_code == 200
    assert health.get_json() == {"ok": True}

    logo = client.get("/static/logo/logo.png")
    assert logo.status_code == 200
    logo.close()


def test_upload_validation_and_success(env):
    client, _, fake_s3, fake_db = env

    assert client.post("/api/upload").status_code == 400
    assert (
        client.post(
            "/api/upload",
            data={"file": (io.BytesIO(b"not-a-pdf"), "bad.txt", "text/plain")},
            content_type="multipart/form-data",
        ).status_code
        == 400
    )
    assert (
        client.post(
            "/api/upload",
            data={"file": (io.BytesIO(b"not-a-pdf"), "bad.pdf", "application/pdf")},
            content_type="multipart/form-data",
        ).status_code
        == 400
    )

    pdf = make_pdf_bytes(num_pages=2)
    res = client.post(
        "/api/upload",
        data={"file": (io.BytesIO(pdf), "../weird/doc.pdf", "application/pdf")},
        content_type="multipart/form-data",
    )
    assert res.status_code == 201
    body = res.get_json()
    assert body["pages"] == 2
    assert body["filename"] == "doc.pdf"
    fid = body["file_id"]
    assert ("test-bucket", f"uploads/{fid}.pdf") in fake_s3.objects
    assert fid in fake_db.uploaded


def test_merge_and_download_flow(env):
    client, fake_redis, fake_s3, fake_db = env
    pdf1 = make_pdf_bytes(num_pages=1, text_hint="first")
    pdf2 = make_pdf_bytes(num_pages=3, text_hint="second")

    id1 = client.post("/api/upload", data={"file": (io.BytesIO(pdf1), "a.pdf")}).get_json()["file_id"]
    id2 = client.post("/api/upload", data={"file": (io.BytesIO(pdf2), "b.pdf")}).get_json()["file_id"]

    # Bad merge inputs
    assert client.post("/api/merge", json={"file_ids": [id1]}).status_code == 400
    assert client.post("/api/merge", json={"file_ids": ["not-a-uuid", id2]}).status_code == 400
    assert client.post("/api/merge", json={"file_ids": [id1, str(uuid.uuid4())]}).status_code == 404

    # Merge miss -> creates merged PDF in S3 + DB + Redis
    m1 = client.post("/api/merge", json={"file_ids": [id1, id2]})
    assert m1.status_code == 200
    j1 = m1.get_json()
    assert j1["cached"] is False
    assert j1["pages"] == 4

    # Merge hit -> served from Layerbase Redis cache
    m2 = client.post("/api/merge", json={"file_ids": [id1, id2]})
    assert m2.status_code == 200
    j2 = m2.get_json()
    assert j2["cached"] is True
    assert j2["download_token"] == j1["download_token"]

    # Download redirects to presigned S3 URL
    dl = client.get(j2["download_url"], follow_redirects=False)
    assert dl.status_code == 302
    assert dl.headers["Cache-Control"] == "no-store"
    assert "https://s3.example.test/test-bucket/merged/" in dl.headers["Location"]

    # Verify merged PDF pages in FakeS3 match combined pages
    merged_id = fake_redis.get(f"dl:{j2['download_token']}")
    merged_bytes = fake_s3.objects[("test-bucket", f"merged/{merged_id}.pdf")]
    assert len(PdfReader(io.BytesIO(merged_bytes)).pages) == 4

    # Expired token returns 410
    assert client.get("/api/download/missing-token").status_code == 410


def test_merge_size_limit(env):
    client, _, _, fake_db = env
    id1 = str(uuid.uuid4())
    id2 = str(uuid.uuid4())
    now = datetime.now(timezone.utc) + timedelta(minutes=30)
    fake_db.uploaded[id1] = {
        "id": id1,
        "filename": "1.pdf",
        "file_size": 15 * 1024 * 1024,
        "page_count": 1,
        "s3_key": "uploads/1.pdf",
        "expires_at": now,
    }
    fake_db.uploaded[id2] = {
        "id": id2,
        "filename": "2.pdf",
        "file_size": 15 * 1024 * 1024,
        "page_count": 1,
        "s3_key": "uploads/2.pdf",
        "expires_at": now,
    }

    res = client.post("/api/merge", json={"file_ids": [id1, id2]})
    assert res.status_code == 413


def test_cron_cleanup_auth_and_s3_deletion(env):
    client, _, fake_s3, fake_db = env

    assert client.get("/api/cron/cleanup").status_code == 401
    assert client.get("/api/cron/cleanup", headers={"Authorization": "Bearer wrong"}).status_code == 401

    past = datetime.now(timezone.utc) - timedelta(minutes=5)
    uid = str(uuid.uuid4())
    mid = str(uuid.uuid4())
    fake_db.uploaded[uid] = {
        "id": uid,
        "filename": "old.pdf",
        "file_size": 100,
        "page_count": 1,
        "s3_key": f"uploads/{uid}.pdf",
        "expires_at": past,
    }
    fake_db.merged[mid] = {
        "id": mid,
        "source_hash": "abc",
        "filename": "m.pdf",
        "file_size": 100,
        "page_count": 2,
        "s3_key": f"merged/{mid}.pdf",
        "expires_at": past,
    }
    fake_s3.objects[("test-bucket", f"uploads/{uid}.pdf")] = b"%PDF-1.4"
    fake_s3.objects[("test-bucket", f"merged/{mid}.pdf")] = b"%PDF-1.4"

    res = client.get("/api/cron/cleanup", headers={"Authorization": "Bearer cron-secret-123"})
    assert res.status_code == 200
    assert res.get_json() == {"uploaded_files_deleted": 1, "merged_files_deleted": 1}
    assert ("test-bucket", f"uploads/{uid}.pdf") not in fake_s3.objects
    assert ("test-bucket", f"merged/{mid}.pdf") not in fake_s3.objects
