"""Stateless otter-merge: Flask + Neon (Postgres & S3) + Layerbase (Redis-compatible).

No local disk writes. All PDF work happens in memory (io.BytesIO + pypdf),
binaries live in Neon S3-compatible object storage, metadata lives in Neon
Postgres, and caching/rate-limiting/tokens live in Layerbase Redis.
"""

import hashlib
import hmac
import io
import json
import logging
import os
import random
import re
import secrets
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import boto3
import psycopg
import redis
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from flask import Flask, g, jsonify, redirect, render_template, request
from pypdf import PdfReader, PdfWriter
from werkzeug.exceptions import HTTPException, RequestEntityTooLarge

load_dotenv()

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "")
LAYERBASE_URL = os.environ.get("LAYERBASE_URL", "")
CRON_SECRET = os.environ.get("CRON_SECRET", "")

AWS_ENDPOINT_URL_S3 = os.environ.get("AWS_ENDPOINT_URL_S3", "")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.environ.get("AWS_REGION", "")
AWS_BUCKET = os.environ.get("AWS_BUCKET", "")

MAX_UPLOAD_BYTES = 4 * 1024 * 1024  # per-file cap (Vercel body limit is 4.5MB)
MAX_REQUEST_BYTES = int(4.4 * 1024 * 1024)  # hard cap incl. multipart overhead
MAX_FILES_PER_MERGE = 10
MAX_MERGE_TOTAL_BYTES = 25 * 1024 * 1024
MIN_FILES_PER_MERGE = 2

UPLOAD_TTL_SECONDS = 60 * 60  # staged uploads live 1 hour
MERGED_TTL_SECONDS = 60 * 60  # merged files live 1 hour
CACHE_TTL_SECONDS = 15 * 60  # Layerbase merge cache + download token
PRESIGNED_URL_TTL_SECONDS = 5 * 60  # S3 presigned download URL lifetime

RATE_LIMITS = {  # (max requests, window seconds) per IP
    "upload": (30, 60),
    "merge": (10, 60),
    "download": (30, 60),
}
CLEANUP_PROBABILITY = 0.02

UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["MAX_CONTENT_LENGTH"] = MAX_REQUEST_BYTES

# --------------------------------------------------------------------------
# Structured logging (JSON to stdout -> Vercel Logs)
# --------------------------------------------------------------------------
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
log = logging.getLogger("otter-merge")


def emit(**fields):
    log.info(json.dumps(fields, default=str))


@app.before_request
def _start():
    g.start = time.perf_counter()
    g.request_id = request.headers.get("x-vercel-id") or uuid.uuid4().hex[:12]
    g.cache_hit = None
    g.file_count = None


@app.after_request
def _finish(resp):
    if request.path.startswith("/static/"):
        return resp
    emit(
        request_id=getattr(g, "request_id", None),
        route=request.path,
        method=request.method,
        cache_hit=getattr(g, "cache_hit", None),
        file_count=getattr(g, "file_count", None),
        duration_ms=round((time.perf_counter() - getattr(g, "start", time.perf_counter())) * 1000, 1),
        status=resp.status_code,
    )
    resp.headers["X-Request-Id"] = getattr(g, "request_id", "")
    return resp


class ApiError(Exception):
    def __init__(self, status, message, headers=None):
        self.status, self.message, self.headers = status, message, headers or {}


@app.errorhandler(ApiError)
def _api_error(e):
    resp = jsonify({"error": e.message})
    resp.status_code = e.status
    for k, v in e.headers.items():
        resp.headers[k] = v
    return resp


@app.errorhandler(RequestEntityTooLarge)
def _too_large(_):
    return _api_error(ApiError(413, "Request too large. Each PDF must be 4MB or smaller."))


@app.errorhandler(HTTPException)
def _http_error(e):
    if request.path.startswith("/api/"):
        return _api_error(ApiError(e.code or 500, e.description or "Error"))
    return e


@app.errorhandler(Exception)
def _unhandled(e):
    emit(request_id=getattr(g, "request_id", None), route=request.path, level="error", error=repr(e))
    return _api_error(ApiError(500, "Internal server error."))


# --------------------------------------------------------------------------
# Neon (Postgres). Use the "-pooler" connection string; one short-lived
# connection per use, always closed by the context manager.
# --------------------------------------------------------------------------
_schema_ready = False
_schema_lock = threading.Lock()
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")


def db():
    if not DATABASE_URL:
        raise ApiError(503, "DATABASE_URL is not configured.")
    # prepare_threshold=None: required for PgBouncer transaction pooling (Neon pooler)
    return psycopg.connect(DATABASE_URL, prepare_threshold=None, connect_timeout=10)


def ensure_schema():
    """Run schema.sql once per warm instance (idempotent)."""
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            ddl = f.read()
        try:
            with db() as conn:
                conn.execute(ddl)
        except psycopg.Error as e:  # concurrent cold starts can race on CREATE
            emit(level="warn", msg="schema init race/error", error=repr(e))
        _schema_ready = True


def run_cleanup():
    with db() as conn:
        up_rows = conn.execute("SELECT id, s3_key FROM uploaded_files WHERE expires_at < NOW()").fetchall()
        mg_rows = conn.execute("SELECT id, s3_key FROM merged_files WHERE expires_at < NOW()").fetchall()
        s3_delete_keys([r[1] for r in up_rows] + [r[1] for r in mg_rows])
        a = 0
        if up_rows:
            a = conn.execute(
                "DELETE FROM uploaded_files WHERE id = ANY(%s::uuid[])", ([r[0] for r in up_rows],)
            ).rowcount
        b = 0
        if mg_rows:
            b = conn.execute("DELETE FROM merged_files WHERE id = ANY(%s::uuid[])", ([r[0] for r in mg_rows],)).rowcount
    return {"uploaded_files_deleted": a, "merged_files_deleted": b}


def maybe_cleanup():
    if random.random() < CLEANUP_PROBABILITY:
        try:
            emit(request_id=g.request_id, route=request.path, lazy_cleanup=run_cleanup())
        except Exception as e:  # never fail a request because of cleanup
            emit(level="warn", msg="lazy cleanup failed", error=repr(e))


# --------------------------------------------------------------------------
# Neon Object Storage (S3-compatible, path-style addressing)
# --------------------------------------------------------------------------
_s3 = None
_s3_lock = threading.Lock()


def s3():
    global _s3
    if _s3 is None:
        with _s3_lock:
            if _s3 is None:
                if not all([AWS_ENDPOINT_URL_S3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_BUCKET]):
                    raise ApiError(503, "S3 storage is not configured.")
                _s3 = boto3.client(
                    "s3",
                    endpoint_url=AWS_ENDPOINT_URL_S3,
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    region_name=AWS_REGION or "us-east-1",
                    config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
                )
    return _s3


def s3_put_pdf(key, data, download_name=None):
    kwargs = {
        "Bucket": AWS_BUCKET,
        "Key": key,
        "Body": data,
        "ContentType": "application/pdf",
    }
    if download_name:
        safe_name = clean_filename(download_name).replace('"', "")
        kwargs["ContentDisposition"] = f'attachment; filename="{safe_name}"'
    s3().put_object(**kwargs)


def s3_get_bytes(key):
    try:
        return s3().get_object(Bucket=AWS_BUCKET, Key=key)["Body"].read()
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
            return None
        raise


def s3_delete_keys(keys):
    unique_keys = [k for k in dict.fromkeys(keys) if k]
    if not unique_keys:
        return
    client = s3()
    for i in range(0, len(unique_keys), 1000):
        batch = [{"Key": k} for k in unique_keys[i : i + 1000]]
        client.delete_objects(Bucket=AWS_BUCKET, Delete={"Objects": batch, "Quiet": True})


def s3_presigned_download_url(key, download_name, expires_in=PRESIGNED_URL_TTL_SECONDS):
    safe_name = clean_filename(download_name).replace('"', "")
    return s3().generate_presigned_url(
        "get_object",
        Params={
            "Bucket": AWS_BUCKET,
            "Key": key,
            "ResponseContentType": "application/pdf",
            "ResponseContentDisposition": f'attachment; filename="{safe_name}"',
        },
        ExpiresIn=expires_in,
    )


# --------------------------------------------------------------------------
# Layerbase (Redis-compatible)
# --------------------------------------------------------------------------
_redis = None


def cache():
    global _redis
    if _redis is None:
        if not LAYERBASE_URL:
            raise ApiError(503, "LAYERBASE_URL is not configured.")
        _redis = redis.from_url(
            LAYERBASE_URL,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
            health_check_interval=30,
        )
    return _redis


def client_ip():
    xff = request.headers.get("x-forwarded-for", "")
    return (
        (xff.split(",")[0].strip() if xff else None)
        or request.headers.get("x-real-ip")
        or request.remote_addr
        or "unknown"
    )


def rate_limit(bucket):
    """Fixed-window per-IP limiter. Fails open if Layerbase is unreachable."""
    limit, window = RATE_LIMITS[bucket]
    key = f"rl:{bucket}:{client_ip()}:{int(time.time() // window)}"
    try:
        r = cache()
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, window + 1)
        count = pipe.execute()[0]
    except (redis.RedisError, ApiError) as e:
        emit(request_id=g.request_id, level="warn", msg="rate limiter unavailable", error=repr(e))
        return
    if count > limit:
        retry = window - int(time.time() % window)
        raise ApiError(429, "Too many requests. Please wait a moment.", {"Retry-After": str(retry)})


def new_download_token(merged_id):
    token = secrets.token_urlsafe(32)
    cache().setex(f"dl:{token}", CACHE_TTL_SECONDS, merged_id)
    return token


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def clean_filename(name):
    name = os.path.basename((name or "document.pdf").replace("\\", "/"))
    name = re.sub(r"[\x00-\x1f\x7f]", "", name).strip()[:255]
    return name or "document.pdf"


def validate_pdf(data):
    """Return page count or raise ApiError(400)."""
    if not data.startswith(b"%PDF-"):
        raise ApiError(400, "File is not a valid PDF (missing %PDF- header).")
    try:
        reader = PdfReader(io.BytesIO(data), strict=False)
        if reader.is_encrypted:
            raise ApiError(400, "Password-protected PDFs are not supported.")
        pages = len(reader.pages)
    except ApiError:
        raise
    except Exception:
        raise ApiError(400, "PDF is corrupt or unreadable.")
    if pages < 1:
        raise ApiError(400, "PDF has no pages.")
    return pages


def download_url(token):
    return f"/api/download/{token}"


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.get("/")
def index():
    return render_template("index.html", upload_ttl=UPLOAD_TTL_SECONDS)


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})


@app.post("/api/upload")
def upload():
    rate_limit("upload")
    ensure_schema()

    f = request.files.get("file")
    if f is None:
        raise ApiError(400, "No file provided (multipart field 'file').")
    if f.mimetype not in ("application/pdf", "application/x-pdf", "application/octet-stream"):
        raise ApiError(400, "Only PDF files are accepted.")

    data = f.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        raise ApiError(413, "Each PDF must be 4MB or smaller.")
    if not data:
        raise ApiError(400, "File is empty.")

    pages = validate_pdf(data)
    file_id = str(uuid.uuid4())
    filename = clean_filename(f.filename)
    s3_key = f"uploads/{file_id}.pdf"

    s3_put_pdf(s3_key, data)
    try:
        with db() as conn:
            conn.execute(
                """INSERT INTO uploaded_files (id, filename, file_size, page_count, s3_key, expires_at)
                   VALUES (%s, %s, %s, %s, %s, NOW() + make_interval(secs => %s))""",
                (file_id, filename, len(data), pages, s3_key, UPLOAD_TTL_SECONDS),
            )
    except Exception:
        try:
            s3_delete_keys([s3_key])
        except Exception as cleanup_err:
            emit(request_id=g.request_id, level="warn", msg="orphan s3 cleanup failed", error=repr(cleanup_err))
        raise
    g.file_count = 1
    maybe_cleanup()
    return jsonify({"file_id": file_id, "filename": filename, "pages": pages, "expires_in": UPLOAD_TTL_SECONDS}), 201


@app.post("/api/merge")
def merge():
    rate_limit("merge")
    ensure_schema()

    body = request.get_json(silent=True) or {}
    ids = body.get("file_ids")
    if not isinstance(ids, list) or not all(isinstance(i, str) and UUID_RE.match(i) for i in ids):
        raise ApiError(400, "'file_ids' must be a list of UUID strings.")
    ids = [str(uuid.UUID(i)) for i in ids]  # normalise for stable hashing
    g.file_count = len(ids)
    if not (MIN_FILES_PER_MERGE <= len(ids) <= MAX_FILES_PER_MERGE):
        raise ApiError(400, f"Provide between {MIN_FILES_PER_MERGE} and {MAX_FILES_PER_MERGE} files.")

    source_hash = hashlib.sha256(",".join(ids).encode()).hexdigest()
    r = cache()
    cache_key = f"merge:{source_hash}"

    # 1) Layerbase cache hit -> reuse merged file and its token
    cached = r.get(cache_key)
    if cached:
        try:
            info = json.loads(cached)
            token = info["token"]
            if not r.exists(f"dl:{token}"):
                token = new_download_token(info["merged_file_id"])
                info["token"] = token
                r.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(info))
            g.cache_hit = True
            return jsonify(
                {
                    "download_token": token,
                    "download_url": download_url(token),
                    "pages": info.get("pages"),
                    "cached": True,
                    "expires_in": CACHE_TTL_SECONDS,
                }
            )
        except (ValueError, KeyError):
            r.delete(cache_key)

    g.cache_hit = False

    merged_id = None
    pages = None
    keys = {}
    with db() as conn:
        # 2) Neon-level dedupe (survives Layerbase TTL expiry)
        row = conn.execute(
            "SELECT id, page_count FROM merged_files WHERE source_hash=%s AND expires_at > NOW()",
            (source_hash,),
        ).fetchone()
        if row:
            merged_id, pages = str(row[0]), row[1]
        else:
            # Validate presence & total size before downloading blobs from S3
            meta = conn.execute(
                "SELECT id::text, file_size, s3_key FROM uploaded_files "
                "WHERE id = ANY(%s::uuid[]) AND expires_at > NOW()",
                (list(set(ids)),),
            ).fetchall()
            sizes = {m[0]: m[1] for m in meta}
            keys = {m[0]: m[2] for m in meta}
            missing = [i for i in set(ids) if i not in sizes]
            if missing:
                raise ApiError(404, "Some files were not found or have expired. Please upload them again.")
            if sum(sizes[i] for i in ids) > MAX_MERGE_TOTAL_BYTES:
                raise ApiError(413, "Combined size exceeds the 25MB limit.")

    if merged_id is None:
        blobs = {}
        for uid in set(ids):
            blob = s3_get_bytes(keys[uid])
            if blob is None:
                raise ApiError(404, "Some files were not found or have expired. Please upload them again.")
            blobs[uid] = blob

        # 3) In-memory merge, in the exact requested order
        writer = PdfWriter()
        try:
            for i in ids:
                writer.append(PdfReader(io.BytesIO(blobs[i]), strict=False))
            out = io.BytesIO()
            writer.write(out)
        except Exception as e:
            emit(request_id=g.request_id, level="error", msg="merge failed", error=repr(e))
            raise ApiError(422, "Could not merge these PDFs. One may be damaged.")
        finally:
            writer.close()
        merged = out.getvalue()
        pages = len(PdfReader(io.BytesIO(merged)).pages)
        del blobs

        merged_id = str(uuid.uuid4())
        merged_filename = f"merged-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}.pdf"
        merged_key = f"merged/{merged_id}.pdf"
        s3_put_pdf(merged_key, merged, download_name=merged_filename)

        try:
            with db() as conn:
                expired = conn.execute(
                    "DELETE FROM merged_files WHERE source_hash=%s AND expires_at <= NOW() RETURNING s3_key",
                    (source_hash,),
                ).fetchone()
                if expired:
                    try:
                        s3_delete_keys([expired[0]])
                    except Exception as cleanup_err:
                        emit(
                            request_id=g.request_id,
                            level="warn",
                            msg="expired s3 cleanup failed",
                            error=repr(cleanup_err),
                        )
                ins = conn.execute(
                    """INSERT INTO merged_files (id, source_hash, filename, file_size, page_count, s3_key, expires_at)
                       VALUES (%s, %s, %s, %s, %s, %s, NOW() + make_interval(secs => %s))
                       ON CONFLICT (source_hash) DO NOTHING RETURNING id""",
                    (merged_id, source_hash, merged_filename, len(merged), pages, merged_key, MERGED_TTL_SECONDS),
                ).fetchone()
                if ins is None:  # concurrent request won the race
                    won = conn.execute(
                        "SELECT id, page_count FROM merged_files WHERE source_hash=%s", (source_hash,)
                    ).fetchone()
                    merged_id, pages = str(won[0]), won[1]
                    s3_delete_keys([merged_key])
        except Exception:
            try:
                s3_delete_keys([merged_key])
            except Exception as cleanup_err:
                emit(
                    request_id=g.request_id,
                    level="warn",
                    msg="orphan merged s3 cleanup failed",
                    error=repr(cleanup_err),
                )
            raise

    token = new_download_token(merged_id)
    r.setex(cache_key, CACHE_TTL_SECONDS, json.dumps({"merged_file_id": merged_id, "token": token, "pages": pages}))
    maybe_cleanup()
    return jsonify(
        {
            "download_token": token,
            "download_url": download_url(token),
            "pages": pages,
            "cached": False,
            "expires_in": CACHE_TTL_SECONDS,
        }
    )


@app.get("/api/download/<token>")
def download(token):
    rate_limit("download")
    ensure_schema()
    merged_id = cache().get(f"dl:{token}")
    if not merged_id:
        raise ApiError(410, "This download link has expired. Merge your files again.")

    with db() as conn:
        row = conn.execute(
            "SELECT filename, s3_key FROM merged_files WHERE id=%s AND expires_at > NOW()", (merged_id,)
        ).fetchone()
    if not row:
        raise ApiError(410, "The merged file is no longer available.")

    url = s3_presigned_download_url(row[1], row[0])
    resp = redirect(url, code=302)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/api/cron/cleanup")
def cron_cleanup():
    auth = request.headers.get("Authorization", "")
    if not CRON_SECRET or not hmac.compare_digest(auth, f"Bearer {CRON_SECRET}"):
        raise ApiError(401, "Unauthorized.")
    ensure_schema()
    return jsonify(run_cleanup())


if __name__ == "__main__":
    app.run(debug=True, port=5000)
