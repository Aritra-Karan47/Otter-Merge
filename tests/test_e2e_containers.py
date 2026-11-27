import io
import os
import urllib.request

import pytest
from pypdf import PdfReader, PdfWriter

import app as otter_app

E2E_ENABLED = os.environ.get("CI_E2E") == "1"


def make_pdf_bytes():
    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    buf = io.BytesIO()
    writer.write(buf)
    writer.close()
    return buf.getvalue()


@pytest.mark.integration
@pytest.mark.skipif(not E2E_ENABLED, reason="CI_E2E=1 not set")
def test_e2e_containers(monkeypatch):
    monkeypatch.setattr(otter_app, "CLEANUP_PROBABILITY", 0.0)
    client = otter_app.app.test_client()

    assert client.get("/healthz").status_code == 200
    assert otter_app.cache().ping() is True

    r1 = client.post("/api/upload", data={"file": (io.BytesIO(make_pdf_bytes()), "a.pdf")})
    assert r1.status_code == 201, r1.get_data(as_text=True)
    id1 = r1.get_json()["file_id"]

    r2 = client.post("/api/upload", data={"file": (io.BytesIO(make_pdf_bytes()), "b.pdf")})
    assert r2.status_code == 201, r2.get_data(as_text=True)
    id2 = r2.get_json()["file_id"]

    m1 = client.post("/api/merge", json={"file_ids": [id1, id2]})
    assert m1.status_code == 200, m1.get_data(as_text=True)
    j1 = m1.get_json()
    assert j1["cached"] is False
    assert j1["pages"] == 2

    m2 = client.post("/api/merge", json={"file_ids": [id1, id2]})
    assert m2.status_code == 200
    assert m2.get_json()["cached"] is True

    dl = client.get(j1["download_url"], follow_redirects=False)
    assert dl.status_code == 302
    with urllib.request.urlopen(dl.headers["Location"], timeout=10) as res:
        assert res.status == 200
        merged_pdf = res.read()
    assert len(PdfReader(io.BytesIO(merged_pdf)).pages) == 2

    # Force expiry and verify cron cleanup removes both DB rows and S3 objects
    merged_id = otter_app.cache().get(f"dl:{j1['download_token']}")
    with otter_app.db() as conn:
        conn.execute("UPDATE uploaded_files SET expires_at = NOW() - interval '1 minute'")
        conn.execute("UPDATE merged_files SET expires_at = NOW() - interval '1 minute'")

    cron = client.get(
        "/api/cron/cleanup",
        headers={"Authorization": f"Bearer {otter_app.CRON_SECRET}"},
    )
    assert cron.status_code == 200
    assert cron.get_json() == {"uploaded_files_deleted": 2, "merged_files_deleted": 1}
    assert otter_app.s3_get_bytes(f"uploads/{id1}.pdf") is None
    assert otter_app.s3_get_bytes(f"uploads/{id2}.pdf") is None
    assert otter_app.s3_get_bytes(f"merged/{merged_id}.pdf") is None
