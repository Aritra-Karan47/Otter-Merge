import json
import threading
import time

import pytest
from werkzeug.serving import make_server

import app as otter_app


@pytest.fixture(scope="module")
def live_url():
    server = make_server("127.0.0.1", 0, otter_app.app)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()


def make_file_payload(name):
    return {"name": name, "mimeType": "application/pdf", "buffer": b"%PDF-1.4\n%test\n"}


@pytest.mark.browser
def test_upload_queue_does_not_stall_on_sequential_add(live_url):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        release_first = threading.Event()
        upload_calls = []

        def handle_upload(route):
            idx = len(upload_calls) + 1
            upload_calls.append(idx)
            if idx == 1:
                release_first.wait(timeout=5)
            route.fulfill(
                status=201,
                content_type="application/json",
                body=json.dumps(
                    {
                        "file_id": f"00000000-0000-4000-8000-00000000000{idx}",
                        "filename": f"f{idx}.pdf",
                        "pages": idx,
                        "expires_in": 3600,
                    }
                ),
            )

        page.route("**/api/upload", handle_upload)
        page.goto(live_url)

        # Pick first file (stays in Uploading... until release_first is set)
        page.set_input_files("#picker", [make_file_payload("first.pdf")])
        page.locator(".item .meta", has_text="Uploading…").wait_for(timeout=3000)

        # Immediately add second file while first is uploading
        page.set_input_files("#picker", [make_file_payload("second.pdf")])
        page.locator(".item .meta", has_text="Waiting…").wait_for(timeout=3000)

        # Release first upload; second must automatically drain to done
        release_first.set()
        page.locator(".item.done").nth(1).wait_for(timeout=5000)
        assert page.locator(".item .meta", has_text="Waiting…").count() == 0
        assert not page.locator("#merge").is_disabled()
        browser.close()


@pytest.mark.browser
def test_reorder_abort_and_merge_flow(live_url):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        counter = {"n": 0}
        merged_payload = {}

        def handle_upload(route):
            counter["n"] += 1
            n = counter["n"]
            if n == 1:
                time.sleep(0.4)
            route.fulfill(
                status=201,
                content_type="application/json",
                body=json.dumps(
                    {
                        "file_id": f"00000000-0000-4000-8000-00000000000{n}",
                        "filename": f"file{n}.pdf",
                        "pages": 1,
                        "expires_in": 3600,
                    }
                ),
            )

        def handle_merge(route):
            merged_payload.update(route.request.post_data_json)
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "download_token": "tok123",
                        "download_url": "/api/download/tok123",
                        "pages": 2,
                        "cached": False,
                        "expires_in": 900,
                    }
                ),
            )

        page.route("**/api/upload", handle_upload)
        page.route("**/api/merge", handle_merge)
        page.goto(live_url)

        # Add slow file1 + queued file2, remove file1 while uploading, add file3
        page.set_input_files("#picker", [make_file_payload("one.pdf"), make_file_payload("two.pdf")])
        page.locator('button[aria-label="Remove one.pdf"]').click()
        page.set_input_files("#picker", [make_file_payload("three.pdf")])

        page.locator(".item.done").nth(1).wait_for(timeout=5000)

        # Move three.pdf up before two.pdf
        page.locator('button[aria-label="Move three.pdf up"]').click()
        page.locator("#merge").click()

        page.locator("#download").wait_for(state="visible", timeout=3000)
        assert page.locator("#download").get_attribute("href") == "/api/download/tok123"
        assert "left)" in page.locator("#countdown").inner_text()
        assert merged_payload["file_ids"] == [
            "00000000-0000-4000-8000-000000000003",
            "00000000-0000-4000-8000-000000000002",
        ]
        browser.close()


@pytest.mark.browser
def test_ttl_expiry_and_reupload_recovery(live_url):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        counter = {"n": 0}

        def handle_upload(route):
            counter["n"] += 1
            n = counter["n"]
            # First pair expires in 1s; re-uploads get 3600s
            ttl = 1 if n <= 2 else 3600
            route.fulfill(
                status=201,
                content_type="application/json",
                body=json.dumps(
                    {
                        "file_id": f"00000000-0000-4000-8000-00000000000{n}",
                        "filename": f"f{n}.pdf",
                        "pages": 1,
                        "expires_in": ttl,
                    }
                ),
            )

        def handle_merge(route):
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "download_token": "shorttok",
                        "download_url": "/api/download/shorttok",
                        "pages": 2,
                        "cached": False,
                        "expires_in": 1,
                    }
                ),
            )

        page.route("**/api/upload", handle_upload)
        page.route("**/api/merge", handle_merge)
        page.goto(live_url)

        page.set_input_files("#picker", [make_file_payload("a.pdf"), make_file_payload("b.pdf")])
        page.locator(".item.done").nth(1).wait_for(timeout=5000)
        page.locator("#merge").click()
        page.locator("#download").wait_for(state="visible", timeout=3000)

        # Wait ~2.2s for 1s TTL timers to mark items expired and hide download
        page.locator("#expired-prompt").wait_for(state="visible", timeout=5000)
        assert page.locator("#download").is_hidden()
        assert page.locator(".item.expired").count() == 2

        # Click "Re-upload expired" and verify both recover to done
        page.locator("#reupload-all").click()
        page.locator(".item.done").nth(1).wait_for(timeout=5000)
        assert page.locator("#expired-prompt").is_hidden()
        browser.close()
