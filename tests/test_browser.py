import asyncio
import json
import threading

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
    from playwright.async_api import async_playwright

    async def _run():
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()

            release_first = asyncio.Event()
            upload_calls = []

            async def handle_upload(route):
                idx = len(upload_calls) + 1
                upload_calls.append(idx)
                if idx == 1:
                    await release_first.wait()
                await route.fulfill(
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

            await page.route("**/api/upload", handle_upload)
            await page.goto(live_url)

            # Pick first file (stays in Uploading... until release_first is set)
            await page.set_input_files("#picker", [make_file_payload("first.pdf")])
            await page.locator(".item .meta", has_text="Uploading…").wait_for(timeout=3000)

            # Immediately add second file while first is uploading
            await page.set_input_files("#picker", [make_file_payload("second.pdf")])
            await page.locator(".item .meta", has_text="Waiting…").wait_for(timeout=3000)

            # Release first upload; second must automatically drain to done
            release_first.set()
            await page.locator(".item.done").nth(1).wait_for(timeout=5000)
            assert await page.locator(".item .meta", has_text="Waiting…").count() == 0
            assert not await page.locator("#merge").is_disabled()
            await browser.close()

    asyncio.run(_run())


@pytest.mark.browser
def test_reorder_abort_and_merge_flow(live_url):
    from playwright.async_api import async_playwright

    async def _run():
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()

            counter = {"n": 0}
            merged_payload = {}

            async def handle_upload(route):
                counter["n"] += 1
                n = counter["n"]
                if n == 1:
                    await asyncio.sleep(0.4)
                await route.fulfill(
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

            async def handle_merge(route):
                merged_payload.update(route.request.post_data_json)
                await route.fulfill(
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

            await page.route("**/api/upload", handle_upload)
            await page.route("**/api/merge", handle_merge)
            await page.goto(live_url)

            # Add slow file1 + queued file2, remove file1 while uploading, add file3
            await page.set_input_files("#picker", [make_file_payload("one.pdf"), make_file_payload("two.pdf")])
            await page.locator('button[aria-label="Remove one.pdf"]').click()
            await page.set_input_files("#picker", [make_file_payload("three.pdf")])

            await page.locator(".item.done").nth(1).wait_for(timeout=5000)

            # Move three.pdf up before two.pdf
            await page.locator('button[aria-label="Move three.pdf up"]').click()
            await page.locator("#merge").click()

            await page.locator("#download").wait_for(state="visible", timeout=3000)
            assert await page.locator("#download").get_attribute("href") == "/api/download/tok123"
            assert "left)" in await page.locator("#countdown").inner_text()
            assert merged_payload["file_ids"] == [
                "00000000-0000-4000-8000-000000000003",
                "00000000-0000-4000-8000-000000000002",
            ]
            await browser.close()

    asyncio.run(_run())


@pytest.mark.browser
def test_ttl_expiry_and_reupload_recovery(live_url):
    from playwright.async_api import async_playwright

    async def _run():
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()

            counter = {"n": 0}

            async def handle_upload(route):
                counter["n"] += 1
                n = counter["n"]
                # First pair expires in 1s; re-uploads get 3600s
                ttl = 1 if n <= 2 else 3600
                await route.fulfill(
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

            async def handle_merge(route):
                await route.fulfill(
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

            await page.route("**/api/upload", handle_upload)
            await page.route("**/api/merge", handle_merge)
            await page.goto(live_url)

            await page.set_input_files("#picker", [make_file_payload("a.pdf"), make_file_payload("b.pdf")])
            await page.locator(".item.done").nth(1).wait_for(timeout=5000)
            await page.locator("#merge").click()
            await page.locator("#download").wait_for(state="visible", timeout=3000)

            # Wait for 1s TTL timers on both uploads and download link
            await page.locator("#expired-prompt").wait_for(state="visible", timeout=5000)
            await page.locator("#download").wait_for(state="hidden", timeout=5000)
            assert await page.locator(".item.expired").count() == 2

            # Click "Re-upload expired" and verify both recover to done
            await page.locator("#reupload-all").click()
            await page.locator(".item.done").nth(1).wait_for(timeout=5000)
            assert await page.locator("#expired-prompt").is_hidden()
            await browser.close()

    asyncio.run(_run())
