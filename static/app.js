(() => {
  const MAX_FILES = 10, MAX_BYTES = 4 * 1024 * 1024;
  const $ = (id) => document.getElementById(id);
  const picker = $("picker"), list = $("list"), empty = $("empty"),
        mergeBtn = $("merge"), clearBtn = $("clear"), statusEl = $("status"),
        dl = $("download"), drop = $("drop");

  let items = []; // {file, state: queued|uploading|done|error, id, pages, error}
  let uploading = false, merging = false;

  const fmt = (n) => (n < 1024 * 1024 ? (n / 1024).toFixed(0) + " KB" : (n / 1024 / 1024).toFixed(1) + " MB");
  const setStatus = (msg, err = false) => { statusEl.textContent = msg; statusEl.classList.toggle("error", err); };
  const resetResult = () => { dl.hidden = true; dl.removeAttribute("href"); };

  function render() {
    list.textContent = "";
    items.forEach((it, i) => {
      const li = document.createElement("li");
      li.className = "item " + (it.state === "error" ? "error" : it.state === "done" ? "done" : "");
      const text = document.createElement("div");
      text.className = "name";
      text.textContent = it.file.name;
      const meta = document.createElement("span");
      meta.className = "meta";
      meta.textContent = it.state === "done" ? `${it.pages} page${it.pages === 1 ? "" : "s"} · ${fmt(it.file.size)}`
        : it.state === "uploading" ? "Uploading…"
        : it.state === "queued" ? "Waiting…"
        : it.error;
      text.appendChild(meta);
      li.appendChild(text);

      const mk = (label, aria, onClick, disabled) => {
        const b = document.createElement("button");
        b.type = "button"; b.textContent = label; b.setAttribute("aria-label", aria);
        b.disabled = disabled || merging; b.addEventListener("click", onClick);
        return b;
      };
      li.appendChild(mk("↑", `Move ${it.file.name} up`, () => move(i, -1), i === 0));
      li.appendChild(mk("↓", `Move ${it.file.name} down`, () => move(i, 1), i === items.length - 1));
      li.appendChild(mk("✕", `Remove ${it.file.name}`, () => { items.splice(i, 1); resetResult(); render(); }, false));
      list.appendChild(li);
    });
    const ready = items.filter((x) => x.state === "done").length;
    empty.hidden = items.length > 0;
    mergeBtn.disabled = merging || uploading || ready < 2;
    clearBtn.disabled = merging || items.length === 0;
  }

  function move(i, d) {
    const j = i + d;
    if (j < 0 || j >= items.length) return;
    [items[i], items[j]] = [items[j], items[i]];
    resetResult(); render();
  }

  async function readError(res) {
    try { return (await res.json()).error || `Request failed (${res.status}).`; }
    catch { return `Request failed (${res.status}).`; }
  }

  async function uploadQueue() {
    if (uploading) return;
    uploading = true;
    for (const it of items.slice()) {
      if (it.state !== "queued") continue;
      it.state = "uploading"; render();
      try {
        const fd = new FormData();
        fd.append("file", it.file, it.file.name);
        const res = await fetch("/api/upload", { method: "POST", body: fd });
        if (!res.ok) throw new Error(await readError(res));
        const data = await res.json();
        it.id = data.file_id; it.pages = data.pages; it.state = "done";
      } catch (e) {
        it.state = "error"; it.error = e.message || "Upload failed.";
      }
      render();
    }
    uploading = false; render();
  }

  function addFiles(files) {
    resetResult(); setStatus("");
    for (const file of files) {
      if (items.length >= MAX_FILES) { setStatus(`You can merge up to ${MAX_FILES} files.`, true); break; }
      const isPdf = file.type === "application/pdf" || /\.pdf$/i.test(file.name);
      const it = { file, state: "queued" };
      if (!isPdf) { it.state = "error"; it.error = "Not a PDF file."; }
      else if (file.size > MAX_BYTES) { it.state = "error"; it.error = `Larger than 4MB (${fmt(file.size)}).`; }
      items.push(it);
    }
    render(); uploadQueue();
  }

  picker.addEventListener("change", () => { addFiles(Array.from(picker.files)); picker.value = ""; });
  ["dragenter", "dragover"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.add("over"); }));
  ["dragleave", "drop"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.remove("over"); }));
  drop.addEventListener("drop", (e) => addFiles(Array.from(e.dataTransfer.files)));

  clearBtn.addEventListener("click", () => { items = []; resetResult(); setStatus(""); render(); });

  mergeBtn.addEventListener("click", async () => {
    // Files that failed to upload are skipped; the rest merge in list order.
    const ids = items.filter((x) => x.state === "done").map((x) => x.id);
    merging = true; resetResult(); setStatus("Merging…"); render();
    try {
      const res = await fetch("/api/merge", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_ids: ids }),
      });
      if (!res.ok) throw new Error(await readError(res));
      const data = await res.json();
      dl.href = data.download_url; dl.hidden = false;
      setStatus(`Merged ${data.pages ?? ""} pages. This link expires in ${Math.round(data.expires_in / 60)} minutes.`);
    } catch (e) {
      setStatus(e.message || "Merge failed.", true);
    } finally {
      merging = false; render();
    }
  });

  render();
})();
