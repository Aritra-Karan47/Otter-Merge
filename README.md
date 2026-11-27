# PDF Merger (Flask + Neon + Layerbase on Vercel)

```
.
├── app.py
├── schema.sql
├── requirements.txt
├── vercel.json
├── .env.example
├── templates/index.html
└── static/
    ├── style.css
    ├── app.js
    └── logo/logo.png      <- replace with your logo
```

## Deploy

1. **Neon**: create a project, copy the **pooled** connection string (host contains `-pooler`) as `DATABASE_URL`.
   Tables are created automatically on first request from `schema.sql` (or run it manually in the Neon SQL editor).
2. **Layerbase**: create a Redis-compatible instance, copy its URL (`rediss://...`) as `LAYERBASE_URL`.
3. Generate a secret: `openssl rand -hex 32` -> `CRON_SECRET`.
4. Add all three in Vercel: Project -> Settings -> Environment Variables (or `vercel env add`).
5. Drop your logo at `static/logo/logo.png`.
6. Deploy: `npm i -g vercel && vercel --prod` (or import the Git repo).
7. Vercel Cron calls `/api/cron/cleanup` daily at 03:00 UTC with `Authorization: Bearer $CRON_SECRET`.

## Local run
```
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export $(grep -v '^#' .env | xargs)   # after copying .env.example to .env
flask --app app run
```

## Limits
- 4MB per uploaded PDF, 10 files and 25MB total per merge, uploads/merged files expire after 1 hour, download tokens after 15 minutes.
- Rate limits per IP: 30 uploads/min, 10 merges/min, 30 downloads/min.

## Important caveat: Vercel response size
Vercel caps request **and** non-streamed response bodies at 4.5MB. Uploads are staged one file at a time to
stay under that, but a merged PDF larger than ~4.5MB may fail to download from a standard function.
If you need larger outputs, lower the merge cap, or serve the final file through a streaming-capable
route / object storage with presigned URLs.
