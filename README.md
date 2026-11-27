# otter-merge (Flask + Neon + Layerbase on Vercel)

```
.
├── app.py
├── schema.sql
├── requirements.txt
├── vercel.json
├── .env.example
├── templates/index.html
└── static/
    └── logo/logo.png      <- replace with your logo
```

## Deploy

1. **Neon (Postgres + Object Storage)**: create a project, copy the **pooled** connection string (host contains `-pooler`) as `DATABASE_URL`, and create an S3 storage bucket + credentials for `AWS_ENDPOINT_URL_S3`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and `AWS_BUCKET`.
   Tables are created automatically on first request from `schema.sql` (or run it manually in the Neon SQL editor).
2. **Layerbase**: create a Redis-compatible instance, copy its URL (`rediss://...`) as `LAYERBASE_URL`.
3. Generate a secret: `openssl rand -hex 32` -> `CRON_SECRET`.
4. Add all environment variables in Vercel: Project -> Settings -> Environment Variables (or `vercel env add`).
5. Drop your logo at `static/logo/logo.png`.
6. Deploy: `npm i -g vercel && vercel --prod` (or import the Git repo).
7. Vercel Cron calls `/api/cron/cleanup` daily at 03:00 UTC with `Authorization: Bearer $CRON_SECRET`.

## Local run
```
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in Neon, Layerbase, S3, and CRON_SECRET
flask --app app run
```

## Limits
- 4MB per uploaded PDF, 10 files and 25MB total per merge, uploads/merged files expire after 1 hour, download tokens after 15 minutes.
- Rate limits per IP: 30 uploads/min, 10 merges/min, 30 downloads/min.

## Notes on Vercel limits
Vercel caps request bodies at 4.5MB. Uploads are staged one file at a time to stay under that,
and `/api/download/<token>` redirects to a short-lived presigned Neon S3 URL so merged PDFs download directly from object storage.
