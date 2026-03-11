# CVU City Slides

Automated slide deck generator for the Council on Vertical Urbanism.  
Select a city → configure slides → download a populated `.pptx` in seconds.

**Stack:** Preact frontend on GitHub Pages · FastAPI backend on Fly.io · Two Aiven PostgreSQL databases

---

## One-time Setup

### Step 1 — Deploy the backend to Fly.io

You'll need the [Fly.io CLI](https://fly.io/docs/hands-on/install-flyctl/) installed.

```bash
# Install CLI (Mac)
brew install flyctl

# Log in
fly auth login

# From the backend/ directory:
cd backend
fly launch          # follow prompts; say YES to using the existing fly.toml
                    # choose app name e.g. "cvu-slides-api"
                    # choose region closest to your team
```

After `fly launch` creates the app, set your environment variables:

```bash
fly secrets set \
  PASSWORD="CVU->CTBUHin2026" \
  CVU_DSN="postgres://avnadmin:AVNS_tnbgy7eoqDmdFg-NsLA@buildingdb-buildingdb.a.aivencloud.com:13020/defaultdb?sslmode=require" \
  GHSL_DSN="postgres://avnadmin:AVNS_6tFbHFoB3cJA1ORIbAE@vui-vui.i.aivencloud.com:15955/defaultdb?sslmode=require" \
  MYSQL_HOST="mysql.ctbuh.org" \
  MYSQL_USER="build_db_prod_RO" \
  MYSQL_PASSWORD="<mysql password>" \
  MYSQL_DATABASE="buldingdb"
```

Then deploy:

```bash
fly deploy
```

Your API will be live at `https://cvu-slides-api.fly.dev` (or whatever name you chose).  
Confirm it's running: `curl https://cvu-slides-api.fly.dev/api/health`

### Step 2 — Configure GitHub repository

1. Go to your repo → **Settings → Pages**  
   Set Source to **GitHub Actions**

2. Go to **Settings → Secrets and variables → Actions**  
   Add a **secret** named `BACKEND_URL` with your Fly.io URL:
   ```
   https://cvu-slides-api.fly.dev
   ```

3. Push to `main` — the GitHub Action builds and deploys the frontend automatically.

Your app will be live at `https://<your-org>.github.io/<repo-name>/`

---

## Daily Sync

The MySQL → PostgreSQL sync runs automatically at **03:00 UTC daily** via APScheduler inside the Fly.io container.

To trigger manually from the app: click **Sync Now** in the sidebar.  
To trigger from the CLI: `curl -X POST https://cvu-slides-api.fly.dev/api/sync -H "Authorization: Bearer <token>"`

---

## Slide Reference

| # | Slide | Always included |
|---|-------|-----------------|
| 1 | Cover (`{city}`, `{country}`, date) | ✓ |
| 2 | 50 Years of Tall Building Growth | optional |
| 3 | vs. Other Cities in Country | optional |
| 4 | Projected Tall Building Growth | optional |
| 5 | Buildings & Population Growth | optional |
| 6 | Tall Building Characteristics (pie charts) | optional |
| 7 | CVU closing slide | ✓ |

---

## Schema Verification (first run)

If charts show no data, check that column names in `backend/queries.py` match your actual database.  
Call the health endpoint to inspect:

```bash
# Check ctbuh_building columns
curl "https://cvu-slides-api.fly.dev/api/health" -H "Authorization: Bearer <token>"
```

Key column names to verify in `queries.py`:
- `height_m` — building height column
- `year_completed` — completion year column  
- `status` — status values (e.g. `'Completed'`, `'Under Construction'`)
- `function` — building use/function column
- `material` — structural material column

---

## Updating the password

```bash
fly secrets set PASSWORD="your-new-password"
```

The frontend prompts for the password at login — no redeploy needed on the frontend side.

---

## Repo structure

```
backend/
  main.py              FastAPI app + APScheduler daily sync
  db.py                Connection pools (CVU + GHSL Aiven databases)
  queries.py           All SQL for slide data + city selector
  pptx_gen.py          PPTX generator (patches chart XML + embedded Excel)
  sync_mysql_to_pg.py  MySQL → PostgreSQL sync script
  template.pptx        CVU slide template
  Dockerfile
  fly.toml

frontend/
  index.html           Complete self-contained app (Preact + Tailwind CDN)

.github/workflows/
  pages.yml            Build + deploy frontend to GitHub Pages
```
