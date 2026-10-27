# CS5224 Lab 2 — Part 2: Twitter follow counts

A full-stack app that counts followers and followees per user in `twitter_combined.txt`,
using Spark on AWS EMR Serverless.

```
frontend (S3 static site) ──fetch──► backend (FastAPI on EC2) ──boto3──► EMR Serverless (Spark)
                                              │                                │
                                              └──── S3: input/ scripts/ output/ ┘
```

| Path | What |
|---|---|
| `analysis/follow_counts.py` | Spark job: clean edges (drop malformed rows, self-follows, duplicates), count followers/followees, write CSV |
| `analysis/test_local.py` | Runs the job locally and checks the two required users |
| `backend/app.py` | API: `POST /api/jobs`, `GET /api/jobs/{id}`, `GET /api/users?ids=…`, `GET /api/dataset`, `GET /api/health` |
| `frontend/` | Static page that triggers the job and prints `<id> has N followers` / `<id> has N followees` |
| `infra/` | IAM policy templates, EC2 bootstrap, and [setup.md](infra/setup.md) (step-by-step AWS setup) |

## Local check

```bash
pip install pyspark
python analysis/test_local.py /path/to/twitter_combined.txt
```

Expected: `214328887` → 180 followers, 214 followees; `107830991` → 180 followers, 157 followees.

## Run the backend locally against AWS

```bash
cd backend
pip install -r requirements.txt
cp .env.example .env   # fill in, then export the values
uvicorn app:app --reload --port 8000
```

Then open `frontend/index.html`. `config.js` points at `http://localhost:8000` by default.
