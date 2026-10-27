"""Backend API: manages the dataset in S3, runs the Spark job on EMR Serverless,
and serves follower/followee counts to the frontend.

Configuration is read from environment variables (see backend/.env.example).
"""
import csv
import io
import os
import threading
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
BUCKET = os.environ["DATA_BUCKET"]
EMR_APP_ID = os.environ["EMR_APP_ID"]
EMR_ROLE_ARN = os.environ["EMR_ROLE_ARN"]
INPUT_KEY = os.environ.get("INPUT_KEY", "input/twitter_combined.txt")
SCRIPT_KEY = os.environ.get("SCRIPT_KEY", "scripts/follow_counts.py")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")
# Optional local copy of the dataset, uploaded if the S3 input is missing.
LOCAL_DATASET = os.environ.get("LOCAL_DATASET")
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")

JOB_NAME_PREFIX = "follow-counts-"

s3 = boto3.client("s3", region_name=REGION)
emr = boto3.client("emr-serverless", region_name=REGION)

app = FastAPI(title="Twitter follow counts")
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_methods=["GET", "POST"], allow_headers=["*"])

_lock = threading.Lock()
_results: dict[str, tuple[int, int]] = {}
_loaded_run: str | None = None


def _s3_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def _ensure_dataset() -> None:
    if _s3_exists(INPUT_KEY):
        return
    if not LOCAL_DATASET or not Path(LOCAL_DATASET).is_file():
        raise HTTPException(409, f"s3://{BUCKET}/{INPUT_KEY} is missing and no LOCAL_DATASET to upload")
    s3.upload_file(LOCAL_DATASET, BUCKET, INPUT_KEY)


def _run_prefix(run_id: str) -> str:
    return f"{OUTPUT_PREFIX}{run_id}/"


def _load_run(run_id: str) -> bool:
    """Load the CSV written by a finished run into memory. Returns False if not ready."""
    global _results, _loaded_run
    prefix = _run_prefix(run_id)
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])]
    csv_keys = [k for k in keys if k.endswith(".csv")]
    if prefix + "_SUCCESS" not in keys or not csv_keys:
        return False

    results: dict[str, tuple[int, int]] = {}
    for key in csv_keys:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
        for row in csv.DictReader(io.StringIO(body)):
            results[row["user_id"]] = (int(row["followers"]), int(row["followees"]))

    with _lock:
        _results, _loaded_run = results, run_id
    return True


def _load_latest() -> None:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=OUTPUT_PREFIX, Delimiter="/")
    run_ids = sorted((p["Prefix"][len(OUTPUT_PREFIX):].rstrip("/") for p in resp.get("CommonPrefixes", [])), reverse=True)
    for run_id in run_ids:
        if _load_run(run_id):
            return


@app.on_event("startup")
def startup() -> None:
    _load_latest()


@app.get("/api/health")
def health():
    return {"status": "ok", "loaded_run": _loaded_run, "users": len(_results)}


@app.get("/api/dataset")
def dataset():
    try:
        head = s3.head_object(Bucket=BUCKET, Key=INPUT_KEY)
    except ClientError:
        return {"uri": f"s3://{BUCKET}/{INPUT_KEY}", "exists": False}
    return {
        "uri": f"s3://{BUCKET}/{INPUT_KEY}",
        "exists": True,
        "size_bytes": head["ContentLength"],
        "last_modified": head["LastModified"].isoformat(),
    }


@app.post("/api/jobs")
def start_job():
    _ensure_dataset()
    run_id = time.strftime("%Y%m%d-%H%M%S")
    resp = emr.start_job_run(
        applicationId=EMR_APP_ID,
        executionRoleArn=EMR_ROLE_ARN,
        # The run id is encoded in the name so job status can be mapped back to
        # its output prefix without keeping server-side state.
        name=JOB_NAME_PREFIX + run_id,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"s3://{BUCKET}/{SCRIPT_KEY}",
                "entryPointArguments": [f"s3://{BUCKET}/{INPUT_KEY}", f"s3://{BUCKET}/{_run_prefix(run_id)}"],
                "sparkSubmitParameters": (
                    "--conf spark.driver.cores=1 --conf spark.driver.memory=2g "
                    "--conf spark.executor.cores=1 --conf spark.executor.memory=2g "
                    "--conf spark.executor.instances=2 --conf spark.dynamicAllocation.maxExecutors=4"
                ),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": f"s3://{BUCKET}/logs/"}}
        },
    )
    return {"job_id": resp["jobRunId"], "run_id": run_id}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    try:
        run = emr.get_job_run(applicationId=EMR_APP_ID, jobRunId=job_id)["jobRun"]
    except ClientError as e:
        raise HTTPException(404, e.response["Error"]["Message"])

    run_id = run["name"].removeprefix(JOB_NAME_PREFIX)
    if run["state"] == "SUCCESS" and _loaded_run != run_id:
        _load_run(run_id)
    return {
        "job_id": job_id,
        "state": run["state"],
        "details": run.get("stateDetails", ""),
        "results_loaded": _loaded_run == run_id,
    }


@app.get("/api/users")
def users(ids: str = Query(..., description="Comma-separated user IDs")):
    user_ids = [i.strip() for i in ids.split(",") if i.strip()]
    if not user_ids or not all(i.isdigit() for i in user_ids):
        raise HTTPException(400, "ids must be a comma-separated list of numeric user IDs")
    if not _results:
        raise HTTPException(503, "No results loaded yet; run the analysis job first")

    out = []
    for uid in user_ids:
        followers, followees = _results.get(uid, (0, 0))
        out.append({"user_id": uid, "followers": followers, "followees": followees, "found": uid in _results})
    return {"run_id": _loaded_run, "users": out}
