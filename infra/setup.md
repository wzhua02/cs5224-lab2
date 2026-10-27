# AWS setup

Builds this stack in `ap-southeast-1` (Singapore):

| Piece | AWS service | Name |
|---|---|---|
| Dataset, script, results, logs | S3 | `cs5224-lab2-data-<suffix>` |
| Spark job | EMR Serverless application | `cs5224-lab2-spark` |
| Job permissions | IAM role | `cs5224-lab2-emr-runtime` |
| Backend (FastAPI) | EC2 t3.micro + IAM role | `cs5224-lab2-backend` |
| Frontend | S3 static website | `cs5224-lab2-web-<suffix>` |

Run all commands from the repo root in **Git Bash**. Each step also says what to
screenshot for the report.

> **Cost:** S3 and t3.micro EC2 are cheap or covered by free-tier credits. EMR Serverless
> is billed per vCPU-second and per GB-second. One run of this job costs a few cents. The
> application auto-stops when idle, so it costs nothing between runs. Do step 9 when you're done.

---

## 0. Sign in and set variables

```bash
aws login --profile old        # browser sign-in; sign in as the account you want to use
export AWS_PROFILE=old         # every command below uses this profile
aws sts get-caller-identity    # confirm the right account

export AWS_REGION=ap-southeast-1
export AWS_DEFAULT_REGION=$AWS_REGION
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export SUFFIX=<something-unique>          # e.g. your student id, lowercase
export DATA_BUCKET=cs5224-lab2-data-$SUFFIX
export WEB_BUCKET=cs5224-lab2-web-$SUFFIX
export DATASET="/c/Users/Lenovo/Documents/Homework/CS_Y4S1/CS5224/Labs/twitter_combined.txt"

# Fills the __PLACEHOLDER__ values in the infra/*.json templates.
fill() {
  sed -e "s/__ACCOUNT_ID__/$ACCOUNT_ID/g" -e "s/__REGION__/$AWS_REGION/g" \
      -e "s/__DATA_BUCKET__/$DATA_BUCKET/g" -e "s/__WEB_BUCKET__/$WEB_BUCKET/g" \
      -e "s/__EMR_APP_ID__/${EMR_APP_ID:-none}/g" "$1"
}
mkdir -p infra/rendered
```

If you open a new terminal later, re-run this block. After step 3, also re-export `EMR_APP_ID`.

## 1. Data bucket: dataset and Spark script

```bash
aws s3 mb s3://$DATA_BUCKET
aws s3 cp "$DATASET" s3://$DATA_BUCKET/input/twitter_combined.txt
aws s3 cp analysis/follow_counts.py s3://$DATA_BUCKET/scripts/follow_counts.py
aws s3 ls --recursive s3://$DATA_BUCKET
```

S3 buckets block public access by default. Keep it that way for this bucket.

📸 *Screenshot:* the S3 console showing `input/twitter_combined.txt` and `scripts/follow_counts.py`.

## 2. IAM role for the Spark job (EMR Serverless runtime role)

EMR Serverless assumes this role while the job runs. The role can read the bucket
and can write only to `output/` and `logs/`.

```bash
fill infra/emr-runtime-trust.json  > infra/rendered/emr-runtime-trust.json
fill infra/emr-runtime-policy.json > infra/rendered/emr-runtime-policy.json

aws iam create-role --role-name cs5224-lab2-emr-runtime \
  --assume-role-policy-document file://infra/rendered/emr-runtime-trust.json
aws iam put-role-policy --role-name cs5224-lab2-emr-runtime --policy-name s3-access \
  --policy-document file://infra/rendered/emr-runtime-policy.json

export EMR_ROLE_ARN=arn:aws:iam::$ACCOUNT_ID:role/cs5224-lab2-emr-runtime
```

## 3. EMR Serverless application

```bash
# emr-7.5.0 (Spark 3.5) is known to work; a newer emr-7.x label from the console's
# "Release version" dropdown is fine too
export EMR_APP_ID=$(aws emr-serverless create-application \
  --name cs5224-lab2-spark --type SPARK --release-label emr-7.5.0 \
  --maximum-capacity cpu=8vCPU,memory=32GB \
  --auto-stop-configuration enabled=true,idleTimeoutMinutes=5 \
  --query applicationId --output text)
echo $EMR_APP_ID
aws emr-serverless get-application --application-id $EMR_APP_ID --query application.state
```

Wait until the state is `CREATED`. `--maximum-capacity` is a hard cost cap.

*Console alternative:* go to EMR → EMR Serverless → Get started. This creates EMR
Studio the first time. Then go to Create application, choose Type Spark, and use the
default settings.

## 4. Run the job once from the CLI (proves the analysis works on the cloud)

```bash
RUN_ID=$(date +%Y%m%d-%H%M%S)   # same format the backend uses; the newest run is loaded on startup
JOB_ID=$(aws emr-serverless start-job-run --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_ROLE_ARN --name follow-counts-$RUN_ID \
  --job-driver "{\"sparkSubmit\":{
      \"entryPoint\":\"s3://$DATA_BUCKET/scripts/follow_counts.py\",
      \"entryPointArguments\":[\"s3://$DATA_BUCKET/input/twitter_combined.txt\",\"s3://$DATA_BUCKET/output/$RUN_ID/\"],
      \"sparkSubmitParameters\":\"--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.executor.instances=2\"}}" \
  --configuration-overrides "{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"s3://$DATA_BUCKET/logs/\"}}}" \
  --query jobRunId --output text)

# Re-run until SUCCESS (about 2–4 min, most of it startup)
aws emr-serverless get-job-run --application-id $EMR_APP_ID --job-run-id $JOB_ID --query jobRun.state

aws s3 ls s3://$DATA_BUCKET/output/$RUN_ID/
aws s3 cp s3://$DATA_BUCKET/output/$RUN_ID/ - --recursive --exclude "*" --include "*.csv" \
  | grep -E '^(214328887|107830991),'
```

Expected output: `214328887,180,214` and `107830991,180,157`.

If the job fails, read the driver's stderr:
`aws s3 ls --recursive s3://$DATA_BUCKET/logs/applications/$EMR_APP_ID/jobs/$JOB_ID/`
Then `aws s3 cp <...>/SPARK_DRIVER/stderr.gz - | gunzip`.

📸 *Screenshots:*
- EMR Serverless → application → Job runs, with the run showing **Success**.
- That run → **Spark UI**, showing the stages and executors. This shows the work was distributed.
- The S3 `output/<run>/` folder showing the `part-*.csv` file and `_SUCCESS`.

## 5. IAM role for the backend EC2 instance

```bash
fill infra/backend-policy.json > infra/rendered/backend-policy.json

aws iam create-role --role-name cs5224-lab2-backend \
  --assume-role-policy-document file://infra/ec2-trust.json
aws iam put-role-policy --role-name cs5224-lab2-backend --policy-name app-access \
  --policy-document file://infra/rendered/backend-policy.json
# Lets you open a shell via Session Manager, so no SSH key is needed
aws iam attach-role-policy --role-name cs5224-lab2-backend \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore

aws iam create-instance-profile --instance-profile-name cs5224-lab2-backend
aws iam add-role-to-instance-profile --instance-profile-name cs5224-lab2-backend \
  --role-name cs5224-lab2-backend
```

## 6. Launch the backend on EC2

```bash
# Ship the backend code via the data bucket. The instance pulls it at boot.
aws s3 cp backend/app.py s3://$DATA_BUCKET/deploy/backend/app.py
aws s3 cp backend/requirements.txt s3://$DATA_BUCKET/deploy/backend/requirements.txt

# Security group in the default VPC: open port 80 for the API (8000 is often blocked by campus/corporate firewalls)
VPC_ID=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
SG_ID=$(aws ec2 create-security-group --group-name cs5224-lab2-backend \
  --description "lab2 backend API" --vpc-id $VPC_ID --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 80 --cidr 0.0.0.0/0

# MSYS_NO_PATHCONV stops Git Bash rewriting the /aws/... name into a Windows path
AMI_ID=$(MSYS_NO_PATHCONV=1 aws ssm get-parameter \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query Parameter.Value --output text)

fill infra/user-data.sh > infra/rendered/user-data.sh

# A new instance profile can take ~10 s to become usable. Retry if run-instances complains.
INSTANCE_ID=$(aws ec2 run-instances --image-id $AMI_ID --instance-type t3.micro \
  --security-group-ids $SG_ID --iam-instance-profile Name=cs5224-lab2-backend \
  --associate-public-ip-address --user-data file://infra/rendered/user-data.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=cs5224-lab2-backend}]' \
  --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --instance-ids $INSTANCE_ID
export BACKEND_HOST=$(aws ec2 describe-instances --instance-ids $INSTANCE_ID \
  --query 'Reservations[0].Instances[0].PublicDnsName' --output text)
echo "http://$BACKEND_HOST"
```

Bootstrapping takes 2–3 minutes. Then check:

```bash
curl http://$BACKEND_HOST/api/health        # should show loaded_run from step 4
curl "http://$BACKEND_HOST/api/users?ids=214328887,107830991"
```

If the backend doesn't respond, open a shell with EC2 → Instance → Connect → **Session
Manager** and run `sudo journalctl -u backend -e` or `sudo cat /var/log/cloud-init-output.log`.

To redeploy after you edit `backend/app.py`, copy the file to S3 again. Then, in a Session Manager shell, run
`sudo aws s3 cp s3://<data-bucket>/deploy/backend/app.py /opt/backend/ && sudo systemctl restart backend`.

📸 *Screenshot:* the EC2 instance running, and/or `/api/users` returning JSON in a browser.

## 7. Frontend on an S3 static website

```bash
sed -i "s#http://localhost:8000#http://$BACKEND_HOST#" frontend/config.js

aws s3 mb s3://$WEB_BUCKET
aws s3api put-public-access-block --bucket $WEB_BUCKET --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=false,RestrictPublicBuckets=false
fill infra/website-bucket-policy.json > infra/rendered/website-bucket-policy.json
aws s3api put-bucket-policy --bucket $WEB_BUCKET --policy file://infra/rendered/website-bucket-policy.json
aws s3 website s3://$WEB_BUCKET --index-document index.html
aws s3 sync frontend/ s3://$WEB_BUCKET/

echo "http://$WEB_BUCKET.s3-website-$AWS_REGION.amazonaws.com"
```

If `put-bucket-policy` returns AccessDenied, your account blocks public buckets at the
account level. Turn that off under S3 → *Block Public Access settings for this account*.
Or skip S3 hosting and open `frontend/index.html` locally. It still calls the cloud backend.

Optional: restrict CORS to the site. Set `ALLOWED_ORIGINS=http://$WEB_BUCKET.s3-website-$AWS_REGION.amazonaws.com`
in `/opt/backend/.env`, then run `sudo systemctl restart backend`.

## 8. End-to-end run

1. Open the website URL.
2. Click **Run analysis**. The backend calls EMR Serverless `StartJobRun`, and the page polls the job until it shows `SUCCESS`.
3. Click **Show statistics** with `214328887, 107830991`:

```
214328887 has 180 followers
214328887 has 214 followees
107830991 has 180 followers
107830991 has 157 followees
```

📸 *Screenshots:* the page with these results (include the URL bar), and the new job run
in the EMR Serverless console.

## 9. Teardown

```bash
aws ec2 terminate-instances --instance-ids $INSTANCE_ID
aws ec2 wait instance-terminated --instance-ids $INSTANCE_ID
aws ec2 delete-security-group --group-id $SG_ID

aws emr-serverless stop-application --application-id $EMR_APP_ID
aws emr-serverless delete-application --application-id $EMR_APP_ID

aws s3 rb s3://$WEB_BUCKET --force
aws s3 rb s3://$DATA_BUCKET --force

aws iam remove-role-from-instance-profile --instance-profile-name cs5224-lab2-backend --role-name cs5224-lab2-backend
aws iam delete-instance-profile --instance-profile-name cs5224-lab2-backend
aws iam detach-role-policy --role-name cs5224-lab2-backend --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam delete-role-policy --role-name cs5224-lab2-backend --policy-name app-access
aws iam delete-role --role-name cs5224-lab2-backend
aws iam delete-role-policy --role-name cs5224-lab2-emr-runtime --policy-name s3-access
aws iam delete-role --role-name cs5224-lab2-emr-runtime
```

The first time you open EMR Studio in the console, it may create an S3 bucket and roles. Delete those too if you don't need them.
