#!/bin/bash
# EC2 bootstrap for the backend (Amazon Linux 2023). Placeholders are filled in by infra/setup.md.
set -euxo pipefail

dnf install -y python3.11 python3.11-pip
mkdir -p /opt/backend
aws s3 cp --recursive s3://__DATA_BUCKET__/deploy/backend/ /opt/backend/
python3.11 -m venv /opt/backend/.venv
/opt/backend/.venv/bin/pip install -r /opt/backend/requirements.txt

cat > /opt/backend/.env <<ENV
AWS_REGION=__REGION__
DATA_BUCKET=__DATA_BUCKET__
EMR_APP_ID=__EMR_APP_ID__
EMR_ROLE_ARN=arn:aws:iam::__ACCOUNT_ID__:role/cs5224-lab2-emr-runtime
ALLOWED_ORIGINS=*
ENV

cat > /etc/systemd/system/backend.service <<UNIT
[Unit]
Description=CS5224 lab 2 backend
After=network-online.target

[Service]
WorkingDirectory=/opt/backend
EnvironmentFile=/opt/backend/.env
ExecStart=/opt/backend/.venv/bin/uvicorn app:app --host 0.0.0.0 --port 80
Restart=always

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now backend
