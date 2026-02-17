# PIMA Postgres → CommCare Sync Service

This service replaces the old Salesforce trigger callouts.

It scans Postgres for records marked to be sent to CommCare, marks them as “Processing”, packages them into the same payload shape, and sends them to the existing CommCare processor service (the one that already handles the real CommCare logic) hosted on GCP.

## What it does

For each supported entity, the service:

* Queries Postgres for records where `send_to_commcare = true` and `send_to_commcare_status = 'Pending'`.
* Locks the rows safely using `FOR UPDATE SKIP LOCKED` so concurrent runs won’t double-send the same records.
* Marks them as `Processing`.
* Builds the JSON payload expected by the CommCare processor service.
* POSTs the payload to the CommCare processor service.
* If the response is HTTP 200:

  * sets `send_to_commcare_status = 'Complete'`
  * resets `send_to_commcare = false` so the record is not re-sent
* If the response is not HTTP 200:

  * resets the record back to `Pending` so it can retry on the next run

## Entities supported

* `project_staff_roles`
* `farmer_groups`
* `training_sessions`
* `households`
* `farmers`

## API Endpoints

### Run one managed batch for every entity

`POST /sync`

Response example:

```json
{
  "totals": { "picked": 150, "sent": 150 },
  "results": {
    "project_staff_roles": { "picked": 50, "sent": 50, "groups": 2 },
    "farmer_groups": { "picked": 20, "sent": 20, "groups": 1 }
  }
}
```

### Send a single record for a specific entity

`POST /sync/{entity}/{record_id}`

Example:

`POST /sync/project_staff_roles/6c59f5f2-1d59-4c7d-8e3c-3b2be2d4e7b1`

## Environment variables

See `.env.example`. At minimum you need:

* `PG_DSN` – Postgres connection string
* `COMMCARE_PROCESSOR_URL` – CommCare processor endpoint
* `REQUEST_TIMEOUT_SECS` – request timeout when calling processor (seconds)

## Running locally

### 1) Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Set environment variables

Copy the example file:

```bash
cp .env.example .env
```

### 3) Start the server

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

### 4) Test it

Health check:

```bash
curl http://localhost:8080/health
```

Run one sync cycle (all entities):

```bash
curl -X POST http://localhost:8080/sync
```

Send one specific record:

```bash
curl -X POST http://localhost:8080/sync/project_staff_roles/<record_uuid>
```

---

## Deployment (Cloud Run + Cloud SQL private IP)

This service is designed to run on Cloud Run and connect to Cloud SQL (Postgres) over private networking.

### Prerequisites

* Cloud Run region: `europe-west1`
* Cloud SQL instance: `pima-postgres-prod` (Private IP enabled)
* VPC connector: `run-conn` in `europe-west1`
* Secrets in Secret Manager:

  * `PG_DSN`
  * `COMMCARE_PROCESSOR_URL`
  * `REQUEST_TIMEOUT_SECS`

### 1) Create / confirm VPC connector (one-time)

If you already have one (example `run-conn`), you can reuse it.

```bash
gcloud compute networks vpc-access connectors list \
  --project=pima-gcp \
  --region=europe-west1
```

### 2) Store env vars in Secret Manager (one-time)

Recommended `PG_DSN` for Cloud Run uses the Cloud SQL Unix socket:

`postgresql://USER:PASSWORD@/DBNAME?host=/cloudsql/PROJECT:REGION:INSTANCE`

For this project:

```bash
# PG_DSN
echo -n "postgresql://admin:pima2023@/pima?host=/cloudsql/pima-gcp:europe-west1:pima-postgres-prod" \
 | gcloud secrets create PG_DSN --data-file=-

# COMMCARE_PROCESSOR_URL
echo -n "https://<COMMCARE_PROCESSOR_URL>" \
 | gcloud secrets create COMMCARE_PROCESSOR_URL --data-file=-

# REQUEST_TIMEOUT_SECS
echo -n "2000" \
 | gcloud secrets create REQUEST_TIMEOUT_SECS --data-file=-
```

If the secrets already exist, add a new version instead:

```bash
echo -n "postgresql://admin:pima2023@/pima?host=/cloudsql/pima-gcp:europe-west1:pima-postgres-prod" \
 | gcloud secrets versions add PG_DSN --data-file=-
```

### 3) Service accounts and IAM (one-time)

This setup uses two service accounts:

* Runtime service account (Cloud Run runs as this): `pima-sync-run@pima-gcp.iam.gserviceaccount.com`
* CI service account (GitHub deploys with this): `pima-sync-ci@pima-gcp.iam.gserviceaccount.com`

Grant runtime SA access to Cloud SQL + Secret Manager:

```bash
gcloud projects add-iam-policy-binding pima-gcp \
  --member="serviceAccount:pima-sync-run@pima-gcp.iam.gserviceaccount.com" \
  --role="roles/cloudsql.client"

gcloud projects add-iam-policy-binding pima-gcp \
  --member="serviceAccount:pima-sync-run@pima-gcp.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

Grant CI SA permissions for deploy + build + Artifact Registry:

```bash
gcloud projects add-iam-policy-binding pima-gcp \
  --member="serviceAccount:pima-sync-ci@pima-gcp.iam.gserviceaccount.com" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding pima-gcp \
  --member="serviceAccount:pima-sync-ci@pima-gcp.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

gcloud projects add-iam-policy-binding pima-gcp \
  --member="serviceAccount:pima-sync-ci@pima-gcp.iam.gserviceaccount.com" \
  --role="roles/cloudbuild.builds.editor"

gcloud artifacts repositories add-iam-policy-binding cloud-run-source-deploy \
  --project=pima-gcp \
  --location=europe-west1 \
  --member="serviceAccount:pima-sync-ci@pima-gcp.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"
```

### 4) Deploy to Cloud Run (manual)

Cloud SQL connection name:

`pima-gcp:europe-west1:pima-postgres-prod`

```bash
gcloud run deploy pima-postgres-to-commcare-sync \
  --project=pima-gcp \
  --region=europe-west1 \
  --source=. \
  --vpc-connector=run-conn \
  --vpc-egress=private-ranges-only \
  --add-cloudsql-instances=pima-gcp:europe-west1:pima-postgres-prod \
  --set-secrets=PG_DSN=PG_DSN:latest,COMMCARE_PROCESSOR_URL=COMMCARE_PROCESSOR_URL:latest,REQUEST_TIMEOUT_SECS=REQUEST_TIMEOUT_SECS:latest \
  --timeout=600 \
  --allow-unauthenticated
```

Test:

```bash
curl https://<CLOUD_RUN_URL>/health
curl -X POST https://<CLOUD_RUN_URL>/sync
```

---

## CI/CD (deploy on push to `main`)

This repo deploys automatically to Cloud Run on every push to `main` using GitHub Actions + Workload Identity Federation (no JSON keys).

### Workload Identity Federation (one-time)

Project number: `484255681882`
Repo: `TechnoServe/pima-pg-to-commcare-sync`

Create the pool:

```bash
gcloud iam workload-identity-pools create github-pool \
  --location="global" \
  --display-name="GitHub Actions Pool"
```

Create the provider (restricted to this repo + `main`):

```bash
gcloud iam workload-identity-pools providers create-oidc github-provider \
  --location="global" \
  --workload-identity-pool="github-pool" \
  --display-name="GitHub Provider" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.ref=assertion.ref" \
  --attribute-condition="attribute.repository=='TechnoServe/pima-pg-to-commcare-sync' && attribute.ref=='refs/heads/main'"
```

Bind the repo identity to the CI service account:

```bash
gcloud iam service-accounts add-iam-policy-binding \
  pima-sync-ci@pima-gcp.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/484255681882/locations/global/workloadIdentityPools/github-pool/attribute.repository/TechnoServe/pima-pg-to-commcare-sync"
```

### GitHub Actions workflow

Add this file: `.github/workflows/deploy-cloudrun.yml`

```yaml
name: Deploy to Cloud Run (prod)

on:
  push:
    branches: ["main"]

permissions:
  contents: read
  id-token: write

env:
  PROJECT_ID: pima-gcp
  REGION: europe-west1
  SERVICE: pima-postgres-to-commcare-sync

  RUNTIME_SA: pima-sync-run@pima-gcp.iam.gserviceaccount.com
  CLOUDSQL_INSTANCE: pima-gcp:europe-west1:pima-postgres-prod
  VPC_CONNECTOR: run-conn

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Auth to Google Cloud (WIF)
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: projects/484255681882/locations/global/workloadIdentityPools/github-pool/providers/github-provider
          service_account: pima-sync-ci@pima-gcp.iam.gserviceaccount.com

      - name: Setup gcloud
        uses: google-github-actions/setup-gcloud@v2

      - name: Deploy to Cloud Run
        run: |
          gcloud run deploy "$SERVICE" \
            --project="$PROJECT_ID" \
            --region="$REGION" \
            --source="." \
            --service-account="$RUNTIME_SA" \
            --vpc-connector="$VPC_CONNECTOR" \
            --vpc-egress=private-ranges-only \
            --add-cloudsql-instances="$CLOUDSQL_INSTANCE" \
            --set-secrets=PG_DSN=PG_DSN:latest,COMMCARE_PROCESSOR_URL=COMMCARE_PROCESSOR_URL:latest,REQUEST_TIMEOUT_SECS=REQUEST_TIMEOUT_SECS:latest \
            --timeout=600 \
            --allow-unauthenticated
```

### Verify

Push to `main` and check GitHub Actions logs. A successful run will create a new Cloud Run revision.
