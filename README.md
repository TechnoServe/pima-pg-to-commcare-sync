# PIMA Postgres → CommCare Sync Service

This service replaces the old Salesforce trigger callouts.

It scans Postgres for records marked to be sent to CommCare, marks them as “Processing”, packages them into the same payload shape, and sends them to the existing CommCare processor service (the one that already handles the real CommCare logic) hosted on GCP.

## What it does

For each supported entity, the service:

- Queries Postgres for records where `send_to_commcare = true` and `send_to_commcare_status = 'Pending'`.
- Locks the rows safely using `FOR UPDATE SKIP LOCKED` so concurrent runs won’t double-send the same records.
- Marks them as `Processing`.
- Builds the JSON payload expected by the CommCare processor service.
- POSTs the payload to the CommCare processor service.
- If the response is HTTP 200:
  - sets `send_to_commcare_status = 'Complete'`
  - resets `send_to_commcare = false` so the record is not re-sent
- If the response is not HTTP 200:
  - resets the record back to `Pending` so it can retry on the next run

## Entities supported

- `project_staff_roles`
- `farmer_groups`
- `training_sessions`
- `households`
- `farmers`

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
````

### Send a single record for a specific entity

`POST /sync/{entity}/{record_id}`

Example:

`POST /sync/project_staff_roles/6c59f5f2-1d59-4c7d-8e3c-3b2be2d4e7b1`

## Environment variables

See `.env.example`. At minimum you need:

* `PG_DSN` – Postgres connection string
* `COMMCARE_PROCESSOR_URL` (commcare processor endpoint)


## Running locally

### 1) Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Set environment variables

copy the example file

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
