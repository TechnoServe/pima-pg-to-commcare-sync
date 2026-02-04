# PIMA Postgres → CommCare Sender (Cloud Run)

This service replaces the Salesforce trigger callouts. It:

- Queries Postgres for records where `send_to_commcare = true` and `send_to_commcare_status = 'Pending'`.
- Marks them `Processing` (using `FOR UPDATE SKIP LOCKED` so concurrent runs are safe).
- Builds JSON data to be sent into the commcare processor for processing.
- POSTs the payload to the CommCare processor service.
- On HTTP 200: sets `send_to_commcare_status = 'Complete'` and **resets** `send_to_commcare = false`.
- On non-200: resets status back to `Pending`.

## Send to CommCare entities
- `project_staff_roles`
- `farmer_groups` 
- `training_sessions`
- `households`
- `farmers`

## Endpoints

### Run one managed batch for every entity

`POST /sync`

Response example:
```json
{
  "totals": {"picked": 150, "sent": 150},
  "results": {
    "project_staff_roles": {"picked": 50, "sent": 50, "groups": 2},
    "farmer_groups": {"picked": 20, "sent": 20, "groups": 1}
  }
}
```

### Send a single record for a specific entity

`POST /sync/{entity}/{record_id}`


## Env vars
See `.env.example`.
