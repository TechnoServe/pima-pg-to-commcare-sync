from __future__ import annotations

from fastapi import FastAPI, HTTPException

from app.sync.registry import HANDLERS, ORDERED_ENTITIES

app = FastAPI(title="PIMA Postgres → CommCare Triggers", version="0.1.0")


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/sync")
def sync_all():
    """Runs a single managed batch for all entities.

    Intended to be called by Cloud Scheduler on a fixed interval.
    """

    results = {}
    totals = {"picked": 0, "sent": 0}

    #try:
    for entity in ORDERED_ENTITIES:
        handler = HANDLERS[entity]
        r = handler.sync_batch()
        results[entity] = r
        totals["picked"] += int(r.get("picked", 0))
        totals["sent"] += int(r.get("sent", 0))

    return {"totals": totals, "results": results}
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=f"Sync failed: {type(e).__name__}")


@app.post("/sync/{entity}/{record_id}")
def sync_one(entity: str, record_id: str):
    """Endpoint to trigger single record: send a single record for a given entity."""
    handler = HANDLERS.get(entity)
    if not handler:
        raise HTTPException(status_code=400, detail=f"Unsupported entity: {entity}")
    try:
        r = handler.sync_one(record_id)
        return {"entity": entity, **r}
    except KeyError:
        raise HTTPException(status_code=404, detail="Record not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync failed: {type(e).__name__}")
