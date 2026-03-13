from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List

from psycopg2.extras import RealDictCursor

from app.db.pg import get_conn
from app.gcp.inbox import post_to_gcp_inbox
from app.sync.base import SyncHandler, SyncResult


@dataclass(frozen=True)
class FarmerGroupRow:
    id: str
    sf_id: str | None
    tns_id: str
    commcare_case_id: str
    ffg_name: str
    group_status: str
    project_id: str
    project_sf_id: str | None
    project_name: str
    project_unique_id: str
    location_name: str | None
    location_id: str
    staff_role_id: str | None
    staff_sf_id: str | None
    cc_mobile_worker_group_id: str | None
    household_count: int | None
    focal_farmer_id: str | None = None
    assistant_focal_farmer_id: str | None = None
    focal_farmer_sf_id: str | None = None
    assistant_focal_farmer_sf_id: str | None = None


def _rows_to_payload(rows: List[FarmerGroupRow]) -> Dict[str, Any]:
    project_unique_id = rows[0].project_unique_id if rows else None
    training_groups: List[Dict[str, Any]] = []
    for r in rows:
        training_groups.append(
            {
                "trainingGroupId": r.sf_id or r.id,
                "trainingGroupName": r.ffg_name,
                "project": r.project_sf_id or r.project_id,
                "projectName": r.project_name,
                "projectUniqueId": r.project_unique_id,
                "type": None,
                "description": None,
                "tnsId": r.tns_id,
                "commCareCaseId": r.commcare_case_id,
                "locationName": r.location_name,
                # "market": None,
                "groupStatus": r.group_status,
                "projectLocationId": r.location_id,
                "ccMobileWorkerGroupId": r.cc_mobile_worker_group_id,
                "staffId": r.staff_sf_id or r.staff_role_id, # Confirm whether project role or staff id is more useful here
                # "maleGuestAttendance": None, NOT SURE WHY WE NEEED THIS
                # "measurementGroup": None, NO MEASUREMENT GROUPS IN NEW DB
                # "cooperative": None, MIGHT NOT BE TRACKING THIS
                "cooperativeName": None,
                "householdCounter": r.household_count,
                "focalFarmerId": r.focal_farmer_sf_id or r.focal_farmer_id,
                "assistantFocalFarmerId": r.assistant_focal_farmer_sf_id or r.assistant_focal_farmer_id,
                
                
                
            }
        )

    return {
        "source": "postgres",
        "jobType": "Training Group",
        "uniqueProjectKey": project_unique_id,
        "trainingGroups": training_groups,
        "entity": "farmer_groups",
    }


def _lock_and_mark_processing(limit: int) -> List[FarmerGroupRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH locked AS (
                        SELECT fg.id
                        FROM pima.farmer_groups fg
                        WHERE fg.send_to_commcare = true
                          AND fg.send_to_commcare_status = 'Pending'
                        ORDER BY fg.updated_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    ),
                    updated AS (
                        UPDATE pima.farmer_groups fg
                        SET send_to_commcare_status = 'Processing',
                            updated_at = now()
                        FROM locked
                        WHERE fg.id = locked.id
                        RETURNING fg.*
                    )
                    SELECT
                        u.id::text AS id,
                        u.sf_id::text AS sf_id,
                        u.tns_id,
                        u.commcare_case_id,
                        u.ffg_name,
                        u.status AS group_status,
                        ff.id::text AS focal_farmer_id,
                        aff.id::text AS assistant_focal_farmer_id,
                        ff.sf_id::text AS focal_farmer_sf_id,
                        aff.sf_id::text AS assistant_focal_farmer_sf_id,
                        p.id AS project_id,
                        p.sf_id::text AS project_sf_id,
                        p.project_name,
                        p.project_unique_id,
                        l.location_name,
                        u.location_id::text AS location_id,
                        psr.id::text AS staff_role_id,
                        psr.sf_id::text AS staff_sf_id,
                        psr.commcare_location_id AS cc_mobile_worker_group_id,
                        (
                          SELECT count(*)
                          FROM pima.households h
                          WHERE h.farmer_group_id = u.id
                        ) AS household_count
                    FROM updated u
                    JOIN pima.projects p ON p.id = u.project_id
                    LEFT JOIN pima.locations l ON l.id = u.location_id
                    LEFT JOIN pima.project_staff_roles psr
                    LEFT JOIN pima.farmers ff ON ff.id = u.focal_farmer_id
                    LEFT JOIN pima.farmers aff ON aff.id = u.assistant_focal_farmer_id
                        ON psr.project_id = u.project_id
                       AND psr.staff_id = u.responsible_staff_id
                    ORDER BY p.project_unique_id, u.updated_at
                    """,
                    (limit,),
                )

                rows = cur.fetchall()
                conn.commit()

                if not rows:
                    return []

                return [
                    FarmerGroupRow(
                        id=r["id"],
                        sf_id=r.get("sf_id"),
                        tns_id=r["tns_id"],
                        commcare_case_id=r["commcare_case_id"],
                        ffg_name=r["ffg_name"],
                        group_status=r["group_status"],
                        project_id=r["project_id"],
                        project_sf_id=r.get("project_sf_id"),
                        project_name=r["project_name"],
                        project_unique_id=r["project_unique_id"],
                        location_name=r.get("location_name"),
                        location_id=r["location_id"],
                        staff_role_id=r.get("staff_role_id"),
                        staff_sf_id=r.get("staff_sf_id"),
                        cc_mobile_worker_group_id=r.get("cc_mobile_worker_group_id"),
                        household_count=r["household_count"],  
                        focal_farmer_id=r.get("focal_farmer_id"),
                        assistant_focal_farmer_id=r.get("assistant_focal_farmer_id"),
                        focal_farmer_sf_id=r.get("focal_farmer_sf_id"),
                        assistant_focal_farmer_sf_id=r.get("assistant_focal_farmer_sf_id"),
                    )
                    for r in rows
                ]

        except Exception:
            conn.rollback()
            raise
        
        
# psycopg2.errors.UndefinedTable: invalid reference to FROM-clause entry for table "u"

# at .execute ( /usr/local/lib/python3.11/site-packages/psycopg2/extras.py:236 )
# at ._lock_and_mark_processing ( /app/app/sync/farmer_groups.py:86 )
# at .sync_batch ( /app/app/sync/farmer_groups.py:360 )
# at .sync_all ( /app/app/main.py:28 )
# at .run ( /usr/local/lib/python3.11/site-packages/anyio/_backends/_asyncio.py:986 )
# at .run_sync_in_worker_thread ( /usr/local/lib/python3.11/site-packages/anyio/_backends/_asyncio.py:2502 )
# at .run_sync ( /usr/local/lib/python3.11/site-packages/anyio/to_thread.py:63 )
# at .run_in_threadpool ( /usr/local/lib/python3.11/site-packages/starlette/concurrency.py:39 )
# at .run_endpoint_function ( /usr/local/lib/python3.11/site-packages/fastapi/routing.py:214 )
# at .app ( /usr/local/lib/python3.11/site-packages/fastapi/routing.py:301 )
# at .app ( /usr/local/lib/python3.11/site-packages/starlette/routing.py:73 )
# at .wrapped_app ( /usr/local/lib/python3.11/site-packages/starlette/_exception_handler.py:42 )
# at .wrapped_app ( /usr/local/lib/python3.11/site-packages/starlette/_exception_handler.py:53 )
# at .app ( /usr/local/lib/python3.11/site-packages/starlette/routing.py:76 )
# at .handle ( /usr/local/lib/python3.11/site-packages/starlette/routing.py:288 )
# at .app ( /usr/local/lib/python3.11/site-packages/starlette/routing.py:735 )
# at .__call__ ( /usr/local/lib/python3.11/site-packages/starlette/routing.py:715 )
# at .wrapped_app ( /usr/local/lib/python3.11/site-packages/starlette/_exception_handler.py:42 )
# at .wrapped_app ( /usr/local/lib/python3.11/site-packages/starlette/_exception_handler.py:53 )
# at .__call__ ( /usr/local/lib/python3.11/site-packages/starlette/middleware/exceptions.py:62 )
# at .__call__ ( /usr/local/lib/python3.11/site-packages/starlette/middleware/errors.py:165 )
# at .__call__ ( /usr/local/lib/python3.11/site-packages/starlette/middleware/errors.py:187 )
# at .__call__ ( /usr/local/lib/python3.11/site-packages/starlette/applications.py:113 )
# at .__call__ ( /usr/local/lib/python3.11/site-packages/fastapi/applications.py:1054 )
# at .__call__ ( /usr/local/lib/python3.11/site-packages/uvicorn/middleware/proxy_headers.py:70 )
# at .run_asgi ( /usr/local/lib/python3.11/site-packages/uvicorn/protocols/http/httptools_impl.py:401 )


def _lock_one_and_mark_processing(record_id: str) -> List[FarmerGroupRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH locked AS (
                        SELECT fg.id
                        FROM pima.farmer_groups fg
                        WHERE fg.id = %s::uuid
                          AND fg.send_to_commcare = true
                          AND fg.send_to_commcare_status = 'Pending'
                        FOR UPDATE
                    ),
                    updated AS (
                        UPDATE pima.farmer_groups fg
                        SET send_to_commcare_status = 'Processing',
                            updated_at = now()
                        FROM locked
                        WHERE fg.id = locked.id
                        RETURNING fg.*
                    )
                    SELECT
                        u.id::text AS id,
                        u.sf_id::text AS sf_id,
                        u.tns_id,
                        u.commcare_case_id,
                        u.ffg_name,
                        u.status AS group_status,
                        ff.id::text AS focal_farmer_id,
                        aff.id::text AS assistant_focal_farmer_id,
                        ff.sf_id::text AS focal_farmer_sf_id,
                        aff.sf_id::text AS assistant_focal_farmer_sf_id,
                        p.id AS project_id,
                        p.sf_id::text AS project_sf_id,
                        p.project_name,
                        p.project_unique_id,
                        l.location_name,
                        u.location_id::text AS location_id,
                        psr.id::text AS staff_role_id,
                        psr.sf_id::text AS staff_sf_id,
                        psr.commcare_location_id AS cc_mobile_worker_group_id,
                        (
                          SELECT count(*)
                          FROM pima.households h
                          WHERE h.farmer_group_id = u.id
                        ) AS household_count
                    FROM updated u
                    JOIN pima.projects p ON p.id = u.project_id
                    LEFT JOIN pima.locations l ON l.id = u.location_id
                    LEFT JOIN pima.project_staff_roles psr
                    LEFT JOIN pima.farmers ff ON ff.id = u.focal_farmer_id
                    LEFT JOIN pima.farmers aff ON aff.id = u.assistant_focal_farmer_id
                        ON psr.project_id = u.project_id
                       AND psr.staff_id = u.responsible_staff_id
                    ORDER BY p.project_unique_id, u.updated_at
                    """,
                    (record_id,),
                )

                rows = cur.fetchall()
                conn.commit()

                if not rows:
                    return []

                return [
                    FarmerGroupRow(
                        id=r["id"],
                        sf_id=r.get("sf_id"),
                        tns_id=r["tns_id"],
                        commcare_case_id=r["commcare_case_id"],
                        ffg_name=r["ffg_name"],
                        group_status=r["group_status"],
                        project_id=r["project_id"],
                        project_sf_id=r.get("project_sf_id"),
                        project_name=r["project_name"],
                        project_unique_id=r["project_unique_id"],
                        location_name=r.get("location_name"),
                        location_id=r["location_id"],
                        staff_role_id=r.get("staff_role_id"),
                        staff_sf_id=r.get("staff_sf_id"),
                        cc_mobile_worker_group_id=r.get("cc_mobile_worker_group_id"),
                        household_count=r["household_count"],
                        focal_farmer_id=r.get("focal_farmer_id"),
                        assistant_focal_farmer_id=r.get("assistant_focal_farmer_id"),
                        focal_farmer_sf_id=r.get("focal_farmer_sf_id"),
                        assistant_focal_farmer_sf_id=r.get("assistant_focal_farmer_sf_id"),
                    )
                    for r in rows
                ]

        except Exception:
            conn.rollback()
            raise


def _mark_done(ids: List[str]) -> None:
    if not ids:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pima.farmer_groups
                SET send_to_commcare_status = 'Complete',
                    send_to_commcare = false,
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
        conn.commit()

# Mark records as failed on failure
def _mark_failed(ids: List[str]) -> None:
    if not ids:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pima.farmer_groups
                SET send_to_commcare_status = 'Failed',
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
        conn.commit()

def _mark_pending(ids: List[str]) -> None:
    if not ids:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pima.farmer_groups
                SET send_to_commcare_status = 'Pending',
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
        conn.commit()


# Send grouped data by project_unique_id
def _send_grouped(rows: List[FarmerGroupRow]) -> SyncResult:
    if not rows:
        return SyncResult(picked=0, sent=0, groups=0)

    groups: Dict[str, List[FarmerGroupRow]] = defaultdict(list)
    for r in rows:
        groups[r.project_unique_id].append(r)

    total_sent = 0
    for _, group_rows in groups.items():
        payload = _rows_to_payload(group_rows)
        resp = post_to_gcp_inbox(payload)
        ok = resp.status_code == 200

        ids = [r.id for r in group_rows]
        if ok:
            _mark_done(ids)
            total_sent += len(ids)
        else:
            _mark_failed(ids)

    return SyncResult(picked=len(rows), sent=total_sent, groups=len(groups))


class FarmerGroupsHandler(SyncHandler):
    entity = "farmer_groups"
    batch_limit = 500

    # Executes a batch sync for farmer groups.
    def sync_batch(self) -> Dict[str, Any]:
        return _send_grouped(_lock_and_mark_processing(self.batch_limit)).as_dict()

    # Executes a sync for a single farmer group by record ID.
    def sync_one(self, record_id: str) -> Dict[str, Any]:
        rows = _lock_one_and_mark_processing(record_id)
        if not rows:
            raise KeyError("not found")
        return _send_grouped(rows).as_dict()
