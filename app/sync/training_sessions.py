from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List

from psycopg2.extras import RealDictCursor

from app.db.pg import get_conn
from app.gcp.inbox import post_to_gcp_inbox
from app.sync.base import SyncHandler, SyncResult


@dataclass(frozen=True)
class TrainingSessionRow:
    id: str
    sf_id: str | None
    commcare_case_id: str
    module_name: str | None
    module_number: int | None
    training_group_commcare_id: str
    training_group_name: str
    training_group_location_name: str | None
    cc_mobile_worker_group_id: str | None
    session_one_date: str | None
    session_two_date: str | None
    session_status: str | None
    project_unique_id: str
    responsible_staff_id: str | None
    responsible_staff_sf_id: str | None


def _rows_to_payload(rows: List[TrainingSessionRow]) -> Dict[str, Any]:
    project_unique_id = rows[0].project_unique_id if rows else None
    sessions: List[Dict[str, Any]] = []
    for r in rows:
        sessions.append(
            {
                "sessionId": r.sf_id or r.id,
                "trainingModuleName": r.module_name,
                "trainingModuleNumber": r.module_number,
                "commCareCaseId": r.commcare_case_id,
                "trainingGroupCommCareId": r.training_group_commcare_id,
                "trainingGroupLocationName": r.training_group_location_name,
                "ccMobileWorkerGroupId": r.cc_mobile_worker_group_id,
                "sessionStatus": 'Active',
                "sessionOneDate": r.session_one_date,
                "sessionTwoDate": r.session_two_date,
                "trainingGroupName": r.training_group_name,
                "trainingGroupResponsibleStaff":  r.responsible_staff_sf_id or r.responsible_staff_id,
            }
        )

    return {
        "source": "postgres",
        "jobType": "Training Session",
        "uniqueProjectKey": project_unique_id,
        "trainingSessions": sessions,
        "entity": "training_sessions",
    }


def _lock_and_mark_processing(limit: int) -> List[TrainingSessionRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        ts.id::text AS id,
                        ts.sf_id::text AS sf_id,
                        ts.commcare_case_id,
                        tm.module_name,
                        tm.module_number,
                        fg.commcare_case_id AS training_group_commcare_id,
                        fg.ffg_name AS training_group_name,
                        fg.responsible_staff_id::text AS responsible_staff_id,
                        l.location_name AS training_group_location_name,
                        psr.commcare_location_id AS cc_mobile_worker_group_id,
                        u.id::text AS responsible_staff_id,
                        u.sf_id::text AS responsible_staff_sf_id,
                        ts.date_session_1::text AS session_one_date,
                        ts.date_session_2::text AS session_two_date,
                        NULL::text AS session_status,
                        p.project_unique_id
                    FROM pima.training_sessions ts
                    JOIN pima.training_modules tm ON tm.id = ts.module_id
                    JOIN pima.farmer_groups fg ON fg.id = ts.farmer_group_id
                    JOIN pima.projects p ON p.id = fg.project_id
                    JOIN pima.users u ON u.id = fg.responsible_staff_id
                    LEFT JOIN pima.locations l ON l.id = fg.location_id
                    LEFT JOIN pima.project_staff_roles psr
                        ON psr.project_id = fg.project_id
                       AND psr.staff_id = fg.responsible_staff_id
                    WHERE ts.send_to_commcare = true
                      AND ts.send_to_commcare_status = 'Pending'
                    ORDER BY p.project_unique_id, ts.updated_at
                    LIMIT %s
                    FOR UPDATE OF ts SKIP LOCKED
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
                if not rows:
                    conn.commit()
                    return []

                ids = [r["id"] for r in rows]
                cur.execute(
                    """
                    UPDATE pima.training_sessions
                    SET send_to_commcare_status = 'Processing',
                        updated_at = now()
                    WHERE id = ANY(%s::uuid[])
                    """,
                    (ids,),
                )
            conn.commit()

            return [
                TrainingSessionRow(
                    id=r["id"],
                    sf_id=r.get("sf_id"),
                    commcare_case_id=r["commcare_case_id"],
                    module_name=r.get("module_name"),
                    module_number=r.get("module_number"),
                    training_group_commcare_id=r["training_group_commcare_id"],
                    training_group_name=r["training_group_name"],
                    responsible_staff_id=r.get("responsible_staff_id"),
                    responsible_staff_sf_id=r.get("responsible_staff_sf_id"),
                    training_group_location_name=r.get("training_group_location_name"),
                    cc_mobile_worker_group_id=r.get("cc_mobile_worker_group_id"),
                    session_one_date=r.get("session_one_date"),
                    session_two_date=r.get("session_two_date"),
                    session_status=r.get("session_status"),
                    project_unique_id=r["project_unique_id"],
                )
                for r in rows
            ]
        except Exception:
            conn.rollback()
            raise


def _lock_one_and_mark_processing(record_id: str) -> List[TrainingSessionRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        ts.id::text AS id,
                        ts.sf_id::text AS sf_id,
                        ts.commcare_case_id,
                        tm.module_name,
                        tm.module_number,
                        fg.commcare_case_id AS training_group_commcare_id,
                        fg.ffg_name AS training_group_name,
                        fg.responsible_staff_id::text AS responsible_staff_id,
                        l.location_name AS training_group_location_name,
                        psr.commcare_location_id AS cc_mobile_worker_group_id,
                        u.id::text AS responsible_staff_id,
                        u.sf_id::text AS responsible_staff_sf_id,
                        ts.date_session_1::text AS session_one_date,
                        ts.date_session_2::text AS session_two_date,
                        NULL::text AS session_status,
                        p.project_unique_id
                    FROM pima.training_sessions ts
                    JOIN pima.training_modules tm ON tm.id = ts.module_id
                    JOIN pima.farmer_groups fg ON fg.id = ts.farmer_group_id
                    JOIN pima.projects p ON p.id = fg.project_id
                    JOIN pima.users u ON u.id = fg.responsible_staff_id
                    LEFT JOIN pima.locations l ON l.id = fg.location_id
                    LEFT JOIN pima.project_staff_roles psr
                        ON psr.project_id = fg.project_id
                       AND psr.staff_id = fg.responsible_staff_id
                    WHERE ts.id = %s::uuid
                      AND ts.send_to_commcare = true
                      AND ts.send_to_commcare_status = 'Pending'
                    FOR UPDATE OF ts
                    """,
                    (record_id,),
                )
                row = cur.fetchone()
                if not row:
                    conn.commit()
                    return []

                cur.execute(
                    """
                    UPDATE pima.training_sessions
                    SET send_to_commcare_status = 'Processing',
                        updated_at = now()
                    WHERE id = %s::uuid
                    """,
                    (record_id,),
                )
            conn.commit()
            return [
                TrainingSessionRow(
                    id=row["id"],
                    sf_id=row.get("sf_id"),
                    commcare_case_id=row["commcare_case_id"],
                    module_name=row.get("module_name"),
                    module_number=row.get("module_number"),
                    training_group_commcare_id=row["training_group_commcare_id"],
                    training_group_name=row["training_group_name"],
                    responsible_staff_id=row.get("responsible_staff_id"),
                    responsible_staff_sf_id=row.get("responsible_staff_sf_id"),
                    training_group_location_name=row.get("training_group_location_name"),
                    cc_mobile_worker_group_id=row.get("cc_mobile_worker_group_id"),
                    session_one_date=row.get("session_one_date"),
                    session_two_date=row.get("session_two_date"),
                    session_status=row.get("session_status"),
                    project_unique_id=row["project_unique_id"],
                )
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
                UPDATE pima.training_sessions
                SET send_to_commcare_status = 'Complete',
                    send_to_commcare = false,
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
                UPDATE pima.training_sessions
                SET send_to_commcare_status = 'Pending',
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
        conn.commit()

def _mark_failed(ids: List[str]) -> None:
    if not ids:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pima.training_sessions
                SET send_to_commcare_status = 'Failed',
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
        conn.commit()

def _send_grouped(rows: List[TrainingSessionRow]) -> SyncResult:
    if not rows:
        return SyncResult(picked=0, sent=0, groups=0)

    groups: Dict[str, List[TrainingSessionRow]] = defaultdict(list)
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


class TrainingSessionsHandler(SyncHandler):
    entity = "training_sessions"
    batch_limit = 300

    def sync_batch(self) -> Dict[str, Any]:
        return _send_grouped(_lock_and_mark_processing(self.batch_limit)).as_dict()

    def sync_one(self, record_id: str) -> Dict[str, Any]:
        rows = _lock_one_and_mark_processing(record_id)
        if not rows:
            raise KeyError("not found")
        return _send_grouped(rows).as_dict()
