from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List

from psycopg2.extras import RealDictCursor

from app.db.pg import get_conn
from app.gcp.inbox import post_to_gcp_inbox
from app.sync.base import SyncHandler, SyncResult


@dataclass(frozen=True)
class ProjectStaffRoleRow:
    id: str
    sf_id: str | None
    project_id: str
    tns_id: str
    role: str
    commcare_location_id: str | None
    commcare_case_id: str
    project_name: str
    project_unique_id: str
    staff_id: str | None
    staff_sf_id: str | None
    staff_first_name: str
    staff_middle_name: str | None
    staff_last_name: str
    staff_status: str | None


def _fetch_project_modules(project_id: str) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT current_previous, module_number, module_name
                FROM pima.training_modules
                WHERE project_id = %s::uuid
                """,
                (project_id,),
            )
            rows = cur.fetchall() or []

    current = next((r for r in rows if r.get("current_previous") == "Current"), None)
    previous = next((r for r in rows if r.get("current_previous") == "Previous"), None)

    return {
        "currentModule": current.get("module_number") if current else None,
        "currentModuleName": current.get("module_name") if current else None,
        "previousModule": previous.get("module_number") if previous else None,
        "previousModuleName": previous.get("module_name") if previous else None,
    }


def _rows_to_payload(rows: List[ProjectStaffRoleRow], module_info: Dict[str, Any]) -> Dict[str, Any]:
    project_unique_id = rows[0].project_unique_id if rows else None

    project_roles: List[Dict[str, Any]] = []
    for r in rows:
        staff_name = " ".join([p for p in [r.staff_first_name, r.staff_middle_name, r.staff_last_name] if p])
        project_roles.append(
            {
                "projectRoleId": r.sf_id or r.id,
                "projectRoleName": staff_name,
                "staffName": staff_name,
                "staffStatus": r.staff_status,
                "tnsId": r.tns_id,
                "commCareCaseId": r.commcare_case_id,
                "roleForCommCare": r.role,
                "ccMobileWorkerGroupId": r.commcare_location_id,
                "staffId": r.staff_sf_id or r.staff_id,
                "currentModule": module_info.get("currentModule"),
                "currentModuleName": module_info.get("currentModuleName"),
                "previousModule": module_info.get("previousModule"),
                "previousModuleName": module_info.get("previousModuleName"),
                "projectName": r.project_name,
                "projectUniqueId": r.project_unique_id,
            }
        )

    return {
        "source": "postgres",
        "jobType": "Project Role",
        "uniqueProjectKey": project_unique_id,
        "projectRoles": project_roles,
        "entity": "project_staff_roles",
    }


def _lock_and_mark_processing(limit: int) -> List[ProjectStaffRoleRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        psr.id::text AS id,
                        psr.sf_id::text AS sf_id,
                        psr.project_id::text AS project_id,
                        psr.tns_id,
                        psr.role,
                        psr.commcare_location_id,
                        psr.commcare_case_id,
                        p.project_name,
                        p.project_unique_id,
                        u.id::text AS staff_id,
                        u.sf_id::text AS staff_sf_id,
                        u.first_name AS staff_first_name,
                        u.middle_name AS staff_middle_name,
                        u.last_name AS staff_last_name,
                        u.status AS staff_status
                    FROM pima.project_staff_roles psr
                    JOIN pima.projects p ON p.id = psr.project_id
                    JOIN pima.users u ON u.id = psr.staff_id
                    WHERE psr.send_to_commcare = true
                      AND psr.send_to_commcare_status = 'Pending'
                    ORDER BY p.project_unique_id, psr.updated_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
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
                    UPDATE pima.project_staff_roles
                    SET send_to_commcare_status = 'Processing',
                        updated_at = now()
                    WHERE id = ANY(%s::uuid[])
                    """,
                    (ids,),
                )

            conn.commit()

            return [
                ProjectStaffRoleRow(
                    id=r["id"],
                    sf_id=r.get("sf_id"),
                    project_id=r["project_id"],
                    tns_id=r["tns_id"],
                    role=r["role"],
                    commcare_location_id=r.get("commcare_location_id"),
                    commcare_case_id=r["commcare_case_id"],
                    project_name=r["project_name"],
                    project_unique_id=r["project_unique_id"],
                    staff_id=r.get("staff_id"),
                    staff_sf_id=r.get("staff_sf_id"),
                    staff_first_name=r["staff_first_name"],
                    staff_middle_name=r.get("staff_middle_name"),
                    staff_last_name=r["staff_last_name"],
                    staff_status=r.get("staff_status"),
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
                UPDATE pima.project_staff_roles
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
                UPDATE pima.project_staff_roles
                SET send_to_commcare_status = 'Pending',
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
        conn.commit()


def _lock_one_and_mark_processing(record_id: str) -> List[ProjectStaffRoleRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        psr.id::text AS id,
                        psr.sf_id::text AS sf_id,
                        psr.project_id::text AS project_id,
                        psr.tns_id,
                        psr.role,
                        psr.commcare_location_id,
                        psr.commcare_case_id,
                        p.project_name,
                        p.project_unique_id,
                        u.staff_id::text AS staff_id,
                        u.sf_id::text AS staff_sf_id,
                        u.first_name AS staff_first_name,
                        u.middle_name AS staff_middle_name,
                        u.last_name AS staff_last_name,
                        u.status AS staff_status
                    FROM pima.project_staff_roles psr
                    JOIN pima.projects p ON p.id = psr.project_id
                    JOIN pima.users u ON u.id = psr.staff_id
                    WHERE psr.id = %s::uuid
                      AND psr.send_to_commcare = true
                      AND psr.send_to_commcare_status = 'Pending'
                    FOR UPDATE
                    """,
                    (record_id,),
                )
                row = cur.fetchone()
                if not row:
                    conn.commit()
                    return []

                cur.execute(
                    """
                    UPDATE pima.project_staff_roles
                    SET send_to_commcare_status = 'Processing',
                        updated_at = now()
                    WHERE id = %s::uuid
                    """,
                    (record_id,),
                )

            conn.commit()
            return [
                ProjectStaffRoleRow(
                    id=row["id"],
                    sf_id=row.get("sf_id"),
                    project_id=row["project_id"],
                    tns_id=row["tns_id"],
                    role=row["role"],
                    commcare_location_id=row.get("commcare_location_id"),
                    commcare_case_id=row["commcare_case_id"],
                    project_name=row["project_name"],
                    project_unique_id=row["project_unique_id"],
                    staff_id=row.get("staff_id"),
                    staff_sf_id=row.get("staff_sf_id"),
                    staff_first_name=row["staff_first_name"],
                    staff_middle_name=row.get("staff_middle_name"),
                    staff_last_name=row["staff_last_name"],
                    staff_status=row.get("staff_status"),
                )
            ]
        except Exception:
            conn.rollback()
            raise


def _send_grouped(rows: List[ProjectStaffRoleRow]) -> SyncResult:
    if not rows:
        return SyncResult(picked=0, sent=0, groups=0)

    groups: Dict[str, List[ProjectStaffRoleRow]] = defaultdict(list)
    for r in rows:
        groups[r.project_unique_id].append(r)

    total_sent = 0
    modules_cache: Dict[str, Dict[str, Any]] = {}

    for _, group_rows in groups.items():
        project_id = group_rows[0].project_id
        module_info = modules_cache.get(project_id)
        if module_info is None:
            module_info = _fetch_project_modules(project_id)
            modules_cache[project_id] = module_info

        payload = _rows_to_payload(group_rows, module_info)
        resp = post_to_gcp_inbox(payload)
        ok = resp.status_code == 200

        ids = [r.id for r in group_rows]
        if ok:
            _mark_done(ids)
            total_sent += len(ids)
        else:
            _mark_pending(ids)

    return SyncResult(picked=len(rows), sent=total_sent, groups=len(groups))


class ProjectStaffRolesHandler(SyncHandler):
    entity = "project_staff_roles"
    batch_limit = 500

    def sync_batch(self) -> Dict[str, Any]:
        return _send_grouped(_lock_and_mark_processing(self.batch_limit)).as_dict()

    def sync_one(self, record_id: str) -> Dict[str, Any]:
        rows = _lock_one_and_mark_processing(record_id)
        if not rows:
            raise KeyError("not found")
        return _send_grouped(rows).as_dict()