from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List

from psycopg2.extras import RealDictCursor

from app.db.pg import get_conn
from app.gcp.inbox import post_to_gcp_inbox
from app.sync.base import SyncHandler, SyncResult


@dataclass(frozen=True)
class HouseholdRow:
    id: str
    sf_id: str | None
    household_name: str
    number_of_members: int
    tns_id: str
    commcare_case_id: str
    household_status: str | None
    fv_aa_visited: bool | None
    fv_aa_sampled: bool | None
    fv_aa_current_sampling_round: int | None
    cc_mobile_worker_group_id: str | None
    training_group_id: str
    training_group_sf_id: str | None
    training_group_name: str
    module_name: str | None
    module_number: int | None
    project_unique_id: str
    household_participants: str | None


def _rows_to_payload(rows: List[HouseholdRow]) -> Dict[str, Any]:
    project_unique_id = rows[0].project_unique_id if rows else None
    households: List[Dict[str, Any]] = []
    for r in rows:
        if r.number_of_members == 0:
            continue    
        
        households.append(
            {
                "householdId": r.sf_id or r.id,
                "householdName": r.household_name,
                "numberOfMembers": r.number_of_members,
                "tnsId": r.tns_id,
                "fvAAVisited": "Yes" if r.fv_aa_visited else "No",
                "fvAASampled": "Yes",
                "fvAACurrentSamplingRound": r.fv_aa_current_sampling_round,
                "householdStatus": r.household_status,
                "commCareCaseId": r.sf_id or r.id,
                "ccMobileWorkerGroupId": r.cc_mobile_worker_group_id,
                "trainingGroupId": r.training_group_id,
                "trainingGroupName": r.training_group_name,
                "projectUniqueId": r.project_unique_id,
                "householdParticipants": r.household_participants,
                "moduleName": r.module_name,
                "moduleNumber": r.module_number,
            }
        )

    return {
        "source": "postgres",
        "jobType": "Household Sampling",
        "uniqueProjectKey": project_unique_id,
        "households": households,
        "entity": "households",
    }


def _lock_and_mark_processing(limit: int) -> List[HouseholdRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH locked AS (
                        SELECT h.id
                        FROM pima.households h
                        WHERE h.send_to_commcare = true
                          AND h.send_to_commcare_status = 'Pending'
                        ORDER BY h.updated_at
                        LIMIT %s
                        FOR UPDATE OF h SKIP LOCKED
                    ),
                    updated AS (
                        UPDATE pima.households h
                        SET send_to_commcare_status = 'Processing',
                            updated_at = now()
                        FROM locked
                        WHERE h.id = locked.id
                        RETURNING h.*
                    )
                    SELECT
                        u.id::text AS id,
                        u.sf_id::text AS sf_id,
                        u.household_name,
                        COALESCE(cnt.member_count, 0) AS number_of_members,
                        u.tns_id,
                        u.commcare_case_id,
                        u.status AS household_status,
                        u.visited_for_fv_aa AS fv_aa_visited,
                        u.sampled_for_fv_aa AS fv_aa_sampled,
                        u.fv_aa_sampling_round AS fv_aa_current_sampling_round,
                        psr.commcare_location_id AS cc_mobile_worker_group_id,
                        fg.id::text AS training_group_id,
                        fg.sf_id::text AS training_group_sf_id,
                        fg.ffg_name AS training_group_name,
                        cm.module_name,
                        cm.module_number,
                        p.project_unique_id,
                        names.household_participants
                    FROM updated u
                    JOIN pima.farmer_groups fg ON fg.id = u.farmer_group_id
                    JOIN pima.projects p ON p.id = fg.project_id
                    LEFT JOIN pima.project_staff_roles psr
                        ON psr.project_id = fg.project_id
                       AND psr.staff_id = fg.responsible_staff_id
                    LEFT JOIN LATERAL (
                        SELECT COUNT(*)::int AS member_count
                        FROM pima.farmers f
                        WHERE f.household_id = u.id
                          AND f.is_deleted = false
                    ) cnt ON true
                    LEFT JOIN LATERAL (
                        SELECT
                            CASE
                                WHEN array_length(nms, 1) = 1 THEN nms[1]
                                WHEN array_length(nms, 1) = 2 THEN nms[1] || ' & ' || nms[2]
                                ELSE NULL
                            END AS household_participants
                        FROM (
                            SELECT array_agg(full_name) AS nms
                            FROM (
                                SELECT (f.first_name) AS full_name
                                FROM pima.farmers f
                                WHERE f.household_id = u.id
                                  AND f.is_deleted = false
                                ORDER BY f.is_primary_household_member DESC, f.updated_at DESC
                                LIMIT 2
                            ) x
                        ) y
                    ) names ON true
                    LEFT JOIN LATERAL (
                        SELECT tm.module_name, tm.module_number
                        FROM pima.training_modules tm
                        WHERE tm.project_id = fg.project_id
                          AND tm.current_module = true
                          AND tm.is_deleted = false
                        ORDER BY tm.updated_at DESC
                        LIMIT 1
                    ) cm ON true
                    ORDER BY p.project_unique_id, u.updated_at
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
                conn.commit()

                if not rows:
                    return []

                return [
                    HouseholdRow(
                        id=r["id"],
                        sf_id=r.get("sf_id"),
                        household_name=r["household_name"],
                        number_of_members=r["number_of_members"],
                        tns_id=r["tns_id"],
                        commcare_case_id=r["commcare_case_id"],
                        household_status=r.get("household_status"),
                        fv_aa_visited=r.get("fv_aa_visited"),
                        fv_aa_sampled=r.get("fv_aa_sampled"),
                        fv_aa_current_sampling_round=r.get("fv_aa_current_sampling_round"),
                        cc_mobile_worker_group_id=r.get("cc_mobile_worker_group_id"),
                        training_group_id=r["training_group_id"],
                        training_group_name=r["training_group_name"],
                        training_group_sf_id=r.get("training_group_sf_id"),
                        module_name=r.get("module_name"),
                        module_number=r.get("module_number"),
                        project_unique_id=r["project_unique_id"],
                        household_participants=r.get("household_participants"),
                    )
                    for r in rows
                ]
        except Exception:
            conn.rollback()
            raise


def _lock_one_and_mark_processing(record_id: str) -> List[HouseholdRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH locked AS (
                        SELECT h.id
                        FROM pima.households h
                        WHERE h.id = %s::uuid
                          AND h.send_to_commcare = true
                          AND h.send_to_commcare_status = 'Pending'
                        FOR UPDATE OF h
                    ),
                    updated AS (
                        UPDATE pima.households h
                        SET send_to_commcare_status = 'Processing',
                            updated_at = now()
                        FROM locked
                        WHERE h.id = locked.id
                        RETURNING h.*
                    )
                    SELECT
                        u.id::text AS id,
                        u.sf_id::text AS sf_id,
                        u.household_name,
                        COALESCE(cnt.member_count, 0) AS number_of_members,
                        u.tns_id,
                        u.commcare_case_id,
                        u.status AS household_status,
                        u.visited_for_fv_aa AS fv_aa_visited,
                        u.sampled_for_fv_aa AS fv_aa_sampled,
                        u.fv_aa_sampling_round AS fv_aa_current_sampling_round,
                        psr.commcare_location_id AS cc_mobile_worker_group_id,
                        fg.id::text AS training_group_id,
                        fg.sf_id::text AS training_group_sf_id,
                        fg.ffg_name AS training_group_name,
                        cm.module_name,
                        cm.module_number,
                        p.project_unique_id,
                        names.household_participants
                    FROM updated u
                    JOIN pima.farmer_groups fg ON fg.id = u.farmer_group_id
                    JOIN pima.projects p ON p.id = fg.project_id
                    LEFT JOIN pima.project_staff_roles psr
                        ON psr.project_id = fg.project_id
                       AND psr.staff_id = fg.responsible_staff_id
                    LEFT JOIN LATERAL (
                        SELECT COUNT(*)::int AS member_count
                        FROM pima.farmers f
                        WHERE f.household_id = u.id
                          AND f.is_deleted = false
                    ) cnt ON true
                    LEFT JOIN LATERAL (
                        SELECT
                            CASE
                                WHEN array_length(nms, 1) = 1 THEN nms[1]
                                WHEN array_length(nms, 1) = 2 THEN nms[1] || ' & ' || nms[2]
                                ELSE NULL
                            END AS household_participants
                        FROM (
                            SELECT array_agg(full_name) AS nms
                            FROM (
                                SELECT (f.first_name) AS full_name
                                FROM pima.farmers f
                                WHERE f.household_id = u.id
                                  AND f.is_deleted = false
                                ORDER BY f.is_primary_household_member DESC, f.updated_at DESC
                                LIMIT 2
                            ) x
                        ) y
                    ) names ON true
                    LEFT JOIN LATERAL (
                        SELECT tm.module_name, tm.module_number
                        FROM pima.training_modules tm
                        WHERE tm.project_id = fg.project_id
                          AND tm.current_module = true
                          AND tm.is_deleted = false
                        ORDER BY tm.updated_at DESC
                        LIMIT 1
                    ) cm ON true
                    ORDER BY p.project_unique_id, u.updated_at
                    """,
                    (record_id,),
                )
                rows = cur.fetchall()
                conn.commit()

                if not rows:
                    return []

                return [
                    HouseholdRow(
                        id=r["id"],
                        sf_id=r.get("sf_id"),
                        household_name=r["household_name"],
                        number_of_members=r["number_of_members"],
                        tns_id=r["tns_id"],
                        commcare_case_id=r["commcare_case_id"],
                        household_status=r.get("household_status"),
                        fv_aa_visited=r.get("fv_aa_visited"),
                        fv_aa_sampled=r.get("fv_aa_sampled"),
                        fv_aa_current_sampling_round=r.get("fv_aa_current_sampling_round"),
                        cc_mobile_worker_group_id=r.get("cc_mobile_worker_group_id"),
                        training_group_id=r["training_group_id"],
                        training_group_sf_id=r.get("training_group_sf_id"),
                        training_group_name=r["training_group_name"],
                        module_name=r.get("module_name"),
                        module_number=r.get("module_number"),
                        project_unique_id=r["project_unique_id"],
                        household_participants=r.get("household_participants"),
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
                UPDATE pima.households
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
                UPDATE pima.households
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
                UPDATE pima.households
                SET send_to_commcare_status = 'Failed',
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
        conn.commit()
        
def _send_grouped(rows: List[HouseholdRow]) -> SyncResult:
    if not rows:
        return SyncResult(picked=0, sent=0, groups=0)

    groups: Dict[str, List[HouseholdRow]] = defaultdict(list)
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


class HouseholdsHandler(SyncHandler):
    entity = "households"
    batch_limit = 200

    def sync_batch(self) -> Dict[str, Any]:
        return _send_grouped(_lock_and_mark_processing(self.batch_limit)).as_dict()

    def sync_one(self, record_id: str) -> Dict[str, Any]:
        rows = _lock_one_and_mark_processing(record_id)
        if not rows:
            raise KeyError("not found")
        return _send_grouped(rows).as_dict()
    
    def count_pending(self) -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*)
                    FROM pima.households
                    WHERE send_to_commcare_status = 'Pending'
                    """,
                )
                count = cur.fetchone()[0]
                return count
