from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List

from psycopg2.extras import RealDictCursor

from app.db.pg import get_conn
from app.gcp.inbox import post_to_gcp_inbox
from app.sync.base import SyncHandler, SyncResult


@dataclass(frozen=True)
class FarmerRow:
    id: str
    sf_id: str | None
    first_name: str
    middle_name: str | None
    last_name: str
    age: int
    gender: str
    phone_number: str | None
    other_id: str | None
    is_primary_household_member: bool | None
    status: str
    tns_id: str
    commcare_case_id: str
    household_id: str
    household_sf_id: str | None
    household_name: str
    household_number: int
    household_farm_size: str | None
    training_group_id: str
    training_group_sf_id: str | None
    training_group_name: str
    training_group_commcare_id: str
    training_group_location_name: str | None
    cc_mobile_worker_group_id: str | None
    project_unique_id: str


# change DB queried data into payload to be sent 
def _rows_to_payload(rows: List[FarmerRow]) -> Dict[str, Any]:
    project_unique_id = rows[0].project_unique_id if rows else None
    participants: List[Dict[str, Any]] = []
    for r in rows:
        participants.append(
            {
                "participantName": r.first_name,
                "participantId": r.sf_id or r.id,
                "trainingGroupCommCareId": r.training_group_commcare_id,
                "commCareCaseId": r.commcare_case_id,
                "participantMiddleName": r.middle_name,
                "participantLastName": r.last_name,
                "participantAge": r.age,
                "participantGender": r.gender,
                "participantPhoneNumber": r.phone_number,
                "participantOtherIDNumber": r.other_id,
                "participantPrimaryHouseholdMember": str(r.is_primary_household_member) if r.is_primary_household_member is not None else None,
                "householdId": r.household_sf_id or r.household_id,
                "householdName": r.household_name,
                "HHID": r.household_number,
                "householdFarmSize": r.household_farm_size,
                "tnsId": r.tns_id,
                "market": None,
                "trainingGroupLocationName": r.training_group_location_name,
                "ccMobileWorkerGroupId": r.cc_mobile_worker_group_id,
                "trainingGroupId": r.training_group_sf_id or r.training_group_id,
                "status": r.status,
                # Do we not need these fields anymore? They are not mapped on the new DB
                "measurementGroupId": None,
                "measurementGroupCommCareId": None,
                "participantVisibility": None,
                "commCareCaseStatus": r.status, 
            }
        )

    return {
        "source": "postgres",
        "jobType": "Participant",
        "uniqueProjectKey": project_unique_id,
        "participants": participants,
        "entity": "farmers",
    }


def _lock_and_mark_processing(limit: int) -> List[FarmerRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH locked AS (
                        SELECT f.id
                        FROM pima.farmers f
                        WHERE f.send_to_commcare = true
                          AND f.send_to_commcare_status = 'Pending'
                        ORDER BY f.updated_at
                        LIMIT %s
                        FOR UPDATE OF f SKIP LOCKED
                    ),
                    updated AS (
                        UPDATE pima.farmers f
                        SET send_to_commcare_status = 'Processing',
                            updated_at = now()
                        FROM locked
                        WHERE f.id = locked.id
                        RETURNING f.*
                    )
                    SELECT
                        u.id::text AS id,
                        u.sf_id::text AS sf_id,
                        u.first_name,
                        u.middle_name,
                        u.last_name,
                        u.age,
                        u.gender,
                        u.phone_number,
                        u.other_id,
                        u.is_primary_household_member,
                        u.status,
                        u.tns_id,
                        u.commcare_case_id,
                        h.id::text AS household_id,
                        h.sf_id::text AS household_sf_id,
                        h.household_name,
                        h.household_number,
                        h.farm_size::text AS household_farm_size,
                        fg.id::text AS training_group_id,
                        fg.sf_id::text AS training_group_sf_id,
                        fg.ffg_name AS training_group_name,
                        fg.commcare_case_id AS training_group_commcare_id,
                        l.location_name AS training_group_location_name,
                        psr.commcare_location_id AS cc_mobile_worker_group_id,
                        p.project_unique_id
                    FROM updated u
                    JOIN pima.households h ON h.id = u.household_id
                    JOIN pima.farmer_groups fg ON fg.id = u.farmer_group_id
                    JOIN pima.projects p ON p.id = fg.project_id
                    LEFT JOIN pima.locations l ON l.id = fg.location_id
                    LEFT JOIN pima.project_staff_roles psr
                        ON psr.project_id = fg.project_id
                       AND psr.staff_id = fg.responsible_staff_id
                    ORDER BY p.project_unique_id, u.updated_at
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
                conn.commit()

                if not rows:
                    return []

                return [
                    FarmerRow(
                        id=r["id"],
                        sf_id=r.get("sf_id"),
                        first_name=r["first_name"],
                        middle_name=r.get("middle_name"),
                        last_name=r["last_name"],
                        age=r["age"],
                        gender=r["gender"],
                        phone_number=r.get("phone_number"),
                        other_id=r.get("other_id"),
                        is_primary_household_member=r.get("is_primary_household_member"),
                        status=r["status"],
                        tns_id=r["tns_id"],
                        commcare_case_id=r["commcare_case_id"],
                        household_id=r["household_id"],
                        household_sf_id=r.get("household_sf_id"),
                        household_name=r["household_name"],
                        household_number=r["household_number"],
                        household_farm_size=r.get("household_farm_size"),
                        training_group_id=r["training_group_id"],
                        training_group_sf_id=r.get("training_group_sf_id"),
                        training_group_name=r["training_group_name"],
                        training_group_commcare_id=r["training_group_commcare_id"],
                        training_group_location_name=r.get("training_group_location_name"),
                        cc_mobile_worker_group_id=r.get("cc_mobile_worker_group_id"),
                        project_unique_id=r["project_unique_id"],
                    )
                    for r in rows
                ]
        except Exception:
            conn.rollback()
            raise


def _lock_one_and_mark_processing(record_id: str) -> List[FarmerRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH locked AS (
                        SELECT f.id
                        FROM pima.farmers f
                        WHERE f.id = %s::uuid
                          AND f.send_to_commcare = true
                          AND f.send_to_commcare_status = 'Pending'
                        FOR UPDATE OF f
                    ),
                    updated AS (
                        UPDATE pima.farmers f
                        SET send_to_commcare_status = 'Processing',
                            updated_at = now()
                        FROM locked
                        WHERE f.id = locked.id
                        RETURNING f.*
                    )
                    SELECT
                        u.id::text AS id,
                        u.sf_id::text AS sf_id,
                        u.first_name,
                        u.middle_name,
                        u.last_name,
                        u.age,
                        u.gender,
                        u.phone_number,
                        u.other_id,
                        u.is_primary_household_member,
                        u.status,
                        u.tns_id,
                        u.commcare_case_id,
                        h.id::text AS household_id,
                        h.sf_id::text AS household_sf_id,
                        h.household_name,
                        h.household_number,
                        h.farm_size::text AS household_farm_size,
                        fg.id::text AS training_group_id,
                        fg.sf_id::text AS training_group_sf_id,
                        fg.ffg_name AS training_group_name,
                        fg.commcare_case_id AS training_group_commcare_id,
                        l.location_name AS training_group_location_name,
                        psr.commcare_location_id AS cc_mobile_worker_group_id,
                        p.project_unique_id
                    FROM updated u
                    JOIN pima.households h ON h.id = u.household_id
                    JOIN pima.farmer_groups fg ON fg.id = u.farmer_group_id
                    JOIN pima.projects p ON p.id = fg.project_id
                    LEFT JOIN pima.locations l ON l.id = fg.location_id
                    LEFT JOIN pima.project_staff_roles psr
                        ON psr.project_id = fg.project_id
                       AND psr.staff_id = fg.responsible_staff_id
                    ORDER BY p.project_unique_id, u.updated_at
                    """,
                    (record_id,),
                )
                rows = cur.fetchall()
                conn.commit()

                if not rows:
                    return []

                return [
                    FarmerRow(
                        id=r["id"],
                        sf_id=r.get("sf_id"),
                        first_name=r["first_name"],
                        middle_name=r.get("middle_name"),
                        last_name=r["last_name"],
                        age=r["age"],
                        gender=r["gender"],
                        phone_number=r.get("phone_number"),
                        other_id=r.get("other_id"),
                        is_primary_household_member=r.get("is_primary_household_member"),
                        status=r["status"],
                        tns_id=r["tns_id"],
                        commcare_case_id=r["commcare_case_id"],
                        household_id=r["household_id"],
                        household_sf_id=r.get("household_sf_id"),
                        household_name=r["household_name"],
                        household_number=r["household_number"],
                        household_farm_size=r.get("household_farm_size"),
                        training_group_id=r["training_group_id"],
                        training_group_sf_id=r.get("training_group_sf_id"),
                        training_group_name=r["training_group_name"],
                        training_group_commcare_id=r["training_group_commcare_id"],
                        training_group_location_name=r.get("training_group_location_name"),
                        cc_mobile_worker_group_id=r.get("cc_mobile_worker_group_id"),
                        project_unique_id=r["project_unique_id"],
                    )
                    for r in rows
                ]
        except Exception:
            conn.rollback()
            raise

# Mark as complete after successful send
def _mark_done(ids: List[str]) -> None:
    if not ids:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pima.farmers
                SET send_to_commcare_status = 'Complete',
                    send_to_commcare = false,
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
        conn.commit()

# Mark as failed after unsuccessful send
def _mark_failed(ids: List[str]) -> None:
    if not ids:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pima.farmers
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
                UPDATE pima.farmers
                SET send_to_commcare_status = 'Pending',
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
        conn.commit()


def _send_grouped(rows: List[FarmerRow]) -> SyncResult:
    if not rows:
        return SyncResult(picked=0, sent=0, groups=0)

    groups: Dict[str, List[FarmerRow]] = defaultdict(list)
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


class FarmersHandler(SyncHandler):
    entity = "farmers"
    batch_limit = 500

    def sync_batch(self) -> Dict[str, Any]:
        return _send_grouped(_lock_and_mark_processing(self.batch_limit)).as_dict()

    def sync_one(self, record_id: str) -> Dict[str, Any]:
        rows = _lock_one_and_mark_processing(record_id)
        if not rows:
            raise KeyError("not found")
        return _send_grouped(rows).as_dict()
