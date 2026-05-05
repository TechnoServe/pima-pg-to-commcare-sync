from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List
from datetime import date
from psycopg2.extras import RealDictCursor

from app.db.pg import get_conn
from app.gcp.inbox import post_to_gcp_inbox
from app.sync.base import SyncHandler, SyncResult


@dataclass(frozen=True)
class WetmillRow:
    # Completed Surveys & Dates
    manager_needs_assessment_completed: bool
    manager_needs_assessment_date: date | None
    infrastructure_completed: bool
    infrastructure_date: date | None
    waste_water_management_completed: bool
    waste_water_management_date: date | None
    kpis_completed: bool
    kpis_date: date | None
    financials_completed: bool
    financials_date: date | None
    employees_completed: bool
    employees_date: date | None
    cpqi_completed: bool
    cpqi_date: date | None
    water_and_energy_use_completed: bool
    water_and_energy_use_date: date | None
    wet_mill_training_date: date | None
    routine_visit_date: date | None
    gender_equitable_business_practices_completed: bool
    gender_equitable_business_practices_date: date | None
    
    # Basic Info
    id: str
    tns_id: str
    commcare_case_id: str | None
    wetmill_name: str
    mill_status: str # Coopertive, Private, etc.
    status: str # Active, Inactive, etc.
    comments: str | None
    country: str | None
    # date_ba_signature: date | None # Not in query but needed for payload
    exporting_status: str | None
    manager_name: str | None
    manager_role: str | None
    programme: str | None
    registration_date: date | None
    project_id: str
    project_sf_id: str | None
    project_name: str
    project_unique_id: str
    staff_role_id: str | None
    staff_commcare_case_id: str | None
    cc_mobile_worker_group_id: str | None
    mill_external_id: str | None

def _rows_to_payload(rows: List[WetmillRow]) -> Dict[str, Any]:
    project_unique_id = rows[0].project_unique_id if rows else None
    wetmills: List[Dict[str, Any]] = []
    for r in rows:
        wetmills.append(
            {
                "commCareCaseId": r.commcare_case_id,
                "tnsId": r.tns_id,
                "comments": r.comments,
                "country": r.country,
                "dateBASignature": r.registration_date.isoformat() if r.registration_date else None,
                "exportingStatus": r.exporting_status,
                "managerName": r.manager_name,
                "managerRole": r.manager_role,
                "programme": r.programme,
                "registrationDate": r.registration_date.isoformat() if r.registration_date else None,
                "wetmillName": r.wetmill_name,
                "millStatus": r.mill_status,
                "status": r.status,
                "managerNeedsAssessmentCompleted": "TRUE" if r.manager_needs_assessment_completed else "FALSE",
                "managerNeedsAssessmentDate": r.manager_needs_assessment_date.isoformat() if r.manager_needs_assessment_date else None,
                "infrastructureCompleted": "TRUE" if r.infrastructure_completed else "FALSE",
                "infrastructureDate": r.infrastructure_date.isoformat() if r.infrastructure_date else None,
                "wasteWaterManagementCompleted": "TRUE" if r.waste_water_management_completed else "FALSE",
                "wasteWaterManagementDate": r.waste_water_management_date.isoformat() if r.waste_water_management_date else None,
                "kpisCompleted": "TRUE" if r.kpis_completed else "FALSE",
                "kpisDate": r.kpis_date.isoformat() if r.kpis_date else None,
                "financialsCompleted": "TRUE" if r.financials_completed else "FALSE",
                "financialsDate": r.financials_date.isoformat() if r.financials_date else None,
                "employeesCompleted": "TRUE" if r.employees_completed else "FALSE",
                "employeesDate": r.employees_date.isoformat() if r.employees_date else None,
                "cpqiCompleted": "TRUE" if r.cpqi_completed else "FALSE",
                "cpqiDate": r.cpqi_date.isoformat() if r.cpqi_date else None,
                "waterAndEnergyUseCompleted": "TRUE" if r.water_and_energy_use_completed else "FALSE",
                "waterAndEnergyUseDate": r.water_and_energy_use_date.isoformat() if r.water_and_energy_use_date else None,
                "wetMillTrainingDate": r.wet_mill_training_date.isoformat() if r.wet_mill_training_date else None,
                "routineVisitDate": r.routine_visit_date.isoformat() if r.routine_visit_date else None,
                "genderEquitableBusinessPracticesCompleted": "TRUE" if r.gender_equitable_business_practices_completed else "FALSE",
                "genderEquitableBusinessPracticesDate": r.gender_equitable_business_practices_date.isoformat() if r.gender_equitable_business_practices_date else None, 
                "ccMobileWorkerGroupId": r.cc_mobile_worker_group_id,
                "staffId": r.staff_commcare_case_id or r.staff_role_id,
                "millExternalId": r.mill_external_id if r.mill_external_id else None
            }
        )

    return {
        "source": "postgres",
        "jobType": "Wetmill",
        "uniqueProjectKey": project_unique_id,
        "wetmills": wetmills,
        "entity": "wetmills",
    }


def _lock_and_mark_processing(limit: int) -> List[WetmillRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH locked AS (
                        SELECT wm.id
                        FROM pima.wetmills wm
                        WHERE wm.send_to_commcare = true
                          AND wm.send_to_commcare_status = 'Pending'
                        ORDER BY wm.updated_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    ),
                    updated AS (
                        UPDATE pima.wetmills wm
                        SET send_to_commcare_status = 'Processing',
                            updated_at = now()
                        FROM locked
                        WHERE wm.id = locked.id
                        RETURNING wm.*
                    ),
                    -- 1. CPQI
                    cpqi AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'cpqi'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 2. KPIs
                    kpis AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'kpis'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 3. Manager Needs Assessment
                    manager_needs_assessment AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'manager_needs_assessment'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 4. Infrastructure
                    infrastructure AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'infrastructure'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 5. Waste Water Management
                    waste_water_management AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'waste_water_management'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 6. Financials
                    financials AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'financials'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 7. Employees
                    employees AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'employees'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 8. Water and Energy Use
                    water_and_energy_use AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'water_and_energy_use'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 9. Gender Equitable Business Practices
                    gender_equitable_business_practices AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'gender_equitable_business_practices'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 10. Wet Mill Training
                    wet_mill_training AS (
                        SELECT wv.wetmill_id, MAX(wv.visit_date) AS visit_date
                        FROM pima.wetmill_visits wv
                        JOIN pima.wv_survey_responses wvsr ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'wet_mill_training'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 11. Routine Visit
                    routine_visits AS (
                        SELECT wv.wetmill_id, MAX(wv.visit_date) AS visit_date
                        FROM pima.wetmill_visits wv
                        JOIN pima.wv_survey_responses wvsr ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'routine_visit'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    )

                    SELECT
                        w.id::text AS id,
                        psr.commcare_case_id::text AS staff_commcare_case_id,
                        psr.commcare_location_id::text AS cc_mobile_worker_group_id,
                        psr.id::text AS staff_role_id,
                        w.wet_mill_unique_id::text AS tns_id,
                        w.commcare_case_id::text AS commcare_case_id,
                        w.name::text AS wetmill_name,
                        w.mill_status::text AS mill_status,
                        w.exporting_status::text AS exporting_status,
                        w.programme::text AS programme,
                        w.country::text AS country,
                        w.manager_name::text AS manager_name,
                        w.manager_role::text AS manager_role,
                        w.comments::text AS comments,
                        w.registration_date::date AS registration_date,
                        w.status::text AS status,
                        w.mill_external_id::text AS mill_external_id,
                        p.id AS project_id,
                        p.sf_id::text AS project_sf_id,
                        p.project_name::text AS project_name,
                        LOWER(p.project_unique_id) AS project_unique_id,
                        cpqi.completed_date IS NOT NULL AS cpqi_completed,
                        cpqi.completed_date AS cpqi_date,
                        kpis.completed_date IS NOT NULL AS kpis_completed,
                        kpis.completed_date AS kpis_date,
                        manager_needs_assessment.completed_date IS NOT NULL AS manager_needs_assessment_completed,
                        manager_needs_assessment.completed_date AS manager_needs_assessment_date,
                        infrastructure.completed_date IS NOT NULL AS infrastructure_completed,
                        infrastructure.completed_date AS infrastructure_date,
                        waste_water_management.completed_date IS NOT NULL AS waste_water_management_completed,
                        waste_water_management.completed_date AS waste_water_management_date,
                        financials.completed_date IS NOT NULL AS financials_completed,
                        financials.completed_date AS financials_date,
                        employees.completed_date IS NOT NULL AS employees_completed,
                        employees.completed_date AS employees_date,
                        water_and_energy_use.completed_date IS NOT NULL AS water_and_energy_use_completed,
                        water_and_energy_use.completed_date AS water_and_energy_use_date,
                        gender_equitable_business_practices.completed_date IS NOT NULL AS gender_equitable_business_practices_completed,
                        gender_equitable_business_practices.completed_date AS gender_equitable_business_practices_date,
                        wet_mill_training.visit_date AS wet_mill_training_date,
                        routine_visits.visit_date AS routine_visit_date
                    FROM updated w
                    -- JOIN pima.wetmill_visits wv ON wv.wetmill_id = w.id
                    -- CPQI
                    LEFT JOIN cpqi ON cpqi.wetmill_id = w.id
                    -- KPIs
                    LEFT JOIN kpis ON kpis.wetmill_id = w.id
                    -- Manager Needs Assessment
                    LEFT JOIN manager_needs_assessment ON manager_needs_assessment.wetmill_id = w.id
                    -- Infrastructure
                    LEFT JOIN infrastructure ON infrastructure.wetmill_id = w.id
                    -- Waste Water Management
                    LEFT JOIN waste_water_management ON waste_water_management.wetmill_id = w.id
                    -- Financials
                    LEFT JOIN financials ON financials.wetmill_id = w.id
                    -- Employees
                    LEFT JOIN employees ON employees.wetmill_id = w.id
                    -- Water and Energy Use
                    LEFT JOIN water_and_energy_use ON water_and_energy_use.wetmill_id = w.id
                    -- Gender Equitable Business Practices
                    LEFT JOIN gender_equitable_business_practices ON gender_equitable_business_practices.wetmill_id = w.id
                    -- Wet Mill Training
                    LEFT JOIN wet_mill_training ON wet_mill_training.wetmill_id = w.id
                    -- Routine Visit
                    LEFT JOIN routine_visits ON routine_visits.wetmill_id = w.id
                    JOIN pima.project_staff_roles psr ON w.user_id = psr.staff_id
                    JOIN pima.projects p ON p.id = psr.project_id
                    AND LOWER(p.project_name) LIKE '%%' || LOWER(w.programme) || '%%' 
                    AND LOWER(p.project_unique_id) = 'coffee_global_sustainability'
                    WHERE w.is_deleted IS FALSE
                    ORDER BY w.id
                    """,
                    (limit,),
                )

                rows = cur.fetchall()
                conn.commit()

                if not rows:
                    return []

                return [
                    WetmillRow(
                        id=r["id"],
                        staff_commcare_case_id=r.get("staff_commcare_case_id"),
                        cc_mobile_worker_group_id=r.get("cc_mobile_worker_group_id"),
                        staff_role_id=r.get("staff_role_id"),
                        tns_id=r["tns_id"],
                        commcare_case_id=r["commcare_case_id"],
                        wetmill_name=r["wetmill_name"],
                        mill_status=r["mill_status"],
                        status=r["status"],
                        exporting_status=r["exporting_status"],
                        programme=r["programme"],
                        country=r["country"],
                        manager_name=r["manager_name"],
                        manager_role=r["manager_role"],
                        comments=r["comments"],
                        registration_date=r["registration_date"],
                        project_id=r["project_id"],
                        project_sf_id=r.get("project_sf_id"),
                        project_name=r["project_name"],
                        project_unique_id=r["project_unique_id"],
                        cpqi_completed=r["cpqi_completed"],
                        cpqi_date=r["cpqi_date"],
                        kpis_completed=r["kpis_completed"],
                        kpis_date=r["kpis_date"],
                        manager_needs_assessment_completed=r["manager_needs_assessment_completed"],
                        manager_needs_assessment_date=r["manager_needs_assessment_date"],
                        infrastructure_completed=r["infrastructure_completed"],
                        infrastructure_date=r["infrastructure_date"],
                        waste_water_management_completed=r["waste_water_management_completed"],
                        waste_water_management_date=r["waste_water_management_date"],
                        financials_completed=r["financials_completed"],
                        financials_date=r["financials_date"],
                        employees_completed=r["employees_completed"],
                        employees_date=r["employees_date"],
                        water_and_energy_use_completed=r["water_and_energy_use_completed"],
                        water_and_energy_use_date=r["water_and_energy_use_date"],
                        gender_equitable_business_practices_completed=r["gender_equitable_business_practices_completed"],
                        gender_equitable_business_practices_date=r["gender_equitable_business_practices_date"],
                        wet_mill_training_date=r["wet_mill_training_date"],
                        routine_visit_date=r["routine_visit_date"],
                        mill_external_id=r["mill_external_id"],
                    )
                    for r in rows
                ]

        except Exception:
            conn.rollback()
            raise


def _lock_one_and_mark_processing(record_id: str) -> List[WetmillRow]:
    with get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    WITH locked AS (
                        SELECT wm.id
                        FROM pima.wetmills wm
                        WHERE wm.id = %s::uuid
                          AND wm.send_to_commcare = true
                          AND wm.send_to_commcare_status = 'Pending'
                        FOR UPDATE
                    ),
                    updated AS (
                        UPDATE pima.wetmills wm
                        SET send_to_commcare_status = 'Processing',
                            updated_at = now()
                        FROM locked
                        WHERE wm.id = locked.id
                        RETURNING wm.*
                    ),
                    -- 1. CPQI
                    cpqi AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'cpqi'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 2. KPIs
                    kpis AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'kpis'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 3. Manager Needs Assessment
                    manager_needs_assessment AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'manager_needs_assessment'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 4. Infrastructure
                    infrastructure AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'infrastructure'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 5. Waste Water Management
                    waste_water_management AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'waste_water_management'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 6. Financials
                    financials AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'financials'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 7. Employees
                    employees AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'employees'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 8. Water and Energy Use
                    water_and_energy_use AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'water_and_energy_use'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 9. Gender Equitable Business Practices
                    gender_equitable_business_practices AS (
                        SELECT 
                            wv.wetmill_id, 
                            MAX(wvsr.completed_date) AS completed_date
                        FROM pima.wv_survey_responses wvsr
                        JOIN pima.wetmill_visits wv ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'gender_equitable_business_practices'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 10. Wet Mill Training
                    wet_mill_training AS (
                        SELECT wv.wetmill_id, MAX(wv.visit_date) AS visit_date
                        FROM pima.wetmill_visits wv
                        JOIN pima.wv_survey_responses wvsr ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'wet_mill_training'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    ),
                    -- 11. Routine Visit
                    routine_visits AS (
                        SELECT wv.wetmill_id, MAX(wv.visit_date) AS visit_date
                        FROM pima.wetmill_visits wv
                        JOIN pima.wv_survey_responses wvsr ON wvsr.form_visit_id = wv.id AND wvsr.survey_type = 'routine_visit'
                        WHERE wv.wetmill_id IN (SELECT id FROM updated)
                            AND wv.is_deleted IS FALSE
                        GROUP BY wv.wetmill_id
                    )

                    SELECT
                        w.id::text AS id,
                        psr.commcare_case_id::text AS staff_commcare_case_id,
                        psr.commcare_location_id::text AS cc_mobile_worker_group_id,
                        psr.id::text AS staff_role_id,
                        w.wet_mill_unique_id::text AS tns_id,
                        w.commcare_case_id::text AS commcare_case_id,
                        w.name::text AS wetmill_name,
                        w.mill_status::text AS mill_status,
                        w.status::text AS status,
                        w.mill_external_id::text AS mill_external_id,
                        w.exporting_status::text AS exporting_status,
                        w.programme::text AS programme,
                        w.country::text AS country,
                        w.manager_name::text AS manager_name,
                        w.manager_role::text AS manager_role,
                        w.comments::text AS comments,
                        w.registration_date::date AS registration_date,
                        p.id AS project_id,
                        p.sf_id::text AS project_sf_id,
                        p.project_name::text AS project_name,
                        LOWER(p.project_unique_id) AS project_unique_id,
                        cpqi.completed_date IS NOT NULL AS cpqi_completed,
                        cpqi.completed_date AS cpqi_date,
                        kpis.completed_date IS NOT NULL AS kpis_completed,
                        kpis.completed_date AS kpis_date,
                        manager_needs_assessment.completed_date IS NOT NULL AS manager_needs_assessment_completed,
                        manager_needs_assessment.completed_date AS manager_needs_assessment_date,
                        infrastructure.completed_date IS NOT NULL AS infrastructure_completed,
                        infrastructure.completed_date AS infrastructure_date,
                        waste_water_management.completed_date IS NOT NULL AS waste_water_management_completed,
                        waste_water_management.completed_date AS waste_water_management_date,
                        financials.completed_date IS NOT NULL AS financials_completed,
                        financials.completed_date AS financials_date,
                        employees.completed_date IS NOT NULL AS employees_completed,
                        employees.completed_date AS employees_date,
                        water_and_energy_use.completed_date IS NOT NULL AS water_and_energy_use_completed,
                        water_and_energy_use.completed_date AS water_and_energy_use_date,
                        gender_equitable_business_practices.completed_date IS NOT NULL AS gender_equitable_business_practices_completed,
                        gender_equitable_business_practices.completed_date AS gender_equitable_business_practices_date,
                        wet_mill_training.visit_date AS wet_mill_training_date,
                        routine_visits.visit_date AS routine_visit_date
                    FROM updated w
                    -- JOIN pima.wetmill_visits wv ON wv.wetmill_id = w.id
                    -- CPQI
                    LEFT JOIN cpqi ON cpqi.wetmill_id = w.id
                    -- KPIs
                    LEFT JOIN kpis ON kpis.wetmill_id = w.id
                    -- Manager Needs Assessment
                    LEFT JOIN manager_needs_assessment ON manager_needs_assessment.wetmill_id = w.id
                    -- Infrastructure
                    LEFT JOIN infrastructure ON infrastructure.wetmill_id = w.id
                    -- Waste Water Management
                    LEFT JOIN waste_water_management ON waste_water_management.wetmill_id = w.id
                    -- Financials
                    LEFT JOIN financials ON financials.wetmill_id = w.id
                    -- Employees
                    LEFT JOIN employees ON employees.wetmill_id = w.id
                    -- Water and Energy Use
                    LEFT JOIN water_and_energy_use ON water_and_energy_use.wetmill_id = w.id
                    -- Gender Equitable Business Practices
                    LEFT JOIN gender_equitable_business_practices ON gender_equitable_business_practices.wetmill_id = w.id
                    -- Wet Mill Training
                    LEFT JOIN wet_mill_training ON wet_mill_training.wetmill_id = w.id
                    -- Routine Visit
                    LEFT JOIN routine_visits ON routine_visits.wetmill_id = w.id
                    JOIN pima.project_staff_roles psr ON w.user_id = psr.staff_id
                    JOIN pima.projects p ON p.id = psr.project_id
                    AND LOWER(p.project_name) LIKE '%%' || LOWER(w.programme) || '%%' 
                    AND LOWER(p.project_unique_id) = 'coffee_global_sustainability'
                    WHERE w.is_deleted IS FALSE
                    ORDER BY w.id
                    """,
                    (record_id,),
                )

                rows = cur.fetchall()
                conn.commit()

                if not rows:
                    return []

                return [
                    WetmillRow(
                        id=r["id"],
                        staff_commcare_case_id=r.get("staff_commcare_case_id"),
                        cc_mobile_worker_group_id=r.get("cc_mobile_worker_group_id"),
                        staff_role_id=r.get("staff_role_id"),
                        tns_id=r["tns_id"],
                        commcare_case_id=r["commcare_case_id"],
                        wetmill_name=r["wetmill_name"],
                        mill_status=r["mill_status"],
                        status=r["status"],
                        exporting_status=r["exporting_status"],
                        programme=r["programme"],
                        country=r["country"],
                        manager_name=r["manager_name"],
                        manager_role=r["manager_role"],
                        comments=r["comments"],
                        registration_date=r["registration_date"],
                        project_id=r["project_id"],
                        project_sf_id=r.get("project_sf_id"),
                        project_name=r["project_name"],
                        project_unique_id=r["project_unique_id"],
                        cpqi_completed=r["cpqi_completed"],
                        cpqi_date=r["cpqi_date"],
                        kpis_completed=r["kpis_completed"],
                        kpis_date=r["kpis_date"],
                        manager_needs_assessment_completed=r["manager_needs_assessment_completed"],
                        manager_needs_assessment_date=r["manager_needs_assessment_date"],
                        infrastructure_completed=r["infrastructure_completed"],
                        infrastructure_date=r["infrastructure_date"],
                        waste_water_management_completed=r["waste_water_management_completed"],
                        waste_water_management_date=r["waste_water_management_date"],
                        financials_completed=r["financials_completed"],
                        financials_date=r["financials_date"],
                        employees_completed=r["employees_completed"],
                        employees_date=r["employees_date"],
                        water_and_energy_use_completed=r["water_and_energy_use_completed"],
                        water_and_energy_use_date=r["water_and_energy_use_date"],
                        gender_equitable_business_practices_completed=r["gender_equitable_business_practices_completed"],
                        gender_equitable_business_practices_date=r["gender_equitable_business_practices_date"],
                        wet_mill_training_date=r["wet_mill_training_date"],
                        routine_visit_date=r["routine_visit_date"],
                        mill_external_id=r["mill_external_id"],
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
                UPDATE pima.wetmills
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
                UPDATE pima.wetmills
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
                UPDATE pima.wetmills
                SET send_to_commcare_status = 'Pending',
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                """,
                (ids,),
            )
        conn.commit()


# Send grouped data by project_unique_id
def _send_grouped(rows: List[WetmillRow]) -> SyncResult:
    if not rows:
        return SyncResult(picked=0, sent=0, groups=0)

    groups: Dict[str, List[WetmillRow]] = defaultdict(list)
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


class WetmillsHandler(SyncHandler):
    entity = "wetmills"
    batch_limit = 500

    # Executes a batch sync for wetmills.
    def sync_batch(self) -> Dict[str, Any]:
        return _send_grouped(_lock_and_mark_processing(self.batch_limit)).as_dict()

    # Executes a sync for a single wetmill by record ID.
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
                    FROM pima.wetmills
                    WHERE send_to_commcare_status = 'Pending'
                    """,
                )
                count = cur.fetchone()[0]
                return count
