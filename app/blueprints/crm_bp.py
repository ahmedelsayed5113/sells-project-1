"""
CRM Report ingestion endpoints.

Phase 1a — three routes, all under /api/crm:

  POST /campaigns/<id>/upload      — kick off an ingest from an .xlsx
  GET  /uploads/<id>/status        — poll progress of one upload
  GET  /campaigns/<id>/uploads     — recent uploads for a campaign

The heavy lifting (parse + insert) runs on a daemon thread spawned in
app/crm_processor.py so the HTTP request returns immediately and the
client polls /status. We don't surface lead/event data here yet — that
lands once KPI recalc + the timeline endpoint ship in P1b/P3.
"""
import logging

import psycopg2.extras
from flask import Blueprint, jsonify, request, session

from app.auth import (
    csrf_protect,
    error_response,
    login_required,
    role_required,
)
from app.crm_processor import start_processing_thread
from app.database import get_conn

log = logging.getLogger(__name__)
crm_bp = Blueprint("crm", __name__, url_prefix="/api/crm")


# Upload cap. 10 MB is roughly ~40k rows of typical CRM exports; anything
# larger is almost certainly a multi-month export that should be split
# upstream. The cap also doubles as a cheap DoS guard since the file goes
# straight into a worker thread's memory.
_MAX_UPLOAD_BYTES = 10 * 1024 * 1024


def _campaign_exists(conn, campaign_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM marketing_campaigns WHERE id = %s",
            (campaign_id,),
        )
        return cur.fetchone() is not None


# ─── POST upload ────────────────────────────────────────────────────────

@crm_bp.route("/campaigns/<int:campaign_id>/upload", methods=["POST"])
@login_required
@role_required("admin", "manager", "marketing")
@csrf_protect
def upload_crm_report(campaign_id: int):
    # Validate the multipart payload before we even consider opening the file.
    if "file" not in request.files:
        return error_response("required_fields_missing", 400)

    f = request.files["file"]
    if not f or not f.filename:
        return error_response("required_fields_missing", 400)

    # openpyxl reads .xlsx; we don't support .xls (old binary format) or
    # .csv on this endpoint to keep the parser focused.
    if not f.filename.lower().endswith(".xlsx"):
        return error_response("invalid_input", 400)

    # Slurp the bytes once. werkzeug streams from a SpooledTemporaryFile;
    # by reading it into memory we (a) decouple parsing from the HTTP
    # request lifecycle (the thread keeps working after we return) and
    # (b) get an unambiguous size check.
    blob = f.read()
    if not blob:
        return error_response("invalid_input", 400)
    if len(blob) > _MAX_UPLOAD_BYTES:
        # 413 Payload Too Large is the strict-correct status; the frontend
        # already handles range_too_large the same way, so reuse the
        # surface here. We pick a specific code so the toast localizes.
        return error_response("range_too_large", 413)

    conn = None
    try:
        conn = get_conn()
        if not _campaign_exists(conn, campaign_id):
            return error_response("not_found", 404)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO crm_report_uploads (
                    campaign_id, file_name, uploaded_by, status
                )
                VALUES (%s, %s, %s, 'PENDING')
                RETURNING id
                """,
                (campaign_id, f.filename[:255], session.get("user_id")),
            )
            upload_id = cur.fetchone()[0]
        conn.commit()
    except Exception as e:
        log.error("CRM upload insert failed: %s", e)
        return error_response("server", 500)
    finally:
        if conn is not None:
            conn.close()

    # Hand off to the daemon thread. start_processing_thread is fire-and-
    # forget; status/errors land on the upload row via the thread itself.
    start_processing_thread(upload_id, blob, campaign_id)

    return jsonify({
        "ok": True,
        "upload_id": upload_id,
        "status": "PROCESSING",
        "message": "Upload received, processing in background",
    }), 202


# ─── GET status ─────────────────────────────────────────────────────────

@crm_bp.route("/uploads/<int:upload_id>/status", methods=["GET"])
@login_required
@role_required("admin", "manager", "marketing")
def upload_status(upload_id: int):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, campaign_id, status,
                       total_rows, total_leads, total_events,
                       new_events, duplicate_events,
                       unmatched_sales_reps, unmatched_stages, warnings,
                       error_message, processed_at
                FROM crm_report_uploads
                WHERE id = %s
                """,
                (upload_id,),
            )
            row = cur.fetchone()
        if not row:
            return error_response("not_found", 404)
        return jsonify({
            "upload_id": row["id"],
            "campaign_id": row["campaign_id"],
            "status": row["status"],
            "total_rows": row["total_rows"],
            "total_leads": row["total_leads"],
            "total_events": row["total_events"],
            "new_events": row["new_events"],
            "duplicate_events": row["duplicate_events"],
            "unmatched_sales_reps": row["unmatched_sales_reps"] or [],
            "unmatched_stages": row["unmatched_stages"] or [],
            "warnings": row["warnings"] or [],
            "error_message": row["error_message"],
            "processed_at": row["processed_at"].isoformat() if row["processed_at"] else None,
        })
    except Exception as e:
        log.error("upload_status %s: %s", upload_id, e)
        return error_response("server", 500)
    finally:
        if conn is not None:
            conn.close()


# ─── GET campaign overview ──────────────────────────────────────────────

@crm_bp.route("/campaigns/<int:campaign_id>/overview", methods=["GET"])
@login_required
@role_required("admin", "manager", "marketing")
def campaign_overview(campaign_id: int):
    """Snapshot view rendered on the per-campaign page.

    Reads from campaign_kpis (rolled up after each upload) plus a small
    intervention breakdown for the HIGH/MEDIUM badge counts. The "last
    upload summary" is pulled from crm_report_uploads — useful for the
    "Last uploaded by X · N new events" line on the page header.

    Stage labels are NOT localized here — the frontend resolves
    `crm.stages.<TOKEN>` keys so the same payload renders in either
    language without a server round-trip.
    """
    conn = None
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, campaign_name FROM marketing_campaigns WHERE id = %s",
                (campaign_id,),
            )
            camp = cur.fetchone()
            if not camp:
                return error_response("not_found", 404)

            # Read the KPI rollup if it exists; new campaigns with no
            # uploads yet just get zeros.
            cur.execute(
                """
                SELECT total_leads, stage_counts,
                       manager_intervention_count, last_upload_at, updated_at
                FROM campaign_kpis WHERE campaign_id = %s
                """,
                (campaign_id,),
            )
            kpi_row = cur.fetchone()

            cur.execute(
                """
                SELECT priority, COUNT(*) AS n
                FROM manager_intervention_flags
                WHERE campaign_id = %s AND status = 'OPEN'
                GROUP BY priority
                """,
                (campaign_id,),
            )
            breakdown = {"HIGH": 0, "MEDIUM": 0}
            for r in cur.fetchall():
                if r["priority"] in breakdown:
                    breakdown[r["priority"]] = r["n"]

            # Most recent COMPLETED upload; surfaces "by whom, new vs dup".
            cur.execute(
                """
                SELECT u.id, u.processed_at, u.total_events, u.new_events,
                       u.duplicate_events, u.uploaded_by,
                       usr.full_name AS uploaded_by_name
                FROM crm_report_uploads u
                LEFT JOIN users usr ON usr.id = u.uploaded_by
                WHERE u.campaign_id = %s
                  AND u.status = 'COMPLETED'
                  AND u.is_voided = FALSE
                ORDER BY u.processed_at DESC NULLS LAST
                LIMIT 1
                """,
                (campaign_id,),
            )
            last_upload = cur.fetchone()

        stage_counts = (kpi_row["stage_counts"] if kpi_row else None) or {}
        return jsonify({
            "campaign_id": camp["id"],
            "campaign_name": camp["campaign_name"],
            "total_leads": (kpi_row["total_leads"] if kpi_row else 0),
            "stage_counts": stage_counts,
            "manager_intervention_count": (
                kpi_row["manager_intervention_count"] if kpi_row else 0
            ),
            "intervention_breakdown": breakdown,
            "last_upload_at": (
                kpi_row["last_upload_at"].isoformat()
                if kpi_row and kpi_row["last_upload_at"] else None
            ),
            "updated_at": (
                kpi_row["updated_at"].isoformat()
                if kpi_row and kpi_row["updated_at"] else None
            ),
            "last_upload_summary": ({
                "upload_id": last_upload["id"],
                "uploaded_at": (
                    last_upload["processed_at"].isoformat()
                    if last_upload["processed_at"] else None
                ),
                "uploaded_by": last_upload["uploaded_by"],
                "uploaded_by_name": last_upload["uploaded_by_name"],
                "total_events": last_upload["total_events"],
                "new_events": last_upload["new_events"],
                "duplicate_events": last_upload["duplicate_events"],
            } if last_upload else None),
        })
    except Exception as e:
        log.error("campaign_overview %s: %s", campaign_id, e)
        return error_response("server", 500)
    finally:
        if conn is not None:
            conn.close()


# ─── GET intervention list for a campaign ───────────────────────────────

_VALID_INTERVENTION_STATUSES = {"OPEN", "REVIEWED", "CLOSED"}
_VALID_INTERVENTION_PRIORITIES = {"HIGH", "MEDIUM"}


@crm_bp.route("/campaigns/<int:campaign_id>/intervention", methods=["GET"])
@login_required
@role_required("admin", "manager", "marketing")
def campaign_intervention(campaign_id: int):
    """Manager-intervention inbox for a single campaign.

    Filters:
      status   default OPEN — accept OPEN | REVIEWED | CLOSED | all
      priority default all  — accept HIGH | MEDIUM | all

    Order: priority (HIGH → MEDIUM), then last_no_answer_date DESC so the
    most recently-broken-down conversations bubble up. Caps at 200 rows —
    pagination ships in P3 once we have a real inbox page.
    """
    status_arg = (request.args.get("status") or "OPEN").upper()
    priority_arg = (request.args.get("priority") or "all").upper()

    if status_arg != "ALL" and status_arg not in _VALID_INTERVENTION_STATUSES:
        return error_response("invalid_input", 400)
    if priority_arg != "ALL" and priority_arg not in _VALID_INTERVENTION_PRIORITIES:
        return error_response("invalid_input", 400)

    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM marketing_campaigns WHERE id = %s",
                (campaign_id,),
            )
            if not cur.fetchone():
                return error_response("not_found", 404)

        clauses = ["m.campaign_id = %s"]
        params = [campaign_id]
        if status_arg != "ALL":
            clauses.append("m.status = %s")
            params.append(status_arg)
        if priority_arg != "ALL":
            clauses.append("m.priority = %s")
            params.append(priority_arg)
        where_sql = " AND ".join(clauses)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT m.id, m.lead_id, m.campaign_id, m.sales_user_id,
                       m.trigger_type, m.current_stage, m.previous_positive_stage,
                       m.priority, m.last_positive_stage_date, m.last_no_answer_date,
                       m.last_comment, m.status, m.created_at, m.updated_at,
                       l.client_name, l.mobile,
                       usr.full_name AS current_sales_rep_name
                FROM manager_intervention_flags m
                JOIN leads l ON l.id = m.lead_id
                LEFT JOIN users usr ON usr.id = m.sales_user_id
                WHERE {where_sql}
                ORDER BY
                  CASE m.priority WHEN 'HIGH' THEN 0 WHEN 'MEDIUM' THEN 1 ELSE 2 END,
                  m.last_no_answer_date DESC NULLS LAST,
                  m.id DESC
                LIMIT 200
                """,
                params,
            )
            rows = cur.fetchall()

        out = []
        for r in rows:
            out.append({
                "id": r["id"],
                "lead_id": r["lead_id"],
                "campaign_id": r["campaign_id"],
                "client_name": r["client_name"],
                "mobile": r["mobile"],
                "current_sales_rep_id": r["sales_user_id"],
                "current_sales_rep_name": r["current_sales_rep_name"],
                "trigger_type": r["trigger_type"],
                "current_stage": r["current_stage"],
                "previous_positive_stage": r["previous_positive_stage"],
                "priority": r["priority"],
                "last_positive_stage_date": (
                    r["last_positive_stage_date"].isoformat()
                    if r["last_positive_stage_date"] else None
                ),
                "last_no_answer_date": (
                    r["last_no_answer_date"].isoformat()
                    if r["last_no_answer_date"] else None
                ),
                "last_comment": r["last_comment"],
                "status": r["status"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            })
        return jsonify(out)
    except Exception as e:
        log.error("campaign_intervention %s: %s", campaign_id, e)
        return error_response("server", 500)
    finally:
        if conn is not None:
            conn.close()


# ─── GET cross-campaign summary (powers the /marketing CRM table) ───────

@crm_bp.route("/campaigns-summary", methods=["GET"])
@login_required
@role_required("admin", "manager", "marketing")
def campaigns_summary():
    """List of campaigns with their CRM rollup — feeds the table on
    /marketing under "CRM Reports". Returns every campaign visible to the
    caller (no scoping in marketing_bp either, so we match it).
    """
    conn = None
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT c.id, c.campaign_name, c.created_at,
                       COALESCE(k.total_leads, 0) AS total_leads,
                       COALESCE(k.manager_intervention_count, 0) AS open_intervention,
                       k.last_upload_at,
                       k.stage_counts
                FROM marketing_campaigns c
                LEFT JOIN campaign_kpis k ON k.campaign_id = c.id
                ORDER BY
                    k.last_upload_at DESC NULLS LAST,
                    c.created_at DESC
                """
            )
            rows = cur.fetchall()
        out = []
        for r in rows:
            out.append({
                "campaign_id": r["id"],
                "campaign_name": r["campaign_name"],
                "total_leads": r["total_leads"],
                "open_intervention": r["open_intervention"],
                "last_upload_at": (
                    r["last_upload_at"].isoformat() if r["last_upload_at"] else None
                ),
                "stage_counts": r["stage_counts"] or {},
            })
        return jsonify(out)
    except Exception as e:
        log.error("campaigns_summary: %s", e)
        return error_response("server", 500)
    finally:
        if conn is not None:
            conn.close()


# ─── GET recent uploads for a campaign ──────────────────────────────────

@crm_bp.route("/campaigns/<int:campaign_id>/uploads", methods=["GET"])
@login_required
@role_required("admin", "manager", "marketing")
def list_uploads(campaign_id: int):
    conn = None
    try:
        conn = get_conn()
        if not _campaign_exists(conn, campaign_id):
            return error_response("not_found", 404)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT u.id, u.file_name, u.status,
                       u.total_rows, u.total_leads, u.total_events,
                       u.new_events, u.duplicate_events,
                       u.is_voided, u.error_message,
                       u.created_at, u.processed_at,
                       u.uploaded_by, usr.full_name AS uploaded_by_name
                FROM crm_report_uploads u
                LEFT JOIN users usr ON usr.id = u.uploaded_by
                WHERE u.campaign_id = %s
                ORDER BY u.created_at DESC
                LIMIT 50
                """,
                (campaign_id,),
            )
            rows = cur.fetchall()

        out = []
        for r in rows:
            out.append({
                "upload_id": r["id"],
                "file_name": r["file_name"],
                "status": r["status"],
                "total_rows": r["total_rows"],
                "total_leads": r["total_leads"],
                "total_events": r["total_events"],
                "new_events": r["new_events"],
                "duplicate_events": r["duplicate_events"],
                "is_voided": r["is_voided"],
                "error_message": r["error_message"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "processed_at": r["processed_at"].isoformat() if r["processed_at"] else None,
                "uploaded_by": r["uploaded_by"],
                "uploaded_by_name": r["uploaded_by_name"],
            })
        return jsonify(out)
    except Exception as e:
        log.error("list_uploads campaign=%s: %s", campaign_id, e)
        return error_response("server", 500)
    finally:
        if conn is not None:
            conn.close()
