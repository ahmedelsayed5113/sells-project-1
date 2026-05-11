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
