"""
CRM Report ingestion — single source of truth for normalization & matching.

This module is to the CRM upload pipeline what app/kpi_logic.py is to the
monthly KPI flow: every helper that the parser, the background processor,
the blueprint, and (future) KPI recalculators need lives here.

Phase 1a deliberately scopes this to NORMALIZATION + MATCHING + HASHING.
Recalc functions (campaign/sales KPIs, manager intervention) land in P1b.
Do not add side-effecting recalcs here in this phase.
"""
import hashlib
import logging
import re
from typing import Optional

log = logging.getLogger(__name__)


# ─── Stage mapping ──────────────────────────────────────────────────────
#
# Source-of-truth table for "raw CRM stage string" → "internal token". KPIs
# and Manager-Intervention rules operate on the TOKEN, never on the raw
# string. Lookup at runtime is three-tier (see normalize_stage):
#
#   1. stage_mappings WHERE campaign_id = <this campaign>     (override)
#   2. stage_mappings WHERE campaign_id IS NULL               (admin global)
#   3. DEFAULT_STAGE_MAP                                      (this dict)
#   4. None → bubbles up as an "unmatched stage" warning
#
# Keys are compared after lower() + strip(). New tokens added here must
# also be handled wherever stage logic branches (intervention rules, etc.).
DEFAULT_STAGE_MAP = {
    "no answer":           "NO_ANSWER",
    "1st call no answer":  "NO_ANSWER",
    "no response":         "NO_ANSWER",
    "following":           "FOLLOWING",
    "follow up":           "FOLLOWING",
    "followup":            "FOLLOWING",
    "meeting":             "MEETING",
    "zoom meeting":        "MEETING",
    "meeting done":        "MEETING",
    "cancellation":        "CANCELLATION",
    "cancelled":           "CANCELLATION",
    "canceled":            "CANCELLATION",
    "interested":          "INTERESTED",
    "request":             "REQUEST",
}


# ─── Mobile normalization ───────────────────────────────────────────────

_MOBILE_STRIP_CHARS = re.compile(r"[\s\-\.\(\)\+]")


def normalize_mobile(raw) -> Optional[str]:
    """Coerce a CRM mobile cell to canonical (E.164-ish, no +) digits.

    Rules — tuned for the dominant Egyptian (and occasional Gulf) numbering
    we see in practice. Strict: we'd rather drop a row than collide on a
    mobile that's actually ambiguous.

      "01012345678"        → "201012345678"   (EG, 11 digits starting "01")
      "+20 100 123 4567"   → "201001234567"
      "00201012345678"     → "201012345678"
      "+971569116811"      → "971569116811"   (foreign code untouched)
      ""                   → None
      "abcd"               → None
      "1234567"            → None             (too short)
    """
    if raw is None:
        return None
    # openpyxl returns int/float when Excel formats the cell as a number —
    # 01012345678 round-trips as 1012345678 which then loses the leading 0.
    # Force-format anything numeric back to a digit string before stripping.
    if isinstance(raw, float):
        if raw != raw:  # NaN
            return None
        raw = f"{raw:.0f}"
    elif isinstance(raw, int):
        raw = str(raw)
    s = str(raw).strip()
    if not s:
        return None

    # Strip formatting (spaces, dashes, dots, parens, plus). Do this BEFORE
    # the "00" prefix check so "+00 …" inputs aren't ambiguous.
    s = _MOBILE_STRIP_CHARS.sub("", s)

    # International "00" prefix → drop, country code remains.
    if s.startswith("00"):
        s = s[2:]

    if not s.isdigit():
        return None

    # 11-digit "01..." → Egyptian local; promote to "20" country code.
    # 10-digit "1..."  → Egyptian without the leading 0; same rule, prepend 20.
    if len(s) == 11 and s.startswith("01"):
        s = "20" + s[1:]
    elif len(s) == 10 and s.startswith("1"):
        s = "20" + s

    if len(s) < 8:
        return None
    return s


# ─── Sales-name normalization & matching ────────────────────────────────

_WS_COLLAPSE = re.compile(r"\s+")


def normalize_sales_name(raw) -> str:
    """Trim, lowercase, collapse repeated whitespace to a single space.

    "  Mahmoud   Amr " → "mahmoud amr"
    """
    if raw is None:
        return ""
    return _WS_COLLAPSE.sub(" ", str(raw).strip().lower())


# Allowed roles for CRM sales-rep matching. Team leaders are included since
# they can show up as the responsible rep in mixed-team campaigns; managers
# and admins are not — a "sale" attributed to those roles would skew KPIs.
_SALES_USER_ROLES = ("sales", "team_leader")


def match_sales_user(raw_name, campaign_id: int, conn) -> Optional[int]:
    """Resolve a CRM `Sales Rep` cell to a `users.id`.

    Lookup order:
      1. sales_rep_mappings WHERE campaign_id = <this campaign>   (override)
      2. sales_rep_mappings WHERE campaign_id IS NULL             (global)
      3. users.full_name match (normalized: lower + trimmed + ws-collapsed)
         filtered to role ∈ {sales, team_leader}
      4. None → row is still ingested but flagged as unmatched
    """
    norm = normalize_sales_name(raw_name)
    if not norm:
        return None

    with conn.cursor() as cur:
        # 1 + 2 — explicit mappings, per-campaign first, then global. Using
        # ORDER BY (campaign_id IS NULL) ASC puts non-null first, which gives
        # the per-campaign row precedence when both exist.
        cur.execute(
            """
            SELECT sales_user_id
            FROM sales_rep_mappings
            WHERE LOWER(TRIM(raw_name)) = %s
              AND (campaign_id = %s OR campaign_id IS NULL)
            ORDER BY (campaign_id IS NULL) ASC
            LIMIT 1
            """,
            (norm, campaign_id),
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # 3 — fuzzy match against users.full_name with the same normalization.
        cur.execute(
            """
            SELECT id
            FROM users
            WHERE REGEXP_REPLACE(LOWER(TRIM(full_name)), '\\s+', ' ', 'g') = %s
              AND role = ANY(%s)
              AND active = TRUE
            ORDER BY id ASC
            LIMIT 1
            """,
            (norm, list(_SALES_USER_ROLES)),
        )
        row = cur.fetchone()
        if row:
            return row[0]

    return None


# ─── Stage normalization ────────────────────────────────────────────────

def normalize_stage(raw, campaign_id: Optional[int] = None, conn=None) -> Optional[str]:
    """Map a raw stage cell to the internal token, or None if unmatched.

    `conn` is optional — if not provided we skip the DB-backed mappings and
    fall straight to DEFAULT_STAGE_MAP. The parser passes `conn` so admin
    overrides work; unit tests that only exercise the default behavior can
    omit it.
    """
    if raw is None:
        return None
    key = str(raw).strip().lower()
    if not key:
        return None

    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT normalized_stage
                FROM stage_mappings
                WHERE LOWER(TRIM(raw_stage)) = %s
                  AND (campaign_id = %s OR campaign_id IS NULL)
                ORDER BY (campaign_id IS NULL) ASC
                LIMIT 1
                """,
                (key, campaign_id),
            )
            row = cur.fetchone()
            if row and row[0]:
                return row[0]

    return DEFAULT_STAGE_MAP.get(key)


# ─── Event hashing for dedup ────────────────────────────────────────────

def compute_event_hash(
    campaign_id: int,
    mobile: str,
    follow_date,
    raw_sales_rep,
    normalized_stage,
    comment,
) -> str:
    """SHA-256 of the natural-key tuple that identifies a CRM activity.

    Used as a UNIQUE constraint on lead_events. The same sheet re-uploaded
    produces the same hashes → ON CONFLICT DO NOTHING handles dedup.

    NOTE on raw_sales_rep: we hash the RAW name (not the resolved user_id)
    so that two events differing only by which rep wrote them stay distinct
    even if both names match the same user. follow_date is rendered as ISO
    so DST/microsecond noise doesn't cause spurious mismatches.
    """
    if follow_date is None:
        date_part = ""
    else:
        try:
            date_part = follow_date.isoformat(sep=" ", timespec="seconds")
        except (AttributeError, TypeError):
            date_part = str(follow_date)
    payload = "|".join([
        str(campaign_id),
        mobile or "",
        date_part,
        (raw_sales_rep or "").strip(),
        normalized_stage or "",
        (comment or "").strip(),
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ═══ Manager Intervention rules ═════════════════════════════════════════
# The token vocabulary used by the recalc and by every consumer that
# branches on intervention state. Keep these as constants — never inline
# the strings — so renames stay refactor-safe.

TRIGGER_NO_ANSWER_AFTER_FOLLOWING = "NO_ANSWER_AFTER_FOLLOWING"
TRIGGER_NO_ANSWER_AFTER_MEETING   = "NO_ANSWER_AFTER_MEETING"

PRIORITY_HIGH   = "HIGH"
PRIORITY_MEDIUM = "MEDIUM"

# Positive stages, in escalation order. Manager Intervention is ONLY raised
# when latest stage is NO_ANSWER AND at least one of these appeared earlier.
# MEETING outranks FOLLOWING — if both appear, the priority is HIGH.
POSITIVE_STAGES = {"FOLLOWING", "MEETING"}

# Status workflow for manager_intervention_flags. The recalc only ever
# writes 'OPEN'; the inbox PATCH endpoint moves rows to REVIEWED/CLOSED.
STATUS_OPEN     = "OPEN"
STATUS_REVIEWED = "REVIEWED"
STATUS_CLOSED   = "CLOSED"


# ═══ Recalc — Campaign KPIs ═════════════════════════════════════════════
#
# Rule (from the spec, do not loosen): the stage_counts buckets count
# LEADS, not events. For each unique (campaign, mobile) we take the lead's
# LATEST event by (follow_date DESC, id DESC) and bucket the lead under
# that event's normalized_stage. Events with NULL normalized_stage are
# excluded — they only show up in the unmatched_stages warnings.

def recalc_campaign_kpis(campaign_id: int, conn) -> dict:
    """Recompute total_leads and stage_counts for the campaign; upsert into
    campaign_kpis. Returns the new row as a dict (used by the overview
    endpoint to avoid an immediate re-read)."""
    with conn.cursor() as cur:
        # DISTINCT ON lets PG hand us the latest qualifying event per lead
        # in a single pass without a window function. Sort key is
        # (follow_date DESC, id DESC) so the tiebreaker for two events at
        # the exact same timestamp is "the one written last wins" — which
        # is how the CRM displays them too.
        cur.execute(
            """
            WITH latest AS (
                SELECT DISTINCT ON (le.lead_id)
                       le.lead_id, le.normalized_stage
                FROM lead_events le
                JOIN leads l ON l.id = le.lead_id
                WHERE l.campaign_id = %s
                  AND le.normalized_stage IS NOT NULL
                  AND le.is_voided = FALSE
                ORDER BY le.lead_id, le.follow_date DESC NULLS LAST, le.id DESC
            )
            SELECT normalized_stage, COUNT(*) AS n
            FROM latest
            GROUP BY normalized_stage
            """,
            (campaign_id,),
        )
        stage_counts = {row[0]: row[1] for row in cur.fetchall()}

        # total_leads = unique leads that have at least one valid event.
        # If we counted from leads directly we'd over-count rows whose
        # events all had unmatched stages.
        cur.execute(
            """
            SELECT COUNT(DISTINCT l.id)
            FROM leads l
            JOIN lead_events le ON le.lead_id = l.id
            WHERE l.campaign_id = %s
              AND le.normalized_stage IS NOT NULL
              AND le.is_voided = FALSE
            """,
            (campaign_id,),
        )
        total_leads = cur.fetchone()[0] or 0

        # manager_intervention_count tracks OPEN flags only. Closed/reviewed
        # rows live in the table for the audit trail but don't add to the
        # "needs attention" badge on the overview.
        cur.execute(
            """
            SELECT COUNT(*) FROM manager_intervention_flags
            WHERE campaign_id = %s AND status = %s
            """,
            (campaign_id, STATUS_OPEN),
        )
        intervention_count = cur.fetchone()[0] or 0

        cur.execute(
            """
            SELECT MAX(processed_at) FROM crm_report_uploads
            WHERE campaign_id = %s AND status = 'COMPLETED' AND is_voided = FALSE
            """,
            (campaign_id,),
        )
        last_upload_at = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO campaign_kpis (
                campaign_id, total_leads, stage_counts,
                manager_intervention_count, last_upload_at, updated_at
            )
            VALUES (%s, %s, %s::jsonb, %s, %s, NOW())
            ON CONFLICT (campaign_id) DO UPDATE SET
                total_leads                = EXCLUDED.total_leads,
                stage_counts               = EXCLUDED.stage_counts,
                manager_intervention_count = EXCLUDED.manager_intervention_count,
                last_upload_at             = EXCLUDED.last_upload_at,
                updated_at                 = NOW()
            """,
            (
                campaign_id,
                total_leads,
                _json_dumps_safe(stage_counts),
                intervention_count,
                last_upload_at,
            ),
        )
    conn.commit()
    return {
        "campaign_id": campaign_id,
        "total_leads": total_leads,
        "stage_counts": stage_counts,
        "manager_intervention_count": intervention_count,
        "last_upload_at": last_upload_at,
    }


# ═══ Recalc — Manager Intervention ══════════════════════════════════════
#
# Spec rules (these are the only triggers, do not add to them):
#
#   latest stage = NO_ANSWER
#     AND timeline contains MEETING earlier   → AFTER_MEETING   (HIGH)
#     ELSE timeline contains FOLLOWING earlier → AFTER_FOLLOWING (MEDIUM)
#     ELSE                                     → no flag
#
# Anything else (latest = NO_ANSWER from first contact, latest = MEETING,
# latest = CANCELLATION, rep change only, ...) → the recalc DELETEs any
# stale flag for that lead.
#
# Existing flag preservation:
#   - status = REVIEWED/CLOSED + trigger unchanged → only update descriptive
#     fields; leave status alone (the manager already touched it).
#   - status = REVIEWED/CLOSED + trigger changed → reset to OPEN and clear
#     reviewed_by/reviewed_at — the situation has materially shifted, so
#     the previous decision shouldn't carry forward.

def recalc_manager_intervention(campaign_id: int, conn) -> int:
    """Recompute the manager_intervention_flags rows for this campaign.
    Returns the number of OPEN flags after recalc."""
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Pull every event for the campaign in one query, ordered so the
        # Python pass can iterate per-lead with a simple groupby pattern.
        # We need ALL events (not just the latest) so we can scan for a
        # prior MEETING / FOLLOWING.
        cur.execute(
            """
            SELECT le.id, le.lead_id, le.normalized_stage, le.follow_date,
                   le.comment, le.sales_user_id
            FROM lead_events le
            JOIN leads l ON l.id = le.lead_id
            WHERE l.campaign_id = %s
              AND le.normalized_stage IS NOT NULL
              AND le.is_voided = FALSE
            ORDER BY le.lead_id, le.follow_date ASC NULLS LAST, le.id ASC
            """,
            (campaign_id,),
        )
        all_events = cur.fetchall()

        # Group events by lead_id. dict-of-lists is fine — sheets in the
        # tens of thousands of events still fit comfortably.
        by_lead: dict = {}
        for ev in all_events:
            by_lead.setdefault(ev["lead_id"], []).append(ev)

        # Existing flags so we can do preservation logic without a per-lead
        # SELECT inside the loop.
        cur.execute(
            """
            SELECT lead_id, trigger_type, status
            FROM manager_intervention_flags
            WHERE campaign_id = %s
            """,
            (campaign_id,),
        )
        existing_flags = {row["lead_id"]: row for row in cur.fetchall()}

        keep_lead_ids: set = set()

        for lead_id, events in by_lead.items():
            verdict = _classify_lead_intervention(events)
            if verdict is None:
                continue
            keep_lead_ids.add(lead_id)

            trigger = verdict["trigger"]
            existing = existing_flags.get(lead_id)

            if existing and existing["status"] in (STATUS_REVIEWED, STATUS_CLOSED):
                if existing["trigger_type"] == trigger:
                    # Same trigger, manager already actioned it — update
                    # descriptive fields only. Don't reset status.
                    cur.execute(
                        """
                        UPDATE manager_intervention_flags SET
                            current_stage            = %s,
                            previous_positive_stage  = %s,
                            priority                 = %s,
                            last_positive_stage_date = %s,
                            last_no_answer_date      = %s,
                            last_comment             = %s,
                            sales_user_id            = %s,
                            updated_at               = NOW()
                        WHERE lead_id = %s
                        """,
                        (
                            verdict["current_stage"],
                            verdict["previous_positive_stage"],
                            verdict["priority"],
                            verdict["last_positive_stage_date"],
                            verdict["last_no_answer_date"],
                            verdict["last_comment"],
                            verdict["sales_user_id"],
                            lead_id,
                        ),
                    )
                    continue
                # Trigger flipped (e.g. AFTER_FOLLOWING → AFTER_MEETING).
                # Material change → reopen so the manager re-evaluates.
                # Falls through to the upsert below with status=OPEN.

            cur.execute(
                """
                INSERT INTO manager_intervention_flags (
                    lead_id, campaign_id, sales_user_id, trigger_type,
                    current_stage, previous_positive_stage, priority,
                    last_positive_stage_date, last_no_answer_date,
                    last_comment, status, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (lead_id) DO UPDATE SET
                    campaign_id              = EXCLUDED.campaign_id,
                    sales_user_id            = EXCLUDED.sales_user_id,
                    trigger_type             = EXCLUDED.trigger_type,
                    current_stage            = EXCLUDED.current_stage,
                    previous_positive_stage  = EXCLUDED.previous_positive_stage,
                    priority                 = EXCLUDED.priority,
                    last_positive_stage_date = EXCLUDED.last_positive_stage_date,
                    last_no_answer_date      = EXCLUDED.last_no_answer_date,
                    last_comment             = EXCLUDED.last_comment,
                    status                   = %s,
                    reviewed_by              = NULL,
                    reviewed_at              = NULL,
                    updated_at               = NOW()
                """,
                (
                    lead_id, campaign_id, verdict["sales_user_id"], trigger,
                    verdict["current_stage"], verdict["previous_positive_stage"],
                    verdict["priority"],
                    verdict["last_positive_stage_date"], verdict["last_no_answer_date"],
                    verdict["last_comment"],
                    STATUS_OPEN,
                    # Second STATUS_OPEN — the DO UPDATE branch needs it
                    # via the literal, not EXCLUDED.status (which would
                    # mirror the INSERT value but read less clearly).
                    STATUS_OPEN,
                ),
            )

        # DELETE flags for leads in this campaign that no longer qualify.
        # We narrow by campaign_id so we never touch other campaigns' rows.
        if keep_lead_ids:
            cur.execute(
                """
                DELETE FROM manager_intervention_flags
                WHERE campaign_id = %s AND lead_id <> ALL(%s)
                """,
                (campaign_id, list(keep_lead_ids)),
            )
        else:
            cur.execute(
                "DELETE FROM manager_intervention_flags WHERE campaign_id = %s",
                (campaign_id,),
            )

        cur.execute(
            "SELECT COUNT(*) AS n FROM manager_intervention_flags "
            "WHERE campaign_id = %s AND status = %s",
            (campaign_id, STATUS_OPEN),
        )
        open_count = cur.fetchone()["n"]

    conn.commit()
    return open_count


def _classify_lead_intervention(events):
    """Apply the trigger rules to one lead's ordered events. Returns the
    flag fields if the lead qualifies, else None.

    Events come in ASC by (follow_date, id) so events[-1] is the latest.
    """
    if not events:
        return None
    latest = events[-1]
    if latest["normalized_stage"] != "NO_ANSWER":
        return None

    # Scan history before the latest for any positive stage. We walk in
    # reverse so the FIRST hit is the MOST RECENT positive stage — that
    # date goes into last_positive_stage_date.
    previous = events[:-1]
    last_meeting = None
    last_following = None
    for ev in reversed(previous):
        stage = ev["normalized_stage"]
        if stage == "MEETING" and last_meeting is None:
            last_meeting = ev
        elif stage == "FOLLOWING" and last_following is None:
            last_following = ev
        if last_meeting is not None and last_following is not None:
            break

    if last_meeting is not None:
        positive = last_meeting
        positive_stage = "MEETING"
        trigger = TRIGGER_NO_ANSWER_AFTER_MEETING
        priority = PRIORITY_HIGH
    elif last_following is not None:
        positive = last_following
        positive_stage = "FOLLOWING"
        trigger = TRIGGER_NO_ANSWER_AFTER_FOLLOWING
        priority = PRIORITY_MEDIUM
    else:
        return None

    return {
        "trigger": trigger,
        "priority": priority,
        "current_stage": "NO_ANSWER",
        "previous_positive_stage": positive_stage,
        "last_positive_stage_date": positive["follow_date"],
        "last_no_answer_date": latest["follow_date"],
        "last_comment": latest["comment"],
        "sales_user_id": latest["sales_user_id"],
    }


# ═══ Aggregator ═════════════════════════════════════════════════════════

def recalc_after_upload(campaign_id: int, conn) -> dict:
    """Called by the background upload thread after lead_events are written.

    Order matters: manager_intervention runs FIRST so campaign_kpis can read
    its own OPEN-count via the second recalc's SQL pass. Either step can
    raise — the processor catches and marks the upload FAILED so the user
    sees the actual error instead of "completed but the dashboard is wrong".
    """
    open_count = recalc_manager_intervention(campaign_id, conn)
    kpis = recalc_campaign_kpis(campaign_id, conn)
    return {"intervention_open": open_count, "kpis": kpis}


# ─── Local helpers ──────────────────────────────────────────────────────

def _json_dumps_safe(obj) -> str:
    """Stable JSON for jsonb columns — sorted keys, ASCII-safe defaults.
    Defined inline so the module's only third-party-ish import stays
    psycopg2 (used elsewhere)."""
    import json
    return json.dumps(obj, sort_keys=True, ensure_ascii=False)
