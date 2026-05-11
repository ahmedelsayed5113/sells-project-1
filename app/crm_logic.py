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
