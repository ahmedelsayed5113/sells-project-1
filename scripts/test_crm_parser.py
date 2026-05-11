"""
Smoke tests for the CRM upload pipeline (P1a).

Covers what's runnable WITHOUT touching the real database:
  - normalize_mobile     — every Egyptian/Gulf shape we promised to handle
  - normalize_stage      — DEFAULT_STAGE_MAP path (conn=None)
  - normalize_sales_name — whitespace/case collapsing
  - compute_event_hash   — stable across runs, varies on every component
  - parse_crm_excel      — forward-fill on Client name / Mobile, header
                           aliases, unmatched rep collection, comment
                           passthrough
  - dedup via event_hash — same row twice → same hash

The parser smoke test feeds in a FakeConn so we don't need PostgreSQL
running locally. The blueprint, the background thread, and the live INSERTs
into lead_events live in app/crm_processor.py — those need an actual DB
and are smoke-tested with `curl` once the server is up.

Run: PYTHONIOENCODING=utf-8 DISABLE_SYNC=true python scripts/test_crm_parser.py
"""
import io
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openpyxl import Workbook  # noqa: E402

from app.crm_logic import (  # noqa: E402
    compute_event_hash,
    normalize_mobile,
    normalize_sales_name,
    normalize_stage,
)
from app.crm_parser import parse_crm_excel  # noqa: E402


# ─── Tiny harness ───────────────────────────────────────────────────────

_failures = 0


def _check(name, ok, detail=""):
    global _failures
    if ok:
        print(f"  ok   {name}")
    else:
        _failures += 1
        print(f"  FAIL {name}: {detail}")


# ─── FakeConn — satisfies the bits parse_crm_excel needs ───────────────
#
# normalize_stage and match_sales_user both call `conn.cursor()` as a
# context manager and run a SELECT. We return zero rows for every query
# so the helpers fall back to DEFAULT_STAGE_MAP / users-table-not-found
# (also empty under FakeConn). That's enough to exercise the parser end-
# to-end without standing up Postgres.

class _FakeCursor:
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, *_args, **_kwargs): return None
    def fetchone(self): return None
    def fetchall(self): return []


class _FakeConn:
    def cursor(self, *a, **kw): return _FakeCursor()


# ─── Mobile normalization ───────────────────────────────────────────────

def test_normalize_mobile():
    print("─── normalize_mobile ───")
    _check("01012345678 → 201012345678", normalize_mobile("01012345678") == "201012345678")
    _check("+20 100 123 4567 → 201001234567",
           normalize_mobile("+20 100 123 4567") == "201001234567")
    _check("00201012345678 → 201012345678",
           normalize_mobile("00201012345678") == "201012345678")
    _check("+971569116811 → 971569116811",
           normalize_mobile("+971569116811") == "971569116811")
    _check("with dashes → digits",
           normalize_mobile("010-1234-5678") == "201012345678")
    _check("with parens → digits",
           normalize_mobile("(010) 1234 5678") == "201012345678")
    _check("10-digit '1...' → prepend 20",
           normalize_mobile("1012345678") == "201012345678")
    _check("None → None", normalize_mobile(None) is None)
    _check("empty → None", normalize_mobile("") is None)
    _check("whitespace → None", normalize_mobile("   ") is None)
    _check("letters → None", normalize_mobile("abcd") is None)
    _check("too short → None", normalize_mobile("1234567") is None)
    # openpyxl returns an int when Excel formatted the cell as a number —
    # the int form must reach the same canonical output.
    _check("int input round-trips",
           normalize_mobile(201012345678) == "201012345678")
    _check("float input round-trips",
           normalize_mobile(201012345678.0) == "201012345678")


# ─── Stage normalization (default map only) ────────────────────────────

def test_normalize_stage():
    print("─── normalize_stage (DEFAULT_STAGE_MAP, conn=None) ───")
    _check("No Answer → NO_ANSWER", normalize_stage("No Answer") == "NO_ANSWER")
    _check("  Following → FOLLOWING (trim)",
           normalize_stage("  Following  ") == "FOLLOWING")
    _check("Zoom Meeting → MEETING", normalize_stage("Zoom Meeting") == "MEETING")
    _check("Meeting Done → MEETING", normalize_stage("Meeting Done") == "MEETING")
    _check("Cancelled → CANCELLATION", normalize_stage("Cancelled") == "CANCELLATION")
    _check("Canceled  → CANCELLATION (US spelling)",
           normalize_stage("Canceled") == "CANCELLATION")
    _check("Interested → INTERESTED", normalize_stage("Interested") == "INTERESTED")
    _check("unknown → None", normalize_stage("Discounted via Zoom") is None)
    _check("None → None", normalize_stage(None) is None)
    _check("empty → None", normalize_stage("") is None)


# ─── Sales name normalization ───────────────────────────────────────────

def test_normalize_sales_name():
    print("─── normalize_sales_name ───")
    _check("trim + lower", normalize_sales_name("  Mahmoud Amr  ") == "mahmoud amr")
    _check("collapse multi spaces",
           normalize_sales_name("Mahmoud   Amr") == "mahmoud amr")
    _check("mixed → consistent",
           normalize_sales_name(" mahmoud   AMR ") == "mahmoud amr")
    _check("None → empty", normalize_sales_name(None) == "")


# ─── Event hashing ──────────────────────────────────────────────────────

def test_compute_event_hash():
    print("─── compute_event_hash ───")
    args = dict(
        campaign_id=12, mobile="201012345678",
        follow_date=datetime(2026, 4, 23, 13, 42, 50),
        raw_sales_rep="Mahmoud Amr", normalized_stage="NO_ANSWER",
        comment="بعتله واتس",
    )
    h1 = compute_event_hash(**args)
    h2 = compute_event_hash(**args)
    _check("deterministic across calls", h1 == h2)
    _check("64-char hex", len(h1) == 64 and all(c in "0123456789abcdef" for c in h1))

    diff = dict(args)
    diff["mobile"] = "201019999999"
    _check("changes with mobile", compute_event_hash(**diff) != h1)
    diff = dict(args)
    diff["follow_date"] = datetime(2026, 4, 24, 13, 42, 50)
    _check("changes with date", compute_event_hash(**diff) != h1)
    diff = dict(args)
    diff["normalized_stage"] = "FOLLOWING"
    _check("changes with stage", compute_event_hash(**diff) != h1)


# ─── Parser end-to-end with FakeConn ────────────────────────────────────

def _build_sample_xlsx() -> bytes:
    """Build an in-memory .xlsx that exercises:
      - header aliases ("Phone" instead of "Mobile", "Notes" instead of "Comment")
      - forward-fill (rows 2 and 3 inherit row 1's client/mobile)
      - mixed stages (one known, one unknown — unknown lands in unmatched_stages)
      - a totally blank row (skipped)
      - a row with mobile-but-no-client (still attached to the client_name in scope)
    """
    wb = Workbook()
    ws = wb.active
    ws.append(["Client name", "Phone", "Stage", "Follow Date", "Sales Rep", "Notes"])

    # Lead 1: 3 events, forward-fill from row 1
    ws.append(["Ahmed Yehia", "01012345678", "Following",
               datetime(2026, 4, 21, 11, 0), "Mahmoud Amr", "first call"])
    ws.append([None, None, "Meeting",
               datetime(2026, 4, 22, 14, 0), "Mahmoud Amr", "zoom done"])
    ws.append([None, None, "No Answer",
               datetime(2026, 4, 23, 12, 0), "Reham Hany", "stopped responding"])

    # Totally blank row — should be skipped
    ws.append([None, None, None, None, None, None])

    # Lead 2: brand new client+mobile in one row
    ws.append(["Sara Ali", "+971569116811", "Interested",
               datetime(2026, 4, 24, 9, 30), "Mahmoud Amr", "wants brochure"])

    # Unknown stage — should land in unmatched_stages
    ws.append([None, None, "Discounted Special",
               datetime(2026, 4, 24, 10, 0), "Mahmoud Amr", "promo offer"])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


def test_parser():
    print("─── parse_crm_excel ───")
    xlsx_bytes = _build_sample_xlsx()
    result = parse_crm_excel(io.BytesIO(xlsx_bytes), campaign_id=42, conn=_FakeConn())

    rows = result["rows"]
    _check(f"parsed 5 event rows (got {len(rows)})", len(rows) == 5)

    # Forward-fill
    if len(rows) >= 3:
        first_three = rows[:3]
        all_ahmed = all(r["client_name"] == "Ahmed Yehia" for r in first_three)
        _check("forward-fill carries client_name", all_ahmed,
               detail=str([r["client_name"] for r in first_three]))
        all_same_mobile = all(r["mobile"] == "201012345678" for r in first_three)
        _check("forward-fill carries mobile (normalized)", all_same_mobile,
               detail=str([r["mobile"] for r in first_three]))

    # Stage normalization
    if len(rows) >= 3:
        _check("row 2 stage Following → FOLLOWING",
               rows[0]["normalized_stage"] == "FOLLOWING")
        _check("row 3 stage Meeting → MEETING",
               rows[1]["normalized_stage"] == "MEETING")
        _check("row 4 stage No Answer → NO_ANSWER",
               rows[2]["normalized_stage"] == "NO_ANSWER")

    # Sara is row 4 in `rows` because the totally-blank one was skipped
    if len(rows) >= 5:
        sara = rows[3]
        _check("Sara's mobile normalized",
               sara["mobile"] == "971569116811", detail=sara["mobile"])
        _check("Sara's stage Interested → INTERESTED",
               sara["normalized_stage"] == "INTERESTED")

        unknown = rows[4]
        _check("unknown stage → normalized_stage=None",
               unknown["normalized_stage"] is None)
        # The unknown row still carries the previous client/mobile (forward-fill)
        _check("unknown row inherits Sara's mobile",
               unknown["mobile"] == "971569116811")

    # Unmatched stages / reps
    _check("unmatched_stages contains 'Discounted Special'",
           "Discounted Special" in result["unmatched_stages"],
           detail=str(result["unmatched_stages"]))
    # Without a DB, no rep can resolve to a user → both reps land here
    _check("unmatched_sales_reps includes both reps",
           set(result["unmatched_sales_reps"]) == {"Mahmoud Amr", "Reham Hany"},
           detail=str(result["unmatched_sales_reps"]))

    # Comment passthrough
    if rows:
        _check("comment preserved", rows[0]["comment"] == "first call",
               detail=repr(rows[0].get("comment")))


# ─── Dedup via event_hash ──────────────────────────────────────────────

def test_event_hash_dedup():
    print("─── event_hash dedup (same input → same hash) ───")
    # Two parses of the same bytes should produce identical event_hash sets,
    # which is what the UNIQUE constraint exploits on re-upload.
    xlsx_bytes = _build_sample_xlsx()
    r1 = parse_crm_excel(io.BytesIO(xlsx_bytes), campaign_id=42, conn=_FakeConn())
    r2 = parse_crm_excel(io.BytesIO(xlsx_bytes), campaign_id=42, conn=_FakeConn())

    def hashes(rows):
        return [
            compute_event_hash(
                campaign_id=42,
                mobile=r["mobile"],
                follow_date=r["follow_date"],
                raw_sales_rep=r["raw_sales_rep_name"],
                normalized_stage=r["normalized_stage"],
                comment=r["comment"],
            )
            for r in rows
        ]

    h1, h2 = hashes(r1["rows"]), hashes(r2["rows"])
    _check("same sheet → identical hash sequence", h1 == h2)
    _check("hashes within a sheet are unique",
           len(set(h1)) == len(h1),
           detail=f"got {len(h1)} hashes, {len(set(h1))} unique")


# ─── Required-column enforcement ───────────────────────────────────────

def test_missing_required_column_raises():
    print("─── missing required column raises ValueError ───")
    wb = Workbook()
    ws = wb.active
    # Intentionally drop "Sales Rep"
    ws.append(["Client name", "Mobile", "Stage", "Follow Date", "Notes"])
    ws.append(["X", "01012345678", "Following",
               datetime(2026, 4, 21, 11, 0), "first call"])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    raised = False
    try:
        parse_crm_excel(buf, campaign_id=1, conn=_FakeConn())
    except ValueError as e:
        raised = True
        msg_ok = "sales_rep" in str(e).lower() or "sales rep" in str(e).lower()
        _check("error message names the missing column", msg_ok, detail=str(e))
    _check("ValueError was raised", raised)


# ─── Driver ────────────────────────────────────────────────────────────

def main():
    test_normalize_mobile()
    test_normalize_stage()
    test_normalize_sales_name()
    test_compute_event_hash()
    test_parser()
    test_event_hash_dedup()
    test_missing_required_column_raises()

    print()
    if _failures:
        print(f"❌ {_failures} failure(s)")
        sys.exit(1)
    print("✅ all green")


if __name__ == "__main__":
    main()
