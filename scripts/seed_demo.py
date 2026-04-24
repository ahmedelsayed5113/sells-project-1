"""
Demo-data seeder for Ain KPI.

Creates:
  - 1 Sales Manager
  - 2 Team Leaders (each leading a team of 5 Sales)
  - 1 Data Entry user
  - 1 Marketing Manager
  - 10 Sales (5 per team)
  - 3 months of KPI entries (last 3 months) with varied performance
  - 2 marketing campaigns (one with full actuals, one with partial)

Idempotent: running it again will upsert users and KPI rows, not duplicate.

Usage:
  # From project root — must have DATABASE_URL in env (or local DB config):
  python scripts/seed_demo.py

  # On Railway:
  railway run python scripts/seed_demo.py
"""
import os
import random
import sys
from datetime import datetime, timedelta

# Make "app" importable when invoked as a one-off script.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.auth import hash_password
from app.database import get_conn
from app.kpi_logic import KPI_CONFIG, compute_score


DEMO_PASSWORD = "Demo1234!"

USERS = [
    # username, full_name, role, email
    ("omar.manager", "Omar Hassan", "manager",     "omar.manager@demo.ain"),
    ("ali.tl",       "Ali Ahmed",   "team_leader", "ali.tl@demo.ain"),
    ("sara.tl",      "Sara Ibrahim","team_leader", "sara.tl@demo.ain"),
    ("menna",        "Menna Farouk","dataentry",   "menna@demo.ain"),
    ("nour.mkt",     "Nour Mahmoud","marketing",   "nour.mkt@demo.ain"),
]

TEAMS = [
    # team_name, leader_username, sales_members [(username, full_name), ...]
    ("Team Alpha", "ali.tl", [
        ("sales.ahmed",   "Ahmed Salah"),
        ("sales.mostafa", "Mostafa Tarek"),
        ("sales.laila",   "Laila Kamal"),
        ("sales.khaled",  "Khaled Naguib"),
        ("sales.hana",    "Hana Adel"),
    ]),
    ("Team Beta", "sara.tl", [
        ("sales.youssef", "Youssef Amr"),
        ("sales.mariam",  "Mariam Hany"),
        ("sales.tamer",   "Tamer Wael"),
        ("sales.reem",    "Reem Gamal"),
        ("sales.bassel",  "Bassel Ezz"),
    ]),
]

# Three most recent months (YYYY-MM)
def _recent_months(n=3):
    today = datetime.utcnow()
    out = []
    y, m = today.year, today.month
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12; y -= 1
    return list(reversed(out))


# ─── User / team seeding ─────────────────────────────────────────────────

def upsert_user(conn, username, full_name, role, email, password=DEMO_PASSWORD):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
        row = cur.fetchone()
        if row:
            uid = row[0]
            cur.execute("""
                UPDATE users SET full_name=%s, role=%s, email=%s,
                    password_hash=%s, active=true, updated_at=NOW()
                WHERE id=%s
            """, (full_name, role, email, hash_password(password), uid))
            return uid
        cur.execute("""
            INSERT INTO users (username, full_name, role, email, password_hash, active)
            VALUES (%s, %s, %s, %s, %s, true)
            RETURNING id
        """, (username, full_name, role, email, hash_password(password)))
        return cur.fetchone()[0]


def upsert_team(conn, name, description, leader_id):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM teams WHERE name=%s", (name,))
        row = cur.fetchone()
        if row:
            tid = row[0]
            cur.execute("UPDATE teams SET description=%s, leader_id=%s WHERE id=%s",
                        (description, leader_id, tid))
            return tid
        cur.execute("""
            INSERT INTO teams (name, description, leader_id)
            VALUES (%s, %s, %s) RETURNING id
        """, (name, description, leader_id))
        return cur.fetchone()[0]


def attach_members(conn, team_id, user_ids):
    with conn.cursor() as cur:
        # Clear any previous team assignment for these users, then set to this team.
        cur.execute("UPDATE users SET team_id=%s, updated_at=NOW() WHERE id = ANY(%s)",
                    (team_id, user_ids))


# ─── KPI seeding ─────────────────────────────────────────────────────────

# Performance profiles — drive how close each rep gets to target.
PROFILES = {
    "excellent": {"lo": 0.92, "hi": 1.05, "passfail_fail_chance": 0.02},
    "vgood":     {"lo": 0.80, "hi": 0.94, "passfail_fail_chance": 0.06},
    "good":      {"lo": 0.60, "hi": 0.82, "passfail_fail_chance": 0.12},
    "medium":    {"lo": 0.45, "hi": 0.65, "passfail_fail_chance": 0.22},
    "weak":      {"lo": 0.25, "hi": 0.50, "passfail_fail_chance": 0.35},
}

# One stable profile per sales rep so trends make sense.
SALES_PROFILES = {
    "sales.ahmed":   "excellent",
    "sales.mostafa": "vgood",
    "sales.laila":   "good",
    "sales.khaled":  "medium",
    "sales.hana":    "weak",
    "sales.youssef": "excellent",
    "sales.mariam":  "good",
    "sales.tamer":   "vgood",
    "sales.reem":    "medium",
    "sales.bassel":  "good",
}


def seed_kpi_for_rep(conn, user_id, username, month, rng: random.Random):
    profile = PROFILES[SALES_PROFILES.get(username, "good")]
    lo, hi = profile["lo"], profile["hi"]

    fresh_leads = rng.randint(80, 160)

    # For each numeric KPI, aim for profile_factor × target
    numbers = {}
    for key, cfg in KPI_CONFIG.items():
        if cfg.get("input_type") == "passfail":
            # 0 or 100
            numbers[key] = 0 if rng.random() < profile["passfail_fail_chance"] else 100
            continue

        factor = rng.uniform(lo, hi)
        tgt_type = cfg.get("target_type")
        if tgt_type == "fixed":
            target = cfg["target"]
        elif tgt_type == "leads_pct":
            target = fresh_leads * cfg["target_pct"]
        else:
            target = 100

        actual = target * factor
        # Percent fields: keep as percent (0..100+), cap sanely
        if cfg.get("input_type") == "percent":
            actual = round(min(actual, 100.0), 1)
        else:
            actual = round(actual)
        numbers[key] = actual

    # Write the row
    params = {
        "user_id": user_id,
        "month": month,
        "fresh_leads": fresh_leads,
        "calls":        int(numbers.get("calls", 0)),
        "meetings":     int(numbers.get("meetings", 0)),
        "crm_pct":      float(numbers.get("crm_pct", 0)),
        "deals":        int(numbers.get("deals", 0)),
        "reports":      int(numbers.get("reports", 0)),
        "reservations": int(numbers.get("reservations", 0)),
        "followup_pct": float(numbers.get("followup_pct", 0)),
        "attendance_pct": float(numbers.get("attendance_pct", 0)),
        "attitude":     int(numbers.get("attitude", 100)),
        "presentation": int(numbers.get("presentation", 100)),
        "behaviour":    int(numbers.get("behaviour", 100)),
        "appearance":   int(numbers.get("appearance", 100)),
        "hr_roles":     int(numbers.get("hr_roles", 100)),
    }

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO kpi_entries (user_id, month,
                fresh_leads, calls, meetings, crm_pct, deals,
                reports, reservations, followup_pct, attendance_pct,
                attitude, presentation, behaviour, appearance, hr_roles,
                sales_submitted_at, dataentry_submitted_at)
            VALUES (%(user_id)s, %(month)s,
                %(fresh_leads)s, %(calls)s, %(meetings)s, %(crm_pct)s, %(deals)s,
                %(reports)s, %(reservations)s, %(followup_pct)s, %(attendance_pct)s,
                %(attitude)s, %(presentation)s, %(behaviour)s, %(appearance)s, %(hr_roles)s,
                NOW(), NOW())
            ON CONFLICT (user_id, month) DO UPDATE SET
                fresh_leads = EXCLUDED.fresh_leads,
                calls = EXCLUDED.calls,
                meetings = EXCLUDED.meetings,
                crm_pct = EXCLUDED.crm_pct,
                deals = EXCLUDED.deals,
                reports = EXCLUDED.reports,
                reservations = EXCLUDED.reservations,
                followup_pct = EXCLUDED.followup_pct,
                attendance_pct = EXCLUDED.attendance_pct,
                attitude = EXCLUDED.attitude,
                presentation = EXCLUDED.presentation,
                behaviour = EXCLUDED.behaviour,
                appearance = EXCLUDED.appearance,
                hr_roles = EXCLUDED.hr_roles,
                sales_submitted_at = NOW(),
                dataentry_submitted_at = NOW(),
                updated_at = NOW()
            RETURNING id
        """, params)

    # Compute total score
    total, rating, _ = compute_score(params)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE kpi_entries SET total_score=%s, rating=%s, updated_at=NOW()
            WHERE user_id=%s AND month=%s
        """, (total, rating, user_id, month))


def seed_tl_manual_eval(conn, tl_id, month, rng: random.Random):
    """TL's own manual fields (what Sales Manager fills via /tl-evaluation)."""
    params = {
        "user_id": tl_id,
        "month": month,
        "crm_pct":          round(rng.uniform(80, 100), 1),
        "reports":          rng.randint(3, 5),
        "clients_pipeline": round(rng.uniform(60, 95), 1),
        "attitude":         100,
        "presentation":     100 if rng.random() > 0.1 else 0,
        "behaviour":        100,
        "appearance":       100,
        "attendance_pct":   100 if rng.random() > 0.1 else 0,
        "hr_roles":         100,
    }
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO kpi_entries (user_id, month,
                crm_pct, reports, clients_pipeline,
                attitude, presentation, behaviour, appearance, attendance_pct, hr_roles,
                dataentry_submitted_at)
            VALUES (%(user_id)s, %(month)s,
                %(crm_pct)s, %(reports)s, %(clients_pipeline)s,
                %(attitude)s, %(presentation)s, %(behaviour)s, %(appearance)s,
                %(attendance_pct)s, %(hr_roles)s,
                NOW())
            ON CONFLICT (user_id, month) DO UPDATE SET
                crm_pct          = EXCLUDED.crm_pct,
                reports          = EXCLUDED.reports,
                clients_pipeline = EXCLUDED.clients_pipeline,
                attitude         = EXCLUDED.attitude,
                presentation     = EXCLUDED.presentation,
                behaviour        = EXCLUDED.behaviour,
                appearance       = EXCLUDED.appearance,
                attendance_pct   = EXCLUDED.attendance_pct,
                hr_roles         = EXCLUDED.hr_roles,
                dataentry_submitted_at = NOW(),
                updated_at       = NOW()
        """, params)


# ─── Marketing campaigns ─────────────────────────────────────────────────

def upsert_campaign(conn, user_id, name, avg_price, comm_input, ctype, cr, budget,
                    actuals=None, month=None):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM marketing_campaigns WHERE campaign_name=%s", (name,))
        row = cur.fetchone()
        if row:
            cid = row[0]
            cur.execute("""
                UPDATE marketing_campaigns SET
                    avg_unit_price=%s, commission_input=%s, commission_type=%s,
                    expected_close_rate=%s, campaign_budget=%s, month=%s,
                    tax_rate=0.19, recommended_scenario='balanced',
                    updated_at=NOW()
                WHERE id=%s
            """, (avg_price, comm_input, ctype, cr, budget, month, cid))
        else:
            cur.execute("""
                INSERT INTO marketing_campaigns
                    (user_id, campaign_name, avg_unit_price, commission_input, commission_type,
                     tax_rate, expected_close_rate, campaign_budget, recommended_scenario, month)
                VALUES (%s, %s, %s, %s, %s, 0.19, %s, %s, 'balanced', %s)
                RETURNING id
            """, (user_id, name, avg_price, comm_input, ctype, cr, budget, month))
            cid = cur.fetchone()[0]

        if actuals is not None:
            cur.execute("""
                INSERT INTO marketing_actuals (campaign_id,
                    actual_spend, actual_leads, actual_qualified_leads,
                    actual_meetings, actual_follow_ups, actual_deals)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (campaign_id) DO UPDATE SET
                    actual_spend = EXCLUDED.actual_spend,
                    actual_leads = EXCLUDED.actual_leads,
                    actual_qualified_leads = EXCLUDED.actual_qualified_leads,
                    actual_meetings = EXCLUDED.actual_meetings,
                    actual_follow_ups = EXCLUDED.actual_follow_ups,
                    actual_deals = EXCLUDED.actual_deals,
                    updated_at = NOW()
            """, (cid,
                  actuals.get("spend", 0), actuals.get("leads", 0),
                  actuals.get("ql", 0), actuals.get("meetings", 0),
                  actuals.get("fu", 0), actuals.get("deals", 0)))
    return cid


# ─── Main ────────────────────────────────────────────────────────────────

def main():
    rng = random.Random(42)  # stable seeds for reproducible demo
    months = _recent_months(3)
    print(f"→ Seeding months: {months}")

    conn = get_conn()
    conn.autocommit = False

    try:
        # 1. Users
        user_ids = {}
        for username, full_name, role, email in USERS:
            uid = upsert_user(conn, username, full_name, role, email)
            user_ids[username] = uid
            print(f"   ✓ user {username:<16} → id {uid}")

        # 2. Sales reps (one per team entry)
        for team_name, _, members in TEAMS:
            for username, full_name in members:
                uid = upsert_user(conn, username, full_name, "sales",
                                  f"{username}@demo.ain")
                user_ids[username] = uid
                print(f"   ✓ sales  {username:<16} → id {uid}")

        # 3. Teams
        for team_name, leader_username, members in TEAMS:
            leader_id = user_ids[leader_username]
            tid = upsert_team(conn, team_name, f"{team_name} demo team", leader_id)
            member_ids = [user_ids[u] for u, _ in members]
            attach_members(conn, tid, member_ids)
            print(f"   ✓ team  {team_name} (leader={leader_username}, members={len(member_ids)})")

        conn.commit()

        # 4. KPI entries for sales reps × 3 months
        for team_name, _, members in TEAMS:
            for username, _ in members:
                uid = user_ids[username]
                for m in months:
                    seed_kpi_for_rep(conn, uid, username, m, rng)
        print(f"   ✓ KPI entries for {sum(len(m) for _,_,m in TEAMS)} reps × {len(months)} months")

        # 5. TL manual evaluations × 3 months
        for _, leader_username, _ in TEAMS:
            tl_id = user_ids[leader_username]
            for m in months:
                seed_tl_manual_eval(conn, tl_id, m, rng)
        print(f"   ✓ TL manual evaluations × {len(months)} months")

        conn.commit()

        # 6. Marketing campaigns
        mk_uid = user_ids["nour.mkt"]
        current_month = months[-1]
        prev_month = months[0]

        upsert_campaign(
            conn, mk_uid,
            name="North Coast Summer 2026",
            avg_price=8_500_000, comm_input=4.5, ctype="percentage",
            cr=0.02, budget=450_000, month=current_month,
            actuals={"spend": 380_000, "leads": 2100, "ql": 520,
                     "meetings": 140, "fu": 1600, "deals": 42},
        )
        upsert_campaign(
            conn, mk_uid,
            name="New Cairo Launch",
            avg_price=6_200_000, comm_input=3.5, ctype="percentage",
            cr=0.025, budget=300_000, month=current_month,
            actuals={"spend": 310_000, "leads": 1650, "ql": 380,
                     "meetings": 95, "fu": 1100, "deals": 28},
        )
        upsert_campaign(
            conn, mk_uid,
            name="Sahel Premium",
            avg_price=12_000_000, comm_input=5.0, ctype="percentage",
            cr=0.015, budget=600_000, month=prev_month,
            actuals=None,  # No actuals yet — template state
        )
        print("   ✓ 3 marketing campaigns (2 with actuals, 1 template)")

        conn.commit()

        print("\n✅ Demo data seeded.\n")
        _print_credentials_table()

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Seed failed: {e}")
        raise
    finally:
        conn.close()


def _print_credentials_table():
    print("=" * 72)
    print("DEMO CREDENTIALS")
    print("=" * 72)
    print(f"{'Username':<22} {'Password':<14} {'Role':<14} {'Full Name'}")
    print("-" * 72)
    for username, full_name, role, _ in USERS:
        print(f"{username:<22} {DEMO_PASSWORD:<14} {role:<14} {full_name}")
    for _, _, members in TEAMS:
        for username, full_name in members:
            print(f"{username:<22} {DEMO_PASSWORD:<14} {'sales':<14} {full_name}")
    print("=" * 72)
    print()
    print("Login at: /login")
    print("Admin account was created on first deploy — keep using that for admin access.")
    print()


if __name__ == "__main__":
    main()
