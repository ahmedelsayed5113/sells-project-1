"""
Database: connection + schema initialization
Extended schema with Finance, HR, Teams, and more roles
"""
import logging
import threading
import time
import psycopg2
import psycopg2.extras
import psycopg2.pool
from config import Config

log = logging.getLogger(__name__)

_POOL = None
_POOL_LOCK = threading.Lock()
_POOL_MIN = 1
_POOL_MAX = 10


def _build_pool():
    """Create the connection pool. Threaded so Flask + Gunicorn workers are safe."""
    if Config.DATABASE_URL:
        return psycopg2.pool.ThreadedConnectionPool(
            _POOL_MIN, _POOL_MAX,
            dsn=Config.DATABASE_URL,
            connect_timeout=10,
        )
    return psycopg2.pool.ThreadedConnectionPool(
        _POOL_MIN, _POOL_MAX,
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        connect_timeout=10,
    )


def _get_pool():
    global _POOL
    if _POOL is not None:
        return _POOL
    with _POOL_LOCK:
        if _POOL is None:
            _POOL = _build_pool()
    return _POOL


class _PooledConnection:
    """Proxy that returns the underlying connection to the pool on .close().

    All 42 existing call sites use `conn.close()` in a finally block; this
    wrapper makes that release the connection back to the pool instead of
    actually closing the socket.
    """

    def __init__(self, conn, pool):
        self.__dict__["_conn"] = conn
        self.__dict__["_pool"] = pool
        self.__dict__["_returned"] = False

    def close(self):
        if self.__dict__["_returned"]:
            return
        self.__dict__["_returned"] = True
        conn = self.__dict__["_conn"]
        pool = self.__dict__["_pool"]
        try:
            try:
                # Clear any aborted-transaction state before recycling.
                conn.rollback()
            except Exception:
                pass
            pool.putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    def __getattr__(self, name):
        return getattr(self.__dict__["_conn"], name)

    def __setattr__(self, name, value):
        setattr(self.__dict__["_conn"], name, value)

    def __enter__(self):
        # psycopg2 connections used as a context manager commit/rollback the
        # current transaction but do NOT close the connection. Mirror that.
        return self.__dict__["_conn"].__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.__dict__["_conn"].__exit__(exc_type, exc_val, exc_tb)


def get_conn(retries=2):
    """Return a pooled connection. .close() returns it to the pool."""
    pool = _get_pool()
    last_err = None
    for attempt in range(retries + 1):
        try:
            raw = pool.getconn()
            # Liveness check: pooled connections can go stale (server restart,
            # idle-timeout). A cheap SELECT 1 catches it before the caller does.
            try:
                with raw.cursor() as cur:
                    cur.execute("SELECT 1")
                raw.commit()
            except Exception:
                try:
                    pool.putconn(raw, close=True)
                except Exception:
                    pass
                raise psycopg2.OperationalError("stale pooled connection")
            return _PooledConnection(raw, pool)
        except psycopg2.OperationalError as e:
            last_err = e
            if attempt < retries:
                time.sleep(1)
                continue
            raise
    raise last_err


def table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )
        """, (table_name,))
        return cur.fetchone()[0]


def column_exists(conn, table_name: str, column_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
            )
        """, (table_name, column_name))
        return cur.fetchone()[0]


def init_all_tables():
    """Create all tables + migrate existing ones."""
    conn = None
    try:
        conn = get_conn()

        with conn.cursor() as cur:
            # ═══ USERS ══════════════════════════════════════════════════════
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100) NOT NULL UNIQUE,
                    full_name VARCHAR(150) NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(20) NOT NULL DEFAULT 'sales',
                    email VARCHAR(150),
                    phone VARCHAR(30),
                    active BOOLEAN DEFAULT true,
                    team_id INTEGER,
                    preferred_lang VARCHAR(5) DEFAULT 'ar',
                    preferred_theme VARCHAR(10) DEFAULT 'dark',
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    last_login TIMESTAMP
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(LOWER(username));")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_team_id ON users(team_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_team_role_active ON users(team_id, role, active);")

            # Migrate old users table
            for col, ddl in [
                ("team_id", "ALTER TABLE users ADD COLUMN IF NOT EXISTS team_id INTEGER"),
                ("preferred_lang", "ALTER TABLE users ADD COLUMN IF NOT EXISTS preferred_lang VARCHAR(5) DEFAULT 'ar'"),
                ("preferred_theme", "ALTER TABLE users ADD COLUMN IF NOT EXISTS preferred_theme VARCHAR(10) DEFAULT 'dark'"),
                ("failed_logins", "ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_logins INTEGER DEFAULT 0"),
                ("locked_until", "ALTER TABLE users ADD COLUMN IF NOT EXISTS locked_until TIMESTAMP"),
            ]:
                if not column_exists(conn, "users", col):
                    cur.execute(ddl)

            # Case-insensitive uniqueness on email (when present)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_lower
                ON users (LOWER(email))
                WHERE email IS NOT NULL AND email <> ''
            """)

            # ═══ PASSWORD RESET TOKENS ══════════════════════════════════════
            cur.execute("""
                CREATE TABLE IF NOT EXISTS password_reset_tokens (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    token_hash VARCHAR(128) NOT NULL UNIQUE,
                    expires_at TIMESTAMP NOT NULL,
                    used_at TIMESTAMP,
                    created_ip VARCHAR(64),
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_prt_user ON password_reset_tokens(user_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_prt_exp ON password_reset_tokens(expires_at);")

            # ═══ TEAMS ══════════════════════════════════════════════════════
            cur.execute("""
                CREATE TABLE IF NOT EXISTS teams (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE,
                    leader_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_teams_leader_id ON teams(leader_id);")

            # ═══ KPI ENTRIES (extended) ═════════════════════════════════════
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kpi_entries (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    month VARCHAR(7) NOT NULL,

                    fresh_leads INTEGER DEFAULT 0,
                    calls INTEGER DEFAULT 0,
                    meetings INTEGER DEFAULT 0,
                    crm_pct NUMERIC(5,2) DEFAULT 0,
                    deals INTEGER DEFAULT 0,
                    reports INTEGER DEFAULT 0,
                    reservations INTEGER DEFAULT 0,
                    followup_pct NUMERIC(5,2) DEFAULT 0,
                    attendance_pct NUMERIC(5,2) DEFAULT 0,
                    sales_submitted_at TIMESTAMP,

                    attitude INTEGER DEFAULT 0,
                    presentation INTEGER DEFAULT 0,
                    behaviour INTEGER DEFAULT 0,
                    appearance INTEGER DEFAULT 0,
                    hr_roles INTEGER DEFAULT 0,
                    dataentry_submitted_at TIMESTAMP,
                    dataentry_by INTEGER REFERENCES users(id) ON DELETE SET NULL,

                    revenue_generated NUMERIC(12,2) DEFAULT 0,
                    training_hours INTEGER DEFAULT 0,
                    client_compliments INTEGER DEFAULT 0,
                    client_complaints INTEGER DEFAULT 0,

                    notes TEXT,
                    total_score NUMERIC(5,2) DEFAULT 0,
                    rating VARCHAR(20) DEFAULT 'Pending',

                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),

                    UNIQUE(user_id, month)
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_user_month ON kpi_entries(user_id, month);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_month ON kpi_entries(month);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_dataentry_submitted ON kpi_entries(dataentry_submitted_at);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_sales_submitted ON kpi_entries(sales_submitted_at);")

            # Migrate kpi_entries if needed
            for col, ddl in [
                ("revenue_generated", "ALTER TABLE kpi_entries ADD COLUMN IF NOT EXISTS revenue_generated NUMERIC(12,2) DEFAULT 0"),
                ("training_hours", "ALTER TABLE kpi_entries ADD COLUMN IF NOT EXISTS training_hours INTEGER DEFAULT 0"),
                ("client_compliments", "ALTER TABLE kpi_entries ADD COLUMN IF NOT EXISTS client_compliments INTEGER DEFAULT 0"),
                ("client_complaints", "ALTER TABLE kpi_entries ADD COLUMN IF NOT EXISTS client_complaints INTEGER DEFAULT 0"),
                ("clients_pipeline", "ALTER TABLE kpi_entries ADD COLUMN IF NOT EXISTS clients_pipeline NUMERIC(5,2) DEFAULT 0"),
            ]:
                if not column_exists(conn, "kpi_entries", col):
                    cur.execute(ddl)

            # ═══ FINANCE — salaries + payroll ═══════════════════════════════
            cur.execute("""
                CREATE TABLE IF NOT EXISTS salary_config (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
                    base_salary NUMERIC(12,2) DEFAULT 0,
                    commission_rate NUMERIC(5,2) DEFAULT 0,
                    commission_type VARCHAR(20) DEFAULT 'flat',
                    tier_1_threshold NUMERIC(5,2) DEFAULT 55,
                    tier_1_rate NUMERIC(5,2) DEFAULT 1,
                    tier_2_threshold NUMERIC(5,2) DEFAULT 75,
                    tier_2_rate NUMERIC(5,2) DEFAULT 2,
                    tier_3_threshold NUMERIC(5,2) DEFAULT 90,
                    tier_3_rate NUMERIC(5,2) DEFAULT 3,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS payroll (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    month VARCHAR(7) NOT NULL,

                    base_salary NUMERIC(12,2) DEFAULT 0,
                    kpi_score NUMERIC(5,2) DEFAULT 0,
                    commission_amount NUMERIC(12,2) DEFAULT 0,
                    bonus NUMERIC(12,2) DEFAULT 0,
                    deductions NUMERIC(12,2) DEFAULT 0,
                    gross NUMERIC(12,2) DEFAULT 0,
                    net NUMERIC(12,2) DEFAULT 0,

                    bonus_note TEXT,
                    deduction_note TEXT,
                    status VARCHAR(20) DEFAULT 'pending',
                    payment_date DATE,
                    approved_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    paid_by INTEGER REFERENCES users(id) ON DELETE SET NULL,

                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),

                    UNIQUE(user_id, month)
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_payroll_month ON payroll(month);")

            # ═══ HR — attendance + leaves ════════════════════════════════════
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hr_records (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    month VARCHAR(7) NOT NULL,

                    working_days INTEGER DEFAULT 26,
                    days_attended INTEGER DEFAULT 0,
                    days_absent INTEGER DEFAULT 0,
                    leaves_taken INTEGER DEFAULT 0,
                    late_minutes INTEGER DEFAULT 0,
                    leave_balance INTEGER DEFAULT 21,

                    notes TEXT,
                    recorded_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),

                    UNIQUE(user_id, month)
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_hr_month ON hr_records(month);")

            # ═══ MARKETING CAMPAIGNS ════════════════════════════════════════
            cur.execute("""
                CREATE TABLE IF NOT EXISTS marketing_campaigns (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    campaign_name VARCHAR(200) NOT NULL,
                    avg_unit_price NUMERIC(15,2) NOT NULL,
                    commission_input NUMERIC(12,4) NOT NULL,
                    commission_type VARCHAR(20) NOT NULL DEFAULT 'percentage',
                    tax_rate NUMERIC(5,4) DEFAULT 0.19,
                    expected_close_rate NUMERIC(5,4) NOT NULL,
                    campaign_budget NUMERIC(15,2) NOT NULL,
                    recommended_scenario VARCHAR(20) DEFAULT 'balanced',
                    month VARCHAR(7),
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_mktg_user ON marketing_campaigns(user_id);")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS marketing_actuals (
                    id SERIAL PRIMARY KEY,
                    campaign_id INTEGER NOT NULL UNIQUE REFERENCES marketing_campaigns(id) ON DELETE CASCADE,
                    actual_spend NUMERIC(15,2) DEFAULT 0,
                    actual_leads INTEGER DEFAULT 0,
                    actual_qualified_leads INTEGER DEFAULT 0,
                    actual_meetings INTEGER DEFAULT 0,
                    actual_follow_ups INTEGER DEFAULT 0,
                    actual_deals INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)

            # ═══ UNITS (from PropFinder) — don't touch ══════════════════════

        conn.commit()

        # ═══ Create default admin if no users exist ═════════════════════════
        from app.auth import hash_password
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users")
            count = cur.fetchone()[0]
            if count == 0:
                cur.execute("""
                    INSERT INTO users (username, full_name, password_hash, role, email)
                    VALUES (%s, %s, %s, 'admin', %s)
                """, (
                    Config.DEFAULT_ADMIN_USER,
                    "System Administrator",
                    hash_password(Config.DEFAULT_ADMIN_PASSWORD),
                    Config.DEFAULT_ADMIN_EMAIL,
                ))
                conn.commit()
                log.info(f"✅ Default admin created: {Config.DEFAULT_ADMIN_USER} / {Config.DEFAULT_ADMIN_PASSWORD}")
            else:
                log.info(f"📋 Users table already has {count} user(s)")

        # Check units table
        if table_exists(conn, "units"):
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM units")
                units_count = cur.fetchone()[0]
                log.info(f"📦 `units` table: {units_count:,} rows (from PropFinder)")

            # Indexes for the PropFinder-owned table — only create if the
            # underlying column actually exists in this deployment.
            with conn.cursor() as cur:
                for col, idx in [
                    ("compound_id", "idx_units_compound_id"),
                    ("is_sold",     "idx_units_is_sold"),
                    ("detail_id",   "idx_units_detail_id"),
                ]:
                    if column_exists(conn, "units", col):
                        cur.execute(f"CREATE INDEX IF NOT EXISTS {idx} ON units({col});")
                conn.commit()

        log.info("✅ All tables ensured (users, kpi_entries, salary_config, payroll, hr_records, teams)")

    except Exception as e:
        log.error(f"❌ DB init error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
