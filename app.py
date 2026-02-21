from flask import Flask, jsonify
import psycopg2
import psycopg2.extras
import os

app = Flask(__name__)

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     "postgres.railway.internal"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "database": os.environ.get("DB_NAME",     "railway"),
    "user":     os.environ.get("DB_USER",     "postgres"),
    "password": os.environ.get("DB_PASSWORD", "AdPVLYioZHOYsrpSswoILIvpkHwIReTz")
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

@app.route("/")
def index():
    with open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf-8") as f:
        return f.read()

@app.route("/api/units")
def get_units():
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    city_name, compound_name, compound_id,
                    developer_name, developer_id,
                    phase_name, phase_id, unit_type,
                    bedrooms, built_up_area_sqm,
                    total_price_egp, price_per_sqm_egp,
                    cash_price_from_egp, cash_price_to_egp,
                    delivery_from_months, delivery_to_months,
                    payment_plan, maintenance, club_fees,
                    parking_fees, finishing_type,
                    cash_discount_percent, city_id, detail_id,
                    outdoor_area, status, sub_type,
                    total_price_to_egp, type_id,
                    COALESCE(is_sold, false) as is_sold
                FROM units
                ORDER BY total_price_egp ASC
            """)
            rows = cur.fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/stats")
def get_stats():
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN is_sold = true OR status = 0 THEN 1 END) as sold,
                    AVG(total_price_egp) as avg_price,
                    MIN(total_price_egp) as min_price,
                    MAX(total_price_egp) as max_price,
                    COUNT(DISTINCT compound_name) as compounds
                FROM units
            """)
            stats = dict(cur.fetchone())
        conn.close()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
