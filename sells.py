import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect(
    host="caboose.proxy.rlwy.net",
    port=21778,
    database="railway",
    user="postgres",
    password="AdPVLYioZHOYsrpSswoILIvpkHwIReTz"
)

df = pd.read_csv(r"C:\Users\ae11w\OneDrive\Desktop\sells\all_units.csv")
df.columns = [c.lower() for c in df.columns]

cursor = conn.cursor()

execute_values(cursor, "INSERT INTO units VALUES %s", [tuple(row) for _, row in df.iterrows()])

conn.commit()
cursor.close()
conn.close()
print("Done! تم رفع الداتا ✅")