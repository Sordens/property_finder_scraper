"""
Export properties from PostgreSQL to CSV / Excel
"""
import psycopg2
import csv
import json
from datetime import datetime

import os

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "propertyfinder"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASS", "your_password"),
}



def export_csv(filename: str = None):
    if not filename:
        filename = f"properties_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    external_id, title, price, currency, price_period,
                    property_type, bedrooms, bathrooms, area_sqft,
                    location, community, city, latitude, longitude,
                    agent_name, agency_name,
                    phone_primary, phone_secondary, whatsapp,
                    url, listed_at, scraped_at
                FROM properties
                ORDER BY id
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    
    print(f"✅ Exported {len(rows)} rows to {filename}")


def stats():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM properties")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM properties WHERE phone_primary IS NOT NULL")
            with_phone = cur.fetchone()[0]
            cur.execute("SELECT MIN(scraped_at), MAX(scraped_at) FROM properties")
            first, last = cur.fetchone()
    
    print(f"""
    ── PropertyFinder Stats ──────────────────
    Total properties : {total:,}
    With phone       : {with_phone:,} ({100*with_phone//max(total,1)}%)
    First scraped    : {first}
    Last scraped     : {last}
    ──────────────────────────────────────────
    """)


if __name__ == "__main__":
    stats()
    export_csv()
