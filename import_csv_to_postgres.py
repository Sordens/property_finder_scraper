import csv
import json
import os
import sys
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

# Настройки по умолчанию (пользователь может переопределить через переменные окружения или ввод)
DB_HOST = os.environ.get("DB_HOST", "89.167.81.157")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "dubai_realty_db")
DB_USER = os.environ.get("DB_USER", "realty_bot_user")
DB_PASS = os.environ.get("DB_PASS", "dubai_realty_2026_pass")

CSV_FILE_PATH = "dataset_propertyfinder-ae-scraper_2026-06-02_10-57-28-222.csv"

def get_connection(host, port, dbname, user, password):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

def create_table_if_not_exists(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS properties (
        id                  BIGSERIAL PRIMARY KEY,
        external_id         TEXT UNIQUE,
        url                 TEXT,
        title               TEXT,
        price               NUMERIC,
        currency            TEXT,
        price_period        TEXT,
        price_per_sqft      NUMERIC,
        property_type       TEXT,
        bedrooms            INTEGER,
        bathrooms           INTEGER,
        size_sqft           NUMERIC,
        location            TEXT,
        community           TEXT,
        subcommunity        TEXT,
        city                TEXT,
        latitude            DOUBLE PRECISION,
        longitude           DOUBLE PRECISION,
        agent_name          TEXT,
        agent_phone         TEXT,
        agent_whatsapp      TEXT,
        agent_email         TEXT,
        broker_name         TEXT,
        broker_phone        TEXT,
        building            TEXT,
        completion_status   TEXT,
        furnished           TEXT,
        offering_type       TEXT,
        reference           TEXT,
        rera_number         TEXT,
        share_url           TEXT,
        video_url           TEXT,
        description         TEXT,
        amenities           JSONB,
        images              JSONB,
        listed_date         TIMESTAMPTZ,
        scraped_at          TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_properties_external_id ON properties(external_id);
    CREATE INDEX IF NOT EXISTS idx_properties_price ON properties(price);
    CREATE INDEX IF NOT EXISTS idx_properties_location ON properties(location);
    CREATE INDEX IF NOT EXISTS idx_properties_bedrooms ON properties(bedrooms);
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("✓ Таблица properties успешно подготовлена.")

def safe_int(val):
    if not val:
        return None
    try:
        cleaned = str(val).replace(",", "").strip()
        return int(float(cleaned))
    except ValueError:
        return None

def safe_float(val):
    if not val:
        return None
    try:
        cleaned = str(val).replace(",", "").strip()
        return float(cleaned)
    except ValueError:
        return None

def safe_date(val):
    if not val:
        return None
    try:
        # ISO формат даты: 2026-06-02T10:29:26Z
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except ValueError:
        return None

def import_csv(host, port, dbname, user, password):
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Ошибка: Файл {CSV_FILE_PATH} не найден в текущей директории!")
        sys.exit(1)

    print(f"Подключение к БД {dbname} на {host}:{port}...")
    try:
        conn = get_connection(host, port, dbname, user, password)
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        print("\nПожалуйста, убедитесь, что:")
        print("1. Вы правильно ввели внешний IP вашего VPS.")
        print("2. Порт 5432 открыт в файрволе вашего VPS.")
        print("3. В pg_hba.conf разрешен доступ с вашего текущего IP.")
        sys.exit(1)

    create_table_if_not_exists(conn)

    print(f"Чтение файла {CSV_FILE_PATH}...")
    rows_to_insert = []
    
    with open(CSV_FILE_PATH, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        
        # Получаем список колонок для группировки
        headers = reader.fieldnames
        amenities_headers = [h for h in headers if h.startswith("amenities/")]
        images_headers = [h for h in headers if h.startswith("images/")]

        for row_num, row in enumerate(reader, start=1):
            ext_id = row.get("id", "").strip()
            if not ext_id:
                # Пропускаем строки без ID
                continue
            # Собираем удобства в массив
            amenities = []
            for h in amenities_headers:
                val = row.get(h, "").strip()
                if val:
                    amenities.append(val)
            
            # Собираем фото в массив
            images = []
            for h in images_headers:
                val = row.get(h, "").strip()
                if val:
                    images.append(val)

            # Формируем кортеж для БД
            rows_to_insert.append((
                ext_id,
                row.get("url"),
                row.get("title"),

                safe_float(row.get("price")),
                row.get("currency"),
                row.get("price_period"),
                safe_float(row.get("price_per_sqft")),
                row.get("property_type"),
                safe_int(row.get("bedrooms")),
                safe_int(row.get("bathrooms")),
                safe_float(row.get("size_sqft")),
                row.get("location"),
                row.get("community"),
                row.get("subcommunity"),
                row.get("city"),
                safe_float(row.get("latitude")),
                safe_float(row.get("longitude")),
                row.get("agent_name"),
                row.get("agent_phone"),
                row.get("agent_whatsapp"),
                row.get("agent_email"),
                row.get("broker_name"),
                row.get("broker_phone"),
                row.get("building"),
                row.get("completion_status"),
                row.get("furnished"),
                row.get("offering_type"),
                row.get("reference"),
                row.get("rera_number"),
                row.get("share_url"),
                row.get("video_url"),
                row.get("description"),
                json.dumps(amenities),
                json.dumps(images),
                safe_date(row.get("listed_date"))
            ))

    print(f"Считано {len(rows_to_insert)} строк. Выполняется загрузка в базу данных...")
    
    insert_query = """
    INSERT INTO properties (
        external_id, url, title, price, currency, price_period, price_per_sqft,
        property_type, bedrooms, bathrooms, size_sqft, location, community,
        subcommunity, city, latitude, longitude, agent_name, agent_phone,
        agent_whatsapp, agent_email, broker_name, broker_phone, building,
        completion_status, furnished, offering_type, reference, rera_number,
        share_url, video_url, description, amenities, images, listed_date
    ) VALUES %s
    ON CONFLICT (external_id) DO UPDATE SET
        price = EXCLUDED.price,
        listed_date = EXCLUDED.listed_date,
        description = EXCLUDED.description,
        scraped_at = NOW();
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, rows_to_insert)
        conn.commit()
        print(f"✓ Успешно импортировано {len(rows_to_insert)} объявлений в базу данных!")
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при сохранении в базу данных: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    host = DB_HOST
    if not host:
        print("Переменная окружения DB_HOST не задана.")
        host = input("Введите IP-адрес (Host) вашего VPS: ").strip()
    
    import_csv(host, DB_PORT, DB_NAME, DB_USER, DB_PASS)
