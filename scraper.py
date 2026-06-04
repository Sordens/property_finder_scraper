"""
PropertyFinder.ae Scraper
Подход: перехват API запросов через Playwright (не DOM парсинг)
Выходной формат: Локальный CSV-файл (properties.csv) + PostgreSQL
"""

import asyncio
import json
import logging
import re
import time
import random
import os
import csv
from datetime import datetime
from typing import Optional
import psycopg2
from psycopg2.extras import execute_values
from playwright.async_api import async_playwright, Page, Route


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
CSV_FILENAME = "properties.csv"
PROJECTS_FILENAME = "projects.csv"
STATE_FILENAME = "scrape_state.json"

# Загрузка переменных окружения из локального .env файла (если он есть)
if os.path.exists(".env"):
    try:
        with open(".env", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key.strip()] = val.strip().strip('"').strip("'")
    except Exception as e:
        log.warning(f"Не удалось загрузить .env файл: {e}")

# Настройки PostgreSQL
DB_HOST = os.environ.get("DB_HOST", "YOUR_VPS_IP")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "dubai_realty_db")
DB_USER = os.environ.get("DB_USER", "realty_bot_user")
DB_PASS = os.environ.get("DB_PASS", "YOUR_PASSWORD")

CSV_FIELDS = [
    "external_id", "url", "title", "price", "currency", "price_period", "price_per_sqft",
    "property_type", "bedrooms", "bathrooms", "size_sqft", "location", "community",
    "subcommunity", "city", "latitude", "longitude", "agent_name", "agent_phone",
    "agent_whatsapp", "agent_email", "broker_name", "broker_phone", "building",
    "completion_status", "furnished", "offering_type", "reference", "rera_number",
    "share_url", "video_url", "description", "amenities", "images", "floor_plans", "listed_date", "scraped_at"
]


BASE_URL = "https://www.propertyfinder.ae"
SEARCH_URL = "https://www.propertyfinder.ae/en/search?l=1&c=2&fu=0&rp=y&ob=mr"

# Задержки между запросами (сек) — имитация человека
DELAY_MIN = 2.0
DELAY_MAX = 5.0
DELAY_PAGE = 8.0   # между страницами

MAX_RETRIES = 3
HEADLESS = True    # False для отладки


# ─────────────────────────────────────────
# LOCAL STORAGE (CSV, JSON & POSTGRESQL)
# ─────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=5
    )


def safe_int(val):
    if val is None or val == "":
        return None
    try:
        cleaned = str(val).replace(",", "").strip()
        return int(float(cleaned))
    except ValueError:
        return None


def safe_float(val):
    if val is None or val == "":
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
        if isinstance(val, str):
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        return val
    except ValueError:
        return None


def init_db():
    """Создаёт таблицы в БД, если их нет"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Таблица properties
            cur.execute("""
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
                floor_plans         JSONB,
                listed_date         TIMESTAMPTZ,
                scraped_at          TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_properties_external_id ON properties(external_id);
            """)
            # Добавим колонку floor_plans, если её там вдруг нет (для старых баз)
            cur.execute("""
            ALTER TABLE properties ADD COLUMN IF NOT EXISTS floor_plans JSONB;
            ALTER TABLE projects ADD COLUMN IF NOT EXISTS floor_plans JSONB;
            """)

            # Таблица projects
            cur.execute("""
            CREATE TABLE IF NOT EXISTS projects (
                id                  BIGSERIAL PRIMARY KEY,
                external_id         TEXT UNIQUE,
                url                 TEXT,
                title               TEXT,
                price               NUMERIC,
                currency            TEXT,
                price_period        TEXT,
                price_per_sqft      NUMERIC,
                property_type       TEXT,
                bedrooms            TEXT,
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
                floor_plans         JSONB,
                listed_date         TIMESTAMPTZ,
                scraped_at          TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_projects_external_id ON projects(external_id);
            """)
        conn.commit()
        conn.close()
        log.info("✓ База данных и таблицы успешно инициализированы.")
    except Exception as e:
        log.warning(f"Не удалось инициализировать БД (будем работать только с CSV): {e}")


def init_csv():
    """Создаём CSV-файлы с заголовками, если их нет"""
    if not os.path.exists(CSV_FILENAME):
        with open(CSV_FILENAME, mode="w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_FIELDS)
        log.info(f"Создан новый CSV-файл для объявлений: {CSV_FILENAME}")
    else:
        log.info(f"Файл {CSV_FILENAME} уже существует, будем дописывать новые записи.")
        
    if not os.path.exists(PROJECTS_FILENAME):
        with open(PROJECTS_FILENAME, mode="w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_FIELDS)
        log.info(f"Создан новый CSV-файл для строящихся проектов: {PROJECTS_FILENAME}")
    else:
        log.info(f"Файл {PROJECTS_FILENAME} уже существует, будем дописывать новые записи.")


def load_seen_ids(filename: str) -> set[str]:
    """Считываем все уже сохраненные ID из указанного CSV для дедупликации"""
    seen_ids = set()
    if os.path.exists(filename):
        try:
            with open(filename, mode="r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ext_id = row.get("external_id")
                    if ext_id:
                        seen_ids.add(ext_id)
            log.info(f"[{filename}] Загружено {len(seen_ids)} ID для дедупликации.")
        except Exception as e:
            log.warning(f"Не удалось прочитать ID из {filename}: {e}")
    return seen_ids


def save_to_db(properties: list[dict], projects: list[dict]):
    if not properties and not projects:
        return
        
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if properties:
                rows = []
                for p in properties:
                    rows.append((
                        p.get("external_id"),
                        p.get("url"),
                        p.get("title"),
                        safe_float(p.get("price")),
                        p.get("currency"),
                        p.get("price_period"),
                        safe_float(p.get("price_per_sqft")),
                        p.get("property_type"),
                        safe_int(p.get("bedrooms")),
                        safe_int(p.get("bathrooms")),
                        safe_float(p.get("size_sqft")),
                        p.get("location"),
                        p.get("community"),
                        p.get("subcommunity"),
                        p.get("city"),
                        safe_float(p.get("latitude")),
                        safe_float(p.get("longitude")),
                        p.get("agent_name"),
                        p.get("agent_phone"),
                        p.get("agent_whatsapp"),
                        p.get("agent_email"),
                        p.get("broker_name"),
                        p.get("broker_phone"),
                        p.get("building"),
                        p.get("completion_status"),
                        p.get("furnished"),
                        p.get("offering_type"),
                        p.get("reference"),
                        p.get("rera_number"),
                        p.get("share_url"),
                        p.get("video_url"),
                        p.get("description"),
                        p.get("amenities"),
                        p.get("images"),
                        p.get("floor_plans"),
                        safe_date(p.get("listed_date")),
                        safe_date(p.get("scraped_at"))
                    ))
                
                insert_query = """
                INSERT INTO properties (
                    external_id, url, title, price, currency, price_period, price_per_sqft,
                    property_type, bedrooms, bathrooms, size_sqft, location, community,
                    subcommunity, city, latitude, longitude, agent_name, agent_phone,
                    agent_whatsapp, agent_email, broker_name, broker_phone, building,
                    completion_status, furnished, offering_type, reference, rera_number,
                    share_url, video_url, description, amenities, images, floor_plans, listed_date, scraped_at
                ) VALUES %s
                ON CONFLICT (external_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    listed_date = EXCLUDED.listed_date,
                    description = EXCLUDED.description,
                    scraped_at = NOW();
                """
                execute_values(cur, insert_query, rows)
                
            if projects:
                rows = []
                for p in projects:
                    rows.append((
                        p.get("external_id"),
                        p.get("url"),
                        p.get("title"),
                        safe_float(p.get("price")),
                        p.get("currency"),
                        p.get("price_period"),
                        safe_float(p.get("price_per_sqft")),
                        p.get("property_type"),
                        p.get("bedrooms"),
                        safe_int(p.get("bathrooms")),
                        safe_float(p.get("size_sqft")),
                        p.get("location"),
                        p.get("community"),
                        p.get("subcommunity"),
                        p.get("city"),
                        safe_float(p.get("latitude")),
                        safe_float(p.get("longitude")),
                        p.get("agent_name"),
                        p.get("agent_phone"),
                        p.get("agent_whatsapp"),
                        p.get("agent_email"),
                        p.get("broker_name"),
                        p.get("broker_phone"),
                        p.get("building"),
                        p.get("completion_status"),
                        p.get("furnished"),
                        p.get("offering_type"),
                        p.get("reference"),
                        p.get("rera_number"),
                        p.get("share_url"),
                        p.get("video_url"),
                        p.get("description"),
                        p.get("amenities"),
                        p.get("images"),
                        p.get("floor_plans"),
                        safe_date(p.get("listed_date")),
                        safe_date(p.get("scraped_at"))
                    ))
                
                insert_query = """
                INSERT INTO projects (
                    external_id, url, title, price, currency, price_period, price_per_sqft,
                    property_type, bedrooms, bathrooms, size_sqft, location, community,
                    subcommunity, city, latitude, longitude, agent_name, agent_phone,
                    agent_whatsapp, agent_email, broker_name, broker_phone, building,
                    completion_status, furnished, offering_type, reference, rera_number,
                    share_url, video_url, description, amenities, images, floor_plans, listed_date, scraped_at
                ) VALUES %s
                ON CONFLICT (external_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    listed_date = EXCLUDED.listed_date,
                    description = EXCLUDED.description,
                    scraped_at = NOW();
                """
                execute_values(cur, insert_query, rows)
                
        conn.commit()
        conn.close()
        log.info(f"✓ Успешно экспортировано {len(properties)} объявлений и {len(projects)} проектов в PostgreSQL.")
    except Exception as e:
        log.warning(f"Ошибка экспорта в PostgreSQL (данные сохранены в CSV локально): {e}")


def save_properties(props: list[dict], seen_properties: set[str], seen_projects: set[str]):
    """Распределяет новые объявления и проекты по соответствующим CSV-файлам и БД"""
    if not props:
        return
    
    properties_to_write = []
    projects_to_write = []
    
    for p in props:
        ext_id = p.get("external_id")
        if not ext_id:
            continue
            
        url = p.get("url") or ""
        is_project = "/new-projects/" in url or "/new-project/" in url
        
        if is_project:
            if ext_id in seen_projects:
                continue
        else:
            if ext_id in seen_properties:
                continue
                
        # Превращаем списки в строки/JSON для сохранения в CSV
        amenities = p.get("amenities", [])
        images = p.get("images", [])
        floor_plans = p.get("floor_plans", [])
        
        row_dict = {
            "external_id": ext_id,
            "url": url,
            "title": p.get("title"),
            "price": p.get("price"),
            "currency": p.get("currency", "AED"),
            "price_period": p.get("price_period", "yearly"),
            "price_per_sqft": p.get("price_per_sqft"),
            "property_type": p.get("property_type"),
            "bedrooms": p.get("bedrooms"),
            "bathrooms": p.get("bathrooms"),
            "size_sqft": p.get("size_sqft"),
            "location": p.get("location"),
            "community": p.get("community"),
            "subcommunity": p.get("subcommunity"),
            "city": p.get("city", "Dubai"),
            "latitude": p.get("latitude"),
            "longitude": p.get("longitude"),
            "agent_name": p.get("agent_name"),
            "agent_phone": p.get("agent_phone"),
            "agent_whatsapp": p.get("agent_whatsapp"),
            "agent_email": p.get("agent_email"),
            "broker_name": p.get("broker_name"),
            "broker_phone": p.get("broker_phone"),
            "building": p.get("building"),
            "completion_status": p.get("completion_status"),
            "furnished": p.get("furnished"),
            "offering_type": p.get("offering_type"),
            "reference": p.get("reference"),
            "rera_number": p.get("rera_number"),
            "share_url": p.get("share_url"),
            "video_url": p.get("video_url"),
            "description": p.get("description"),
            "amenities": json.dumps(amenities) if isinstance(amenities, list) else amenities,
            "images": json.dumps(images) if isinstance(images, list) else images,
            "floor_plans": json.dumps(floor_plans) if isinstance(floor_plans, list) else floor_plans,
            "listed_date": p.get("listed_date"),
            "scraped_at": datetime.now().isoformat()
        }
        
        if is_project:
            projects_to_write.append(row_dict)
            seen_projects.add(ext_id)
        else:
            properties_to_write.append(row_dict)
            seen_properties.add(ext_id)
            
    # Записываем квартиры
    if properties_to_write:
        with open(CSV_FILENAME, mode="a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            for r in properties_to_write:
                writer.writerow(r)
        log.info(f"✓ Записано {len(properties_to_write)} новых объявлений в {CSV_FILENAME}.")
        
    # Записываем проекты
    if projects_to_write:
        with open(PROJECTS_FILENAME, mode="a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            for r in projects_to_write:
                writer.writerow(r)
        log.info(f"✓ Записано {len(projects_to_write)} новых строящихся проектов в {PROJECTS_FILENAME}.")
        
    # Экспортируем в БД
    if properties_to_write or projects_to_write:
        save_to_db(properties_to_write, projects_to_write)
        
    if not properties_to_write and not projects_to_write:
        log.info("Все объекты со страницы уже сохранены.")



def save_state(page_num: int, total_pages: int):
    """Сохраняет состояние пагинации в JSON-файл"""
    try:
        with open(STATE_FILENAME, mode="w", encoding="utf-8") as f:
            json.dump({
                "page_num": page_num,
                "total_pages": total_pages,
                "updated_at": datetime.now().isoformat()
            }, f, indent=4)
    except Exception as e:
        log.error(f"Не удалось сохранить состояние скрапинга: {e}")


def load_state() -> tuple[int, int]:
    """Загружает состояние пагинации из JSON-файла"""
    if os.path.exists(STATE_FILENAME):
        try:
            with open(STATE_FILENAME, mode="r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("page_num", 1), data.get("total_pages", 0)
        except Exception as e:
            log.warning(f"Не удалось загрузить состояние: {e}")
    return 1, 0



# ─────────────────────────────────────────
# PARSERS
# ─────────────────────────────────────────
def parse_listing(data: dict) -> dict:
    """Нормализует сырой JSON листинга в наш формат"""
    if not isinstance(data, dict):
        return {}
        
    is_project_listing = "project" in data and isinstance(data["project"], dict)
    
    # Распаковываем вложенный объект 'property' или 'project'
    if "property" in data and isinstance(data["property"], dict):
        data = data["property"]
    elif "project" in data and isinstance(data["project"], dict):
        data = data["project"]

    p = {}
    try:
        p["external_id"] = str(data.get("id", ""))
        
        if is_project_listing:
            p["url"] = f"{BASE_URL}/en/new-projects/{data.get('slug', '')}"
        else:
            p["url"] = data.get("share_url") or data.get("url") or (BASE_URL + "/en/property-for-rent/" + str(data.get("id", "")))
            
        p["title"] = data.get("title") or data.get("name") or ""
        
        # Цена
        price_data = data.get("price", {})
        if isinstance(price_data, dict):
            p["price"] = price_data.get("value") or price_data.get("amount") or price_data.get("from")
            p["currency"] = price_data.get("currency", "AED")
            p["price_period"] = price_data.get("period", "yearly")
            p["price_per_sqft"] = price_data.get("price_per_area")
        else:
            p["price"] = price_data
            p["currency"] = "AED"
            p["price_period"] = "yearly"
            p["price_per_sqft"] = data.get("price_per_area") or data.get("price_per_sqft")

        p["property_type"] = data.get("property_type", {}).get("name") if isinstance(data.get("property_type"), dict) else data.get("property_type")
        
        # Bedrooms
        beds = data.get("bedrooms")
        if isinstance(beds, list):
            p["bedrooms"] = ", ".join(map(str, beds))
        else:
            p["bedrooms"] = beds
            
        p["bathrooms"] = data.get("bathrooms")
        
        # Размер/Площадь
        size_data = data.get("size", {})
        if isinstance(size_data, dict):
            p["size_sqft"] = size_data.get("value")
        else:
            p["size_sqft"] = size_data or data.get("area") or data.get("size_sqft") or (data.get("min_size") or {}).get("value") or (data.get("max_size") or {}).get("value")

        # Локация
        loc = data.get("location", {})
        p["location"] = loc.get("full_name") if isinstance(loc, dict) else loc
        p["city"] = "Dubai"
        
        # Извлекаем community, subcommunity и building из дерева локаций
        location_tree = data.get("location_tree", []) or (loc.get("tree", []) if isinstance(loc, dict) else [])
        building_name = None
        if location_tree and isinstance(location_tree, list):
            for loc_item in location_tree:
                level = str(loc_item.get("level"))
                loc_type = str(loc_item.get("type", "")).upper()
                if level == "1":
                    p["community"] = loc_item.get("name")
                elif level == "2":
                    p["subcommunity"] = loc_item.get("name")
                
                if loc_type in ["TOWER", "BUILDING", "PROJECT", "COMPLEX"]:
                    building_name = loc_item.get("name")
                elif loc_type == "SUBCOMMUNITY" and not p.get("subcommunity"):
                    p["subcommunity"] = loc_item.get("name")
        else:
            if isinstance(loc, dict):
                p["community"] = loc.get("parent", {}).get("name") if isinstance(loc.get("parent"), dict) else None
                p["subcommunity"] = loc.get("subcommunity")

        # Координаты
        if isinstance(loc, dict):
            coords = loc.get("coordinates", {})
            if isinstance(coords, dict):
                p["latitude"] = coords.get("lat")
                p["longitude"] = coords.get("lon") or coords.get("lng")
        
        # Агент
        agent = data.get("agent") or data.get("contact") or {}
        p["agent_name"] = agent.get("name") or (data.get("developer") or {}).get("name")
        p["agent_email"] = agent.get("email")
        
        # Контактные данные
        agent_phone = None
        agent_whatsapp = None
        agent_email = None
        contact_options = data.get("contact_options", [])
        if contact_options and isinstance(contact_options, list):
            for opt in contact_options:
                opt_type = opt.get("type")
                if opt_type == "phone":
                    agent_phone = opt.get("value")
                elif opt_type == "whatsapp":
                    agent_whatsapp = opt.get("value")
                elif opt_type == "email":
                    agent_email = opt.get("value")
                    
        p["agent_phone"] = agent_phone or agent.get("phone") or agent.get("mobile")
        p["agent_whatsapp"] = agent_whatsapp or data.get("whatsapp") or agent.get("whatsapp")
        p["agent_email"] = agent_email or p["agent_email"]
        
        # Брокер/Агентство
        agency = data.get("broker") or data.get("agency") or data.get("client") or {}
        p["broker_name"] = agency.get("name") or (data.get("developer") or {}).get("name")
        p["broker_phone"] = agency.get("phone")
        
        p["building"] = building_name or data.get("building") or data.get("project_name") or (data.get("project") or {}).get("name") or data.get("name")
        p["completion_status"] = data.get("completion_status") or ("off_plan" if is_project_listing else None)
        p["furnished"] = data.get("furnished")
        p["offering_type"] = data.get("offering_type")
        p["reference"] = data.get("reference")
        p["rera_number"] = data.get("rera") or data.get("rera_number")
        p["share_url"] = p["url"]
        p["video_url"] = data.get("video_url")
        p["description"] = data.get("description", "")
        
        p["amenities"] = data.get("amenities", []) or data.get("features", [])
        
        # Фото
        photos = data.get("photos") or data.get("images") or []
        p["images"] = [img.get("url") or img.get("small") or img if isinstance(img, dict) else img for img in photos[:20]]
        
        # Планировки (Floor plans)
        floor_plans = data.get("floor_plans", [])
        fp_list = []
        if isinstance(floor_plans, list):
            for fp in floor_plans:
                if isinstance(fp, dict):
                    fp_url = fp.get("image_url") or fp.get("url")
                    if fp_url:
                        fp_list.append(fp_url)
        p["floor_plans"] = fp_list
        
        p["listed_date"] = data.get("listed_date") or data.get("listed_at") or data.get("created_at") or data.get("delivery_date")
        p["raw"] = data
        
    except Exception as e:
        log.error(f"Parse error for {data.get('id')}: {e}")
    
    return p



# ─────────────────────────────────────────
# SCRAPER
# ─────────────────────────────────────────
class PropertyFinderScraper:
    def __init__(self):
        self.intercepted_data = []
        self.api_base = None

    async def intercept_response(self, response):
        """Перехватываем API ответы с листингами"""
        url = response.url
        # PropertyFinder использует что-то вроде /api/listings или /search/api
        if any(x in url for x in ["/api/", "listings", "properties", "search"]):
            if response.status == 200:
                content_type = response.headers.get("content-type", "")
                if "json" in content_type:
                    try:
                        body = await response.json()
                        if self._looks_like_listings(body):
                            log.info(f"🎯 Intercepted API: {url}")
                            self.intercepted_data.append({
                                "url": url,
                                "data": body
                            })
                    except Exception:
                        pass

    def _looks_like_listings(self, data: dict) -> bool:
        """Проверяем что это данные листингов"""
        if isinstance(data, dict):
            keys = set(data.keys())
            listing_keys = {"listings", "properties", "results", "data", "items"}
            return bool(keys & listing_keys)
        return False

    async def get_page_listings(self, page: Page, url: str) -> list[dict]:
        """Загружаем страницу и собираем листинги"""
        self.intercepted_data = []
        
        response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        if response and response.status in [403, 429]:
            log.warning(f"Обнаружена блокировка (HTTP {response.status}) при загрузке страницы {url}. Ожидаем 5 минут для остывания...")
            await asyncio.sleep(300)
            raise Exception(f"Blocked by server (HTTP {response.status})")
            
        content = await page.content()
        if "cloudflare" in content.lower() or "verify you are human" in content.lower() or "attention required" in content.lower():
            log.warning(f"Обнаружена капча / Cloudflare на странице {url}. Ожидаем 5 минут для остывания...")
            await asyncio.sleep(300)
            raise Exception("Cloudflare / Captcha challenge detected")
            
        await asyncio.sleep(random.uniform(2, 4))
        
        # Ждём появления карточек
        try:
            await page.wait_for_selector("[data-testid='property-card'], .card-list__item, article", timeout=15000)
        except Exception:
            log.warning("Селектор карточек не найден, пробуем через API данные")

        # Пробуем вытащить данные из window.__NEXT_DATA__ (Next.js)
        listings = await self._extract_next_data(page)
        if listings:
            log.info(f"Extracted {len(listings)} listings from __NEXT_DATA__")
            return listings

        # Fallback: из перехваченных API запросов
        if self.intercepted_data:
            for item in self.intercepted_data:
                data = item["data"]
                for key in ["listings", "properties", "results", "data", "items"]:
                    if key in data and isinstance(data[key], list):
                        log.info(f"Got {len(data[key])} from API intercept")
                        return [parse_listing(l) for l in data[key]]

        # Fallback: DOM парсинг (менее надёжный)
        listings = await self._dom_parse(page)
        return listings

    async def _extract_next_data(self, page: Page) -> list[dict]:
        """Извлекаем из Next.js server data"""
        try:
            raw = await page.evaluate("""
                () => {
                    const el = document.getElementById('__NEXT_DATA__');
                    return el ? el.textContent : null;
                }
            """)
            if not raw:
                return []
            
            data = json.loads(raw)
            # Ищем листинги в дереве данных
            listings = self._find_listings_in_tree(data)
            return [parse_listing(l) for l in listings]
        except Exception as e:
            log.debug(f"__NEXT_DATA__ extraction failed: {e}")
            return []

    def _find_listings_in_tree(self, data, depth=0) -> list:
        """Рекурсивно ищем массив листингов в JSON дереве"""
        if depth > 6:
            return []
        if isinstance(data, list) and len(data) > 3:
            # Проверяем что это листинги
            if all(isinstance(i, dict) and ("id" in i or "external_id" in i) for i in data[:3]):
                return data
        if isinstance(data, dict):
            for key in ["listings", "properties", "results", "hits", "items", "data"]:
                if key in data and isinstance(data[key], list) and len(data[key]) > 0:
                    return data[key]
            for v in data.values():
                result = self._find_listings_in_tree(v, depth + 1)
                if result:
                    return result
        return []

    async def _dom_parse(self, page: Page) -> list[dict]:
        """Парсинг DOM как последний вариант"""
        log.warning("Falling back to DOM parsing")
        listings = []
        try:
            cards = await page.query_selector_all("[data-testid='property-card'], .card-list__item")
            for card in cards:
                try:
                    p = {}
                    # URL
                    link = await card.query_selector("a[href*='/property-']")
                    if link:
                        href = await link.get_attribute("href")
                        p["url"] = BASE_URL + href if href else None
                        p["external_id"] = re.search(r"-(\d+)\.html", href or "").group(1) if href else None

                    # Цена
                    price_el = await card.query_selector("[data-testid='property-card-price'], .price")
                    if price_el:
                        price_text = await price_el.inner_text()
                        nums = re.findall(r"[\d,]+", price_text)
                        p["price"] = int(nums[0].replace(",", "")) if nums else None
                        p["currency"] = "AED"

                    # Заголовок
                    title_el = await card.query_selector("h2, h3, [data-testid='property-card-title']")
                    if title_el:
                        p["title"] = await title_el.inner_text()

                    # Локация
                    loc_el = await card.query_selector("[data-testid='property-card-location'], .location")
                    if loc_el:
                        p["location"] = await loc_el.inner_text()

                    p["city"] = "Dubai"
                    listings.append(p)
                except Exception:
                    continue
        except Exception as e:
            log.error(f"DOM parse error: {e}")
        return listings

    async def get_listing_phone(self, page: Page, listing_url: str) -> dict:
        """Переходим на страницу листинга и нажимаем 'Reveal number'"""
        phones = {}
        try:
            await page.goto(listing_url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(random.uniform(1.5, 3))
            
            # Кнопка "Call" / "Reveal"
            reveal_btn = await page.query_selector(
                "button[data-testid='call-button'], "
                "button:has-text('Call'), "
                "button:has-text('Reveal'), "
                "a[href^='tel:']"
            )
            if reveal_btn:
                # Перехватываем запрос на reveal
                phone_data = []
                
                async def capture_phone(response):
                    if "phone" in response.url or "contact" in response.url or "reveal" in response.url:
                        try:
                            body = await response.json()
                            phone_data.append(body)
                        except Exception:
                            pass
                
                page.on("response", capture_phone)
                await reveal_btn.click()
                await asyncio.sleep(2)
                page.remove_listener("response", capture_phone)
                
                if phone_data:
                    for pd in phone_data:
                        phones["phone_primary"] = pd.get("phone") or pd.get("mobile") or pd.get("number")
                        phones["whatsapp"] = pd.get("whatsapp")
                
                # Fallback: читаем из DOM после клика
                tel_links = await page.query_selector_all("a[href^='tel:']")
                for i, link in enumerate(tel_links[:2]):
                    href = await link.get_attribute("href")
                    num = href.replace("tel:", "").strip() if href else None
                    if i == 0:
                        phones["phone_primary"] = phones.get("phone_primary") or num
                    else:
                        phones["phone_secondary"] = num

        except Exception as e:
            log.debug(f"Phone extraction failed for {listing_url}: {e}")
        
        return phones

    async def get_total_pages(self, page: Page) -> int:
        """Считаем общее число страниц"""
        try:
            # Из Next.js данных
            raw = await page.evaluate("() => document.getElementById('__NEXT_DATA__')?.textContent")
            if raw:
                data = json.loads(raw)
                total = self._find_in_tree(data, ["total_pages", "totalPages", "pages", "last_page"])
                if total:
                    return int(total)
            
            # Из пагинации в DOM
            last_page = await page.query_selector("a[aria-label='Last page'], [data-testid='pagination-last']")
            if last_page:
                href = await last_page.get_attribute("href")
                match = re.search(r"[?&]page=(\d+)", href or "")
                if match:
                    return int(match.group(1))
            
            # По количеству: 70k объявлений / 25 на странице
            count_el = await page.query_selector("[data-testid='search-results-count'], h1")
            if count_el:
                text = await count_el.inner_text()
                nums = re.findall(r"[\d,]+", text)
                if nums:
                    total_listings = int(nums[0].replace(",", ""))
                    return (total_listings // 25) + 1
        except Exception as e:
            log.error(f"get_total_pages error: {e}")
        
        return 2800  # ~70k / 25

    def _find_in_tree(self, data, keys: list, depth=0):
        if depth > 5:
            return None
        if isinstance(data, dict):
            for k in keys:
                if k in data:
                    return data[k]
            for v in data.values():
                result = self._find_in_tree(v, keys, depth + 1)
                if result is not None:
                    return result
        return None


# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────
async def run(args=None):
    init_csv()
    init_db()  # Инициализация PostgreSQL
    
    # Загружаем сохраненные ID один раз на старте
    seen_properties = load_seen_ids(CSV_FILENAME)
    seen_projects = load_seen_ids(PROJECTS_FILENAME)
    
    start_page, total_pages = load_state()
    
    if args and args.start is not None:
        start_page = args.start
        log.info(f"Начальная страница переопределена параметром --start: {start_page}")
        
    headless = HEADLESS
    if args and args.visible:
        headless = False
        log.info("Режим браузера: видимый (headless=False)")
    else:
        log.info("Режим браузера: скрытый (headless=True)")
        
    proxy_config = None
    if args and args.proxy:
        from urllib.parse import urlparse
        try:
            parsed = urlparse(args.proxy)
            proxy_config = {
                "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}" if parsed.port else f"{parsed.scheme}://{parsed.hostname}"
            }
            if parsed.username:
                proxy_config["username"] = parsed.username
            if parsed.password:
                proxy_config["password"] = parsed.password
            log.info(f"Используем прокси: {parsed.hostname}")
        except Exception as e:
            log.error(f"Ошибка парсинга прокси {args.proxy}: {e}")

    scraper = PropertyFinderScraper()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            proxy=proxy_config,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
        )
        
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            timezone_id="Asia/Dubai",
        )
        
        page = await context.new_page()
        
        # Скрываем признаки автоматизации
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
        """)
        
        # Перехватываем API
        page.on("response", scraper.intercept_response)
        
        # Первая страница
        log.info(f"Starting from page {start_page}")
        first_url = SEARCH_URL
        
        loaded = False
        for attempt in range(MAX_RETRIES):
            try:
                response = await page.goto(first_url, wait_until="domcontentloaded", timeout=60000)
                if response and response.status in [403, 429]:
                    log.warning(f"Блокировка на первой странице (HTTP {response.status}). Ожидаем 5 минут...")
                    await asyncio.sleep(300)
                    raise Exception(f"Blocked by server (HTTP {response.status})")
                
                content = await page.content()
                if "cloudflare" in content.lower() or "verify you are human" in content.lower() or "attention required" in content.lower():
                    log.warning(f"Капча на первой странице. Ожидаем 5 минут...")
                    await asyncio.sleep(300)
                    raise Exception("Cloudflare / Captcha detected")
                
                loaded = True
                break
            except Exception as e:
                log.error(f"First page load attempt {attempt+1} failed: {e}")
                await asyncio.sleep(15 * (attempt + 1))
                
        if not loaded:
            log.error("Не удалось загрузить первую страницу для определения количества страниц. Выходим.")
            await browser.close()
            return
        
        if total_pages == 0:
            total_pages = await scraper.get_total_pages(page)
            log.info(f"Total pages: {total_pages}")
        
        pages_scraped = 0
        for page_num in range(start_page, total_pages + 1):
            if args and args.pages is not None and pages_scraped >= args.pages:
                log.info(f"Достигнут лимит в {args.pages} страниц на этот запуск. Завершаем работу.")
                break
                
            log.info(f"━━━ Page {page_num}/{total_pages} ━━━")
            
            url = f"{SEARCH_URL}&page={page_num}" if page_num > 1 else SEARCH_URL
            
            for attempt in range(MAX_RETRIES):
                try:
                    listings = await scraper.get_page_listings(page, url)
                    
                    if not listings:
                        log.warning(f"No listings on page {page_num}, attempt {attempt+1}")
                        await asyncio.sleep(10)
                        continue
                    
                    log.info(f"Got {len(listings)} listings")
                    save_properties(listings, seen_properties, seen_projects)
                    save_state(page_num, total_pages)
                    
                    pages_scraped += 1
                    
                    # Пауза между страницами
                    await asyncio.sleep(random.uniform(DELAY_PAGE - 2, DELAY_PAGE + 3))
                    break
                    
                except Exception as e:
                    log.error(f"Page {page_num} attempt {attempt+1} failed: {e}")
                    await asyncio.sleep(15 * (attempt + 1))
            
            # Каждые 50 страниц — большая пауза (анти-бан)
            if page_num % 50 == 0:
                log.info("Taking a longer break (anti-ban)...")
                await asyncio.sleep(random.uniform(30, 60))
        
        await browser.close()
    
    log.info("✅ Scraping complete!")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="PropertyFinder Scraper")
    parser.add_argument("--pages", type=int, default=None, help="Limit number of pages to scrape in this run")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode (default is headless)")
    parser.add_argument("--start", type=int, default=None, help="Force start from a specific page number")
    parser.add_argument("--proxy", type=str, default=None, help="Proxy server URL (e.g., http://user:pass@ip:port)")
    args = parser.parse_args()
    
    asyncio.run(run(args))
