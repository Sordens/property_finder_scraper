"""
Phone Enrichment Worker
Берёт объявления без телефонов из БД, заходит на каждое и нажимает Reveal
Запускать параллельно со скрапером или после
"""

import asyncio
import logging
import random
import re
import psycopg2
from playwright.async_api import async_playwright

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

import os

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "propertyfinder"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASS", "your_password"),
}

BASE_URL = "https://www.propertyfinder.ae"
WORKERS = 2  # Параллельных воркеров (не более 3 чтоб не забанили)


def get_pending_phones(limit=100):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, url, external_id FROM properties
                WHERE phone_primary IS NULL AND url IS NOT NULL
                ORDER BY id
                LIMIT %s
            """, (limit,))
            return cur.fetchall()


def update_phone(prop_id: int, phones: dict):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE properties SET
                    phone_primary   = %s,
                    phone_secondary = %s,
                    whatsapp        = %s
                WHERE id = %s
            """, (
                phones.get("phone_primary"),
                phones.get("phone_secondary"),
                phones.get("whatsapp"),
                prop_id
            ))
        conn.commit()


async def enrich_worker(worker_id: int, queue: asyncio.Queue):
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 800}
        )
        page = await context.new_page()
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        while True:
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            prop_id, url, ext_id = item
            log.info(f"[W{worker_id}] Enriching #{ext_id} - {url}")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(1.5, 3))

                phones = {}
                phone_data_captured = []

                async def capture(response):
                    u = response.url
                    if any(x in u for x in ["phone", "contact", "reveal", "call"]):
                        try:
                            body = await response.json()
                            phone_data_captured.append(body)
                        except Exception:
                            pass

                page.on("response", capture)

                # Клик по кнопке телефона
                btn = await page.query_selector(
                    "button[data-testid='call-button'], "
                    "[data-testid='reveal-number-button'], "
                    "button:has-text('Call'), "
                    "button:has-text('Reveal'), "
                    "button:has-text('Show')"
                )
                if btn:
                    await btn.click()
                    await asyncio.sleep(2.5)

                page.remove_listener("response", capture)

                # Из API ответа
                for pd in phone_data_captured:
                    phones["phone_primary"] = (
                        pd.get("phone") or pd.get("mobile") or
                        pd.get("number") or pd.get("primary_phone")
                    )
                    phones["whatsapp"] = pd.get("whatsapp")

                # Из tel: ссылок в DOM
                tel_links = await page.query_selector_all("a[href^='tel:']")
                for i, link in enumerate(tel_links[:2]):
                    href = await link.get_attribute("href")
                    num = href.replace("tel:", "").strip() if href else None
                    if i == 0 and not phones.get("phone_primary"):
                        phones["phone_primary"] = num
                    elif i == 1:
                        phones["phone_secondary"] = num

                # WhatsApp ссылки
                if not phones.get("whatsapp"):
                    wa_link = await page.query_selector("a[href*='whatsapp'], a[href*='wa.me']")
                    if wa_link:
                        href = await wa_link.get_attribute("href")
                        match = re.search(r"(?:wa\.me/|phone=)(\+?\d+)", href or "")
                        if match:
                            phones["whatsapp"] = match.group(1)

                update_phone(prop_id, phones)
                log.info(f"[W{worker_id}] ✓ Phone: {phones.get('phone_primary', 'not found')}")

            except Exception as e:
                log.error(f"[W{worker_id}] Error on {url}: {e}")

            queue.task_done()
            await asyncio.sleep(random.uniform(2, 4))

        await browser.close()


async def run_enrichment():
    pending = get_pending_phones(limit=1000)
    log.info(f"Pending enrichment: {len(pending)} properties")

    if not pending:
        log.info("All phones already collected!")
        return

    queue = asyncio.Queue()
    for item in pending:
        await queue.put(item)

    workers = [enrich_worker(i, queue) for i in range(WORKERS)]
    await asyncio.gather(*workers)
    log.info("✅ Enrichment done!")


if __name__ == "__main__":
    asyncio.run(run_enrichment())
