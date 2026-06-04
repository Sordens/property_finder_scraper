import csv
import scraper

def sync():
    print("Инициализация базы данных...")
    scraper.init_db()

    print("Загрузка объявлений из properties.csv...")
    properties = []
    with open("properties.csv", mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("external_id"):
                properties.append(row)
    print(f"Загружено {len(properties)} объявлений.")

    print("Загрузка строящихся проектов из projects.csv...")
    projects = []
    with open("projects.csv", mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("external_id"):
                projects.append(row)
    print(f"Загружено {len(projects)} проектов.")

    # Загружаем пачками по 5000 строк для стабильности
    chunk_size = 5000
    
    print("Экспорт объявлений в PostgreSQL...")
    for i in range(0, len(properties), chunk_size):
        chunk = properties[i:i+chunk_size]
        scraper.save_to_db(chunk, [])
        print(f"Синхронизировано объявлений: {i + len(chunk)} из {len(properties)}")

    print("Экспорт строящихся проектов в PostgreSQL...")
    for i in range(0, len(projects), chunk_size):
        chunk = projects[i:i+chunk_size]
        scraper.save_to_db([], chunk)
        print(f"Синхронизировано проектов: {i + len(chunk)} из {len(projects)}")

    print("✓ Синхронизация успешно завершена!")

if __name__ == "__main__":
    sync()
