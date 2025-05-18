import yaml
import sqlite3

def create_table(config):
    conn = sqlite3.connect(config["sqlite_db"])  # путь до файла в config.yaml
    cursor = conn.cursor()
    cursor.execute('DROP TABLE books_all;')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS books_all (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        author TEXT,
        genre TEXT,
        publisher TEXT,
        year INTEGER,
        borrower_name TEXT,
        borrower_address TEXT,
        borrow_date TEXT
    )
    ''')
    conn.commit()
    conn.close()

def fill_example_data(config):
    books = [
        ("Война и мир", "Лев Толстой", "Роман", "Эксмо", 1869, "Иван Иванов", "ул. Ленина, 1", "2024-05-17"),
        ("Преступление и наказание", "Фёдор Достоевский", "Роман", "АСТ", 1866, "Пётр Петров", "ул. Гагарина, 10", "2024-05-01"),
        ("Мастер и Маргарита", "Михаил Булгаков", "Роман", "Азбука", 1967, "Мария Сидорова", "ул. Кирова, 5", "2024-04-20"),
    ]
    conn = sqlite3.connect(config["sqlite_db"])
    cursor = conn.cursor()
    cursor.executemany('''
    INSERT INTO books_all (title, author, genre, publisher, year, borrower_name, borrower_address, borrow_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', books)
    conn.commit()
    conn.close()

def read_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def read_books(sqlite_db):
    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM books_all")
    for row in cursor.fetchall():
        yield row
    conn.close()
