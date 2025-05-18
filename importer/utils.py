import yaml
import psycopg2

def create_table(config):
    pg = config['postgres']
    conn = psycopg2.connect(
        dbname=pg['dbname'],
        user=pg['user'],
        password=pg['password'],
        host=pg['host']
    )
    cursor = conn.cursor()

    cursor.execute("""
        DROP TABLE authors, genres, publishers, books, borrowers, borrows;        
        
        CREATE TABLE IF NOT EXISTS authors (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS genres (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS publishers (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            author_id INTEGER REFERENCES authors(id),
            genre_id INTEGER REFERENCES genres(id),
            publisher_id INTEGER REFERENCES publishers(id),
            year INTEGER,
                   
            CONSTRAINT books_unique UNIQUE (title, author_id, genre_id, publisher_id, year)
        );

        CREATE TABLE IF NOT EXISTS borrowers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT,
                   
            CONSTRAINT borrowers_unique UNIQUE (name, address)
        );

        CREATE TABLE IF NOT EXISTS borrows (
            id SERIAL PRIMARY KEY,
            book_id INTEGER REFERENCES books(id),
            borrower_id INTEGER REFERENCES borrowers(id),
            borrow_date DATE,
                   
            CONSTRAINT borrows_unique UNIQUE (book_id, borrower_id, borrow_date)
        );
    """)

    conn.commit()
    conn.close()

def read_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)
