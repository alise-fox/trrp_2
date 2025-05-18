import psycopg2

def insert_normalized(config, row):
    pg = config['postgres']
    conn = psycopg2.connect(
        dbname=pg['dbname'],
        user=pg['user'],
        password=pg['password'],
        host=pg['host']
    )
    cursor = conn.cursor()
    # Здесь примитивная вставка — надо дописать нормализацию под твою структуру
    cursor.execute(
        "INSERT INTO books_all (id, title, author, genre, publisher, year, borrower_name, borrower_address, borrow_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        row
    )
    conn.commit()
    conn.close()