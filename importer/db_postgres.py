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

    _, title, author, genre, publisher, year, borrower_name, borrower_address, borrow_date = row
    
    cursor.execute("INSERT INTO authors (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id;", (author,))
    res = cursor.fetchone()
    if res:
        author_id = res[0]
    else:
        cursor.execute("SELECT id FROM authors WHERE name = %s;", (author,))
        author_id = cursor.fetchone()[0]

    # 2. Жанр
    cursor.execute("INSERT INTO genres (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id;", (genre,))
    res = cursor.fetchone()
    if res:
        genre_id = res[0]
    else:
        cursor.execute("SELECT id FROM genres WHERE name = %s;", (genre,))
        genre_id = cursor.fetchone()[0]

    # 3. Издательство
    cursor.execute("INSERT INTO publishers (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id;", (publisher,))
    res = cursor.fetchone()
    if res:
        publisher_id = res[0]
    else:
        cursor.execute("SELECT id FROM publishers WHERE name = %s;", (publisher,))
        publisher_id = cursor.fetchone()[0]

    # 4. Книга
    # ON CONFLICT (title, author_id, genre_id, publisher_id, year) DO NOTHING
    cursor.execute("""
        INSERT INTO books (title, author_id, genre_id, publisher_id, year)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT (books_unique) DO NOTHING
        RETURNING id;
    """, (title, author_id, genre_id, publisher_id, year))
    res = cursor.fetchone()
    if res:
        book_id = res[0]
    else:
        cursor.execute("""
            SELECT id FROM books
            WHERE title=%s AND author_id=%s AND genre_id=%s AND publisher_id=%s AND year=%s
        """, (title, author_id, genre_id, publisher_id, year))
        book_id = cursor.fetchone()[0]

    # 5. Читатель
    # ON CONFLICT (name, address) DO NOTHING RETURNING id;
    cursor.execute("""
        INSERT INTO borrowers (name, address) VALUES (%s, %s)
        ON CONFLICT ON CONSTRAINT (borrowers_unique) DO NOTHING RETURNING id;
    """, (borrower_name, borrower_address))
    res = cursor.fetchone()
    if res:
        borrower_id = res[0]
    else:
        cursor.execute("""
            SELECT id FROM borrowers WHERE name=%s AND address=%s
        """, (borrower_name, borrower_address))
        borrower_id = cursor.fetchone()[0]

    # 6. Факт выдачи
    cursor.execute("""
        INSERT INTO borrows (book_id, borrower_id, borrow_date)
        VALUES (%s, %s, %s)
        ON CONFLICT ON CONSTRAINT (borrows_unique) DO NOTHING;
    """, (book_id, borrower_id, borrow_date))

    conn.commit()
    conn.close()