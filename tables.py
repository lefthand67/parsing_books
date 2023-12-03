from psycopg import connection, cursor, sql

relations = [
    # 0
    ["language", ["id", "name"]],
    # 1
    ["author", ["id", "name"]],
    # 2
    ["book", ["id", "title", "year", "author_id", "language_id"]],
    # 3
    ["text", ["id", "body", "book_id"]],
]


def create_language_table(relations, connection, cursor, verbose=False):
    # create a Language  table
    table = relations[0][0]
    columns = relations[0][1]
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {} (
          id SERIAL,
          {} VARCHAR(64),
          PRIMARY KEY(id)
        );
        """
    ).format(sql.Identifier(table), sql.Identifier(columns[1]))
    if verbose:
        print(query.as_string(connection))
    cursor.execute(query)

    return table, columns


def create_author_table(relations, connection, cursor, verbose=False):
    # create an author table
    table = relations[1][0]
    columns = relations[1][1]
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {} (
          id SERIAL,
          {} VARCHAR(128) UNIQUE,
          PRIMARY KEY(id)
        )
        """
    ).format(sql.Identifier(table), sql.Identifier(columns[1]))
    if verbose:
        print(query.as_string(connection))
    cursor.execute(query)

    return table, columns


def create_book_table(relations, connection, cursor, verbose=False):
    # create a book table
    table = relations[2][0]
    columns = relations[2][1]
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {} (
          id SERIAL,
          {title} VARCHAR(128),
          {year} INTEGER,
          {author_id} INTEGER REFERENCES author(id) ON DELETE CASCADE,
          {language_id} INTEGER REFERENCES language(id) ON DELETE CASCADE,
          UNIQUE({title}, {year}, {language_id}),
          PRIMARY KEY(id)
        );
        """
    ).format(
        sql.Identifier(table),
        title=sql.Identifier(columns[1]),
        year=sql.Identifier(columns[2]),
        author_id=sql.Identifier(columns[3]),
        language_id=sql.Identifier(columns[4]),
    )

    if verbose:
        print(query.as_string(connection))
    cursor.execute(query)

    return table, columns


def create_text_table(relations, connection, cursor, verbose=False):
    # create a text table
    table = relations[3][0]
    columns = relations[3][1]
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {} (
          id SERIAL,
          {body} TEXT,
          {book_id} INTEGER REFERENCES book(id) ON DELETE CASCADE,
          PRIMARY KEY(id)
        );
        """
    ).format(
        sql.Identifier(table),
        body=sql.Identifier(columns[1]),
        book_id=sql.Identifier(columns[2]),
    )
    if verbose:
        print(query.as_string(connection))
    cursor.execute(query)

    return table, columns
