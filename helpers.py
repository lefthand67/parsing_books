import re
import time

import requests
from psycopg import sql


def url_check(url):
    """
    Checks whether url is responding
    Returns: bool: False if url is okay
    """

    response = requests.get(url, allow_redirects=True)

    result = response.status_code

    if result == 200:
        return False

    print(f"Error {result}")
    return True


def get_file_name(url):
    """
    Gets a name of the file out of the url address
    Returns: str: file name
    """

    pattern = r"\/([A-Za-z0-9]+\.txt)$"

    match = re.search(pattern, url)

    return match.group(1)


def get_txt(url, file_name, verbose=False):
    """
    Creates a txt file out of a url link
    Returns: str: a name of the created file
    """

    response = requests.get(url, allow_redirects=True)
    content = response.text

    with open(file_name, "w") as file:
        file.write(content)
        if verbose:
            print(f"Text saved as {file_name}")

    return file_name


def get_string_match(pattern, file_handler):
    """
    Gets a string match like author's name, book title, etc.
      from the opened file
    pattern: a compiled regex obj: use re.compile() method
    file_handler: a file object for reading: use
      open(file_name, "r") method
    Returns: str
    """

    for line in file_handler:
        match = pattern.search(line)
        if match:
            file_handler.seek(0)
            return match.group(1)

    file_handler.seek(0)
    return None


def get_book_year(file_handler):
    """
    Returns: str: a year the book was first published.
    file_handler: a file object for reading: use
      open(file_name, "r") method
    """

    # get the caret below the technical info
    for line in file_handler:
        if not line.startswith("***"):
            continue
        break

    pattern = re.compile(r"\b(\d{4})\b")
    for line in file_handler:
        match = pattern.search(line)
        if match:
            file_handler.seek(0)
            return match.group(1)

    file_handler.seek(0)
    return None


def insert_into_table(cursor, table, columns, values):
    """
    Inserts data into attributes of the relation.
    cursor: PostgeSQL cursor object
    table: str: name of the relation;
    columns: list or tuple of strings: list of the attributes' names
    values: list or tuple of strings: list of the corresponding
      to attributes values
    """
    # if the column is one
    if len(columns) == 1 and len(values) == 1:
        query = sql.SQL(
            """
            INSERT INTO {} ({}) VALUES ({});
            """
        ).format(
            sql.Identifier(table), sql.Identifier(columns[0]), sql.Literal(values[0])
        )

    elif len(columns) == len(values):
        query = sql.SQL(
            """
            INSERT INTO {} ({}) VALUES ({});
            """
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Literal, values)),
        )

    else:
        print("Error: Number of columns and values is different")
        return 1

    cursor.execute(query)

    return 0


def get_value(cursor, table, column1, column2, match):
    """
    Returns one value from the select query to database
    cursor: PostgeSQL cursor object
    table: str: name of the relation
    column1: str: name of the select attribute
    column2: str: name of the condition attribute
    match: str: condition value
    """
    query = sql.SQL(
        """
        SELECT {} from {} WHERE {} = {} LIMIT 1;
        """
    ).format(
        sql.Identifier(column1),
        sql.Identifier(table),
        sql.Identifier(column2),
        sql.Literal(match),
    )
    cursor.execute(query)

    return cursor.fetchone()[0]


def text_to_database(
    table,
    columns,
    values,
    file_handler,
    connection,
    cursor,
    verbose=False,
):
    """
    Parses the paragraphs from the txt file,
      creates the table with the name of the book,
      sends the paragraphs to the database into the table
    Returns: tuple: number of chars, of lines, of paragraphs
    table: str: table name
    columns: str: list of columns' names
    values: str: list of values to insert names
    file_handler: object
    connection: of of psycopg
    cursor: object of psycopg
    verbose: bool: print progress statements, default False
    """

    if verbose:
        print(f'  Started table "{table}" populating...')

    paragraph = ""
    chars, count, pcount = 0, 0, 0

    # form a query template
    # if the column is one
    if len(columns) == 1 and len(values) == 1:
        query = sql.SQL(
            """
            INSERT INTO {} ({}) VALUES ({});
            """
        ).format(
            sql.Identifier(table), sql.Identifier(columns[0]), sql.Literal(values[0])
        )
    # if more than one column
    elif len(columns) == len(values):
        query = sql.SQL(
            """
            INSERT INTO {} ({}) VALUES ({});
            """
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Literal, values)),
        )
    # if columns and values differ in number
    else:
        print("Error: Number of columns and values is different")
        return 1

    for line in file_handler:
        count += 1
        line = line.strip()
        chars += len(line)

        # insert paragraphs
        # skip empty lines
        if line == "" and paragraph == "":
            continue

        # when paragraph done
        elif line == "":
            values[0] = paragraph
            cursor.execute(query)
            pcount += 1

            if pcount % 50 == 0:
                connection.commit()
            if pcount % 100 == 0:
                if verbose:
                    print(f"    {pcount} loaded...")
                time.sleep(1)

            paragraph = ""
            continue

        # populating paragraph
        paragraph += " " + line

    connection.commit()

    return chars, count, pcount
