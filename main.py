import random
import re
import time
from pathlib import Path

import psycopg
import requests
from psycopg import sql

import helpers
import info
import tables


def main():
    verbose = True
    clear_database = True

    # how many books we want to parse
    n = 1

    # list of relations' schemata
    relations = tables.relations

    # connect to database
    with psycopg.connect(
        f"""
            host={info.host}
            port={info.port}
            dbname={info.dbname}
            user={info.user}
            password={info.pwd}
         """
    ) as conn:
        if verbose:
            print("Connection with database established")

        # create a cursor
        with conn.cursor() as cur:
            if verbose:
                print("Cursor created")

            # drop tables if needed
            if clear_database:
                for relation in relations:
                    drop_table(relation[0], cur, verbose)

            tables.create_language_table(relations, conn, cur, verbose)

            tables.create_author_table(relations, conn, cur, verbose)
            tables.create_book_table(relations, conn, cur, verbose)
            tables.create_text_table(relations, conn, cur, verbose)
            # and finally let's start parsing
            for i in range(n):
                rand = random.randint(1, 100_000)
                url = f"http://www.gutenberg.org/cache/epub/{rand}/pg{rand}.txt"
                if verbose:
                    print("    Trying to download", url)
                # check url
                if helpers.url_check(url):
                    continue

                parse_book(url, relations, conn, cur, verbose)

        if verbose:
            print("Cursor terminated")

    if verbose:
        print("Connection closed")


def drop_table(relation, cursor, verbose=False):
    query = sql.SQL(
        """
        DROP TABLE IF EXISTS {} CASCADE;
        """
    ).format(sql.Identifier(relation))
    if verbose:
        print(f"Relation {relation} dropped")

    cursor.execute(query)


def parse_book(url, relations, connection, cursor, verbose=False):
    # download book and save it to txt file
    file_name = helpers.get_file_name(url)
    print(f"Downloading a file from {url}")
    helpers.get_txt(url, file_name, verbose)

    # try open the text
    try:
        file_handler = open(file_name, "r")
        if verbose:
            print(f"File {file_name} opened")
    except:
        Path.unlink(file_name)
        print("Could not open a file")
        return 1

    # check if the book has already been parsed
    # get the book's title
    pattern = re.compile(r"Title: (.*)$")
    book_title = helpers.get_string_match(pattern, file_handler)

    query = sql.SQL(
        """
        SELECT EXISTS (SELECT 1 FROM {book} WHERE {title} = %s);
        """
    ).format(
        book=sql.Identifier(relations[2][0]), title=sql.Identifier(relations[2][1][1])
    )
    cursor.execute(query, (book_title,))

    if cursor.fetchone()[0]:
        if verbose:
            print("The book is already in the database")
        Path.unlink(file_name)
        return 2

    # get book's general info
    pattern = re.compile(r"Author: (.*)$")
    book_author = helpers.get_string_match(pattern, file_handler)

    pattern = re.compile(r"Language: ([A-Za-z]+)")
    book_language = helpers.get_string_match(pattern, file_handler)

    book_year = helpers.get_book_year(file_handler)

    # print book's general info
    if verbose:
        print("***")
    print(f'  {book_author} "{book_title}" in {book_language}, {book_year}')
    if verbose:
        print("***")

    # populate language table
    table, columns = tables.create_language_table(relations, connection, cursor)
    values = [book_language]
    helpers.insert_into_table(cursor, table, columns[1:], values)
    # get language_id for book table
    language_id = helpers.get_value(cursor, table, "id", columns[1], book_language)

    # populate author table
    table, columns = tables.create_author_table(relations, connection, cursor)
    values = [book_author]
    helpers.insert_into_table(cursor, table, columns[1:], values)
    # get tauthor_id for book table
    author_id = helpers.get_value(cursor, table, "id", columns[1], book_author)

    # populate book table
    table, columns = tables.create_book_table(relations, connection, cursor)
    values = [book_title, book_year, author_id, language_id]
    helpers.insert_into_table(cursor, table, columns[1:], values)
    book_id = helpers.get_value(cursor, table, "id", columns[1], book_title)

    # populate text table
    table, columns = tables.create_text_table(relations, connection, cursor)
    # value "" represents paragraph
    values = ["", book_id]
    chars, count, pcount = text_to_database(
        table, columns[1:], values, file_handler, connection, cursor, verbose
    )

    if verbose:
        print(f'  Tables "{relations}" have been populated')
    print(
        "  Loaded {} paragraphs, {} lines, {} characters".format(pcount, count, chars)
    )

    # close file
    file_handler.close()
    if verbose:
        print(f"File {file_name} closed")

    # remove the downloaded file
    Path.unlink(file_name)
    if verbose:
        print(f"File {file_name} removed")
        print("Служу Советскому Союзу!\n")

    time.sleep(random.randint(1, 7))

    return 0


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
    if len(columns) == len(values):
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


main()
