import random
import re
import time
from pathlib import Path

import psycopg
import requests
from psycopg import sql

import info
from helpers import *

# database relations list
# we want 4 tables for this time
relations = ["title", "author", "book", "text"]


def main(url):
    verbose = False
    # url = input("Enter the book url: ")
    # rand = random.randint(0, 9)
    # if url == "":
    #    url = f"http://www.gutenberg.org/cache/epub/1933{rand}/pg1933{rand}.txt"

    # check url
    if url_check(url):
        return 1

    # get the file name out of its url
    print(f"Downloading a file from {url}")
    file_name = get_file_name(url)

    # download book and save it to txt file
    get_txt(url, file_name, verbose)

    # try open the text
    try:
        file_handler = open(file_name, "r")
        if verbose:
            print(f"File {file_name} opened")
    except:
        Path.unlink(file_name)
        print("Could not open a file")
        return 2

    # get book's general info
    pattern = re.compile(r"Title: (.*)$")
    book_title = get_string_match(pattern, file_handler)

    pattern = re.compile(r"Author: (.*)$")
    book_author = get_string_match(pattern, file_handler)

    pattern = re.compile(r"Language: ([A-Za-z]+)")
    book_language = get_string_match(pattern, file_handler)

    book_year = get_book_year(file_handler)

    # print book's general info
    if verbose:
        print("***")
    print(f'  {book_author} "{book_title}" {book_language} {book_year}')
    if verbose:
        print("***")

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
                print("  Cursor created")

            # create a table for titles
            query = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                  id SERIAL PRIMARY KEY,
                  book_title VARCHAR(128),
                  year INTEGER,
                  UNIQUE(book_title, year)
                );
                 """
            ).format(sql.Identifier(relations[0]))
            cur.execute(query)

            # create a table for names of authors
            query = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                  id SERIAL PRIMARY KEY,
                  name VARCHAR(128) UNIQUE
                )
                """
            ).format(sql.Identifier(relations[1]))
            cur.execute(query)

            # create a table for names of authors
            query = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                  title_id INTEGER REFERENCES title(id) ON DELETE CASCADE,
                  author_id INTEGER REFERENCES author(id) ON DELETE CASCADE
                );
                """
            ).format(sql.Identifier(relations[2]))
            cur.execute(query)

            # print("\n  ", query.as_string(conn))

            query = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                  id SERIAL PRIMARY KEY,
                  body TEXT,
                  title_id INTEGER REFERENCES title(id) ON DELETE CASCADE
                );
                """
            ).format(sql.Identifier(relations[3]))

            # print("\n  ", query.as_string(conn))
            cur.execute(query)

            # populate title relation
            table = "title"
            columns = ["book_title", "year"]
            values = [book_title, book_year]
            insert_into_table(cur, table, columns, values)
            # get title_id for book table
            title_id = get_value(cur, table, "id", columns[0], book_title)

            # populate author relation
            table = "author"
            columns = ["name"]
            values = [book_author]
            insert_into_table(cur, table, columns, values)
            # get tauthor_id for book table
            author_id = get_value(cur, table, "id", columns[0], book_author)

            # populate book relation (many-to-many helper)
            table = "book"
            columns = ["title_id", "author_id"]
            values = [title_id, author_id]
            insert_into_table(cur, table, columns, values)

            # populate text relation
            # text table
            table = "text"
            columns = ["body", "title_id"]
            # value "" represents paragraph
            values = ["", title_id]
            chars, count, pcount = text_to_database(
                table, columns, values, file_handler, conn, cur, verbose
            )

            if verbose:
                print(f'  Tables "{relations}" have been populated')
            print(
                "  Loaded {} paragraphs, {} lines, {} characters".format(
                    pcount, count, chars
                )
            )

        if verbose:
            print("\n  Cursor terminated")

    if verbose:
        print("Connection closed")

    # close file
    file_handler.close()
    if verbose:
        print(f"File {file_name} closed")

    # remove the downloaded file
    Path.unlink(file_name)
    if verbose:
        print(f"File {file_name} removed")
        print("Служу Советскому Союзу!\n")

    time.sleep(5)

    return 0


def drop_tables(relations):
    with psycopg.connect(
        f"""
            host={info.host}
            port={info.port}
            dbname={info.dbname}
            user={info.user}
            password={info.pwd}
         """
    ) as conn:
        # create a cursor
        with conn.cursor() as cur:
            # drop all relations from the list
            for relation in relations:
                query = sql.SQL(
                    """
                    DROP TABLE IF EXISTS {} CASCADE;
                    """
                ).format(sql.Identifier(relation))
                cur.execute(query)


drop_tables(relations)
# main()

for i in range(10):
    url = f"http://www.gutenberg.org/cache/epub/1933{i}/pg1933{i}.txt"
    main(url)
