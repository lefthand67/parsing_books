import random
import re
import time
from collections import defaultdict
from pathlib import Path

import psycopg
import requests
from psycopg import sql
from tqdm import tqdm

import helpers
import info
import schemata

# complete dictionary of relations' schemata
relations = schemata.relations


def main():
    verbose = True
    clear_database = True

    # how many books we want to parse
    n = 1

    warning_message()

    # connect to database
    with psycopg.connect(
        f"""
            host={info.host}
            port={info.port}
            dbname={info.dbname}
            user={info.user}
            password={info.pwd}
         """,
        # autocommit=True,
    ) as conn:
        if verbose:
            print(f"Connection with database {info.dbname} established")

        # create a cursor
        with conn.cursor() as cur:
            if verbose:
                print("Cursor created")

            # drop tables if True
            if clear_database:
                helpers.drop_tables(conn, cur, verbose)
                # and create tables
                helpers.create_tables(relations, conn, cur, verbose)

            # parse data into tables
            for i in range(n):
                rand = random.randint(1, 100_000)
                url = f"http://www.gutenberg.org/cache/epub/{rand}/pg{rand}.txt"
                # url = 'http://www.gutenberg.org/cache/epub/7623/pg7623.txt'
                #url = 'http://www.gutenberg.org/cache/epub/70056/pg70056.txt'
                url = 'http://www.gutenberg.org/cache/epub/21997/pg21997.txt'

                if verbose:
                    print("Trying to download", url)
                # check url
                if helpers.url_check(url):
                    continue

                parse_book(url, relations, conn, cur, verbose)

        if verbose:
            print("Cursor terminated")

    if verbose:
        print("Connection closed")
        print("Служу Советскому Союзу!")


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

    ## Let's organize rels' variables and their future values
    # variables of the relations' names
    language_rel, author_rel, book_rel, text_rel = relations.keys()
    # dictionary of only attributes' names
    attributes_dict = dict()
    for relation, attributes in relations.items():
        attributes_dict[relation] = [attr for attr, _ in attributes]

    # dictionary of values we want to insert into relations
    values_dict = defaultdict(list)

    ## Get the book's general info
    # get the book's title
    pattern = re.compile(r"Title: (.*)$")
    book_title = helpers.get_string_match(pattern, file_handler)
    # insert value into the values_dict
    values_dict[book_rel].append(book_title)

    # get the book's year
    pattern = re.compile(r"\b(\d{4})\b")
    # get the caret below the technical info
    for line in file_handler:
        if not line.startswith("***"):
            continue
        break
    for line in file_handler:
        if "***" in line:
            book_year = 10_000
            break
        match = re.search(pattern, line)
        if match:
            file_handler.seek(0)
            book_year = match.group(1)
            break
    file_handler.seek(0)
    # insert value into the values_dict
    values_dict[book_rel].append(book_year)

    # check if the book has already been parsed
    if helpers.row_exists(
        book_rel,
        attributes_dict[book_rel][1:3],
        [book_title, book_year],
        connection,
        cursor,
        verbose,
    ):
        print("The book is already in the database")
        Path.unlink(file_name)
        return 2

    # get the book's language
    pattern = re.compile(r"Language: ([A-Za-z]+)")
    book_language = helpers.get_string_match(pattern, file_handler)
    # insert value into the values_dict
    values_dict[language_rel].append(book_language)

    # get the book's author
    pattern = re.compile(r"(Author|Creator): (.*)$")
    book_author = helpers.get_string_match(pattern, file_handler)
    # insert value into the values_dict
    values_dict[author_rel].append(book_author)

    # print book's general info
    if verbose:
        print("***")
    print(f'  {book_author} "{book_title}" in {book_language}, {book_year}')
    if verbose:
        print("***")

    ## populate the relations
    # populate the language relation
    attrs = attributes_dict[language_rel][1:-1]
    vals = values_dict[language_rel]
    # if the language is in the database already
    if helpers.row_exists(language_rel, attrs, vals, connection, cursor, verbose):
        print('Found language')
        language_id = helpers.get_foreign_key(language_rel, attrs[0], book_language, connection, cursor, verbose)
    else:
        language_id = helpers.insert_into_table(
            language_rel,
            attributes_dict[language_rel][1:-1],
            values_dict[language_rel],
            cursor,
        )
    # populate the author relation
    attrs = attributes_dict[author_rel][1:-1]
    vals = values_dict[author_rel]
    # if the author is in the database already
    if helpers.row_exists(author_rel, attrs, vals, connection, cursor, verbose):
        author_id = helpers.get_foreign_key(author_rel, attrs[0], book_author, connection, cursor, verbose)
    else:
        author_id = helpers.insert_into_table(
            author_rel,
            attributes_dict[author_rel][1:-1],
            values_dict[author_rel],
            cursor,
        )

    # add values to values_dict
    values_dict[book_rel].append(language_id)
    values_dict[book_rel].append(author_id)

    # populate the book table
    book_id = helpers.insert_into_table(
        book_rel,
        attributes_dict[book_rel][1:-2],
        values_dict[book_rel],
        cursor,
    )

    # populate text table
    # value "" represents paragraph
    values_dict[text_rel] = ["", book_id]
    chars, count, pcount = text_to_database(
        text_rel,
        attributes_dict[text_rel][1:-1],
        values_dict[text_rel],
        file_handler,
        connection,
        cursor,
        verbose,
    )

    if verbose:
        print(f'Tables {', '.join(relations.keys())} have been populated')
    print(
        "Loaded {} paragraphs, {} lines, {} characters".format(pcount, count, chars)
    )

    # close file
    file_handler.close()
    if verbose:
        print(f"File {file_name} closed")

    # remove the downloaded file
    Path.unlink(file_name)
    if verbose:
        print(f"File {file_name} removed")

    time.sleep(random.randint(1, 7))

    return 0


def text_to_database(
    relation,
    attributes,
    values,
    file_handler,
    connection,
    cursor,
    verbose=False,
):
    """
    Parses the paragraphs from the txt file,
      creates the relation with the name of the book,
      inserts the paragraphs to the database into the relation

    Parameters:
    - relation: str: relation name
    - attributes: str: list of attributes' names
    - values: str: list of values to insert names
    - file_handler: object
    - connection: of of psycopg
    - cursor: object of psycopg
    - verbose: bool: print progress statements, default False

    Returns:
    - tuple: number of chars, of lines, of paragraphs

    """

    if verbose:
        print(f'Started relation "{relation}" populating...')

    paragraph = ""
    chars, count, pcount = 0, 0, 0

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
            helpers.insert_into_table(relation, attributes, values, cursor)
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


def warning_message():
    warning = input(
        "You are about to launch the app with the vulnerable to SQL injection functionality. Provide only trusted files with the relations' schemas. Are you sure you want to continue? yes/no: "
    ).lower()

    if warning == "yes" or warning == "y":
        print("Поехали!")
    elif warning == "no" or warning == "n":
        print("The user decided to quit the program. Chicken!")
        return 1
    else:
        print("Seems like you pressed the wrong button buddy. Try again")
        return main()


main()
