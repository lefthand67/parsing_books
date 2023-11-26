import psycopg
from psycopg import sql
import info
import requests
import re
from pathlib import Path
import time


def main() -> int:

    url = 'http://www.gutenberg.org/cache/epub/19337/pg19337.txt'

    # check url
    if url_check(url):
        return 1

    # get the file name out of its url
    print(f"Downloading a file from {url}")
    fname = get_file_name(url)

    # download book and save it to txt file
    get_txt(url, fname)

    # try open the text
    try:
        file_handler = open(fname, "r")
        print(f"File {fname} opened")
    except:
        Path.unlink(fname)
        print("Could not open a file")
        return 2

    # get book's general info
    book_title = get_book_title(file_handler)
    book_author = get_book_author(file_handler)
    book_language = get_book_language(file_handler)
    book_year = get_book_year(file_handler)

    # print book's general info
    print("***")
    print(f'  {book_author} "{book_title}" {book_language} {book_year}')
    print("***")

    # connect to database
    with psycopg.connect(f"""
            host={info.host}
            dbname={info.dbname}
            user={info.user}
            password={info.pwd}
                        """) as conn:
        print("Connection with database established")

        # create a cursor
        with conn.cursor() as cur:
            print("  Cursor created")

            query = sql.SQL("DROP TABLE IF EXISTS {};").format(
                        sql.Identifier(book_title))

#            print("\n  ", query.as_string(conn))
            cur.execute(query)

            query = sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
                                id SERIAL PRIMARY KEY,
                                body TEXT
                            );
                """).format(sql.Identifier(book_title))

#            print("\n  ", query.as_string(conn))
            cur.execute(query)
            print(f'\n  Table "{book_title}" created')

            # query = sql.SQL()


            # populate the relation
            query = sql.SQL("INSERT INTO {} (body) VALUES (%s);"
                    ).format(sql.Identifier(book_title))
            print(f'  Starting table "{book_title}" population')

            paragraph = ''
            chars = 0
            count = 0
            pcount = 0

            for line in file_handler:
                count += 1
                line = line.strip()
                chars += len(line)

                # skip empty lines
                if line == '' and paragraph == '':
                    continue

                # when paragraph done
                elif line == '':

                    cur.execute(query, (paragraph,))
                    pcount += 1

                    if pcount % 50 == 0:
                        conn.commit()
                    if pcount % 100 == 0:
                        print(f'    {pcount} loaded...')
                        time.sleep(1)

                    paragraph = ''
                    continue

                # populating paragraph
                paragraph += ' ' + line

            print(f'  Table "{book_title}" populated')
            print('  Loaded {} paragraphs, {} lines, {} characters'.format(pcount, count, chars))


        print("\n  Cursor terminated")

    print("Connection closed")

    #close file
    file_handler.close()
    print(f"File {fname} closed")

    # remove the downloaded file
    Path.unlink(fname)
    print(f"File {fname} removed")

    print("See you next time!\n")

    return 0


def url_check(url: str) -> bool:
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


def get_file_name(url: str) -> str:
    """
    Gets a name of the file out of the url address
    Returns: str: file name
    """

    pattern = r"\/([A-Za-z0-9]+\.txt)$"

    result = re.search(pattern, url)

    return result.group(1)


def get_txt(url: str, file_name: str) -> str:
    """
    Creates a txt file out of a url link
    Returns: str: a name of the created file
    """

    response = requests.get(url, allow_redirects=True)

    content = response.text

    with open(file_name, 'w') as file:
        file.write(content)
        print(f"Text saved as {file_name}")

    return file_name


def get_book_title(file_handler):
    """
    Gets a title of the book from the opened file
    Returns: str: a book's title
    """

    pattern = r"Title: (.*)$"

    for line in file_handler:
        if re.match(r"Title: ", line):
            result = re.search(pattern, line)
            file_handler.seek(0)
            return result.group(1)

    file_handler.seek(0)
    return "-"


def get_book_author(file_handler):
    """
    Gets a name of the book's author from the opened file
    Returns: str: a book's author's name
    """

    pattern = r"Author: (.*)$"

    for line in file_handler:
        if re.match(r"Author: ", line):
            result = re.search(pattern, line)
            file_handler.seek(0)
            return result.group(1)

    file_handler.seek(0)
    return "-"


def get_book_language(file_handler):
    """ Gets a language the book's written in from
      the opened file
    Returns: str: a book's language
    """

    pattern = r"Language: ([A-Za-z]+)"

    for line in file_handler:
        if re.match(r"Language: ", line):
            result = re.search(pattern, line)
            file_handler.seek(0)
            return result.group(1)

    file_handler.seek(0)
    return "-"


def get_book_year(file_handler):
    """
    Gets a year the book was originally published from
      the opened file
    Returns: str: a book's first publication year
    """

    pattern = r"(\d{4})$"

    for line in file_handler:
        if re.match(r"Original publication: ", line):
            result = re.search(pattern, line)
            file_handler.seek(0)
            return result.group(1)

    file_handler.seek(0)
    return "-"


main()
