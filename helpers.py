import re
import time

import psycopg
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

    if match:
        return match.group(1)

    return None


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
        match = re.search(pattern, line)
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


def drop_tables(relations, connection, cursor, verbose=False):
    """
    Drops all the tables mentioned in the relations dictionary
      (see `create_table()` function's description).
      The function can also take the list of tables' names if
      you want to delete specified columns.
    relations: dict or list or tuple: a list with the tables names;
      if you use a dictionary, keys must be tables' names;
    connection: psycopg class instance;
    cursor: psycopg class instance;
    """
    for relation in relations:
        query = sql.SQL(
            """
            DROP TABLE IF EXISTS {} CASCADE;
            """
        ).format(sql.Identifier(relation))

        if verbose:
            print(query.as_string(connection))
        cursor.execute(query)

    return 0


def create_tables(relations, connection, cursor, verbose=False):
    """
    WARNING! The SQL injection possibility! Use this function
      only within a trusted environment!
    Creates a relation using a dictionary with the schemas of
      relations; if you want to create just one relation, pass
      one relation schema into the dictionary.
      The dictionary has:
      - a relation name as a key,
      - a list of tuples (attrubute, datatype pairs) as a value.
      Use only strings for the entries.
      Example of a relations dictionary:
      ```
        relations = {
            "table" : [("id", "SERIAL"),
                       ("name", "TEXT"),
                       ("age", "INTEGER"),
                       ("email", "VARCHAR(128)"),
                       ("department_id", "INTEGER REFERENCES department(id) ON DELETE CASCADE"),
                       ("UNIQUE", "(name, age)"),
                       ("PRIMARY KEY", "(id)")]
        }
      ```
    """

    for relation, attributes in relations.items():
        query = f"CREATE TABLE IF NOT EXISTS {relation} ("
        for attribute, datatype in attributes:
            query += f"{attribute} {datatype},"

        query = query.rstrip(",") + ")"

        if verbose:
            print(query.as_string(connection))
        cursor.execute(query)

    # return relation, attributes


def row_exists(
    relation, attributes_list, values_list, connection, cursor, vebose=False
):
    """
    WARNING! The SQL injection possibility! Use this function
      only within a trusted environment!
    Checks whether the value is already in the relation.
    Returns: bool: True if the value exists
    """

    if len(attributes_list) != len(values_list):
        print("Error: Number of attributes and values is different")
        return 1

    query = f"SELECT EXISTS (SELECT 1 FROM {relation} WHERE "
    conditions = list()
    for attr, value in zip(attributes_list, values_list):
        conditions.append(f"{attr} = {value}")
    query = " AND ".join(conditions) + ");"
    query = sql.SQL(query)

    if verbose:
        print(query.as_string(connection))
    cursor.execute(query)

    return cursor.fetchone()[0]


def insert_into_table(relation, attributes, values, cursor):
    """
    Inserts data into attributes of the relation.
    cursor: PostgeSQL cursor object
    table: str: name of the relation;
    attributes: list or tuple of strings: list of the attributes' names
    values: list or tuple of strings: list of the corresponding
      to attributes values
    """
    if len(attributes) == len(values):
        query = sql.SQL(
            """
            INSERT INTO {} ({}) VALUES ({});
            """
        ).format(
            sql.Identifier(relation),
            sql.SQL(", ").join(map(sql.Identifier, attributes)),
            sql.SQL(", ").join(map(sql.Literal, values)),
        )

    else:
        print("Error: Number of attributes and values is different")
        return 1

    cursor.execute(query)

    return 0


def get_value(cursor, relation, attribute1, attribute2, match):
    """
    Returns one value from the select query to database
    cursor: PostgeSQL cursor object
    relation: str: name of the relation
    attribute1: str: name of the select attribute
    attribute2: str: name of the condition attribute
    match: str: condition value
    """
    query = sql.SQL(
        """
        SELECT {} FROM {} WHERE {} = {} LIMIT 1;
        """
    ).format(
        sql.Identifier(attribute1),
        sql.Identifier(relation),
        sql.Identifier(attribute2),
        sql.Literal(match),
    )
    cursor.execute(query)

    return cursor.fetchone()[0]


def get_foreign_key(
    relation,
    attribute_to_search_on,
    value,
    connection,
    cursor,
    pkey="id",
    verbose=False,
):
    """
    Gets the primary key of the relation's tuple.
    """
    query = sql.SQL(
        """
        SELECT {} FROM {} WHERE {} = %s;
        """
    ).format(
        sql.Identifier(pkey),
        sql.Identifier(relation),
        sql.Identifier(attribute_to_search_on),
    )

    if verbose:
        print(query.as_string(connection))
    cursor.execute(query, (value,))

    return cursor.fetchone()[0]
