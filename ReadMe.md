Rot Front! This is a Python program that aimed on parsing the txt format books from Gutenberg Project to a PostgreSQL database.

Program usage:

- To start parse a link, use:
`python3 main.py`

- If you want to parse more than one link (and this i) type the desired number:
`python3 main.py 5`

- If you want the program to tell almost everything it is implementing, use `-V` (be verbose) flag:
`python3 main.py 5 -V`

- If you want to start the database over, use `-C` (clear) flag:
`python3 main.py 5 -C`

- Also you can combine:
`python3 main.py 5 -C -V`

In cases you use `-C` or/and `-V` flags you must explicitly define the number of links to parse.

If you want to try the program on your own, change the database credentials in the `info.py` file for yours. I use my own Debian 12 remote server with the changed port numbers and firewall which adds reality to the process. The default port for PostgreSQL server, just for reminder, is `5432`.

Program is a part of my training on working with Postgres and psycopg 3. The author of the exercise's idea is Dr. Chuck Severance from his "PostgreSQL for everybody course"'s [Lesson 6](https://www.pg4e.com/lessons/week6a). Dr Chuck uses psycopg 2 module, I use the most recent Python and Psycopg (3) release (as of Dec. 2023).

Also, I try to write a working program that catches errors and is pretty readable by other programmers (I use main() function standard that helps to concentrate on the main operations in the upper half of the document while all the functions are written below it). I just learn and experiment.

License is GPLv3.
