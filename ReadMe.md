Rot Front! This is a Python program that aimed at parsing the txt format books from Gutenberg Project to a PostgreSQL database. The program is a way far from being perfect and must be considered as a showcase of working with the psycopg 3 library. No OOP here, only functions, so it is not really readable. Consider using these functions as a basis for the OOP module.

Program usage:

- To start parse a link, use:
`python main.py`

- If you want to parse more than one link (and this i) type the desired number:
`python main.py 5`

- If you want the program to tell almost everything it is implementing, use `-V` (be verbose) flag:
`python main.py 5 -V`

- If you want to start the database over, use `-C` (clear) flag:
`python main.py 5 -C`

- Also you can combine:
`python main.py 5 -C -V`

In cases you use `-C` or/and `-V` flags you must explicitly define the number of links to parse.

If you want to try the program on your own, change the database credentials in the `info.py` file for yours.

Program is a part of my training on working with Postgres and psycopg 3. The idea's author is Dr. Chuck Severance and can be found in his "PostgreSQL for everybody course"'s [Lesson 6](https://www.pg4e.com/lessons/week6a). Dr Chuck uses psycopg 2 module, I use the most recent Python (version 3.12) and Psycopg (version 3) releases (as of Dec. 2023).

One of the unsolved problems is the getting the correct published year because there is no standardization in the Gutenberg Project's files' layout - sometimes you find the published year, sometimes you don't, and the program can take any year from the book's text and not from its description part.

License is GPLv3.
