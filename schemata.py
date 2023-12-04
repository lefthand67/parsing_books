# relations and their schemas
relations = {
    "language": [
        ("id", "SERIAL"),
        ("name", "VARCHAR(64) UNIQUE"),
        ("PRIMARY KEY", "(id)"),
    ],
    "author": [
        ("id", "SERIAL"),
        ("name", "VARCHAR(128) UNIQUE"),
        ("PRIMARY KEY", "(id)"),
    ],
    "book": [
        ("id", "SERIAL"),
        ("title", "VARCHAR(256)"),
        ("year", "INTEGER"),
        ("author_id", "INTEGER REFERENCES author(id) ON DELETE CASCADE"),
        ("language_id", "INTEGER REFERENCES language(id) ON DELETE CASCADE"),
        ("UNIQUE", "(title, year, language_id)"),
        ("PRIMARY KEY", "(id)"),
    ],
    "text": [
        ("id", "SERIAL"),
        ("paragraph", "TEXT"),
        ("book_id", "INTEGER REFERENCES book(id) ON DELETE CASCADE"),
        ("PRIMARY KEY", "(id)"),
    ],
}
