import sqlite3

db = sqlite3.connect("./database.db")
cursor = db.cursor()
cursor.execute("""
CREATE TABLE tickets(
    uuid TEXT PRIMARY KEY,
    status TEXT,
    resource_id TEXT,
    date DATETIME
);
""")
cursor.execute("""
CREATE TABLE resources(
    uuid TEXT PRIMARY KEY,
    resource TEXT,
    date DATETIME
);
""")
cursor.execute("""
CREATE TABLE cache(
    key TEXT PRIMARY KEY,
    content TEXT
);
""")
db.commit()
db.close()