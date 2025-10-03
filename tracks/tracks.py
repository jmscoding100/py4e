import csv
import sqlite3

DB_NAME = 'trackdb.sqlite'
CSV_NAME = 'tracks.csv'   # change if your CSV is named differently

conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS Artist;
DROP TABLE IF EXISTS Genre;
DROP TABLE IF EXISTS Album;
DROP TABLE IF EXISTS Track;

CREATE TABLE Artist (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE Genre (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE Album (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    artist_id  INTEGER,
    title   TEXT UNIQUE
);

CREATE TABLE Track (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    title TEXT UNIQUE,
    album_id  INTEGER,
    genre_id  INTEGER,
    len INTEGER, rating INTEGER, count INTEGER
);
''')

def find_col(header_lower, candidates):
    """Return index of the first candidate present in header_lower, or None."""
    for cand in candidates:
        if cand in header_lower:
            return header_lower.index(cand)
    return None

def safe_int(s, default=0):
    try:
        return int(s)
    except:
        return default

# read CSV into memory (skip completely empty rows)
with open(CSV_NAME, newline='', encoding='utf-8') as fh:
    reader = csv.reader(fh)
    rows = [r for r in reader if any(cell.strip() for cell in r)]

if not rows:
    print("No data found in", CSV_NAME)
    raise SystemExit

# detect header row
first_row = [c.strip() for c in rows[0]]
first_lower = [c.strip().lower() for c in first_row]

header_keywords = {'track','name','artist','album','genre','count','rating','length','total time','play count'}
has_header = any(w in header_keywords for w in first_lower)

start_index = 1 if has_header else 0

# if we have a header, map columns by name; otherwise assume default orders
if has_header:
    header = first_lower
    name_idx   = find_col(header, ['track','name','title'])
    artist_idx = find_col(header, ['artist'])
    album_idx  = find_col(header, ['album','album title'])
    genre_idx  = find_col(header, ['genre'])
    count_idx  = find_col(header, ['count','play count'])
    rating_idx = find_col(header, ['rating'])
    length_idx = find_col(header, ['length','total time','time'])
else:
    # inspect a data row to choose 7-column vs 6-column layout
    sample = rows[0]
    if len(sample) >= 7:
        # probably: Track,Artist,Album,Genre,Count,Rating,Length
        name_idx, artist_idx, album_idx, genre_idx, count_idx, rating_idx, length_idx = 0,1,2,3,4,5,6
        # but if some rows are shorter we'll handle that per-row
    elif len(sample) == 6:
        # probably: Track,Artist,Album,Count,Rating,Length (no genre)
        name_idx, artist_idx, album_idx, genre_idx, count_idx, rating_idx, length_idx = 0,1,2,None,3,4,5
    else:
        # fallback to 7-column mapping and we'll skip problematic rows later
        name_idx, artist_idx, album_idx, genre_idx, count_idx, rating_idx, length_idx = 0,1,2,3,4,5,6

# Process rows
for i in range(start_index, len(rows)):
    row = rows[i]
    # helper to get cell safely
    def cell(idx):
        try:
            return row[idx].strip()
        except:
            return None

    name   = cell(name_idx)
    artist = cell(artist_idx)
    album  = cell(album_idx)
    genre  = cell(genre_idx) if genre_idx is not None else None
    # numeric fields
    count  = safe_int(cell(count_idx))
    rating = safe_int(cell(rating_idx))
    length = safe_int(cell(length_idx))

    # If the file doesn't have a genre column, default to 'Unknown'
    if genre is None:
        genre = 'Unknown'

    # If essential fields missing, skip row
    if not name or not artist or not album:
        # you can uncomment the next line to debug which rows get skipped:
        # print("Skipping row", i, "missing essential data:", row)
        continue

    # Insert artist
    cur.execute('INSERT OR IGNORE INTO Artist (name) VALUES (?)', (artist,))
    cur.execute('SELECT id FROM Artist WHERE name = ?', (artist,))
    artist_id = cur.fetchone()[0]

    # Insert genre
    cur.execute('INSERT OR IGNORE INTO Genre (name) VALUES (?)', (genre,))
    cur.execute('SELECT id FROM Genre WHERE name = ?', (genre,))
    genre_id = cur.fetchone()[0]

    # Insert album
    cur.execute('INSERT OR IGNORE INTO Album (title, artist_id) VALUES (?, ?)', (album, artist_id))
    cur.execute('SELECT id FROM Album WHERE title = ?', (album,))
    album_id = cur.fetchone()[0]

    # Insert track
    cur.execute('''INSERT OR REPLACE INTO Track
        (title, album_id, genre_id, len, rating, count)
        VALUES (?, ?, ?, ?, ?, ?)''',
        (name, album_id, genre_id, length, rating, count))

# commit changes once
conn.commit()

# run the grader query
sql_query = '''
SELECT Track.title, Artist.name, Album.title, Genre.name
FROM Track
JOIN Genre ON Track.genre_id = Genre.id
JOIN Album ON Track.album_id = Album.id
JOIN Artist ON Album.artist_id = Artist.id
ORDER BY Artist.name
LIMIT 3;
'''
print("\n=== Query result (first 3 rows ordered by artist) ===")
for row in cur.execute(sql_query):
    print(row)

cur.close()
conn.close()