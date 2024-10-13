from datetime import time
import os
from collections import namedtuple
import sqlite3

import discord
from common import MediaType

# environment = os.environ['ENV']
# is_prod = environment == 'PROD'


def namedtuple_factory(cursor, row):
    """Returns sqlite rows as named tuples."""
    fields = [col[0] for col in cursor.description]
    Row = namedtuple("Row", fields)
    res = Row(*row)
    # HACK:
    if hasattr(res, 'media_type'):
        return res._replace(media_type=MediaType[res.media_type])
    return res


class Store:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(
            db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.conn.row_factory = namedtuple_factory

    def new_club(self, discord_guild_id, name, code):
        query = 'INSERT INTO clubs (discord_guild_id, code, name) VALUES (?,?,?);'
        with self.conn:
            self.conn.execute(query, (discord_guild_id, code, name))

    def new_book(
        self, discord_guild_id, club_code, name, book_code, points, created_at
    ):
        with self.conn:
            query = """
            INSERT INTO books (discord_guild_id, name, club_code, code, points, created_at)
            VALUES (?,?,?,?,?,?);
            """
            data = (discord_guild_id, name, club_code, book_code, points, created_at)
            self.conn.execute(query, data)
            
    def new_activity(
        self, discord_guild_id, discord_user_id, club_code, book_code, points
    ):
        with self.conn:
            query = """
            INSERT INTO activities (discord_guild_id, discord_user_id, club_code, book_code, points)
            VALUES (?,?,?,?,?);
            """
            data = (discord_guild_id, discord_user_id, club_code, book_code, points)
            self.conn.execute(query, data)

    def new_log(
        self, discord_guild_id, discord_user_id, media_type, amount, note, created_at
    ):
        with self.conn:
            query = """
            INSERT INTO logs (discord_guild_id, discord_user_id, media_type, amount, note, created_at)
            VALUES (?,?,?,?,?,?);
            """
            data = (discord_guild_id, discord_user_id, media_type.value, amount, note, created_at)
            self.conn.execute(query, data)

    def get_logs_by_user(self, discord_guild_id, discord_user_id):
        query = """
        SELECT * FROM logs
        WHERE discord_guild_id= ? AND discord_user_id=?
        ORDER BY created_at DESC;
        """
        data = (discord_guild_id, discord_user_id)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchall()

    def get_logs(self, discord_guild_id):
        query = """
        SELECT * FROM logs
        WHERE discord_guild_id= ?
        ORDER BY created_at DESC;
        """
        data = (discord_guild_id,)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchall()

    def get_leaderboard(self, discord_user_id, timeframe, media_type):
        where_clauses = []
        # timeframe
        if timeframe.value == 'month':
            where_clauses.append("created_at >= date('now', 'start of month')")
        elif timeframe.value == 'week':
            where_clauses.append("strftime('%w', created_at) = strftime('%w', date('now'))")

        if media_type:
            where_clauses.append(f"media_type = '{media_type.value}'")

        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        query = f"""
        WITH scoreboard AS (
            SELECT
                discord_user_id,
                SUM(
                CASE
                    WHEN media_type = 'BOOK' THEN amount
                    WHEN media_type = 'MANGA' THEN amount * 0.2
                    WHEN media_type = 'VN' THEN amount * (1.0 / 350.0)
                    WHEN media_type = 'ANIME' THEN amount * 9.5
                    WHEN media_type = 'READING' THEN amount * (1.0 / 350.0)
                    WHEN media_type = 'READTIME' THEN amount * 0.45
                    WHEN media_type = 'LISTENING' THEN amount * 0.45
                    ELSE 0
                END
                ) AS total
            FROM logs
            {where_clause}
            GROUP BY discord_user_id
            ), leaderboard AS (
            SELECT
                discord_user_id,
                total,
                RANK () OVER (ORDER BY total DESC) AS rank
            FROM scoreboard
            )
            SELECT * FROM leaderboard
            WHERE (
            rank <= 20
            ) OR (
            rank >= (SELECT rank FROM leaderboard WHERE discord_user_id = ?) - 1
            AND
            rank <= (SELECT rank FROM leaderboard WHERE discord_user_id = ?) + 1
            );
        """
        data = (discord_user_id, discord_user_id,)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchall()

    def get_all_logs_by_guild(self, discord_guild_id):
        query = """
        SELECT * FROM logs
        WHERE discord_guild_id= ?
        ORDER BY created_at DESC;
        """
        data = (discord_guild_id,)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchall()

    def get_monthly_logs_by_guild(self, discord_guild_id):
        query = """
        SELECT * FROM logs
        WHERE discord_guild_id= ?
        AND strftime('%m', created_at) = strftime('%m', date('now'))
        AND strftime('%Y', created_at) = strftime('%Y', date('now'))
        """
        data = (discord_guild_id,)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchall()

    def get_weekly_logs_by_guild(self, discord_guild_id):
        query = """
        SELECT * FROM logs
        WHERE discord_guild_id= ?
        AND strftime('%w', created_at) = strftime('%w', date('now'))
        AND strftime('%Y', created_at) = strftime('%Y', date('now'))
        """
        data = (discord_guild_id,)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchall()

    def delete_latest(self, discord_guild_id, discord_user_id):
        with self.conn:
            query = """
            DELETE FROM logs
            WHERE discord_guild_id=? AND discord_user_id=?
            AND created_at=(
                SELECT created_at FROM logs
                WHERE discord_guild_id= ? AND discord_user_id=?
                ORDER BY created_at DESC
                LIMIT 1
            );
            """
            data = (discord_guild_id, discord_user_id,
                    discord_guild_id, discord_user_id)
            return self.conn.execute(query, data).rowcount

    def delete_user_logs(self, discord_guild_id, discord_user_id):
        with self.conn:
            query = """
            DELETE FROM logs
            WHERE discord_guild_id=? AND discord_user_id=?;
            """
            data = (discord_guild_id, discord_user_id)
            return self.conn.execute(query, data).rowcount

    def get_book(self, discord_guild_id, book_code):
        query = f"SELECT * FROM books WHERE code='{book_code}'"
        #data = (discord_guild_id, book_code)
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()

    def delete_book(self, discord_guild_id, book_code):
        query = "DELETE FROM books WHERE discord_guild_id=? AND code=?;"
        data = (discord_guild_id, book_code)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        cursor.fetchone()

        query = "DELETE FROM activities WHERE discord_guild_id=? AND book_code=?;"
        data = (discord_guild_id, book_code)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchone()

    def get_books(self, discord_guild_id, club_code):
        query = "SELECT * FROM books WHERE discord_guild_id=? AND club_code=? ORDER BY created_at DESC;"
        data = (discord_guild_id, club_code)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchall()

    def get_activity(self, discord_guild_id, discord_user_id, book_code):
        query = """
        SELECT * FROM activities
        WHERE discord_guild_id=? AND discord_user_id=? AND book_code=?;
        """
        data = (discord_guild_id, discord_user_id, book_code)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchone()

    def get_activities_by_club(self, discord_guild_id, club_code):
        query = """
        SELECT activities.*, books.created_at, books.name as book_name FROM activities, books
        WHERE activities.discord_guild_id= ?
            AND activities.club_code=?
            AND activities.discord_guild_id = books.discord_guild_id
            AND activities.book_code = books.code
        ORDER BY books.created_at DESC;
        """
        data = (discord_guild_id, club_code)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchall()

    def get_activities_by_user(self, discord_guild_id, discord_user_id):
        query = """
        SELECT activities.*, books.created_at, books.points as book_points, books.name as book_name FROM activities, books
        WHERE activities.discord_guild_id= ?
            AND activities.discord_user_id=?
            AND activities.discord_guild_id = books.discord_guild_id
            AND activities.book_code = books.code
        ORDER BY books.created_at DESC;
        """
        data = (discord_guild_id, discord_user_id)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchall()

    def get_activities_by_book(self, discord_guild_id, book):
        query = "SELECT * FROM activities WHERE discord_guild_id=? AND book_code=?;"
        data = (discord_guild_id, book)
        cursor = self.conn.cursor()
        cursor.execute(query, data)
        return cursor.fetchall()

    def get_club(self, discord_guild_id, code):
        where_clause = f"""code='{code}'"""
        query = f"""SELECT * FROM clubs WHERE {where_clause}"""
        print(query)
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()[0]

    def get_scoreboard(self, discord_guild_id, club_code):
        if club_code:
            if club_code == "VN":
                query = """
            SELECT discord_user_id, SUM(points) as points FROM activities
            WHERE discord_guild_id=? AND club_code=?
            GROUP BY discord_user_id
            ORDER BY points DESC;
            """
            else:
                query = """
                SELECT discord_user_id, SUM(points) as points FROM activities
                WHERE discord_guild_id=? AND club_code=? AND NOT club_code='VN'
                GROUP BY discord_user_id
                ORDER BY points DESC;
                """
            data = (discord_guild_id, club_code)
            cursor = self.conn.cursor()
            cursor.execute(query, data)
            return cursor.fetchall()
        else:
            query = """
            SELECT discord_user_id, SUM(points) as points FROM activities
            WHERE discord_guild_id=? AND NOT club_code='VN'
            GROUP BY discord_user_id
            ORDER BY points DESC;
            """
            data = (discord_guild_id,)
            cursor = self.conn.cursor()
            cursor.execute(query, data)
            return cursor.fetchall()


def init_tables(db_name):
    conn = sqlite3.connect(db_name)
    with conn:
        conn.execute(_CREATE_CLUBS_TABLE)
        conn.execute(_CREATE_BOOKS_TABLE)
        conn.execute(_CREATE_ACTIVITIES_TABLE)
        conn.execute(_CREATE_LOG_TABLE)
        conn.execute(_CREATE_LOG_TABLE_INDEX)
    return


_CREATE_CLUBS_TABLE = """
CREATE TABLE IF NOT EXISTS clubs (
    discord_guild_id INTEGER,
    code TEXT,
    name TEXT,
    PRIMARY KEY (discord_guild_id, code)
);
"""

_CREATE_BOOKS_TABLE = """
CREATE TABLE IF NOT EXISTS books (
    discord_guild_id INTEGER,
    name TEXT,
    code TEXT,
    club_code TEXT,
    points REAL,
    created_at TIMESTAMP,
    PRIMARY KEY (discord_guild_id, code)
);
"""

_CREATE_ACTIVITIES_TABLE = """
CREATE TABLE IF NOT EXISTS activities (
    discord_guild_id INTEGER,
    book_code TEXT,
    discord_user_id INTEGER,
    points REAL,
    FOREIGN KEY (discord_guild_id, book_code) REFERENCES books(discord_guild_id, code),
    FOREIGN KEY (discord_guild_id, club_code) REFERENCES club(discord_guild_id, code),
    PRIMARY KEY (discord_guild_id, club_code, book_code, discord_user_id)
);
"""


_CREATE_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS logs (
    discord_guild_id INTEGER,
    discord_user_id INTEGER,
    media_type TEXT,
    amount REAL,
    note TEXT,
    created_at TIMESTAMP
);
"""

_CREATE_LOG_TABLE_INDEX = """
CREATE INDEX IF NOT EXISTS discord_guild_id_over_created_at_idx ON logs (discord_guild_id, created_at);
"""

_CREATE_WAIFU_TABLE = """
CREATE TABLE IF NOT EXISTS waifus (
    id INTEGER PRIMARY KEY,
    name TEXT,
    rarity INTEGER,
    image_file_path TEXT,
);
"""

_CREATE_CAPTURE_TABLE = """
CREATE TABLE IF NOT EXISTS waifus (
    discord_guild_id INTEGER,
    discord_user_id INTEGER,
    rarity INTEGER,
    file_path TEXT,
);
"""
