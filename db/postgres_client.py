import psycopg2


from db import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


class PostgresClient:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        self.cursor = self.conn.cursor()
        