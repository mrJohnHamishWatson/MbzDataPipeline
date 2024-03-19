from db.postgres_client import PostgresClient


class Postgres:
    def __init__(self):
        self.client = PostgresClient()

    def copy_data(self, path: str, table_name: str):
        copy_sql = """
                   COPY {}
                   FROM stdin
                   WITH CSV HEADER
                   DELIMITER as ','
                   """.format(
            table_name
        )

        with open(path, "r") as f:
            self.client.cursor.copy_expert(sql=copy_sql, file=f)
            self.client.conn.commit()
