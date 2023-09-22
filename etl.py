"""Read data from S3, load into staging tables, then insert into final tables."""
import configparser

import psycopg2

from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copy S3 data into temporary staging tables."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into final Redshift tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Execute ETL job."""
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            config.get("CLUSTER", "DB_HOST"),
            config.get("CLUSTER", "DB_NAME"),
            config.get("CLUSTER", "DB_USER"),
            config.get("CLUSTER", "DB_PASSWORD"),
            config.get("CLUSTER", "DB_PORT"),
        )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    print("Staging tables loaded.")
    insert_tables(cur, conn)
    print("Final tables populated.")

    conn.close()


if __name__ == "__main__":
    main()
