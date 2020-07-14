import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
    drops the all the tables in redshift if they exist

    Args:
        cur: cursor to the connection to the database
        conn: connection to the database

    """
    for i,query in enumerate(drop_table_queries,1):
        print("dropping: {}/{} tables".format(i, len(drop_table_queries)))
        cur.execute(query)
        conn.commit()
        


def create_tables(cur, conn):
    """
    creates all the tables: staging, dimension, and fact tables

    Args:
        cur: cursor to the connection to the database
        conn: connection to the database

    """
    for i, query in enumerate(create_table_queries,1):
        print("creating: {}/{} tables".format(i, len(create_table_queries)))
        cur.execute(query)
        conn.commit()
        


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()