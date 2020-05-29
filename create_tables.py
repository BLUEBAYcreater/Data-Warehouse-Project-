import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        This function iterates over all the drop table queries and executes them.
        INPUTS:
        * cur the cursor variable of the database
        * conn the connection variable of the database
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        This function iterates over all the create table queries and executes them.
        INPUTS:
        * cur the cursor variable of the database
        * conn the connection variable of the database
    """
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
        This function connects to the database using credentials in the config file and then drops
        and recreates the required tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()