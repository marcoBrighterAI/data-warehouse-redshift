import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Function that executes DROP queries in the Redshift cluster.
    :param cur: cursor connexion object on Redshift
    :param conn: connection object on Redshift
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Function that executes CREATE queries in the Redshift cluster.
    :param cur: cursor connexion object on Redshift
    :param conn: connection object on Redshift
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('CONFIG INFO')
    print(*config['CLUSTER'].values())
    print('DONE')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
