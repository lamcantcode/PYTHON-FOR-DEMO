import time
import psycopg2
from config import Settings
from logger import get_logger
from re import *


class DB:
    logger = get_logger(__name__)

    @classmethod
    def release_connection(cls, conn):
        conn.close()

    @classmethod
    def get_connection(cls):
        retry_count = 0
        while True:
            try:
                config = Settings(_env_file="../config/settings.env", )
                conn = psycopg2.connect(host=config.HOST, database=config.NAME, user=config.USER,
                                        password=config.PASSWORD, connect_timeout=config.CONNECTION_TIMEOUT)
                cls.execute(conn, "SET timezone TO 'Asia/Hong_Kong'", ())
                return conn
            except Exception as exp:
                retry_count += 1
                cls.logger.error('Unable to obtain DB connection after {} time(s).'.format(retry_count))
                if retry_count == 3:
                    raise exp


        time.sleep(3)

    @staticmethod
    def execute(conn, query, params):
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        result = cur.rowcount
        cur.close()

        return result

    @staticmethod
    def query(conn, query, params, fetch_type='one'):
        cur = conn.cursor()
        cur.execute(query, params)
        match fetch_type:

            case 'one':
                result = cur.fetchone()[0]
            case 'many':
                result = cur.fetchmany()
            case 'all':
                result = cur.fetchall()
            case _:
                result = None

        cur.close()

        return result
