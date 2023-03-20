import logging
import os
from datetime import datetime, timedelta

from mysql.connector.pooling import MySQLConnectionPool

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

pid = os.getpid()

WRITE_IN_DISK_ENABLED = False
DB_PRIVATE_IP = '10.27.64.3'
DB_PUBLIC_IP = '35.193.69.26'

pool = MySQLConnectionPool(
    pool_name="connection_pool",
    pool_size=5,
    pool_reset_session=True,
    database="moralis",
    host=DB_PRIVATE_IP,
    user='root',
    password='!!DCU_Cloud_SYS_2023!!'
)

API_PAGE_SIZE = 100

# Create a mechanism to identify the blocked API and stop using the blocked one.
# When all get blocked stop the application
API_KEY_ARRAY = []

ROUND_ROBIN_INDEX = 0


def get_database_connection():
    return pool.get_connection()


def fetch_api_keys():
    _conn = get_database_connection()
    _cursor = _conn.cursor(dictionary=True)

    slct_get_api_keys = """
        select api_key as api_key, blocked as blocked, blocked_time as blocked_time
        from moralis_api_keys
    """

    _cursor.execute(slct_get_api_keys)
    _rows = _cursor.fetchall()
    for _row in _rows:
        if _row['api_key'] is not API_KEY_ARRAY:
            API_KEY_ARRAY.append({
                "api_key": _row['api_key'],
                "blocked": _row['blocked'],
                "blocked_time": _row['blocked_time']}
            )

    _conn.commit()
    _conn.close()
    return


def get_api_key():
    global ROUND_ROBIN_INDEX

    if not API_KEY_ARRAY:
        fetch_api_keys()

    api_list = []
    now = datetime.now()

    for item in API_KEY_ARRAY:

        blocked_time = item['blocked_time']

        if blocked_time is not None and now - blocked_time >= timedelta(days=1):
            item['blocked'] = False
            item['blocked_time'] = None

        if not item['blocked'] and (blocked_time is None):
            api_list.append(item['api_key'])

    if not api_list:

        return None
    else:
        api_key = api_list[ROUND_ROBIN_INDEX % len(api_list)]
        ROUND_ROBIN_INDEX += 1
        return api_key


def create_directory(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def is_fully_processed(result):
    try:
        api_page = _get_as_number(result['api_page'])
        api_page_size = _get_as_number(result['api_page_size'])
        api_total = _get_as_number(result['api_total'])

        return api_total != 0 & (api_page * api_page_size) >= api_total
    except:
        return False


def _get_as_number(v):
    if isinstance(v, (int, float)):
        return v
    elif isinstance(v, str) and v.isdigit():
        return int(v)
    return 0
