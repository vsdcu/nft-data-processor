import fcntl
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta

from moralis import evm_api
from mysql.connector.pooling import MySQLConnectionPool

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#  TO STOP THE PROCESS CREATE A FILE 'STOP_PROCESS' AND THE APPLICATION WILL STOP GRACEFULLY


# parser = argparse.ArgumentParser(description='Moralis API Consumer Arguments')
#
# parser.add_argument('--host', type=str, required=True,
#                     help='DB host')
# parser.add_argument('--user', type=str, required=True,
#                     help='DB user')
# parser.add_argument('--password', type=str, required=True,
#                     help='DB password')
#
#
# args = parser.parse_args()

# logger.info(f"DB-INFO: host: {args.host} user: {args.user}")

pid = os.getpid()

WRITE_IN_DISK_ENABLED=False
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


def _build_params(arg_address, arg_cursor: None):
    params = {
        "address": arg_address,
        "chain": "eth",
        "format": "decimal",
        "normalizeMetadata": True,
        "limit": API_PAGE_SIZE,
        "disable_total": False,
    }

    if arg_cursor is not None:
        params['cursor'] = arg_cursor

    return params


def _get_api_key():
    global ROUND_ROBIN_INDEX

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


def _execute_request(arg_address, arg_cursor: None):
    request_params = _build_params(arg_address, arg_cursor)
    api_key = _get_api_key()

    while not api_key:
        now = datetime.now()
        logging.info(f"{pid} - {now} - Moralis  - All API_KEY is blocked waiting 10 minutes to check again")
        time.sleep(60 * 10)

    try:
        result = evm_api.nft.get_contract_nfts(
            api_key=api_key,
            params=request_params,
        )

        # store result on disk
        if WRITE_IN_DISK_ENABLED:
            timestamp = datetime.timestamp(datetime.now())
            f = open(f"RESULT/{arg_address}_{timestamp}.json", "w+")
            f.write(json.dumps(result))
            f.close()

        return result
    except Exception as e:

        if "API KEY is currently blocked" in str(e):
            for item in API_KEY_ARRAY:
                if item['api_key'] in api_key:
                    item['blocked'] = True
                    item['blocked_time'] = datetime.now()

        sleep_array = [i * 10 for i in range(1, 10)]
        sleep_scs = sleep_array[ROUND_ROBIN_INDEX % 10]
        now = datetime.now()
        logging.error(
            f'{pid} - {now} - Moralis  - Error to connect to Moralis. Will wait for {sleep_scs} seconds Exception. {str(e)}')

        time.sleep(sleep_scs)

        _fetch_api_keys()  # Try to fetch new API_KEYS in case new were inserted in the DB.
        return _execute_request(arg_address, arg_cursor)


def _insert_data_moralis_tb(arg_nft_address, arg_token_address, arg_token_id, arg_json_data):
    _conn = get_database_connection()
    _cursor = _conn.cursor(dictionary=True)
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = (arg_nft_address, arg_token_address, arg_token_id, timestamp, arg_json_data)
        _cursor.execute("INSERT IGNORE INTO nft_contracts (nft_address, token_address, token_id, insert_date, "
                        "json_data) VALUES (%s, %s, %s, %s, %s)", data)

    except Exception as e:
        now = datetime.now()
        if "UNIQUE constraint failed" in str(e) or "Duplicate entry" in str(e):
            if WRITE_IN_DISK_ENABLED:
                f = open(f"UNIQUE_ERROR/UNIQUE_ERROR_{arg_nft_address}_{arg_token_address}_{arg_token_id}_{now}", "a+")
                f.write(f"{arg_nft_address};{arg_token_address};{arg_token_id}; {arg_json_data}")
                f.close()
            logging.error(f"- Moralis  UNIQUE CONSTRAINT FOUND {arg_nft_address}")
        else:
            logging.error(
                f"{pid} - {now} - Moralis - Error Inserting Data: {arg_nft_address}, {arg_token_address}, {arg_token_id} {arg_json_data} ")
            logging.error(f"{pid} - {now} - Moralis - Exception - {str(e)}")
            raise
    finally:
        _conn.commit()
        _conn.close()


def _update_tracking_tb(data):
    _conn = get_database_connection()
    _cursor = _conn.cursor(dictionary=True)
    try:
        upt_stmt = """
            UPDATE processed_nft_address
            SET timestamp = %(timestamp)s,
                api_total = %(api_total)s,
                api_page = %(api_page)s,
                api_page_size = %(api_page_size)s,
        """

        if data['api_cursor']:
            upt_stmt += "api_cursor = %(api_cursor)s,"
        else:
            del data['api_cursor']

        upt_stmt += """         
                fully_processed = %(fully_processed)s,
                total_processed = %(total_processed)s
            WHERE nft_address = %(nft_address)s
        """

        logging.info(f"{pid} - Moralis - Updating processed_nft_address with {data}")
        _cursor.execute(upt_stmt, data)

    except Exception as e:
        now = datetime.now()
        logging.error(f"{pid} - {now} - Moralis - Error Updating processed_nft_address table with Data: {data}")
        logging.error(f"{pid} - {now} - Moralis - Exception - {str(e)}")
        raise
    finally:
        _conn.commit()
        _conn.close()


def process(arg_nft_address, arg_cursor=None):
    now = datetime.now()
    logging.info(f'{pid} - {now} - Moralis - PROCESSING NFT_ADDRESS: {arg_nft_address}')

    _update_processing_status(arg_nft_address, True)

    total_processed_items = 0
    while True:

        if os.path.exists('STOP_PROCESS'):
            _update_processing_status(arg_nft_address, False)
            break

        result = None
        try:
            result = _execute_request(arg_nft_address, arg_cursor)

            if result is not None:
                for item in result['result']:
                    json_data = json.dumps(item)
                    _insert_data_moralis_tb(arg_nft_address, item['token_address'], item['token_id'], json_data)
                    total_processed_items += 1

                arg_cursor = result['cursor']

            if result is None or not result['result'] or result['cursor'] is None:
                break

        finally:
            now = datetime.now()
            logging.info(f"{pid} - {now} - Moralis - Processed {total_processed_items} for NFT_ADDRESS: {arg_nft_address}")

            if result is not None:
                processed_nft_address_data = {'nft_address': arg_nft_address, 'timestamp': now,
                                              'api_total': result['total'],
                                              'total_processed': total_processed_items,
                                              'api_page': result['page'], 'api_page_size': result['page_size'],
                                              'api_cursor': arg_cursor, 'fully_processed': arg_cursor is None}
                _update_tracking_tb(processed_nft_address_data)

            else:
                time.sleep(5)  # By pass rate limit issue
    return True


def _update_processing_status(arg_nft_address, arg_status):
    _conn = get_database_connection()
    _cursor = _conn.cursor(dictionary=True)
    try:

        upt_stmt = """
            update processed_nft_address set in_progress = %(in_progress)s
            where nft_address = %(nft_address)s
        """

        _cursor.execute(upt_stmt, {'in_progress': arg_status, 'nft_address': arg_nft_address})

    except Exception as e:
        now = datetime.now()
        logging.error(
            f"{pid} - {now} - Moralis - Error Updating processed_nft_address status table. NFT_ADDRESS: {arg_nft_address} - STATUS: {arg_status}:")
        logging.error(f"{pid} - {now} - Moralis - Exception - {str(e)}")
        raise
    finally:
        _conn.commit()
        _conn.close()


def _fetch_api_keys():
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


def _create_directory(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def update_nft_address_status(nft_address):
    conn = get_database_connection()
    cursor = conn.cursor(dictionary=True)

    slct_stmt = """
    select
        nft_address as nft_address,
        total_token_ids as total_token_ids,
        total_processed as total_processed,
        timestamp as timestamp,
        api_total as api_total,
        api_page as api_page,
        api_page_size as api_page_size,
        api_cursor as api_cursor,
        fully_processed as fully_processed,
        in_progress as in_progress
    from processed_nft_address
    where nft_address = %(nft_address)s
    """

    # Execute the SELECT statement
    cursor.execute(slct_stmt, {'nft_address': nft_address})
    # Fetch the row as a tuple
    row = cursor.fetchone()

    # Convert row to a dictionary
    columns = [column[0] for column in cursor.description]
    result = {}
    for i, value in enumerate(row):
        result[columns[i]] = value


    api_page = _get_as_number(result['api_page'])
    api_page_size = _get_as_number(result['api_page_size'])
    api_total = _get_as_number(result['api_total'])

    fully_processed = api_total != 0 & (api_page * api_page_size) >= api_total

    upt_stmt = """
         update processed_nft_address set in_progress = %(in_progress)s, 
                                          fully_processed = %(fully_processed)s
         where nft_address = %(nft_address)s
     """

    cursor.execute(upt_stmt, {'in_progress': False, 'fully_processed': fully_processed, 'nft_address': nft_address})

    conn.commit()
    conn.close()

    return result

def _get_as_number(v):
    if isinstance(v, (int, float)):
        return v
    elif isinstance(v, str) and v.isdigit():
        return int(v)
    return 0

# MAIN CLASS METHODS

_create_directory("RESULT")
_create_directory("UNIQUE_ERROR")

# _create_db()
_fetch_api_keys()

conn = get_database_connection()
cursor = conn.cursor(dictionary=True)

slct_stmt = """
select
    nft_address as nft_address,
    total_token_ids as total_token_ids,
    total_processed as total_processed,
    timestamp as timestamp,
    api_total as api_total,
    api_page as api_page,
    api_page_size as api_page_size,
    api_cursor as api_cursor,
    fully_processed as fully_processed,
    in_progress as in_progress
from processed_nft_address
where fully_processed = false
and in_progress = false
and api_total > 0
order by api_total asc
"""
cursor.execute(slct_stmt)
rows = cursor.fetchall()
conn.commit()
conn.close()

filename = "current_processing"
if not os.path.exists(filename):
    with open(filename, "w"):
        pass

for row in rows:
    nft_address = row['nft_address']
    api_cursor = row['api_cursor']
    skip = False

    with open(filename, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)

        f.seek(0)
        for line in f:
            if nft_address in line:
                skip = True
                break

        if not skip:
            f.write(nft_address + " \n")

        fcntl.flock(f, fcntl.LOCK_UN)

        if skip:
            logging.info(f"{pid} - Moralis - Skipping address {nft_address}")
            continue

    try:
        process(nft_address, api_cursor)

    except Exception as e:
        now = datetime.now()
        logging.error(f"{pid} - {now} - Exception raised while processing {nft_address}: {str(e)}")
        _update_processing_status(nft_address, False)
    finally:

        update_nft_address_status(nft_address)

        if os.path.exists('STOP_PROCESS'):
            sys.exit(0)

logging.info(f"{pid} - Moralis - {datetime.now()} END THE PROCESS.")