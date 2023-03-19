import fcntl
import json
import sys
import time

from moralis import evm_api

from Utils import *

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#  TO STOP THE PROCESS CREATE A FILE 'STOP_PROCESS' AND THE APPLICATION WILL STOP GRACEFULLY

pid = os.getpid()

API_PAGE_SIZE = 100

# Create a mechanism to identify the blocked API and stop using the blocked one.
# When all get blocked stop the application


ROUND_ROBIN_INDEX = 0

_DIR_RESULT = "RESULT_TRANSFERS"
_DIR_UNIQUE_ERROR = "UNIQUE_ERROR_TRANSFERS"


def process(arg_nft_address, arg_cursor=None):
    now = datetime.now()
    logging.info(f'{pid} - {now} - NftTransfers.  - PROCESSING TRANSFERS FOR NFT_ADDRESS: {arg_nft_address}')

    _update_processing_status_transfers(arg_nft_address, True)

    total_processed_items = 0
    while True:

        if os.path.exists('STOP_PROCESS'):
            _update_processing_status_transfers(arg_nft_address, False)
            break

        result = None
        try:

            result = _execute_request(arg_nft_address, arg_cursor)

            if result is not None:
                transfers_list = []
                for item in result['result']:
                    json_data = json.dumps(item)
                    item['nft_address'] = arg_nft_address
                    item['json_data'] = json_data
                    transfers_list.append(item)
                    total_processed_items += 1

                if transfers_list:

                    for data in transfers_list:
                        data['block_timestamp'] = datetime.strptime(data['block_timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')

                    _insert_nft_transfers(arg_nft_address, transfers_list)
                    total_processed_items += len(transfers_list)
                    arg_cursor = result['cursor']

            if result is None or not result['result'] or result['cursor'] is None:
                break

        finally:
            now = datetime.now()
            logging.info(
                f"{pid} - {now} - NftTransfers - Processed {total_processed_items} for NFT_ADDRESS: {arg_nft_address}")

            if result is not None:
                processed_transfers_nft_address_data = {'nft_address': arg_nft_address, 'timestamp': now,
                                                        'api_total': result['total'],
                                                        'total_processed': total_processed_items,
                                                        'api_page': result['page'],
                                                        'api_page_size': result['page_size'],
                                                        'api_cursor': arg_cursor, 'fully_processed': arg_cursor is None}
                _update_tracking_transfers_tb(processed_transfers_nft_address_data)

            else:
                time.sleep(5)  # By pass rate limit issue
    return True


def _build_params(arg_address, arg_cursor: None):
    params = {
        "address": arg_address,
        "chain": "eth",
        "format": "decimal",
        "limit": API_PAGE_SIZE,
        "disable_total": False
    }

    if arg_cursor is not None:
        params['cursor'] = arg_cursor

    return params


def _execute_request(arg_address, arg_cursor: None):
    request_params = _build_params(arg_address, arg_cursor)
    api_key = get_api_key()

    while not api_key:
        now = datetime.now()
        logging.info(f"{pid} - {now} - All API_KEY is blocked waiting 10 minutes to check again")
        time.sleep(60 * 10)

    try:
        result = evm_api.nft.get_nft_contract_transfers(
            api_key=api_key,
            params=request_params,
        )

        # store result on disk

        if WRITE_IN_DISK_ENABLED:
            timestamp = datetime.timestamp(datetime.now())
            f = open(f"{_DIR_RESULT}/{arg_address}_{timestamp}.json", "w+")
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
            f'{pid} - {now} - NftTransfers.  - Error to connect to Moralis. Will wait for {sleep_scs} seconds Exception. {str(e)}')

        time.sleep(sleep_scs)

        fetch_api_keys()  # Try to fetch new API_KEYS in case new were inserted in the DB.
        return _execute_request(arg_address, arg_cursor)


def _insert_nft_transfers(arg_nft_address, data):
    _conn = get_database_connection()
    _cursor = _conn.cursor(dictionary=True)
    try:

        insert_query = """
            INSERT INTO nft_transfers (
                nft_address,
                block_number, 
                block_timestamp, 
                block_hash, 
                transaction_hash, 
                transaction_index,
                log_index, 
                value, 
                contract_type, 
                transaction_type, 
                token_address, 
                token_id,
                from_address, 
                to_address, 
                amount, 
                verified, 
                operator, 
                json_data
            ) VALUES (
                %(nft_address)s,
                %(block_number)s, 
                %(block_timestamp)s, 
                %(block_hash)s, 
                %(transaction_hash)s, 
                %(transaction_index)s,
                %(log_index)s, 
                %(value)s, 
                %(contract_type)s, 
                %(transaction_type)s, 
                %(token_address)s, 
                %(token_id)s,
                %(from_address)s, 
                %(to_address)s, 
                %(amount)s, 
                %(verified)s, 
                %(operator)s, 
                %(json_data)s
            )
        """

        _cursor.executemany(insert_query, data)

    except Exception as e:
        now = datetime.now()

        if "UNIQUE constraint failed" in str(e) or "Duplicate entry" in str(e):
            logging.error(f"{pid} - {now} - NftTransfers. UNIQUE CONSTRAINT FOUND {arg_nft_address}")
        else:
            logging.error(
                f"{pid} - {now} - NftTransfers - Error Inserting Data: {arg_nft_address} ")
            logging.error(f"{pid} - {now} - NftTransfers - Exception - {str(e)}")
            raise
    finally:
        _conn.commit()
        _conn.close()


def _update_tracking_transfers_tb(data):
    _conn = get_database_connection()
    _cursor = _conn.cursor(dictionary=True)
    try:
        upt_stmt = """
            UPDATE processed_transfers_nft_address
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

        logging.info(f"{pid} - NftTransfers - Updating processed_transfers_nft_address with {data}")
        _cursor.execute(upt_stmt, data)

    except Exception as e:
        now = datetime.now()
        logging.error(
            f"{pid} - {now} - NftTransfers - Error Updating processed_transfers_nft_address table with Data: {data}")
        logging.error(f"{pid} - {now} - NftTransfers - Exception - {str(e)}")
        raise
    finally:
        _conn.commit()
        _conn.close()


def _update_processing_status_transfers(arg_nft_address, arg_status):
    _conn = get_database_connection()
    _cursor = _conn.cursor(dictionary=True)
    try:

        upt_stmt = """
            update processed_transfers_nft_address set in_progress = %(in_progress)s
            where nft_address = %(nft_address)s
        """

        _cursor.execute(upt_stmt, {'in_progress': arg_status, 'nft_address': arg_nft_address})

    except Exception as e:
        now = datetime.now()
        logging.error(
            f'{pid} - {now} - NftTransfers -- Error Updating processed_transfers_nft_address status table. NFT_ADDRESS: {arg_nft_address} - STATUS: {arg_status}:')
        logging.error(f'{pid} - {now} - NftTransfers --Exception - {str(e)}')
        raise
    finally:
        _conn.commit()
        _conn.close()


def update_nft_address_status_transfers(nft_address):
    conn = get_database_connection()
    cursor = conn.cursor(dictionary=True)

    slct_stmt = """
    select
        nft_address as nft_address,
        total_processed as total_processed,
        timestamp as timestamp,
        api_total as api_total,
        api_page as api_page,
        api_page_size as api_page_size,
        api_cursor as api_cursor,
        fully_processed as fully_processed,
        in_progress as in_progress
    from processed_transfers_nft_address
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

    fully_processed = is_fully_processed(result)

    upt_stmt = """
         update processed_transfers_nft_address set in_progress = %(in_progress)s, 
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

create_directory(_DIR_RESULT)
create_directory(_DIR_UNIQUE_ERROR)

# _create_db()
fetch_api_keys()

process("0x60E4d786628Fea6478F785A6d7e704777c86a7c6", None)

conn = get_database_connection()
cursor = conn.cursor(dictionary=True)

slct_stmt = """
select
    nft_address as nft_address,
    total_processed as total_processed,
    timestamp as timestamp,
    api_total as api_total,
    api_page as api_page,
    api_page_size as api_page_size,
    api_cursor as api_cursor,
    fully_processed as fully_processed,
    in_progress as in_progress
from processed_transfers_nft_address
where fully_processed = false
and in_progress = false
order by api_total desc
"""
cursor.execute(slct_stmt)
rows = cursor.fetchall()
conn.commit()
conn.close()

filename = "CURRENT_NFT_TRANSFERS_PROCESSING"
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
            logging.info(f"{pid} - NftTransfers - Skipping address {nft_address}")
            continue

    try:
        process(nft_address, api_cursor)

    except Exception as e:
        now = datetime.now()
        logging.error(f'{pid} - {now} - NftTransfers -- Exception raised while processing {nft_address}: {str(e)}')
        _update_processing_status_transfers(nft_address, False)
    finally:

        update_nft_address_status_transfers(nft_address)

        if os.path.exists('STOP_PROCESS'):
            sys.exit(0)

logging.info(f"{pid} - {datetime.now()} - NftTransfers - END THE PROCESS.")
