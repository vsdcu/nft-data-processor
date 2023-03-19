import json
import logging
import os
from datetime import datetime

from mysql.connector.pooling import MySQLConnectionPool
import argparse

MILLION = 1_000_000

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

DB_PRIVATE_IP = '10.27.64.3'
DB_PUBLIC_IP = '35.193.69.26'

# TABLE WHERE THE CONTRACTS WILL BE RETRIEVED
SELECT_NFT_CONTRACTS_TABLE = 'nft_contracts'

# TABLE THAT THE PROCESSED VALUES WILL BE INSERTED
INSERT_NFT_ENTITY_TABLE = 'mrc_parsed_nft_contract_data'

# DIRECTORY WHERE THE UNPROCESSED DATA WILL BE STORED
ERROR_DIR_NAME = 'PARSING_PROCESSING_ERROR'

# SIZE OF THE BATCH
BATCH_SIZE = 10_000
pool = MySQLConnectionPool(
    pool_name='connection_pool',
    pool_size=5,
    pool_reset_session=True,
    database='moralis',
    host=DB_PUBLIC_IP,
    user='root',
    password='!!DCU_Cloud_SYS_2023!!'
)


def _create_directory(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def process(start, end):
    conn = pool.get_connection()
    cursor = conn.cursor(dictionary=True)

    SLCT_QRY = f""" select nft_address, json_data 
                    from {SELECT_NFT_CONTRACTS_TABLE} 
                    where JSON_VALID(json_data) = 1
                    order by nft_address, token_address, token_id 
                    limit {start}, {end} 
                """

    cursor.execute(SLCT_QRY)

    rows = cursor.fetchall()

    if cursor.rowcount == 0:
        return True

    list_data = []
    count = 0
    try:
        for row in rows:
            count = count + 1
            nft_address = row['nft_address']
            json_data = json.loads(row['json_data'])

            s = json_data['symbol']
            symbol = getAsValidString(json_data.get('symbol', ''), nft_address, start, end, count, json_data)
            name = getAsValidString(json_data.get('name', ''), nft_address, start, end, count, json_data)
            amount = getAsValidString(json_data.get('amount', ''), nft_address, start, end, count, json_data)
            block_number_minted = getAsValidString(json_data.get('block_number_minted', ''), nft_address, start, end,
                                                   count, json_data)
            contract_type = getAsValidString(json_data.get('contract_type', ''), nft_address, start, end, count,
                                             json_data)
            last_token_uri_sync = getAsValidString(json_data.get('last_token_uri_sync', ''), nft_address, start, end,
                                                   count, json_data)
            last_metadata_sync = getAsValidString(json_data.get('last_metadata_sync', ''), nft_address, start, end,
                                                  count, json_data)
            minter_address = getAsValidString(json_data.get('minter_address', ''), nft_address, start, end, count,
                                              json_data)

            data = {
                'nft_address': nft_address,
                'token_hash': json_data.get('token_hash', ''),
                'token_address': json_data.get('token_address', ''),
                'token_id': json_data.get('token_id', ''),
                'name': name,
                'symbol': symbol,
                'amount': amount,
                'block_number_minted': block_number_minted,
                'contract_type': contract_type,
                'last_token_uri_sync': last_token_uri_sync,
                'last_metadata_sync': last_metadata_sync,
                'minter_address': minter_address,
            }

            list_data.append(data)

        INSRT = f'''
                    INSERT IGNORE INTO {INSERT_NFT_ENTITY_TABLE} ( nft_address,
                                                                token_hash,
                                                                token_address,
                                                                token_id,
                                                                block_number_minted,
                                                                amount,
                                                                contract_type,
                                                                name,
                                                                symbol,
                                                                last_token_uri_sync,
                                                                last_metadata_sync,
                                                                minter_address ) 
                                                       VALUES ( %(nft_address)s,
                                                                %(token_hash)s,
                                                                %(token_address)s,
                                                                %(token_id)s,
                                                                %(block_number_minted)s,
                                                                %(amount)s,
                                                                %(contract_type)s,
                                                                %(name)s,
                                                                %(symbol)s,
                                                                %(last_token_uri_sync)s,
                                                                %(last_metadata_sync)s,
                                                                %(minter_address)s)
                '''
        cursor.executemany(INSRT, list_data)
    except Exception as e:
        if "UNIQUE constraint failed" in str(e) or "Duplicate entry" in str(e):
            logging.info(f"Duplicate value nft_address: {nft_address} Start: {start} - End: {end}")
            logging.error(str(e))
        else:
            logging.error(f'Something Went Wrong.Start: {start} End: {end} {str(e)}')
            f = open(f'{ERROR_DIR_NAME}/{count}_{start}_{end}_{datetime.now()}', 'a+')
            f.write(f'{json.dumps(rows)}')
            f.close()

    finally:
        conn.commit()
        conn.close()

    return False


def getAsValidString(arg, nft_address, start, end, count, json_data):
    try:
        if isinstance(arg, str):
            return arg.encode('ascii', 'ignore').decode('ascii')
    except Exception as e:
        logging.error(
            f' NFT_ADDRESS: {nft_address} START: {start} - END: {end} COUNT: {count} - Error Parsing {json_data} - {str(e)}')
        return ''


logging.info(f'Start PROCESSING {datetime.now()}')

# Set the intial million that will be processed numbers between 0 - 19 are allowed as the table has only 18.5 million
parser = argparse.ArgumentParser(description='MAX_PROCESSING_ROWS')
parser.add_argument('--start_row', type=int, default=0, help='Starting row of data')
args = parser.parse_args()
print(args.start_row)

_create_directory(ERROR_DIR_NAME)
start = 0 + (args.start_row * MILLION)
end = BATCH_SIZE
count = 0

while True:

    logging.info(f"Start: {start}")
    result = process(start, BATCH_SIZE)

    if result:
        logging.info(f'FINISHING PROCESS. PROCESSING: {end} ')
        logging.info(f'END PROCESSING {datetime.now()}')
        break

    start = start + BATCH_SIZE
    end = end + BATCH_SIZE

    if count >= MILLION:  # 1 MILLION
        logging.info(f'FINISHING PROCESS AS IT REACH: {count} ')
        logging.info(f'END PROCESSING {datetime.now()}')
        break

    count = count + BATCH_SIZE

#
# create table moralis.mrc_parsed_nft_contract_data
# (
#     id                  int auto_increment
#         primary key,
#     nft_address         varchar(255) null,
#     token_hash          varchar(255) null,
#     token_address       varchar(255) null,
#     token_id            varchar(255) null,
#     block_number_minted varchar(255) null,
#     amount              varchar(255) null,
#     contract_type       varchar(255) null,
#     name                varchar(255) null,
#     symbol              varchar(255) null,
#     token_uri           text         null,
#     last_token_uri_sync varchar(255) null,
#     last_metadata_sync  varchar(255) null,
#     minter_address      varchar(255) null,
#     constraint mrc_parsed_nft_contract_data_pk
#         unique (nft_address, token_address, token_id)
# );
#
