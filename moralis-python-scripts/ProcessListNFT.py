import datetime
import sqlite3

SQLITE_DB = "~/DCU_Cloud/moralis_data.sqlite"

conn = sqlite3.connect(SQLITE_DB,
                       check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS processed_nft_address (
    nft_address TEXT,
    total_token_ids INTEGER,
    total_processed INTEGER,
    timestamp TIMESTAMP,
    api_total INTEGER,
    api_page INTEGER,
    api_page_size INTEGER, 
    api_cursor TEXT, 
    fully_processed BOOLEAN default false
)
""")
# "total": 135337,
# "page": 0,
# "page_size": 100,
# "cursor": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ3aGVyZSI6eyJ0b2tlbl9hZGRyZXNzIjoiMHgxZGZlN2NhMDllOTlkMTA4MzViZjczMDQ0YTIzYjczZmMyMDYyM2RmIn0sInRva2VuX2FkZHJlc3MiOiIweDFkZmU3Y2EwOWU5OWQxMDgzNWJmNzMwNDRhMjNiNzNmYzIwNjIzZGYiLCJsaW1pdCI6MTAwLCJvZmZzZXQiOjAsIm9yZGVyIjpbXSwicGFnZSI6MSwia2V5IjoiZmZkMWVlN2M0Mjg3YjZlYzQ0ODhmNGIxMTdmMzY0YzUiLCJ0b3RhbCI6MTM1MzM3LCJpYXQiOjE2NzYxMTc4NjZ9.roxk6gEPSdQC1ms8qCzjGg8LFuXH5nCrJ0R9ZW8uquM",,
#

with open('DISTINCT_NFT_ADDRESSES', 'r') as file:
    conn = sqlite3.connect(SQLITE_DB, check_same_thread=False)
    cursor = conn.cursor()

    for line in file:
        values = line.strip().split(';')
        print(f"Values: {values[0]} - {values[1]}")
        now = datetime.datetime.now()
        data = (values[1], values[0], now)
        cursor.execute(
            "INSERT INTO processed_nft_address (nft_address, total_token_ids, timestamp) "
            "VALUES ( ?, ?, ? )", data)

    conn.commit()
    conn.close()

