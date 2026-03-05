#!/usr/bin/env python3
"""
Tick Data Backup - Stream Parquet to Local Machine (HTTP)
==========================================================

Streams Parquet directly from ClickHouse to your Windows PC.
No DataFrame - ClickHouse generates Parquet, we save the bytes.

Fill in CLICKHOUSE_PASSWORD and run:
    python tick_backup_test.py
"""

import os
import requests
from datetime import datetime

# =============================================================================
# CONFIGURATION - FILL IN YOUR PASSWORD
# =============================================================================

CLICKHOUSE_HOST = "192.168.1.6"       # Your ClickHouse server
CLICKHOUSE_HTTP_PORT = 5678           # HTTP port
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "Max@2609"  # <-- ENTER YOUR PASSWORD

DATABASE_NAME = "test"                # Database
TABLE_NAME = "commodities_master_mcx_bfo_bse"           # Table
DATE_COLUMN = "exchange_time"         # DateTime column

OUTPUT_DIR = "./test_backup"          # Where to save on YOUR machine

# =============================================================================
# TEST: Just 1 minute of data from December 29, 2025
# =============================================================================

TEST_DATE = "2025-12-29"
TEST_START_TIME = "09:00:00"
TEST_END_TIME = "23:01:00"

# =============================================================================
# SCRIPT
# =============================================================================

def main():
    print("=" * 60)
    print("TICK DATA BACKUP - HTTP Stream Test (1 minute)")
    print("=" * 60)
    print()
    
    start_dt = f"{TEST_DATE} {TEST_START_TIME}"
    end_dt = f"{TEST_DATE} {TEST_END_TIME}"
    
    print(f"Server:     {CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}")
    print(f"Table:      {DATABASE_NAME}.{TABLE_NAME}")
    print(f"Time range: {start_dt} to {end_dt}")
    print()
    
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/"
    
    # Step 1: Count rows
    count_query = f"""
    SELECT count(*) FROM {DATABASE_NAME}.{TABLE_NAME}
    WHERE {DATE_COLUMN} >= '{start_dt}'
    AND {DATE_COLUMN} < '{end_dt}'
    """
    
    print("Counting rows...")
    response = requests.post(
        url,
        data=count_query,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    )
    
    if response.status_code != 200:
        print(f"❌ Error: {response.text}")
        return
    
    row_count = int(response.text.strip())
    print(f"Rows in 1-minute window: {row_count:,}")
    
    if row_count == 0:
        print("No data found! Try different time range.")
        return
    
    # Step 2: Stream as Parquet
    print()
    print("Streaming Parquet from ClickHouse...")
    
    select_query = f"""
    SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}
    WHERE {DATE_COLUMN} >= '{start_dt}'
    AND {DATE_COLUMN} < '{end_dt}'
    ORDER BY {DATE_COLUMN}
    FORMAT Parquet
    """
    
    response = requests.post(
        url,
        data=select_query,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        stream=True
    )
    
    if response.status_code != 200:
        print(f"❌ Error: {response.text}")
        return
    
    # Save to local file
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(
        OUTPUT_DIR, 
        f"commodities_master_mcx_bfo_bse_{TEST_DATE}_{TEST_START_TIME.replace(':', '')}.parquet"
    )
    
    print(f"Saving to: {output_file}")
    
    total_bytes = 0
    with open(output_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            total_bytes += len(chunk)
    
    file_size_mb = total_bytes / 1024 / 1024
    print(f"✅ Saved! Size: {file_size_mb:.2f} MB")
    print()
    
    # Step 3: Verify
    print("Verifying parquet file...")
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(output_file)
        print(f"✅ Valid! Rows: {pf.metadata.num_rows:,}, Columns: {pf.metadata.num_columns}")
    except Exception as e:
        print(f"⚠️ Verify error: {e}")
    
    print()
    print("=" * 60)
    print("TEST COMPLETE!")
    print(f"File: {output_file}")
    print(f"Rows: {row_count:,}")
    print(f"Size: {file_size_mb:.2f} MB")
    print("=" * 60)


if __name__ == '__main__':
    main()
