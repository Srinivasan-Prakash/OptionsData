# Add configuration file and custom log to the Project
import sys
sys.path.append('C:/Users/ADP/OneDrive/Projects/DEV')
import config as cfg
from reference_functions import log as log
from reference_functions import india_time, current_time_in_india

# >>> ************************************************************************************************************ <<< #

import os
import time
import sqlite3
import traceback
from datetime import datetime as dt
# from multiprocessing import Queue

def tick_backer_sqlite(conn, backup_queue):
    """Function to back up tick data into an SQLite database in batches"""
    try:
        log(1, 'SQLite tick backer is starting now.')

        # Step 1: Create database and tick_data table
        batch_data = []
        config = cfg

        # Generate database name based on the current date
        database_name = f"tick_db_{time.strftime('%Y%m%d')}.db"
        database = os.path.join(config.sqlite_database_path, database_name)
        os.makedirs(config.sqlite_database_path, exist_ok=True)

        # Create database table if it doesn't exist
        with sqlite3.connect(database) as connection:
            cursor = connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tick_data (
                    traded_time INTEGER NOT NULL,
                    token TEXT NOT NULL,
                    price DOUBLE PRECISION NOT NULL
                );
            """)
        connection.commit()
        # connection.close()

        # Function to insert a batch of tick data into SQLite
        def insert_data_to_sqlite(database, data_batch):
            """Inserts a batch of tick data into the SQLite database."""
            try:
                with sqlite3.connect(database) as connection:
                    cursor = connection.cursor()
                    cursor.executemany('INSERT INTO tick_data (traded_time, token, price) VALUES(?, ?, ?)', data_batch)
                connection.commit()
                # connection.close()

            except Exception as e:
                log(3, f"Error inserting data batch to database: {e}")
                raise

        # Step 2: Insert data into database

        while current_time_in_india() <= india_time(config.backer_end_time, 10):
            while not backup_queue.empty():
                tick = backup_queue.get()
                batch_data.append(tick)
                if len(batch_data) >= config.backer_tick_batch_size:
                    try:
                        insert_data_to_sqlite(database, batch_data)
                        max_datetime = dt.fromtimestamp(max(int(item[0]) for item in batch_data)/1000)
                        current_datetime = dt.now()
                        time_difference = (current_datetime - max_datetime).total_seconds()
                        log(1, 'Latest Data update (sec):', round(time_difference, 2))

                        batch_data = []

                    except Exception as e:
                        log(4, f"Unexpected error: {e}")
                        log(3, 'Warning: Error in SQLite DB data insertion. Retrying.')
                        log(5, traceback.format_exc())

                        insert_data_to_sqlite(database, batch_data)

                        batch_data = []

            # log(1, 'Backup Queue is Empty. Sleeping for 1 Second.')
            time.sleep(1)

        log(1, 'Current Time is outside of Market Time.')

        if batch_data:
            insert_data_to_sqlite(database, batch_data)
            log(2, "Full Data of Backup has been inserted into the database.")

        log(2, "SQLite tick backer has exited.")
        time.sleep(30)

    except Exception as e:
        log(4, "Error:")
        log(5, traceback.format_exc())
        if conn is not None:
            conn.send("error")

# >>> ************************************************************************************************************ <<< #
#
# if __name__ == "__main__":
#
#     backup_queue = Queue(maxsize=1000000)
#     if tick_backer_sqlite(None, backup_queue):
#         pass