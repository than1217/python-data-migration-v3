import os
import subprocess
import re
import logging
import json
import time
import sys
import getpass
import argparse
import csv
import threading
import mysql.connector
from tqdm import tqdm
from mysql.connector import Error
import config

# Set up logging to migration.log
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_file = os.path.join(base_dir, "csv_migration.log")
state_file = os.path.join(base_dir, "csv_migration_state.json")

MAX_RETRIES = 3
RETRY_DELAY = 5

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)
logger = logging.getLogger(__name__)

def format_time(seconds):
    if seconds > 60:
        return f"{seconds / 60:.2f} minutes"
    return f"{seconds:.2f} seconds"

def load_state():
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error("Error loading state file: %s", e)
    return {"processed_tables": [], "migrated_tables": [], "pattern": None, "from_list": None}

def save_state(state):
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        logger.error("Error saving state file: %s", e)

def get_lib_tables(pattern=None, from_list=None):
    logger.info("Connecting to MySQL Server %s...", config.DB_HOST)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = mysql.connector.connect(
                host=config.DB_HOST,
                database=config.DB_DATABASE,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                connect_timeout=10
            )
            if conn.is_connected():
                logger.info("Successfully connected to database '%s'.", config.DB_DATABASE)
                print(f"Successfully connected to database '{config.DB_DATABASE}'.")
                
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                
                all_tables = [row[0] for row in cursor.fetchall()]
                print(f"Found {len(all_tables)} tables in total. Checking for matches...")
                tables = []
                
                if from_list:
                    tables = [t for t in all_tables if t in from_list]
                else:
                    if not pattern:
                        pattern = r'^lib_.*'
                    regex = re.compile(pattern, re.IGNORECASE)
                    for table_name in all_tables:
                        if regex.search(table_name):
                            tables.append(table_name)
                        
                cursor.close()
                conn.close()
                return tables
                
        except Error as e:
            logger.error("Attempt %s/%s - Error connecting to MySQL: %s", attempt, MAX_RETRIES, e)
            print(f"Attempt {attempt}/{MAX_RETRIES} - Error connecting to MySQL: {e}")
            if e.errno == 2002 and config.DB_HOST.lower() == 'localhost':
                logger.warning("Socket connection failed. Falling back to TCP/IP via 127.0.0.1")
                print("Socket connection failed. Falling back to TCP/IP via 127.0.0.1")
                config.DB_HOST = '127.0.0.1'
                
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return []

def run_mysqldump_schema(table, output_file):
    """Executes mysqldump to export ONLY the schema for the provided table."""
    command = [config.MYSQLDUMP_PATH]
    if config.DB_HOST.lower() not in ['localhost', '127.0.0.1']:
        command.extend([f"-h{config.DB_HOST}"])

    command.extend([
        f"-u{config.DB_USER}", f"--password={config.DB_PASSWORD}",
        "--no-data", "--skip-lock-tables", "--skip-add-locks", "--set-gtid-purged=OFF",
        config.DB_DATABASE, table
    ])
    
    logger.info("Dumping schema for table '%s' to %s...", table, output_file)
    import tempfile
    
    t_start = time.time()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with tempfile.TemporaryFile(mode='w+', encoding='utf-8', errors='ignore') as err_file:
                process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=err_file, text=False)
                
                with open(output_file, "wb") as f, tqdm(desc=f"Dumping Schema {table}", unit="B", unit_scale=True, leave=False) as pbar:
                    if process.stdout is not None:
                        while True:
                            chunk = process.stdout.read(1024 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                            pbar.update(len(chunk))
                        
                process.wait()
                if process.returncode == 0:
                    logger.info("Successfully dumped schema for '%s' in %s.", table, format_time(time.time() - t_start))
                    time.sleep(0.5)
                    return True
                else:
                    err_file.seek(0)
                    logger.error("Attempt %s/%s failed schema dump '%s': %s", attempt, MAX_RETRIES, table, err_file.read())
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error("Attempt %s/%s error during schema dump for '%s': %s", attempt, MAX_RETRIES, table, e)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return False

def process_schema_file(input_file, output_file, table_name, suffix):
    """Reads the raw SQL schema dump, updates engine and collation, saves it, and deletes raw file."""
    logger.info("Processing schema file for '%s'...", table_name)
    try:
        new_table_name = table_name if table_name.endswith(suffix) else f"{table_name}{suffix}"
        table_name_pattern = re.compile(rf"`{table_name}`")

        def update_table_options(match):
            options = match.group(1)
            options = re.sub(r'ENGINE\s*=\s*\w+', 'ENGINE=InnoDB', options, flags=re.IGNORECASE)
            
            if re.search(r'(?:DEFAULT\s+)?CHARSET\s*=\s*\w+', options, flags=re.IGNORECASE):
                options = re.sub(r'(?:DEFAULT\s+)?CHARSET\s*=\s*\w+', 'DEFAULT CHARSET=utf8mb4', options, flags=re.IGNORECASE)
            else:
                options += ' DEFAULT CHARSET=utf8mb4'
                
            if re.search(r'COLLATE\s*=\s*\w+', options, flags=re.IGNORECASE):
                # Ensure utf8mb4_0900_ai_ci or standard utf8mb4 collate per user request
                options = re.sub(r'COLLATE\s*=\s*\w+', 'COLLATE=utf8mb4_0900_ai_ci', options, flags=re.IGNORECASE)
            else:
                options += ' COLLATE=utf8mb4_0900_ai_ci'
                
            if re.search(r'ROW_FORMAT\s*=\s*\w+', options, flags=re.IGNORECASE):
                options = re.sub(r'ROW_FORMAT\s*=\s*\w+', 'ROW_FORMAT=DYNAMIC', options, flags=re.IGNORECASE)
            else:
                options += ' ROW_FORMAT=DYNAMIC'
                
            return f") {options};"

        table_options_pattern = re.compile(r'\)\s*(ENGINE\s*=[^;]+);', flags=re.IGNORECASE)
        charset_pattern = re.compile(r'CHARACTER SET\s+\w+', flags=re.IGNORECASE)
        collate_pattern = re.compile(r'COLLATE\s+\w+', flags=re.IGNORECASE)
        
        def inject_charset_collate(match):
            rest = match.group(2)
            rest = re.sub(r'\s*CHARACTER SET\s+\w+', '', rest, flags=re.IGNORECASE)
            rest = re.sub(r'\s*COLLATE\s+\w+', '', rest, flags=re.IGNORECASE)
            return match.group(1) + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci" + rest

        column_pattern = re.compile(r'(`[^`]+`\s+(?:varchar\([^)]+\)|char\([^)]+\)|enum\([^)]+\)|set\([^)]+\)|text|longtext|mediumtext|tinytext))([^,\n]*)', flags=re.IGNORECASE)
        
        with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                if f"`{table_name}`" in line:
                    line = table_name_pattern.sub(f"`{new_table_name}`", line)
                
                if line.lstrip().startswith(')') and 'ENGINE=' in line.upper():
                    line = table_options_pattern.sub(update_table_options, line)
                elif line.lstrip().startswith('`'):
                    line = charset_pattern.sub('CHARACTER SET utf8mb4', line)
                    line = collate_pattern.sub('COLLATE utf8mb4_0900_ai_ci', line)
                    line = column_pattern.sub(inject_charset_collate, line)

                f_out.write(line)
        
        # Delete the raw schema file as requested
        if os.path.exists(input_file):
            os.remove(input_file)
            logger.info("Deleted raw schema file '%s'.", input_file)

        logger.info("Successfully processed schema for table '%s'.", table_name)
        return True
            
    except Exception as e:
        logger.error("Error processing schema dump file for '%s': %s", table_name, e)
        return False

def export_data_to_csv(table_name, csv_file_path):
    """Exports data directly to a CSV file using python streaming with utf8mb4 charset.
       Uses Primary Key chunking (pagination) to prevent memory spikes on large tables."""
    logger.info("Exporting data from '%s' to CSV...", table_name)
    try:
        conn = mysql.connector.connect(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_DATABASE,
            charset='utf8mb4',
            connect_timeout=10
        )
        if conn.is_connected():
            cursor = conn.cursor()
            
            # Check for a primary key (integer) to use for chunking
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}` WHERE `Key` = 'PRI'")
            pk_cols = cursor.fetchall()
            pk_col = pk_cols[0] if pk_cols else None
            
            # Get total rows for progress bar
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            total_rows = cursor.fetchone()[0]
            cursor.fetchall()

            with open(csv_file_path, 'w', encoding='utf-8', newline='') as f, tqdm(total=total_rows, desc=f"Exporting CSV {table_name}", unit="row", leave=False) as pbar:
                writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                
                # Write headers
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 0")
                if cursor.description:
                    writer.writerow([i[0] for i in cursor.description])
                cursor.fetchall()

                if total_rows == 0:
                    cursor.close()
                    conn.close()
                    logger.info("Table '%s' is empty. Exported headers only.", table_name)
                    return True
                
                # If we have an integer primary key, use chunking (Strategy 2)
                chunk_size = 200000
                if pk_col and ('int' in pk_col[1].lower()):
                    pk_name = pk_col[0]
                    cursor.execute(f"SELECT MIN(`{pk_name}`), MAX(`{pk_name}`) FROM `{table_name}`")
                    min_id, max_id = cursor.fetchone()
                    cursor.fetchall()
                    
                    if min_id is not None and max_id is not None:
                        current_id = min_id
                        while current_id <= max_id:
                            next_id = current_id + chunk_size
                            cursor.execute(f"SELECT * FROM `{table_name}` WHERE `{pk_name}` >= %s AND `{pk_name}` < %s", (current_id, next_id))
                            rows = cursor.fetchall()
                            if rows:
                                writer.writerows(rows)
                                pbar.update(len(rows))
                            current_id = next_id
                
                # Fallback if no suitable primary key: standard fetchmany with unbuffered cursor
                else:
                    unbuffered_cursor = conn.cursor(buffered=False)
                    unbuffered_cursor.execute(f"SELECT * FROM `{table_name}`")
                    while True:
                        rows = unbuffered_cursor.fetchmany(200000)
                        if not rows:
                            break
                        writer.writerows(rows)
                        pbar.update(len(rows))
                    unbuffered_cursor.close()
                    
            cursor.close()
            conn.close()
            logger.info("Successfully exported data from '%s' to CSV.", table_name)
            return True
    except Error as e:
        if e.errno == 2002 and config.DB_HOST.lower() == 'localhost':
            logger.warning("Socket connection failed during CSV export. Falling back to TCP/IP via 127.0.0.1 for '%s'", table_name)
            config.DB_HOST = '127.0.0.1'
            return export_data_to_csv(table_name, csv_file_path)
            
        logger.error("Error exporting to CSV for table '%s': %s", table_name, e)
    return False

def create_destination_db():
    logger.info("Connecting to destination server %s...", config.DEST_DB_HOST)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = mysql.connector.connect(
                host=config.DEST_DB_HOST,
                user=config.DEST_DB_USER,
                password=config.DEST_DB_PASSWORD,
                connect_timeout=10
            )
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.DEST_DB_DATABASE} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
                logger.info("Database '%s' ensured on destination.", config.DEST_DB_DATABASE)
                try:
                    cursor.execute("SET GLOBAL local_infile=1")
                    logger.info("Successfully enabled local_infile on destination server.")
                except Error as e:
                    logger.warning("Could not set GLOBAL local_infile=1. It might already be enabled or lack permissions: %s", e)
                cursor.close()
                conn.close()
                return True
        except Error as e:
            logger.error("Attempt %s/%s - Error creating destination DB: %s", attempt, MAX_RETRIES, e)
            if e.errno == 2002 and config.DEST_DB_HOST.lower() == 'localhost':
                logger.warning("Socket connection failed for destination DB creation. Falling back to TCP/IP via 127.0.0.1")
                config.DEST_DB_HOST = '127.0.0.1'
                
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return False

def load_sql_schema(filepath):
    """Executes the processed schema SQL file on the destination database to create the table."""
    filename = os.path.basename(filepath)
    command = [config.MYSQL_PATH]
    if config.DEST_DB_HOST.lower() not in ['localhost', '127.0.0.1']:
        command.extend([f"-h{config.DEST_DB_HOST}"])
    
    command.extend([
        "--connect_timeout=10", "--max_allowed_packet=1G",
        f"-u{config.DEST_DB_USER}", f"--password={config.DEST_DB_PASSWORD}", config.DEST_DB_DATABASE
    ])

    logger.info("Executing schema file %s into %s...", filename, config.DEST_DB_DATABASE)
    import tempfile
    for attempt in range(1, MAX_RETRIES + 1):
        with tempfile.TemporaryFile(mode='w+', encoding='utf-8', errors='ignore') as err_file:
            try:
                process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err_file, text=False)
                with open(filepath, 'rb') as f:
                    process.stdin.write(f.read())
                process.stdin.close()
                process.wait()
                
                if process.returncode == 0:
                    logger.info("Successfully executed schema %s.", filename)
                    return True
                else:
                    err_file.seek(0)
                    logger.error("Attempt %s/%s failed to execute schema %s: %s", attempt, MAX_RETRIES, filename, err_file.read())
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
            except Exception as e:
                logger.error("Attempt %s/%s unexpected error executing schema %s: %s", attempt, MAX_RETRIES, filename, e)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
    return False

def load_csv_to_dest(target_table_name, csv_file_path):
    """Uses LOAD DATA LOCAL INFILE to bulk load the CSV data into the destination table."""
    command = [
        config.MYSQL_PATH,
        "--local-infile=1",
        "--connect_timeout=10",
        "--max_allowed_packet=1G",
    ]

    if config.DEST_DB_HOST.lower() not in ['localhost', '127.0.0.1']:
        command.extend([f"-h{config.DEST_DB_HOST}"])
    
    command.extend([
        f"-u{config.DEST_DB_USER}", f"--password={config.DEST_DB_PASSWORD}", config.DEST_DB_DATABASE
    ])

    logger.info("Loading CSV into destination table '%s'...", target_table_name)
    mysql_csv_path = csv_file_path.replace('\\', '/')
    
    # Matching Python csv module's default export formatting exactly
    sql_command = f"""
    SET SESSION sql_mode='';
    SET SESSION FOREIGN_KEY_CHECKS=0;
    SET SESSION UNIQUE_CHECKS=0;
    LOAD DATA LOCAL INFILE '{mysql_csv_path}'
    INTO TABLE `{target_table_name}`
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    ESCAPED BY '"'
    LINES TERMINATED BY '\\r\\n'
    IGNORE 1 LINES;
    """
    
    # Try to get total rows for the progress bar
    try:
        with open(csv_file_path, 'rb') as f:
            total_rows = max(0, sum(1 for _ in f) - 1)
    except:
        total_rows = 0

    def progress_monitor(stop_event, pbar):
        try:
            conn = mysql.connector.connect(
                host=config.DEST_DB_HOST, user=config.DEST_DB_USER,
                password=config.DEST_DB_PASSWORD, database=config.DEST_DB_DATABASE, connect_timeout=5
            )
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
                last_count = 0
                while not stop_event.is_set():
                    time.sleep(0.5)
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM `{target_table_name}`")
                        current_count = int(cursor.fetchone()[0])
                        added = current_count - last_count
                        if added > 0:
                            pbar.update(added)
                            last_count = current_count
                    except:
                        pass
                cursor.close()
                conn.close()
        except:
            pass

    import tempfile
    t_start = time.time()
    for attempt in range(1, MAX_RETRIES + 1):
        with tempfile.TemporaryFile(mode='w+', encoding='utf-8', errors='ignore') as err_file:
            try:
                process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err_file, text=True)
                
                stop_event = threading.Event()
                pbar = tqdm(total=total_rows, desc=f"Loading CSV {target_table_name}", unit="row", leave=False)
                monitor_thread = threading.Thread(target=progress_monitor, args=(stop_event, pbar))
                monitor_thread.daemon = True
                monitor_thread.start()
                
                process.communicate(sql_command)
                
                stop_event.set()
                monitor_thread.join(timeout=1)
                pbar.n = total_rows
                pbar.refresh()
                pbar.close()
                
                if process.returncode == 0:
                    logger.info("Successfully loaded CSV into '%s' in %s.", target_table_name, format_time(time.time() - t_start))
                    return True
                else:
                    err_file.seek(0)
                    logger.error("Attempt %s/%s failed to load CSV for '%s': %s", attempt, MAX_RETRIES, target_table_name, err_file.read())
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
            except Exception as e:
                logger.error("Attempt %s/%s unexpected error loading CSV for '%s': %s", attempt, MAX_RETRIES, target_table_name, e)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
    
    logger.info("Falling back to Python mysql.connector for table '%s'...", target_table_name)
    try:
        conn = mysql.connector.connect(
            host=config.DEST_DB_HOST,
            user=config.DEST_DB_USER,
            password=config.DEST_DB_PASSWORD,
            database=config.DEST_DB_DATABASE,
            charset='utf8mb4',
            connect_timeout=10
        )
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SET SESSION sql_mode=''")
            cursor.execute("SET SESSION FOREIGN_KEY_CHECKS=0")
            cursor.execute("SET SESSION UNIQUE_CHECKS=0")
            
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers:
                    logger.warning("CSV is empty for table '%s'.", target_table_name)
                    return True
                
                placeholders = ', '.join(['%s'] * len(headers))
                col_names = ', '.join([f"`{h}`" for h in headers])
                query = f"INSERT INTO `{target_table_name}` ({col_names}) VALUES ({placeholders})"
                
                batch_size = 5000
                batch = []
                
                with tqdm(total=total_rows, desc=f"Loading CSV {target_table_name} (Fallback)", unit="row", leave=False) as pbar:
                    for row in reader:
                        # Convert empty strings to None if you want NULLs, but empty strings are fine if columns allow
                        batch.append(tuple(row))
                        if len(batch) >= batch_size:
                            cursor.executemany(query, batch)
                            conn.commit()
                            batch = []
                            pbar.update(batch_size)
                            
                    if batch:
                        cursor.executemany(query, batch)
                        conn.commit()
                        pbar.update(len(batch))
                    
            cursor.close()
            conn.close()
            logger.info("Successfully loaded CSV via fallback into '%s'.", target_table_name)
            return True
    except Error as e:
        logger.error("Fallback failed to load CSV for '%s': %s", target_table_name, e)
        
    return False

def run_migration(tables, state, suffix):
    folder_name = suffix.strip('_') if suffix else 'v2'

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(project_dir, "output", "raw", folder_name)
    processed_dir = os.path.join(project_dir, "output", "processed", folder_name)
    csv_dir = os.path.join(project_dir, "output", "csv", folder_name)
    
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(csv_dir, exist_ok=True)

    if not tables:
        print("No tables found to migrate.")
        return
        
    print(f"\nMigrating {len(tables)} tables via CSV approach (logging details to csv_migration.log)...")
    start_time = time.time()
    
    if not create_destination_db():
        logger.error("Failed to ensure destination database. Aborting.")
        print("Failed to ensure destination DB. See logs.")
        return

    successful_migrations = sum(1 for t in tables if t in state.get("migrated_tables", []))

    pbar_main = tqdm(tables, desc="Migrating Tables (CSV)", unit="table", leave=True)
    for table in pbar_main:
        pbar_main.set_postfix(table=table)
        pbar_main.refresh()
        
        target_table_name = table if table.endswith(suffix) else f"{table}{suffix}"
        raw_schema = os.path.join(raw_dir, f"{table}_raw_schema{suffix}.sql")
        processed_schema = os.path.join(processed_dir, f"{target_table_name}_schema.sql")
        csv_file = os.path.join(csv_dir, f"{target_table_name}.csv")
        
        if table in state.get("migrated_tables", []):
            logger.info("Skipping '%s', already migrated in previous session.", table)
            continue
            
        t_start = time.time()
        
        # 1. Dump raw schema
        if run_mysqldump_schema(table, raw_schema):
            # 2. Process schema (and delete raw)
            if process_schema_file(raw_schema, processed_schema, table, suffix):
                # 3. Export data as CSV
                if export_data_to_csv(table, csv_file):
                    # 4. Load Schema to destination DB
                    if load_sql_schema(processed_schema):
                        # 5. Load CSV to destination DB
                        if load_csv_to_dest(target_table_name, csv_file):
                            logger.info("Total migration for table '%s' completed in %s.", table, format_time(time.time() - t_start))
                            successful_migrations += 1
                            if "migrated_tables" not in state:
                                state["migrated_tables"] = []
                            state["migrated_tables"].append(table)
                            save_state(state)
                        else:
                            logger.warning("Failed to load CSV to destination for table '%s'.", table)
                    else:
                        logger.warning("Failed to execute schema in destination for table '%s'.", table)
                else:
                    logger.warning("Failed to export CSV for table '%s'.", table)
            else:
                logger.warning("Failed to process schema for table '%s'.", table)
        else:
            logger.warning("Failed to dump schema for table '%s'.", table)
            
    summary = f"Migration complete: {successful_migrations}/{len(tables)} tables migrated."
    print(f"\n{summary}")
    logger.info(summary)
    
    if successful_migrations == len(tables):
        logger.info("All tables migrated successfully. Resetting migration state.")
        save_state({"processed_tables": [], "migrated_tables": [], "pattern": None, "from_list": None})

    elapsed_time = time.time() - start_time
    m, s = divmod(elapsed_time, 60)
    h, m = divmod(m, 60)
    print(f"Total migration time: {int(h):02d}:{int(m):02d}:{int(s):02d}")
    logger.info("--- MIGRATION FINISHED ---")

# The menu system mimics the original tool for ease of use
def choose_database():
    print(f"\nConnecting to Server {config.DB_HOST} to fetch databases...")
    try:
        conn = mysql.connector.connect(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            connect_timeout=10
        )
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall() if row[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')]
            cursor.close()
            conn.close()
            
            if not databases:
                print("No user databases found on this server.")
                return False
                
            print("\nAvailable Databases:")
            for i, db in enumerate(databases, 1):
                print(f"{i}. {db}")
                
            while True:
                choice = input("\nSelect a database number: ").strip()
                if choice.isdigit() and 1 <= int(choice) <= len(databases):
                    config.DB_DATABASE = databases[int(choice) - 1]
                    print(f"Selected Database: {config.DB_DATABASE}")
                    return True
                else:
                    print("Invalid choice.")
    except Error as e:
        if e.errno == 2002 and config.DB_HOST.lower() == 'localhost':
            print("\nSocket connection failed. Falling back to TCP/IP via 127.0.0.1...")
            logger.warning("Socket connection failed. Falling back to TCP/IP via 127.0.0.1 for initial DB fetch.")
            config.DB_HOST = '127.0.0.1'
            return choose_database()
            
        print(f"\nAuthentication or connection failed: {e}")
        return False

def choose_destination_database():
    print(f"\nConnecting to Destination Server {config.DEST_DB_HOST} to fetch databases...")
    try:
        conn = mysql.connector.connect(
            host=config.DEST_DB_HOST,
            user=config.DEST_DB_USER,
            password=config.DEST_DB_PASSWORD,
            connect_timeout=10
        )
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall() if row[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')]
            cursor.close()
            conn.close()
                
            print("\nAvailable Destination Databases:")
            for i, db in enumerate(databases, 1):
                print(f"{i}. {db}")
            print("0. Create a new database")
                
            while True:
                choice = input("\nSelect a destination database number or 0 to create new: ").strip()
                if choice == '0':
                    new_db = input("Enter new destination database name: ").strip()
                    if new_db:
                        config.DEST_DB_DATABASE = new_db
                        print(f"Selected Destination Database: {config.DEST_DB_DATABASE}")
                        return True
                elif choice.isdigit() and 1 <= int(choice) <= len(databases):
                    config.DEST_DB_DATABASE = databases[int(choice) - 1]
                    print(f"Selected Destination Database: {config.DEST_DB_DATABASE}")
                    return True
                else:
                    print("Invalid choice.")
    except Error as e:
        if e.errno == 2002 and config.DEST_DB_HOST.lower() == 'localhost':
            print("\nSocket connection failed for destination. Falling back to TCP/IP via 127.0.0.1...")
            logger.warning("Socket connection failed for destination. Falling back to TCP/IP via 127.0.0.1.")
            config.DEST_DB_HOST = '127.0.0.1'
            return choose_destination_database()
            
        print(f"\nAuthentication failed for destination server: {e}")
        manual_db = input("Enter destination database name manually: ").strip()
        if manual_db:
            config.DEST_DB_DATABASE = manual_db
            return True
        return False

def migration_menu(suffix):
    while True:
        print("\n=============================================")
        print("    CSV MIGRATION OPTIONS")
        print(f"    Source DB: {config.DB_DATABASE}")
        if hasattr(config, 'DEST_DB_DATABASE'):
            print(f"    Dest DB:   {config.DEST_DB_DATABASE}")
        print(f"    Table Suffix: {suffix}")
        print("=============================================")
        print("1. Specify table name pattern (Regular Expression)")
        print("2. Specify exact table names (Comma-separated list)")
        print("3. Exit / Back to main menu")
        print("=============================================")
        
        choice = input("Select an option (1-3): ").strip()
        
        if choice == '1':
            pattern = input("Enter regular expression pattern (e.g., '^lib_.*'): ").strip()
            if not pattern: continue
            state = {"migrated_tables": []}
            save_state(state)
            tables = get_lib_tables(pattern=pattern)
            run_migration(tables, state, suffix)
            
        elif choice == '2':
            tables_input = input("Enter table names separated by commas: ").strip()
            if not tables_input: continue
            table_list = [t.strip() for t in tables_input.split(',')]
            state = {"migrated_tables": []}
            save_state(state)
            tables = get_lib_tables(from_list=table_list)
            run_migration(tables, state, suffix)
            
        elif choice == '3':
            break

def main():
    parser = argparse.ArgumentParser(description="Python CSV Data Migration Utility")
    parser.add_argument('--headless', type=str, help='Path to JSON configuration file for headless execution')
    args = parser.parse_args()

    if args.headless:
        logger.info("Running in headless mode using config file: %s", args.headless)
        if not os.path.exists(args.headless):
            print(f"Error: Config file '{args.headless}' not found.")
            logger.error("Headless config file not found: %s", args.headless)
            sys.exit(1)
            
        try:
            with open(args.headless, 'r') as f:
                headless_config = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON file: {e}")
            logger.error("Headless JSON parsing error: %s", e)
            sys.exit(1)

        # Override config variables if provided in JSON
        if 'db_host' in headless_config: config.DB_HOST = headless_config['db_host']
        if 'db_database' in headless_config: config.DB_DATABASE = headless_config['db_database']
        if 'db_user' in headless_config: config.DB_USER = headless_config['db_user']
        if 'dest_db_host' in headless_config: config.DEST_DB_HOST = headless_config['dest_db_host']
        if 'dest_db_database' in headless_config: config.DEST_DB_DATABASE = headless_config['dest_db_database']
        if 'dest_db_user' in headless_config: config.DEST_DB_USER = headless_config['dest_db_user']

        # Passwords are read from .env via config.py (DB_PASSWORD, DEST_DB_PASSWORD)
        
        suffix = headless_config.get('suffix', '_v2')
        pattern = headless_config.get('pattern', None)
        from_list = headless_config.get('tables', None)

        print(f"\n--- HEADLESS MIGRATION ---")
        print(f"Source: {config.DB_HOST} -> {config.DB_DATABASE}")
        print(f"Target: {config.DEST_DB_HOST} -> {config.DEST_DB_DATABASE}")
        print(f"Suffix: {suffix}")
        
        state = {"migrated_tables": []}
        save_state(state)
        
        if pattern:
            tables = get_lib_tables(pattern=pattern)
        elif from_list:
            tables = get_lib_tables(from_list=from_list)
        else:
            tables = get_lib_tables(pattern=r'^lib_.*')
            
        run_migration(tables, state, suffix)
        sys.exit(0)

    while True:
        print("\n=============================================")
        print("    PYTHON CSV DATA MIGRATION UTILITY        ")
        print("=============================================")
        print("1. PPISv 2 (Suffix: _v2)")
        print("2. PPISv 3 (Suffix: _v3)")
        print("3. Custom Migration (Input custom suffix)")
        print("4. Exit")
        print("=============================================")
        
        main_choice = input("Select an option (1-4): ").strip()
        if main_choice == '4':
            sys.exit(0)
            
        if main_choice not in ['1', '2', '3']:
            print("Invalid choice.")
            continue
            
        if main_choice == '1':
            suffix = '_v2'
            servers = {
                "1": ("10.255.9.100", "PPIS v2 Production"),
                "2": ("10.255.9.104", "PPIS v2 CMS SWDI Production"),
                "3": ("10.255.9.105", "PPIS v2 Staging"),
                "4": ("localhost", "Localhost")
            }
        elif main_choice == '2':
            suffix = '_v3'
            servers = {
                "1": ("10.10.10.96", "PPIS v3 Staging"),
                "2": ("10.255.9.111", "PPIS v3 Slave"),
                "3": ("localhost", "Localhost")
            }
        else:
            suffix = input("Enter custom suffix (e.g., '_v4'): ").strip()
            servers = {
                "1": ("localhost", "Localhost")
            }
            
        print(f"\n--- Select Server for PPIS{suffix.replace('_','')} ---")
        for key, (ip, name) in servers.items():
            print(f"{key}. {name} ({ip})")
        print("0. Enter Custom Server IP")

        srv_choice = input("\nSelect server: ").strip()
        if srv_choice == '0':
            config.DB_HOST = input("Enter custom IP: ").strip()
        elif srv_choice in servers:
            config.DB_HOST = servers[srv_choice][0]
        else:
            print("Invalid selection.")
            continue
        
        print("\n--- Source Server Authentication ---")
        config.DB_USER = input(f"Username [{config.DB_USER}]: ").strip() or config.DB_USER
        config.DB_PASSWORD = getpass.getpass("Password: ")
        
        print("\n--- Destination Server Connection ---")
        config.DEST_DB_HOST = input(f"Enter Destination MySQL Host IP [{config.DEST_DB_HOST}]: ").strip() or config.DEST_DB_HOST
        config.DEST_DB_USER = input(f"Username [{config.DEST_DB_USER}]: ").strip() or config.DEST_DB_USER
        config.DEST_DB_PASSWORD = getpass.getpass("Password: ")

        if choose_database() and choose_destination_database():
            migration_menu(suffix)

if __name__ == "__main__":
    main()