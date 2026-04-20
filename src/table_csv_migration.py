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
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
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

def get_db_connection(host, user, password, database=None, charset=None):
    """Helper to create a MySQL connection, automatically finding the socket file on Linux if localhost."""
    kwargs = {
        'host': host,
        'user': user,
        'password': password,
        'connect_timeout': 10
    }
    if database:
        kwargs['database'] = database
    if charset:
        kwargs['charset'] = charset
        
    if host.lower() == 'localhost' and os.name != 'nt':
        common_sockets = ['/var/run/mysqld/mysqld.sock', '/var/lib/mysql/mysql.sock', '/tmp/mysql.sock']
        for sock in common_sockets:
            if os.path.exists(sock):
                kwargs['unix_socket'] = sock
                break
                
    conn = mysql.connector.connect(**kwargs)
    if 'unix_socket' in kwargs:
        logger.info("Database connection established using Unix Socket: %s", kwargs['unix_socket'])
    else:
        logger.info("Database connection established using TCP/IP (Host: %s)", host)
    return conn

def load_state():
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error("Error loading state file: %s", e)
    return {"processed_tables": [], "migrated_tables": [], "pattern": None, "from_list": None, "csv_load_progress": {}, "final_row_counts": {}}

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
            conn = get_db_connection(
                host=config.DB_HOST,
                database=config.DB_DATABASE,
                user=config.DB_USER,
                password=config.DB_PASSWORD
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
        unique_key_pattern = re.compile(r'^\s*UNIQUE KEY.*?(,\n|\n)', flags=re.IGNORECASE)
        
        with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
            f_out.write("SET FOREIGN_KEY_CHECKS=0;\nSET UNIQUE_CHECKS=0;\n")
            lines = f_in.readlines()
            
            # First pass: clean up trailing commas on the line before a removed UNIQUE KEY
            # if the UNIQUE KEY was the last item before the closing parenthesis.
            cleaned_lines = []
            for i, line in enumerate(lines):
                if unique_key_pattern.match(line):
                    # We are dropping this line.
                    # Check if it was the last definition line.
                    if i + 1 < len(lines) and lines[i+1].lstrip().startswith(')'):
                        # It was the last line, so we need to remove the comma from the previous line if it has one
                        if cleaned_lines and cleaned_lines[-1].strip().endswith(','):
                            cleaned_lines[-1] = cleaned_lines[-1].rstrip()[:-1] + '\n'
                    continue
                cleaned_lines.append(line)

            for line in cleaned_lines:
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
        conn = get_db_connection(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_DATABASE,
            charset='utf8mb4'
        )
        if conn.is_connected():
            cursor = conn.cursor()
            
            # Check for a primary key (integer) to use for chunking
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}` WHERE `Key` = 'PRI'")
            pk_cols = cursor.fetchall()
            pk_col = pk_cols[0] if pk_cols else None
            
            # Get total rows for progress bar (use approximate count for speed)
            cursor.execute(f"SELECT TABLE_ROWS FROM information_schema.tables WHERE table_schema = '{config.DB_DATABASE}' AND table_name = '{table_name}'")
            row = cursor.fetchone()
            total_rows = int(row[0]) if row and row[0] is not None else 0
            cursor.fetchall()

            # Fallback to COUNT(*) if the table is empty according to stats, to be sure
            if total_rows == 0:
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
                    return True, 0
                
                # Use server-side cursor (unbuffered) for streaming directly
                unbuffered_cursor = conn.cursor(buffered=False)
                unbuffered_cursor.execute(f"SELECT * FROM `{table_name}`")
                
                batch_size = 10000
                exact_row_count = 0
                while True:
                    rows = unbuffered_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    writer.writerows(rows)
                    exact_row_count += len(rows)
                    pbar.update(len(rows))
                
                unbuffered_cursor.close()
                    
            cursor.close()
            conn.close()
            logger.info("Successfully exported data from '%s' to CSV.", table_name)
            return True, exact_row_count
    except Error as e:
        if e.errno == 2002 and config.DB_HOST.lower() == 'localhost':
            logger.warning("Socket connection failed during CSV export. Falling back to TCP/IP via 127.0.0.1 for '%s'", table_name)
            config.DB_HOST = '127.0.0.1'
            return export_data_to_csv(table_name, csv_file_path)
            
        logger.error("Error exporting to CSV for table '%s': %s", table_name, e)
    return False, 0

def create_destination_db():
    logger.info("Connecting to destination server %s...", config.DEST_DB_HOST)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = get_db_connection(
                host=config.DEST_DB_HOST,
                user=config.DEST_DB_USER,
                password=config.DEST_DB_PASSWORD
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
        "--connect-timeout=10", "--max-allowed-packet=1G",
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

def _execute_load_data_infile(target_table_name, csv_file_path, use_local=False):
    command = [
        config.MYSQL_PATH,
        "--connect_timeout=10",
        "--max_allowed_packet=1G",
    ]
    
    if use_local:
        command.insert(1, "--local-infile=1")

    if config.DEST_DB_HOST.lower() not in ['localhost', '127.0.0.1']:
        command.extend([f"-h{config.DEST_DB_HOST}"])
    
    command.extend([
        f"-u{config.DEST_DB_USER}", f"--password={config.DEST_DB_PASSWORD}", config.DEST_DB_DATABASE
    ])

    mysql_csv_path = csv_file_path.replace('\\', '/')
    local_str = "LOCAL " if use_local else ""
    
    logger.info("Executing LOAD DATA %sINFILE for '%s'", local_str, target_table_name)
    
    sql_command = f"""
    SET SESSION sql_mode='';
    SET SESSION FOREIGN_KEY_CHECKS=0;
    SET SESSION UNIQUE_CHECKS=0;
    LOAD DATA {local_str}INFILE '{mysql_csv_path}'
    IGNORE INTO TABLE `{target_table_name}`
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    ESCAPED BY '"'
    LINES TERMINATED BY '\\r\\n'
    IGNORE 1 LINES;
    """
    
    import tempfile
    retries = MAX_RETRIES if use_local else 1
    for attempt in range(1, retries + 1):
        with tempfile.TemporaryFile(mode='w+', encoding='utf-8', errors='ignore') as err_file:
            try:
                process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err_file, text=True)
                process.communicate(sql_command)
                
                if process.returncode == 0:
                    return True
                else:
                    err_file.seek(0)
                    error_msg = err_file.read()
                    logger.error("Attempt %s/%s failed to load CSV for '%s' (local=%s): %s", attempt, retries, target_table_name, use_local, error_msg)
                    if attempt < retries:
                        time.sleep(RETRY_DELAY)
            except Exception as e:
                logger.error("Attempt %s/%s unexpected error loading CSV for '%s' (local=%s): %s", attempt, retries, target_table_name, use_local, e)
                if attempt < retries:
                    time.sleep(RETRY_DELAY)
    return False

def _execute_fallback_insert(target_table_name, headers, rows):
    try:
        conn = get_db_connection(
            host=config.DEST_DB_HOST,
            user=config.DEST_DB_USER,
            password=config.DEST_DB_PASSWORD,
            database=config.DEST_DB_DATABASE,
            charset='utf8mb4'
        )
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SET SESSION sql_mode=''")
            cursor.execute("SET SESSION FOREIGN_KEY_CHECKS=0")
            cursor.execute("SET SESSION UNIQUE_CHECKS=0")
            
            placeholders = ', '.join(['%s'] * len(headers))
            col_names = ', '.join([f"`{h}`" for h in headers])
            query = f"INSERT IGNORE INTO `{target_table_name}` ({col_names}) VALUES ({placeholders})"
            
            batch_size = 5000
            batch = []
            for row in rows:
                batch.append(tuple(row))
                if len(batch) >= batch_size:
                    cursor.executemany(query, batch)
                    conn.commit()
                    batch = []
                    
            if batch:
                cursor.executemany(query, batch)
                conn.commit()
                    
            cursor.close()
            conn.close()
            return True
    except Error as e:
        logger.error("Fallback failed to load CSV chunk for '%s': %s", target_table_name, e)
    return False

def load_csv_to_dest(target_table_name, csv_file_path, state, use_multithreading=False):
    """Uses LOAD DATA INFILE for the entire CSV, falling back to LOAD DATA LOCAL INFILE in chunks."""
    file_size = os.path.getsize(csv_file_path)
    state.setdefault("csv_load_progress", {})
    
    is_remote_dest = config.DEST_DB_HOST.lower() not in ['localhost', '127.0.0.1']
    
    import tempfile
    
    progress = state["csv_load_progress"].get(target_table_name, {})
    last_byte_pos = progress.get("last_byte_pos", 0)
    rows_loaded = progress.get("rows_loaded", 0)
    completed_chunks = progress.get("completed_chunks", [])

    t_start = time.time()

    if not use_multithreading and completed_chunks:
        logger.warning("Multi-threaded progress found but running single-threaded. Truncating table '%s' to avoid duplicates...", target_table_name)
        try:
            conn = get_db_connection(
                host=config.DEST_DB_HOST, user=config.DEST_DB_USER,
                password=config.DEST_DB_PASSWORD, database=config.DEST_DB_DATABASE
            )
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE `{target_table_name}`")
                conn.commit()
                cursor.close()
                conn.close()
            last_byte_pos = 0
            rows_loaded = 0
            completed_chunks = []
            state["csv_load_progress"][target_table_name] = {"last_byte_pos": 0, "rows_loaded": 0, "completed_chunks": []}
            save_state(state)
        except Exception as e:
            logger.error("Failed to truncate table '%s': %s", target_table_name, e)
            return False

    if use_multithreading:
        logger.info("Loading CSV into destination table '%s' using MULTI-THREADED chunks (Remote dest: %s)...", target_table_name, is_remote_dest)
        chunk_size = 250000
        temp_dir = tempfile.gettempdir()
        
        completed_chunks_set = set(completed_chunks)
        
        def process_multithread_chunk(t_csv, h_list, b_processed, c_id):
            if is_remote_dest:
                success = _execute_load_data_infile(target_table_name, t_csv, use_local=True)
            else:
                success = _execute_load_data_infile(target_table_name, t_csv, use_local=False)
                if not success:
                    logger.info("Standard LOAD DATA INFILE failed for '%s'. Falling back to LOCAL INFILE...", target_table_name)
                    success = _execute_load_data_infile(target_table_name, t_csv, use_local=True)
            
            rows_count = 0
            if success:
                with open(t_csv, 'r', encoding='utf-8') as tf:
                    rows_count = sum(1 for _ in tf) - 1
            else:
                logger.info("Falling back to Python mysql.connector for chunk of '%s'...", target_table_name)
                with open(t_csv, 'r', encoding='utf-8') as tf:
                    tf.readline()
                    parsed = list(csv.reader(tf))
                success = _execute_fallback_insert(target_table_name, h_list, parsed)
                rows_count = len(parsed)
                
            if os.path.exists(t_csv):
                os.remove(t_csv)
            return success, rows_count, b_processed, c_id

        futures = []
        executor = ThreadPoolExecutor(max_workers=4)
        
        with open(csv_file_path, 'r', encoding='utf-8', newline='') as f:
            header_line = f.readline()
            if not header_line:
                logger.warning("CSV is empty for table '%s'.", target_table_name)
                state["csv_load_progress"][target_table_name] = {"last_byte_pos": 0, "rows_loaded": 0, "completed_chunks": []}
                save_state(state)
                return True
            
            headers = next(csv.reader([header_line]))
            
            chunk_id = 0
            bytes_skipped = 0
            
            # Bridge for Single-Threaded -> Multi-Threaded resume
            if last_byte_pos > 0 and not completed_chunks_set:
                f.seek(last_byte_pos)
                bytes_skipped = last_byte_pos
            
            while True:
                chunk_rows = []
                bytes_processed = 0
                
                is_completed = chunk_id in completed_chunks_set
                
                for _ in range(chunk_size):
                    line = f.readline()
                    if not line: break
                    if not is_completed:
                        chunk_rows.append(line)
                    bytes_processed += len(line.encode('utf-8'))
                    
                if bytes_processed == 0:
                    break
                    
                if is_completed:
                    bytes_skipped += bytes_processed
                else:
                    temp_csv = os.path.join(temp_dir, f"{target_table_name}_chunk_{uuid.uuid4().hex}.tmp")
                    with open(temp_csv, 'w', encoding='utf-8', newline='') as tf:
                        tf.write(header_line)
                        tf.writelines(chunk_rows)
                    
                    futures.append(executor.submit(process_multithread_chunk, temp_csv, headers, bytes_processed, chunk_id))
                    
                chunk_id += 1
                
        total_loaded = rows_loaded
        state_lock = threading.Lock()
        
        with tqdm(total=file_size, initial=bytes_skipped, desc=f"MT Loading {target_table_name}", unit="B", unit_scale=True, leave=False) as pbar:
            for future in as_completed(futures):
                success, loaded_count, b_processed, c_id = future.result()
                if not success:
                    logger.error("A multithreaded chunk failed to load for table '%s'.", target_table_name)
                    executor.shutdown(wait=False, cancel_futures=True)
                    return False
                
                with state_lock:
                    total_loaded += loaded_count
                    completed_chunks_set.add(c_id)
                    state["csv_load_progress"][target_table_name] = {
                        "last_byte_pos": bytes_skipped, # Maintain position point for ST bridge
                        "rows_loaded": total_loaded,
                        "completed_chunks": list(completed_chunks_set)
                    }
                    save_state(state)
                    
                pbar.update(b_processed)
            
        executor.shutdown(wait=True)
        
        state["csv_load_progress"][target_table_name] = {
            "last_byte_pos": file_size,
            "rows_loaded": total_loaded,
            "completed_chunks": []
        }
        save_state(state)
        logger.info("Successfully loaded CSV into '%s' via MT in %s.", target_table_name, format_time(time.time() - t_start))
        return True

    if last_byte_pos == 0:
        if is_remote_dest:
            logger.info("Destination is remote. Attempting full LOAD DATA LOCAL INFILE for '%s'...", target_table_name)
            success = _execute_load_data_infile(target_table_name, csv_file_path, use_local=True)
        else:
            logger.info("Attempting full LOAD DATA INFILE for '%s'...", target_table_name)
            success = _execute_load_data_infile(target_table_name, csv_file_path, use_local=False)
            
        if success:
            try:
                conn = get_db_connection(
                    host=config.DEST_DB_HOST,
                    user=config.DEST_DB_USER,
                    password=config.DEST_DB_PASSWORD,
                    database=config.DEST_DB_DATABASE
                )
                if conn.is_connected():
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM `{target_table_name}`")
                    rows_loaded = cursor.fetchone()[0]
                    cursor.close()
                    conn.close()
            except Exception as e:
                logger.warning("Could not fetch loaded row count for '%s': %s", target_table_name, e)
                
            state["csv_load_progress"][target_table_name] = {
                "last_byte_pos": file_size,
                "rows_loaded": rows_loaded
            }
            save_state(state)
            logger.info("Successfully loaded full CSV into '%s' in %s.", target_table_name, format_time(time.time() - t_start))
            return True
            
        logger.info("Full file LOAD DATA attempt failed. Falling back to chunked loading...")

    logger.info("Loading CSV into destination table '%s' in chunks (Remote dest: %s)...", target_table_name, is_remote_dest)
    chunk_size = 250000
    temp_dir = tempfile.gettempdir()
    temp_csv = os.path.join(temp_dir, f"{target_table_name}_chunk.tmp")
    
    with open(csv_file_path, 'r', encoding='utf-8', newline='') as f:
        header_line = f.readline()
        if not header_line:
            logger.warning("CSV is empty for table '%s'.", target_table_name)
            state["csv_load_progress"][target_table_name] = {"last_byte_pos": 0, "rows_loaded": 0}
            return True
            
        headers = next(csv.reader([header_line]))
        
        if last_byte_pos == 0:
            last_byte_pos = f.tell()
        else:
            f.seek(last_byte_pos)
            
        with tqdm(total=file_size, initial=last_byte_pos, desc=f"Loading CSV {target_table_name}", unit="B", unit_scale=True, leave=False) as pbar:
            while True:
                chunk_rows = []
                for _ in range(chunk_size):
                    line = f.readline()
                    if not line:
                        break
                    chunk_rows.append(line)
                    
                if not chunk_rows:
                    break
                    
                with open(temp_csv, 'w', encoding='utf-8', newline='') as tf:
                    tf.write(header_line)
                    tf.writelines(chunk_rows)
                    
                current_byte_pos = f.tell()
                bytes_processed = current_byte_pos - last_byte_pos
                
                if is_remote_dest:
                    success = _execute_load_data_infile(target_table_name, temp_csv, use_local=True)
                else:
                    # Try standard LOAD DATA INFILE for the chunk first
                    success = _execute_load_data_infile(target_table_name, temp_csv, use_local=False)
                    
                    if not success:
                        logger.info("Standard LOAD DATA INFILE failed for '%s'. Falling back to LOCAL INFILE...", target_table_name)
                        success = _execute_load_data_infile(target_table_name, temp_csv, use_local=True)
                
                if not success:
                    logger.info("Falling back to Python mysql.connector for chunk of '%s'...", target_table_name)
                    parsed_rows = list(csv.reader(chunk_rows))
                    success = _execute_fallback_insert(target_table_name, headers, parsed_rows)
                    
                if not success:
                    logger.error("Failed to load chunk for table '%s'.", target_table_name)
                    if os.path.exists(temp_csv):
                        os.remove(temp_csv)
                    return False
                    
                rows_loaded += len(chunk_rows)
                last_byte_pos = current_byte_pos
                
                state["csv_load_progress"][target_table_name] = {
                    "last_byte_pos": last_byte_pos,
                    "rows_loaded": rows_loaded
                }
                save_state(state)
                
                pbar.update(bytes_processed)

    if os.path.exists(temp_csv):
        os.remove(temp_csv)
        
    logger.info("Successfully loaded CSV into '%s' in %s.", target_table_name, format_time(time.time() - t_start))
    return True

def run_migration(tables, state, suffix, use_multithreading=False, headless_skip_extract=None):
    folder_name = suffix.strip('_') if suffix else 'v2'

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(project_dir, "output", "raw", folder_name)
    processed_dir = os.path.join(project_dir, "output", "processed", folder_name)
    csv_dir = os.path.join(project_dir, "output", "csv", folder_name)
    
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(csv_dir, exist_ok=True)

    summary_csv_path = os.path.join(project_dir, "output", f"migration_summary_{folder_name}.csv")
    summary_data = []

    def get_ddl_content(filepath):
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            except Exception:
                pass
        return ""

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
    global_skip_extract = headless_skip_extract

    pbar_main = tqdm(tables, desc="Migrating Tables (CSV)", unit="table", leave=True)
    for table in pbar_main:
        pbar_main.set_postfix(table=table)
        pbar_main.refresh()
        
        target_table_name = table if table.endswith(suffix) else f"{table}{suffix}"
        raw_schema = os.path.join(raw_dir, f"{table}_raw_schema{suffix}.sql")
        processed_schema = os.path.join(processed_dir, f"{target_table_name}_schema.sql")
        csv_file = os.path.join(csv_dir, f"{target_table_name}.csv")
        
        # Retrieve row counts from state if available
        total_rows_migrated = state.get("final_row_counts", {}).get(target_table_name, 0)

        if table in state.get("migrated_tables", []):
            logger.info("Skipping '%s', already migrated in previous session.", table)
            ddl_content = get_ddl_content(processed_schema)
            summary_data.append([target_table_name, "Skipped", ddl_content, total_rows_migrated, "Success (Already migrated)"])
            continue
            
        t_start = time.time()
        
        csv_load_progress = state.get("csv_load_progress", {}).get(target_table_name, {})
        is_resuming = "last_byte_pos" in csv_load_progress
        remarks = "Success"
        
        if is_resuming:
            logger.info("Resuming table '%s'...", table)
            use_existing = False
            if os.path.exists(csv_file):
                logger.info("Using existing CSV file for resuming '%s'.", table)
                use_existing = True
            
            export_success = False
            if use_existing:
                export_success = True
            else:
                export_success, exact_count = export_data_to_csv(table, csv_file)
            
            if export_success:
                # Skip schema drop/create, go straight to appending CSV data
                if load_csv_to_dest(target_table_name, csv_file, state, use_multithreading):
                    elapsed = format_time(time.time() - t_start)
                    logger.info("Total migration for table '%s' completed in %s.", table, elapsed)
                    successful_migrations += 1
                    if "migrated_tables" not in state:
                        state["migrated_tables"] = []
                    state["migrated_tables"].append(table)
                    
                    final_row_count = state.get("csv_load_progress", {}).get(target_table_name, {}).get("rows_loaded", 0)
                    state.setdefault("final_row_counts", {})[target_table_name] = final_row_count
                    
                    if target_table_name in state.get("csv_load_progress", {}):
                        del state["csv_load_progress"][target_table_name]
                    save_state(state)
                else:
                    remarks = "Failed: Load CSV to destination (Resume)"
                    logger.warning("Failed to load CSV to destination for table '%s'.", table)
            else:
                remarks = "Failed: Export data to CSV (Resume)"
                logger.warning("Failed to export CSV for table '%s'.", table)
        else:
            # 1. Dump raw schema
            if run_mysqldump_schema(table, raw_schema):
                # 2. Process schema (and delete raw)
                if process_schema_file(raw_schema, processed_schema, table, suffix):
                    # 3. Export data as CSV or use existing
                    use_existing = False
                    if os.path.exists(csv_file):
                        if global_skip_extract is not None:
                            choice = global_skip_extract
                        else:
                            choice = input(f"\nExisting CSV file found for '{table}'. Skip extraction and use existing? (y/n/all-y/all-n): ").strip().lower()
                            if choice == 'all-y':
                                global_skip_extract = 'y'
                                choice = 'y'
                            elif choice == 'all-n':
                                global_skip_extract = 'n'
                                choice = 'n'
                                
                        if choice == 'y':
                            logger.info("User chose to use existing CSV for '%s'. Skipping extraction.", table)
                            use_existing = True
                        else:
                            logger.info("User chose to re-extract CSV for '%s'.", table)
                        
                    export_success = False
                    if use_existing:
                        export_success = True
                    else:
                        export_success, exact_count = export_data_to_csv(table, csv_file)
                        
                    if export_success:
                        # 4. Load Schema to destination DB
                        if load_sql_schema(processed_schema):
                            # 5. Load CSV to destination DB
                            if load_csv_to_dest(target_table_name, csv_file, state, use_multithreading):
                                elapsed = format_time(time.time() - t_start)
                                logger.info("Total migration for table '%s' completed in %s.", table, elapsed)
                                successful_migrations += 1
                                if "migrated_tables" not in state:
                                    state["migrated_tables"] = []
                                state["migrated_tables"].append(table)
                                
                                final_row_count = state.get("csv_load_progress", {}).get(target_table_name, {}).get("rows_loaded", 0)
                                state.setdefault("final_row_counts", {})[target_table_name] = final_row_count
                                
                                if target_table_name in state.get("csv_load_progress", {}):
                                    del state["csv_load_progress"][target_table_name]
                                save_state(state)
                            else:
                                remarks = "Failed: Load CSV to destination"
                                logger.warning("Failed to load CSV to destination for table '%s'.", table)
                        else:
                            remarks = "Failed: Execute schema in destination"
                            logger.warning("Failed to execute schema in destination for table '%s'.", table)
                    else:
                        remarks = "Failed: Export data to CSV"
                        logger.warning("Failed to export CSV for table '%s'.", table)
                else:
                    remarks = "Failed: Process schema file"
                    logger.warning("Failed to process schema for table '%s'.", table)
            else:
                remarks = "Failed: Dump schema"
                logger.warning("Failed to dump schema for table '%s'.", table)
            
        elapsed = format_time(time.time() - t_start)
        ddl_content = get_ddl_content(processed_schema)
        
        # Use recorded row count instead of querying information schema
        if "Success" in remarks:
            total_rows_migrated = state.get("final_row_counts", {}).get(target_table_name, 0)
            
        summary_data.append([target_table_name, elapsed, ddl_content, total_rows_migrated, remarks])
            
    summary = f"Migration complete: {successful_migrations}/{len(tables)} tables migrated."
    print(f"\n{summary}")
    logger.info(summary)
    
    # Write summary CSV
    try:
        with open(summary_csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['table_name', 'total_migration', 'ddl_sql', 'total_rows', 'remarks'])
            writer.writerows(summary_data)
        logger.info("Summary CSV successfully generated at '%s'", summary_csv_path)
    except Exception as e:
        logger.error("Failed to write summary CSV: %s", e)
    
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
        conn = get_db_connection(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall() if row[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')] # type: ignore
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
        conn = get_db_connection(
            host=config.DEST_DB_HOST,
            user=config.DEST_DB_USER,
            password=config.DEST_DB_PASSWORD
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
            state = load_state()
            if state.get("migrated_tables") or state.get("csv_load_progress"):
                resume = input("\nExisting migration progress found. Resume? (y/n): ").strip().lower()
                if resume != 'y':
                    state = {"migrated_tables": [], "csv_load_progress": {}, "final_row_counts": {}}
                    save_state(state)
            use_mt = input("Use multi-threaded chunking for faster data loading? (y/n): ").strip().lower() == 'y'
            tables = get_lib_tables(pattern=pattern)
            run_migration(tables, state, suffix, use_multithreading=use_mt)
            
        elif choice == '2':
            tables_input = input("Enter table names separated by commas: ").strip()
            if not tables_input: continue
            table_list = [t.strip() for t in tables_input.split(',')]
            state = load_state()
            if state.get("migrated_tables") or state.get("csv_load_progress"):
                resume = input("\nExisting migration progress found. Resume? (y/n): ").strip().lower()
                if resume != 'y':
                    state = {"migrated_tables": [], "csv_load_progress": {}, "final_row_counts": {}}
                    save_state(state)
            use_mt = input("Use multi-threaded chunking for faster data loading? (y/n): ").strip().lower() == 'y'
            tables = get_lib_tables(from_list=table_list)
            run_migration(tables, state, suffix, use_multithreading=use_mt)
            
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
        
        state = load_state()
        if headless_config.get('force_restart', False):
            state = {"migrated_tables": [], "csv_load_progress": {}, "final_row_counts": {}}
            save_state(state)
        elif state.get("migrated_tables") or state.get("csv_load_progress"):
            logger.info("Existing migration state found. Resuming headless migration.")
            print("Existing migration progress found. Resuming automatically...")
        
        if pattern:
            tables = get_lib_tables(pattern=pattern)
        elif from_list:
            tables = get_lib_tables(from_list=from_list)
        else:
            tables = get_lib_tables(pattern=r'^lib_.*')
            
        use_mt = headless_config.get('multithreaded', False)
        
        # Determine skip_extract preference from config
        skip_extract_cfg = headless_config.get('skip_extract', None)
        headless_skip = None
        if skip_extract_cfg is True:
            headless_skip = 'y'
        elif skip_extract_cfg is False:
            headless_skip = 'n'
            
        run_migration(tables, state, suffix, use_multithreading=use_mt, headless_skip_extract=headless_skip)
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