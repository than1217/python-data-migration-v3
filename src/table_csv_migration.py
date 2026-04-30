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
log_file = os.path.join(base_dir, "migration.log")
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
        'connect_timeout': 60
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
    
    # Try to disable log_bin and redo_logs for every session, ignoring any privilege errors
    try:
        cursor = conn.cursor()
        try:
            cursor.execute("SET SESSION sql_log_bin=0")
        except Error:
            pass
        try:
            cursor.execute("ALTER INSTANCE DISABLE INNODB REDO_LOG")
        except Error:
            pass
        cursor.close()
    except Exception:
        pass

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
                
                with open(output_file, "wb") as f, tqdm(desc=f"Dumping Schema {table}", unit="B", unit_scale=True, leave=False, position=1) as pbar:
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

def run_mysqldump_full(table, output_file):
    """Executes mysqldump to export schema and data for the provided table."""
    command = [config.MYSQLDUMP_PATH]
    if config.DB_HOST.lower() not in ['localhost', '127.0.0.1']:
        command.extend([f"-h{config.DB_HOST}"])

    command.extend([
        f"-u{config.DB_USER}", f"--password={config.DB_PASSWORD}",
        "--skip-lock-tables", "--skip-add-locks", "--set-gtid-purged=OFF",
        config.DB_DATABASE, table
    ])
    
    logger.info("Dumping full table '%s' to %s...", table, output_file)
    import tempfile
    
    t_start = time.time()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with tempfile.TemporaryFile(mode='w+', encoding='utf-8', errors='ignore') as err_file:
                process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=err_file, text=False)
                
                with open(output_file, "wb") as f, tqdm(desc=f"Dumping SQL {table}", unit="B", unit_scale=True, leave=False, position=1) as pbar:
                    if process.stdout is not None:
                        while True:
                            chunk = process.stdout.read(1024 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                            pbar.update(len(chunk))
                        
                process.wait()
                if process.returncode == 0:
                    logger.info("Successfully dumped schema and data for '%s' in %s.", table, format_time(time.time() - t_start))
                    time.sleep(0.5)
                    return True
                else:
                    err_file.seek(0)
                    logger.error("Attempt %s/%s failed full dump '%s': %s", attempt, MAX_RETRIES, table, err_file.read())
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error("Attempt %s/%s error during full dump for '%s': %s", attempt, MAX_RETRIES, table, e)
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
        
        # The raw schema file is kept intact as requested
        logger.info("Kept raw schema file '%s' intact.", input_file)

        logger.info("Successfully processed schema for table '%s'.", table_name)
        return True
            
    except Exception as e:
        logger.error("Error processing schema dump file for '%s': %s", table_name, e)
        return False

def export_data_to_csv(table_name, csv_file_path):
    """Exports data directly to a CSV file using python streaming with utf8mb4 charset.
       Uses Primary Key chunking (pagination) to prevent memory spikes and query timeouts on large tables."""
    logger.info("Exporting data from '%s' to CSV...", table_name)
    t_start = time.time()
    conn = None
    try:
        conn = get_db_connection(
            host=config.DB_HOST,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_DATABASE,
            charset='utf8mb4'
        )

        with conn.cursor(buffered=True) as cursor:
            # Determine if it's a table or a view
            cursor.execute(f"SELECT TABLE_TYPE FROM information_schema.tables WHERE table_schema = '{config.DB_DATABASE}' AND table_name = '{table_name}'")
            table_type_result = cursor.fetchone()
            is_view = table_type_result and table_type_result[0].upper() == 'VIEW'

            # Get total rows for progress bar
            total_rows = 0
            if not is_view:
                try:
                    # Use approximate count for speed, but verify with COUNT(*) if needed
                    cursor.execute(f"SELECT TABLE_ROWS FROM information_schema.tables WHERE table_schema = '{config.DB_DATABASE}' AND table_name = '{table_name}'")
                    row = cursor.fetchone()
                    total_rows = int(row[0]) if row and row[0] is not None else 0
                    if total_rows == 0:
                        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                        total_rows = cursor.fetchone()[0]
                except Error as e:
                    logger.warning("Could not determine row count for '%s': %s. Progress bar may be inaccurate.", table_name, e)
                    total_rows = None # For indeterminate progress bar
            
            desc = f"Exporting CSV {table_name}"
            with open(csv_file_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                
                # Write headers
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 0")
                headers = [i[0] for i in cursor.description] if cursor.description else []
                writer.writerow(headers)
                
                if total_rows == 0 and not is_view:
                    logger.info("Table '%s' is empty. Exported headers only.", table_name)
                    return True, 0

                # Check for a suitable primary key for chunking
                pk_col_name = None
                if not is_view:
                    cursor.execute(f"SHOW COLUMNS FROM `{table_name}` WHERE `Key` = 'PRI'")
                    pk_cols = cursor.fetchall()
                    if len(pk_cols) == 1 and 'int' in pk_cols[0][1]:
                        pk_col_name = pk_cols[0][0]
                
                exact_row_count = 0
                with tqdm(total=total_rows, desc=desc, unit="row", leave=False, position=1) as pbar:
                    # --- PK Chunking Logic ---
                    if pk_col_name:
                        logger.info("Using Primary Key chunking for export of table '%s' on column '%s'.", table_name, pk_col_name)
                        cursor.execute(f"SELECT MIN(`{pk_col_name}`), MAX(`{pk_col_name}`) FROM `{table_name}`")
                        min_pk, max_pk = cursor.fetchone()

                        if min_pk is not None and max_pk is not None:
                            chunk_size = 50000
                            current_pk = min_pk
                            
                            with conn.cursor(buffered=False) as unbuffered_cursor:
                                while current_pk <= max_pk:
                                    query = f"SELECT * FROM `{table_name}` WHERE `{pk_col_name}` >= {current_pk} AND `{pk_col_name}` < {current_pk + chunk_size}"
                                    unbuffered_cursor.execute(query)
                                    
                                    rows_in_chunk = 0
                                    while True:
                                        rows = unbuffered_cursor.fetchmany(10000)
                                        if not rows:
                                            break
                                        writer.writerows(rows)
                                        rows_in_chunk += len(rows)
                                    
                                    exact_row_count += rows_in_chunk
                                    pbar.update(rows_in_chunk)
                                    current_pk += chunk_size
                        else:
                            logger.info("Table '%s' appears empty despite row count. Exporting headers only.", table_name)

                    # --- Fallback to unbuffered streaming for views or tables without a good PK ---
                    else:
                        if is_view:
                            logger.info("'%s' is a view, using unbuffered streaming export.", table_name)
                        else:
                            logger.warning("No suitable single-column integer PK found for '%s'. Using unbuffered streaming export.", table_name)
                        
                        with conn.cursor(buffered=False) as unbuffered_cursor:
                            try:
                                # Increase timeouts for long-running streaming queries
                                unbuffered_cursor.execute("SET SESSION net_read_timeout=7200")
                                unbuffered_cursor.execute("SET SESSION net_write_timeout=7200")
                            except Error as e:
                                logger.warning("Could not set session timeouts: %s", e)
                                
                            query = f"SELECT * FROM `{table_name}`"
                            unbuffered_cursor.execute(query)
                            
                            while True:
                                rows = unbuffered_cursor.fetchmany(10000)
                                if not rows:
                                    break
                                    
                                writer.writerows(rows)
                                rows_fetched = len(rows)
                                exact_row_count += rows_fetched
                                pbar.update(rows_fetched)

        logger.info("Successfully exported %d rows from '%s' to CSV in %s.", exact_row_count, table_name, format_time(time.time() - t_start))
        return True, exact_row_count

    except Error as e:
        if conn and e.errno == 2002 and config.DB_HOST.lower() == 'localhost':
            logger.warning("Socket connection failed during CSV export. Falling back to TCP/IP via 127.0.0.1 for '%s'", table_name)
            return export_data_to_csv(table_name, csv_file_path)
            
        logger.error("Error exporting to CSV for table '%s': %s", table_name, e)
        return False, 0
    finally:
        if conn:
            conn.close()

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
                file_size = os.path.getsize(filepath) if os.path.exists(filepath) else 0
                with open(filepath, 'rb') as f, tqdm(total=file_size, desc=f"Executing {filename}", unit="B", unit_scale=True, leave=False, position=1) as pbar:
                    while True:
                        chunk = f.read(1024 * 1024) # Read in 1MB chunks
                        if not chunk:
                            break
                        if process.stdin:
                            process.stdin.write(chunk)
                            process.stdin.flush()
                        pbar.update(len(chunk))
                if process.stdin:
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
        "-f"
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
    
    import tempfile
    retries = MAX_RETRIES if use_local else 1
    attempt = 1
    
    while attempt <= retries:
        sql_command = f"""
    SET SESSION sql_log_bin=0;
    ALTER INSTANCE DISABLE INNODB REDO_LOG;
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
        
        with tempfile.TemporaryFile(mode='w+', encoding='utf-8', errors='ignore') as err_file:
            try:
                process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err_file, text=True)
                process.communicate(sql_command)
                
                err_file.seek(0)
                error_msg = err_file.read()
                
                is_fatal = False
                if error_msg:
                    for line in error_msg.splitlines():
                        if "ERROR " in line or "Error " in line:
                            # Ignore log_bin and redo_log privilege errors
                            if any(k in line for k in ["sql_log_bin", "SUPER", "SYSTEM_VARIABLES_ADMIN", "REDO_LOG", "Access denied"]):
                                continue
                            is_fatal = True
                            break
                            
                if not is_fatal:
                    return True
                else:
                    logger.error("Attempt %s/%s failed to load CSV for '%s' (local=%s): %s", attempt, retries, target_table_name, use_local, error_msg)
                    if attempt < retries:
                        time.sleep(RETRY_DELAY)
                    attempt += 1
            except Exception as e:
                logger.error("Attempt %s/%s unexpected error loading CSV for '%s' (local=%s): %s", attempt, retries, target_table_name, use_local, e)
                if attempt < retries:
                    time.sleep(RETRY_DELAY)
                attempt += 1
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

def _execute_truncate_table(target_table_name):
    """Executes TRUNCATE TABLE using the mysql CLI to avoid python driver timeouts on large tables."""
    command = [
        config.MYSQL_PATH,
        "--connect_timeout=10",
        "--max_allowed_packet=1G",
        "-f"
    ]
    
    if config.DEST_DB_HOST.lower() not in ['localhost', '127.0.0.1']:
        command.extend([f"-h{config.DEST_DB_HOST}"])
    
    command.extend([
        f"-u{config.DEST_DB_USER}", f"--password={config.DEST_DB_PASSWORD}", config.DEST_DB_DATABASE
    ])

    logger.info("Executing TRUNCATE TABLE for '%s' via CLI", target_table_name)
    
    import tempfile
    attempt = 1
    
    while attempt <= MAX_RETRIES:
        sql_command = f"SET SESSION sql_log_bin=0;\nALTER INSTANCE DISABLE INNODB REDO_LOG;\nSET SESSION FOREIGN_KEY_CHECKS=0;\nTRUNCATE TABLE `{target_table_name}`;\n"
        
        with tempfile.TemporaryFile(mode='w+', encoding='utf-8', errors='ignore') as err_file:
            try:
                process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err_file, text=True)
                process.communicate(sql_command)
                
                err_file.seek(0)
                error_msg = err_file.read()
                
                is_fatal = False
                if error_msg:
                    for line in error_msg.splitlines():
                        if "ERROR " in line or "Error " in line:
                            if any(k in line for k in ["sql_log_bin", "SUPER", "SYSTEM_VARIABLES_ADMIN", "REDO_LOG", "Access denied"]):
                                continue
                            is_fatal = True
                            break

                if not is_fatal:
                    return True
                else:
                    logger.error("Attempt %s/%s failed to truncate '%s': %s", attempt, MAX_RETRIES, target_table_name, error_msg)
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
                    attempt += 1
            except Exception as e:
                logger.error("Attempt %s/%s unexpected error truncating '%s': %s", attempt, MAX_RETRIES, target_table_name, e)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                attempt += 1
    return False

def _process_csv_chunk(target_table_name, t_csv, h_list, is_remote_dest):
    """Processes a single CSV chunk, trying different loading methods."""
    if is_remote_dest:
        success = _execute_load_data_infile(target_table_name, t_csv, use_local=True)
    else:
        success = _execute_load_data_infile(target_table_name, t_csv, use_local=False)
        if not success:
            logger.info("Standard LOAD DATA INFILE failed for chunk of '%s'. Falling back to LOCAL INFILE...", target_table_name)
            success = _execute_load_data_infile(target_table_name, t_csv, use_local=True)
    
    rows_count = 0
    if success:
        try:
            with open(t_csv, 'r', encoding='utf-8') as tf:
                rows_count = sum(1 for _ in tf) - 1
        except Exception:
            pass
    else:
        logger.info("Falling back to Python mysql.connector for chunk of '%s'...", target_table_name)
        with open(t_csv, 'r', encoding='utf-8') as tf:
            tf.readline()
            parsed = list(csv.reader(tf))
        if _execute_fallback_insert(target_table_name, h_list, parsed):
            success = True
            rows_count = len(parsed)
        
    if os.path.exists(t_csv):
        os.remove(t_csv)
    return success, rows_count

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
        if not _execute_truncate_table(target_table_name):
            logger.error("Failed to truncate table '%s'", target_table_name)
            return False
        last_byte_pos = 0
        rows_loaded = 0
        completed_chunks = []
        state["csv_load_progress"][target_table_name] = {"last_byte_pos": 0, "rows_loaded": 0, "completed_chunks": []}
        save_state(state)

    if use_multithreading:
        logger.info("Loading CSV into destination table '%s' using MULTI-THREADED chunks (Remote dest: %s)...", target_table_name, is_remote_dest)
        chunk_size = 250000
        temp_dir = tempfile.gettempdir()
        
        completed_chunks_set = set(completed_chunks)
        
        def process_wrapper(t_csv, h_list, b_processed, c_id):
            """Wrapper to call the shared chunk processor and return values needed for futures."""
            success, rows_count = _process_csv_chunk(target_table_name, t_csv, h_list, is_remote_dest)
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
            
            header_line_bytes = len(header_line.encode('utf-8'))
            bytes_skipped = 0
            
            if last_byte_pos > 0 and not completed_chunks_set:
                f.seek(last_byte_pos)
                bytes_skipped = last_byte_pos
            elif not completed_chunks_set:
                bytes_skipped = header_line_bytes
                
            print(f"Splitting CSV and preparing chunks for {target_table_name}...")
            # Use the actual file sizes of the generated chunks to update the progress bar to avoid CPU overhead
            
            mt_total_bytes = 0
            with tqdm(total=file_size, initial=bytes_skipped, desc=f"Preparing Chunks", unit="B", unit_scale=True, leave=False, position=1) as pbar_split:
                while True:
                    chunk_rows = []
                    is_completed = chunk_id in completed_chunks_set
                    
                    for _ in range(chunk_size):
                        line = f.readline()
                        if not line: break
                        chunk_rows.append(line)
                        
                    if not chunk_rows:
                        break
                        
                    if is_completed:
                        # Fast approximation for skipped chunks
                        bytes_in_chunk = sum(len(line) for line in chunk_rows)
                        bytes_skipped += bytes_in_chunk
                        pbar_split.update(bytes_in_chunk)
                    else:
                        temp_csv = os.path.join(temp_dir, f"{target_table_name}_chunk_{uuid.uuid4().hex}.tmp")
                        with open(temp_csv, 'w', encoding='utf-8', newline='') as tf:
                            tf.write(header_line)
                            tf.writelines(chunk_rows)
                        
                        # Use actual file size of the chunk for accurate tracking
                        actual_chunk_size = os.path.getsize(temp_csv)
                        bytes_for_pbar = max(0, actual_chunk_size - header_line_bytes)
                        mt_total_bytes += bytes_for_pbar
                        futures.append(executor.submit(process_wrapper, temp_csv, headers, bytes_for_pbar, chunk_id))
                        pbar_split.update(bytes_for_pbar)
                        
                    chunk_id += 1
                
        total_loaded = rows_loaded
        state_lock = threading.Lock()
        
        multithread_failed = False
        
        # Calculate total target bytes to load
        total_bytes_to_load = mt_total_bytes if futures else file_size
        
        with tqdm(total=total_bytes_to_load, desc=f"MT Loading {target_table_name}", unit="B", unit_scale=True, leave=False, position=1) as pbar:
            for future in as_completed(futures):
                success, loaded_count, b_processed, c_id = future.result()
                if not success:
                    logger.warning("A multithreaded chunk failed to load for table '%s'. Will fallback to single-threaded mode.", target_table_name)
                    executor.shutdown(wait=False, cancel_futures=True)
                    multithread_failed = True
                    break
                
                with state_lock:
                    total_loaded += loaded_count
                    completed_chunks_set.add(c_id)
                    state["csv_load_progress"][target_table_name] = {
                        "last_byte_pos": bytes_skipped, # Maintain skipped offset for resume compatibility
                        "rows_loaded": total_loaded,
                        "completed_chunks": list(completed_chunks_set)
                    }
                    save_state(state)
                pbar.update(b_processed)
        
        executor.shutdown(wait=True)

        if not multithread_failed:
            state["csv_load_progress"][target_table_name] = {
                "last_byte_pos": file_size,
                "rows_loaded": total_loaded,
                "completed_chunks": []
            }
            save_state(state)
            logger.info("Successfully loaded CSV into '%s' via MT in %s.", target_table_name, format_time(time.time() - t_start))
            return True
        else:
            logger.warning("Multithreaded load for '%s' failed. Falling back to single-threaded mode.", target_table_name)
            if not _execute_truncate_table(target_table_name):
                logger.error("Failed to truncate table '%s' for retry.", target_table_name)
                return False
            logger.info("Truncated table '%s' for single-threaded retry.", target_table_name)

            # Reset progress for single-threaded mode
            state["csv_load_progress"][target_table_name] = {"last_byte_pos": 0, "rows_loaded": 0, "completed_chunks": []}
            save_state(state)
            last_byte_pos = 0
            rows_loaded = 0
            completed_chunks = []

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
    
    with open(csv_file_path, 'r', encoding='utf-8', newline='') as f:
        header_line = f.readline()
        if not header_line:
            logger.warning("CSV is empty for table '%s'.", target_table_name)
            state["csv_load_progress"][target_table_name] = {"last_byte_pos": 0, "rows_loaded": 0}
            return True
            
        headers = next(csv.reader([header_line]))
        header_line_bytes = len(header_line.encode('utf-8'))
        
        if last_byte_pos == 0:
            last_byte_pos = header_line_bytes
        else:
            f.seek(last_byte_pos)
            
        with tqdm(total=file_size, initial=last_byte_pos, desc=f"Loading CSV {target_table_name}", unit="B", unit_scale=True, leave=False, position=1) as pbar:
            chunk_idx = 0
            while True:
                chunk_rows = []
                for _ in range(chunk_size):
                    line = f.readline()
                    if not line:
                        break
                    chunk_rows.append(line)
                    
                if not chunk_rows:
                    break
                    
                temp_csv = os.path.join(temp_dir, f"{target_table_name}_chunk_{uuid.uuid4().hex}.tmp")
                with open(temp_csv, 'w', encoding='utf-8', newline='') as tf:
                    tf.write(header_line)
                    tf.writelines(chunk_rows)
                    
                actual_chunk_size = os.path.getsize(temp_csv)
                bytes_processed = max(0, actual_chunk_size - header_line_bytes)
                
                success, loaded_count = _process_csv_chunk(target_table_name, temp_csv, headers, is_remote_dest)
                    
                if not success:
                    logger.error("Failed to load chunk for table '%s'.", target_table_name)
                    return False
                    
                rows_loaded += loaded_count
                last_byte_pos += bytes_processed
                
                state["csv_load_progress"][target_table_name] = {
                    "last_byte_pos": last_byte_pos,
                    "rows_loaded": rows_loaded
                }
                save_state(state)
                
                pbar.update(bytes_processed)
                chunk_idx += 1

    logger.info("Successfully loaded CSV into '%s' in %s.", target_table_name, format_time(time.time() - t_start))
    return True

def get_view_ddl(view_name, dest_table_name):
    """Gets the column definitions from a view and constructs a CREATE TABLE DDL."""
    logger.info("Getting DDL for view '%s' to create table '%s'", view_name, dest_table_name)
    try:
        conn = get_db_connection(
            host=config.DB_HOST,
            database=config.DB_DATABASE,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        if not conn.is_connected():
            return None
            
        cursor = conn.cursor()
        # Using DESCRIBE which is an alias for SHOW COLUMNS
        cursor.execute(f"DESCRIBE `{view_name}`")
        columns = cursor.fetchall()
        cursor.close()
        conn.close()

        if not columns:
            logger.error("Could not retrieve columns for view '%s'. It might not exist or permissions are missing.", view_name)
            return None

        col_defs = []
        string_types = ['varchar', 'char', 'enum', 'set', 'text', 'tinytext', 'mediumtext', 'longtext']
        
        for col in columns:
            col_name, col_type, nullable, key, default, extra = col
            
            col_def = f"`{col_name}` {col_type}"

            is_string = any(s_type in col_type.lower() for s_type in string_types)
            if is_string:
                col_def += " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"

            if nullable == 'NO':
                col_def += " NOT NULL"

            if default is not None:
                default_val = default
                if isinstance(default, (bytes, bytearray)):
                    default_val = default.decode('utf-8', 'replace')
                
                if isinstance(default_val, str):
                    if default_val.upper() in ["CURRENT_TIMESTAMP", "NULL"]:
                        col_def += f" DEFAULT {default_val}"
                    else:
                        default_val = default_val.replace("'", "''")
                        col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            
            if extra:
                # Filter out unsupported 'DEFAULT_GENERATED' from view descriptions
                clean_extra = re.sub(r'DEFAULT_GENERATED', '', extra, flags=re.IGNORECASE).strip()
                if clean_extra:
                    col_def += f" {clean_extra}"
            
            col_defs.append(col_def)
        
        ddl = f"CREATE TABLE IF NOT EXISTS `{dest_table_name}` (\n  " + ",\n  ".join(col_defs) + "\n)"
        ddl += " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;"
        
        return f"SET FOREIGN_KEY_CHECKS=0;\nSET UNIQUE_CHECKS=0;\n{ddl}\nSET FOREIGN_KEY_CHECKS=1;\nSET UNIQUE_CHECKS=1;\n"

    except Error as e:
        logger.error("Error getting view DDL for '%s': %s", view_name, e)
        return None

def get_ddl_content(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except Exception:
            pass
    return ""

def check_and_handle_existing_table(table_name, headless_action=None, global_action=None):
    """Checks if a table exists and asks the user to drop, truncate or skip it.
    Returns: (proceed: bool, action: str ('drop' or 'truncate'), new_global_action: str)
    """
    try:
        conn = get_db_connection(
            host=config.DEST_DB_HOST, user=config.DEST_DB_USER,
            password=config.DEST_DB_PASSWORD, database=config.DEST_DB_DATABASE
        )
        if not conn.is_connected():
            return False, None, global_action

        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone() is not None

        if table_exists:
            if headless_action in ['drop', 'truncate', 'skip']:
                action = headless_action
                if action == 'skip':
                    logger.info("Headless mode: skipping existing table '%s'.", table_name)
                    print(f"Headless mode: skipping existing table '{table_name}'.")
                    return False, None, global_action
            elif global_action in ['drop', 'truncate', 'skip']:
                action = global_action
                if action == 'skip':
                    return False, None, global_action
            else:
                choice = input(f"Table '{table_name}' already exists. Drop(d), Truncate(t), Skip(s), All-Drop(ad), All-Truncate(at), All-Skip(as)? (d/t/s/ad/at/as): ").strip().lower()
                if choice == 'ad':
                    global_action = 'drop'
                    action = 'drop'
                elif choice == 'at':
                    global_action = 'truncate'
                    action = 'truncate'
                elif choice == 'as':
                    global_action = 'skip'
                    logger.info("User chose to skip table '%s'.", table_name)
                    return False, None, global_action
                elif choice == 'd':
                    action = 'drop'
                elif choice == 't':
                    action = 'truncate'
                else:
                    logger.info("User chose to skip table '%s'. Cancelling migration for this table.", table_name)
                    print(f"Migration for '{table_name}' skipped.")
                    return False, None, global_action

            try:
                conn.ping(reconnect=True, attempts=3, delay=2)
                cursor.execute("SET SESSION net_read_timeout=7200")
                cursor.execute("SET SESSION net_write_timeout=7200")
            except Error as e:
                logger.warning("Reconnection or setting timeouts failed: %s", e)

            if action == 'drop':
                logger.info("User chose to drop existing table '%s'.", table_name)
                cursor.execute(f"DROP TABLE `{table_name}`")
                conn.commit()
                return True, 'drop', global_action
            elif action == 'truncate':
                logger.info("User chose to truncate existing table '%s'.", table_name)
                if not _execute_truncate_table(table_name):
                    logger.error("Failed to truncate table '%s' via CLI.", table_name)
                return True, 'truncate', global_action

        return True, 'drop', global_action  # Table doesn't exist, proceed
    except Error as e:
        logger.error("Error handling existing table '%s': %s", table_name, e)
        print(f"Error handling existing table '{table_name}'. Check logs.")
        return False, None, global_action
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def write_summary_csv(summary_csv_path, summary_data):
    """Writes summary data to a CSV file, appending if it exists."""
    try:
        file_exists = os.path.exists(summary_csv_path)
        with open(summary_csv_path, 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['table_name', 'total_migration', 'ddl_sql', 'total_rows', 'remarks'])
            writer.writerows(summary_data)
        logger.info("Summary CSV successfully updated at '%s'", summary_csv_path)
    except Exception as e:
        logger.error("Failed to write summary CSV: %s", e)

def run_view_to_table_migration(view_name, dest_table_name, state, suffix, use_multithreading=False, headless_action=None):
    """Orchestrates the migration of a single view to a destination table."""
    t_start_total = time.time()
    logger.info("Starting migration from view '%s' to table '%s'", view_name, dest_table_name)
    
    folder_name = suffix.strip('_') if suffix else 'v2'
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed_dir = os.path.join(project_dir, "output", "processed", folder_name)
    csv_dir = os.path.join(project_dir, "output", "csv", folder_name)
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(csv_dir, exist_ok=True)

    summary_csv_path = os.path.join(project_dir, "output", f"migration_summary_{folder_name}.csv")
    processed_schema = os.path.join(processed_dir, f"{dest_table_name}_schema.sql")
    csv_file = os.path.join(csv_dir, f"{dest_table_name}.csv")

    remarks = "Unknown"

    try:
        if dest_table_name in state.get("migrated_tables", []):
            print(f"Skipping '{dest_table_name}', as it's already marked as migrated.")
            logger.info("Skipping view migration for '%s' as it's already completed.", dest_table_name)
            remarks = "Success (Already migrated)"
            return

        csv_load_progress = state.get("csv_load_progress", {}).get(dest_table_name, {})
        is_resuming = "last_byte_pos" in csv_load_progress and os.path.exists(csv_file)

        if not is_resuming:
            print(f"\nStarting new migration from view '{view_name}' to table '{dest_table_name}'.")

            proceed, action, _ = check_and_handle_existing_table(dest_table_name, headless_action=headless_action)
            if not proceed:
                remarks = "Cancelled by user"
                return

            if action == 'truncate':
                logger.info("Table '%s' was truncated. Skipping DDL extraction and creation.", dest_table_name)
                with open(processed_schema, 'w', encoding='utf-8') as f:
                    f.write("-- Schema extraction skipped due to truncate action.\n")
            else:
                ddl = get_view_ddl(view_name, dest_table_name)
                if not ddl:
                    print(f"Failed to generate DDL for view '{view_name}'. Check logs.")
                    remarks = "Failed: DDL Generation"
                    return

                try:
                    with open(processed_schema, 'w', encoding='utf-8') as f:
                        f.write(ddl)
                    logger.info("Successfully generated DDL for view '%s'.", view_name)
                except IOError as e:
                    logger.error("Failed to write schema file %s: %s", processed_schema, e)
                    print(f"Error writing schema file. Check logs.")
                    remarks = "Failed: Write DDL to file"
                    return

            use_existing_csv = False
            if os.path.exists(csv_file):
                choice = input(f"\nExisting CSV file found for '{view_name}'. Skip extraction and use existing? (y/n): ").strip().lower()
                if choice == 'y':
                    logger.info("User chose to use existing CSV for '%s'. Skipping extraction.", view_name)
                    use_existing_csv = True
                else:
                    logger.info("User chose to re-extract CSV for '%s'.", view_name)
            
            export_success = False
            if use_existing_csv:
                export_success = True
                print(f"Using existing CSV file for '{view_name}'.")
            else:
                print(f"Exporting data from view '{view_name}' to CSV...")
                export_success, _ = export_data_to_csv(view_name, csv_file)

            if not export_success:
                print(f"Failed to export data from view '{view_name}'. Check logs.")
                remarks = "Failed: Export data to CSV"
                return

            if action != 'truncate':
                print(f"Creating table '{dest_table_name}' on destination...")
                if not load_sql_schema(processed_schema):
                    print(f"Failed to create table '{dest_table_name}'. Check logs.")
                    remarks = "Failed: Execute schema in destination"
                    return
        else:
            print(f"\nResuming migration for table '{dest_table_name}'.")

        print(f"Loading data into table '{dest_table_name}'...")
        if load_csv_to_dest(dest_table_name, csv_file, state, use_multithreading):
            elapsed_str = format_time(time.time() - t_start_total)
            final_row_count = state.get("csv_load_progress", {}).get(dest_table_name, {}).get("rows_loaded", 0)
            
            print(f"Successfully migrated view '{view_name}' to table '{dest_table_name}' in {elapsed_str} ({final_row_count} rows).")
            logger.info("View to table migration for '%s' completed in %s (%d rows).", dest_table_name, elapsed_str, final_row_count)
            
            if "migrated_tables" not in state:
                state["migrated_tables"] = []
            state["migrated_tables"].append(dest_table_name)
            
            state.setdefault("final_row_counts", {})[dest_table_name] = final_row_count
            
            if dest_table_name in state.get("csv_load_progress", {}):
                del state["csv_load_progress"][dest_table_name]
            save_state(state)
            remarks = "Success"
        else:
            print(f"Failed to load data into table '{dest_table_name}'. Migration incomplete. You can resume later.")
            remarks = "Failed: Load CSV to destination"

    finally:
        elapsed = format_time(time.time() - t_start_total)
        ddl_content = get_ddl_content(processed_schema)
        final_row_count = state.get("final_row_counts", {}).get(dest_table_name, 0)
        if final_row_count == 0:
            final_row_count = state.get("csv_load_progress", {}).get(dest_table_name, {}).get("rows_loaded", 0)
        
        summary_data = [[dest_table_name, elapsed, ddl_content, final_row_count, remarks]]
        write_summary_csv(summary_csv_path, summary_data)
        logger.info("View migration summary for '%s' generated with status: %s", dest_table_name, remarks)

def run_migration(tables, state, suffix, use_multithreading=False, headless_skip_extract=None, headless_action=None):
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

    if not tables:
        print("No tables found to migrate.")
        return
        
    print(f"\nMigrating {len(tables)} tables via CSV approach (logging details to migration.log)...")
    start_time = time.time()
    
    if not create_destination_db():
        logger.error("Failed to ensure destination database. Aborting.")
        print("Failed to ensure destination DB. See logs.")
        return

    successful_migrations = sum(1 for t in tables if t in state.get("migrated_tables", []))
    global_skip_extract = headless_skip_extract
    global_existing_action = None

    pbar_main = tqdm(tables, desc="Migrating Tables (CSV)", unit="table", leave=True, position=0)
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
                    final_row_count = state.get("csv_load_progress", {}).get(target_table_name, {}).get("rows_loaded", 0)
                    logger.info("Total migration for table '%s' completed in %s (%d rows).", table, elapsed, final_row_count)
                    successful_migrations += 1
                    if "migrated_tables" not in state:
                        state["migrated_tables"] = []
                    state["migrated_tables"].append(table)
                    
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
            proceed, action, global_existing_action = check_and_handle_existing_table(target_table_name, headless_action=headless_action, global_action=global_existing_action)
            if not proceed:
                remarks = "Skipped by user"
                summary_data.append([target_table_name, "Skipped", "", 0, remarks])
                continue

            schema_success = True
            if action == 'truncate':
                logger.info("Table '%s' was truncated. Skipping schema extraction and creation.", target_table_name)
                with open(processed_schema, 'w', encoding='utf-8') as f:
                    f.write("-- Schema extraction skipped due to truncate action.\n")
            else:
                # 1. Dump raw schema
                if run_mysqldump_schema(table, raw_schema):
                    # 2. Process schema (and delete raw)
                    if not process_schema_file(raw_schema, processed_schema, table, suffix):
                        schema_success = False
                        remarks = "Failed: Process schema file"
                        logger.warning("Failed to process schema for table '%s'.", table)
                else:
                    schema_success = False
                    remarks = "Failed: Dump schema"
                    logger.warning("Failed to dump schema for table '%s'.", table)

            if schema_success:
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
                    load_schema_success = True
                    if action != 'truncate':
                        load_schema_success = load_sql_schema(processed_schema)
                    
                    if load_schema_success:
                        # 5. Load CSV to destination DB
                        if load_csv_to_dest(target_table_name, csv_file, state, use_multithreading):
                            elapsed = format_time(time.time() - t_start)
                            final_row_count = state.get("csv_load_progress", {}).get(target_table_name, {}).get("rows_loaded", 0)
                            logger.info("Total migration for table '%s' completed in %s (%d rows).", table, elapsed, final_row_count)
                            successful_migrations += 1
                            if "migrated_tables" not in state:
                                state["migrated_tables"] = []
                            state["migrated_tables"].append(table)
                            
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
            
        elapsed = format_time(time.time() - t_start)
        ddl_content = get_ddl_content(processed_schema)
        
        # Use recorded row count instead of querying information schema
        if "Success" in remarks:
            total_rows_migrated = state.get("final_row_counts", {}).get(target_table_name, 0)
            
        summary_data.append([target_table_name, elapsed, ddl_content, total_rows_migrated, remarks])
            
    # Calculate total rows from summary_data
    total_rows_all_tables = sum(row[3] for row in summary_data if isinstance(row[3], int))
    
    summary = f"Migration complete: {successful_migrations}/{len(tables)} tables migrated."
    print(f"\n{summary}")
    logger.info(summary)
    
    # Write summary CSV
    write_summary_csv(summary_csv_path, summary_data)
    
    if successful_migrations == len(tables):
        logger.info("All tables migrated successfully. Resetting migration state.")
        save_state({"processed_tables": [], "migrated_tables": [], "pattern": None, "from_list": None})

    elapsed_time = time.time() - start_time
    m, s = divmod(elapsed_time, 60)
    h, m = divmod(m, 60)
    time_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"
    print(f"Total migration time: {time_str} | Total rows migrated: {total_rows_all_tables}")
    logger.info("--- MIGRATION FINISHED --- Total Time: %s | Total Rows: %d", time_str, total_rows_all_tables)

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

def run_export_only(tables, suffix, export_format='csv'):
    folder_name = suffix.strip('_') if suffix else 'v2'
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    export_dir = os.path.join(project_dir, "output", "export", folder_name)
    os.makedirs(export_dir, exist_ok=True)

    if not tables:
        print("No tables found to export.")
        logger.warning("Export aborted: No tables found.")
        return

    logger.info("Starting export of %d tables to %s format in %s", len(tables), export_format.upper(), export_dir)
    print(f"\nExporting {len(tables)} tables to {export_format.upper()} in '{export_dir}'...")
    
    for table in tqdm(tables, desc="Exporting Tables", unit="table", position=0):
        target_table_name = table if table.endswith(suffix) else f"{table}{suffix}"
        
        if export_format == 'sql':
            output_file = os.path.join(export_dir, f"{target_table_name}_full.sql")
            if run_mysqldump_full(table, output_file):
                logger.info("Successfully exported %s to SQL format.", table)
                print(f"Exported {table} to SQL successfully.")
            else:
                logger.error("Failed to export %s to SQL format.", table)
                print(f"Failed to export {table} to SQL.")
        else:
            raw_schema = os.path.join(export_dir, f"{table}_raw_schema{suffix}.sql")
            processed_schema = os.path.join(export_dir, f"{target_table_name}_schema.sql")
            csv_file = os.path.join(export_dir, f"{target_table_name}.csv")
            
            if run_mysqldump_schema(table, raw_schema):
                process_schema_file(raw_schema, processed_schema, table, suffix)
            
            success, _ = export_data_to_csv(table, csv_file)
            if success:
                logger.info("Successfully exported %s to CSV format.", table)
                print(f"Exported {table} to CSV and Schema successfully.")
            else:
                logger.error("Failed to export %s to CSV format.", table)
                print(f"Failed to export {table} to CSV format.")

    logger.info("Export completed successfully.")
    print(f"\nExport completed. Files saved in: {export_dir}")

def run_import_only(import_format, filepath, target_table=None, use_mt=False, headless_action=None):
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        logger.error("Import aborted: File not found %s", filepath)
        return
        
    if import_format == 'sql':
        logger.info("Starting SQL import from %s", filepath)
        if create_destination_db():
            if load_sql_schema(filepath):
                print("\nSQL file imported successfully.")
                logger.info("Successfully imported SQL file %s", filepath)
            else:
                print("\nFailed to import SQL file. Check logs.")
                logger.error("Failed to import SQL file %s", filepath)
                
    elif import_format == 'csv':
        if not target_table:
            logger.warning("Import aborted: No destination table provided.")
            print("No destination table provided.")
            return
            
        logger.info("Starting CSV import from %s to table %s", filepath, target_table)
        if create_destination_db():
            proceed, action, _ = check_and_handle_existing_table(target_table, headless_action=headless_action)
            if not proceed:
                print("Import cancelled.")
                logger.info("Import cancelled by user for table %s", target_table)
                return
                
            # Usual fallback strategy: execute schema if it exists so the table is created
            if action != 'truncate':
                schema_path = filepath.replace('.csv', '_schema.sql')
                if os.path.exists(schema_path):
                    print(f"\nFound corresponding schema file: {os.path.basename(schema_path)}")
                    logger.info("Found corresponding schema file %s, executing...", schema_path)
                    if load_sql_schema(schema_path):
                        print("Schema loaded successfully.")
                    else:
                        print("Failed to load schema. Check logs.")
                        
            state = load_state()
            
            if load_csv_to_dest(target_table, filepath, state, use_multithreading=use_mt):
                print(f"\nCSV imported to '{target_table}' successfully.")
                logger.info("Successfully imported CSV to %s", target_table)
            else:
                print("\nFailed to import CSV. Check logs.")
                logger.error("Failed to import CSV to %s", target_table)

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
        print("3. Migrate from View to Table")
        print("4. Export Schema and Data only (Download)")
        print("5. Import Schema or Data from file (Upload)")
        print("6. Exit / Back to main menu")
        print("=============================================")
        
        choice = input("Select an option (1-6): ").strip()
        
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
            view_name = input("Enter source view name: ").strip()
            if not view_name: continue
            
            dest_table_name = input(f"Enter destination table name [{view_name}]: ").strip() or view_name
            if not dest_table_name: continue
            
            dest_table_with_suffix = dest_table_name if dest_table_name.endswith(suffix) else f"{dest_table_name}{suffix}"
            print(f"The destination table will be named: {dest_table_with_suffix}")
            
            state = load_state()
            if dest_table_with_suffix in state.get("migrated_tables", []) or state.get("csv_load_progress", {}).get(dest_table_with_suffix):
                 resume = input("\nExisting migration progress found for this table. Resume? (y/n): ").strip().lower()
                 if resume != 'y':
                    if dest_table_with_suffix in state.get("migrated_tables", []):
                        state["migrated_tables"].remove(dest_table_with_suffix)
                    if dest_table_with_suffix in state.get("csv_load_progress", {}):
                        del state["csv_load_progress"][dest_table_with_suffix]
                    if dest_table_with_suffix in state.get("final_row_counts", {}):
                        del state["final_row_counts"][dest_table_with_suffix]
                    save_state(state)
            
            use_mt = input("Use multi-threaded chunking for faster data loading? (y/n): ").strip().lower() == 'y'
            run_view_to_table_migration(view_name, dest_table_with_suffix, state, suffix, use_multithreading=use_mt)
            
        elif choice == '4':
            print("\n--- Export Only ---")
            print("1. Specify pattern")
            print("2. Specify exact table names")
            sub_choice = input("Select option (1-2): ").strip()
            format_choice = input("Select export format (1: SQL Dump, 2: CSV): ").strip()
            export_format = 'sql' if format_choice == '1' else 'csv'
            
            if sub_choice == '1':
                pattern = input("Enter regular expression pattern (e.g., '^lib_.*'): ").strip()
                tables = get_lib_tables(pattern=pattern)
                run_export_only(tables, suffix, export_format)
            elif sub_choice == '2':
                tables_input = input("Enter table names separated by commas: ").strip()
                table_list = [t.strip() for t in tables_input.split(',')]
                tables = get_lib_tables(from_list=table_list)
                run_export_only(tables, suffix, export_format)
                
        elif choice == '5':
            print("\n--- Import Data ---")
            print("1. Import SQL File")
            print("2. Import CSV File")
            fmt_choice = input("Select format (1-2): ").strip()
            import_format = 'sql' if fmt_choice == '1' else 'csv'
            
            filepath = input("Enter full path to file: ").strip()
            target_table = None
            use_mt = False
            
            if import_format == 'csv':
                target_table = input("Enter destination table name: ").strip()
                use_mt = input("Use multi-threaded chunking? (y/n): ").strip().lower() == 'y'
                
            run_import_only(import_format, filepath, target_table, use_mt)
            
        elif choice == '6':
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
        
        headless_action = headless_config.get('existing_table_action', None)
        if headless_action not in ['drop', 'truncate', 'skip']:
            headless_action = None

        action = headless_config.get('action', 'migrate')
        if action == 'export':
            export_format = headless_config.get('export_format', 'csv')
            run_export_only(tables, suffix, export_format)
        elif action == 'import':
            import_format = headless_config.get('import_format', 'sql')
            import_filepath = headless_config.get('import_filepath')
            target_table = headless_config.get('target_table')
            if not import_filepath:
                print("Error: import_filepath is required for import action in headless mode.")
                sys.exit(1)
            run_import_only(import_format, import_filepath, target_table, use_mt, headless_action)
        else:
            skip_extract_cfg = headless_config.get('skip_extract', None)
            headless_skip = None
            if skip_extract_cfg is True:
                headless_skip = 'y'
            elif skip_extract_cfg is False:
                headless_skip = 'n'
            run_migration(tables, state, suffix, use_multithreading=use_mt, headless_skip_extract=headless_skip, headless_action=headless_action)
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