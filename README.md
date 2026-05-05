# Python CSV Data Migration Utility

This utility is a highly efficient, chunk-based CSV data migration tool for MySQL databases. It streams data out of a source database into local CSV files, adjusts schema options for compatibility (forcing `InnoDB`, `utf8mb4` character set, `utf8mb4_0900_ai_ci` collation, and `DYNAMIC` row format), and rapidly loads the data into a destination database using `LOAD DATA INFILE`.

By avoiding massive `.sql` dump files filled with `INSERT` statements, this utility avoids memory spikes, dramatically reduces export/import times, and easily recovers from connection drops or process interruptions via a robust state-tracking system.

## Key Features
- **Low Memory Footprint**: Uses Python's native `mysql.connector` with unbuffered cursors to stream source data directly into chunked `.csv` files.
- **Optimized Chunking & Fallbacks**: Employs Primary Key chunking for rapid table exports. Falls back to `LIMIT/OFFSET` pagination for views or tables lacking clear primary keys to avoid heavy timeouts.
- **Robust Multiline Support**: The chunker securely handles large multiline text fields containing newlines by enforcing quote-parity checks during the split phase, ensuring records are never sliced mid-string.
- **Accurate Row Verification**: Parses real-time `mysql` CLI output (`Records: x`) rather than relying on heavy full-table `COUNT(*)` queries or imprecise `information_schema` statistics, ensuring final tallies are 100% exact without database performance hits.
- **High-Speed Ingestion**: Leverages MySQL's native `LOAD DATA INFILE` for bulk data loading. Falls back to `LOCAL INFILE` or Python-based batch `INSERT` logic seamlessly if standard local loading fails due to server privilege settings (e.g., `--secure-file-priv`).
- **Trigger Management**: Safely and automatically drops triggers on target tables before inserting data, preventing execution errors and invalid `DEFINER` exceptions (like `Access denied; you need ... SYSTEM_USER privilege`) on the destination schema.
- **Standalone Import/Export**: Easily download schema and data as full SQL dumps or chunked CSVs, and upload them directly to destination databases without running the full migration pipeline.
- **Multithreaded Data Loading**: Drastically accelerates data insertion by splitting large CSVs into chunks and loading them into the destination database concurrently. 
- **Granular Progress Tracking**: Features deeply nested `tqdm` progress bars, giving you live visual insight into multi-threaded chunk execution speeds, schema dump processing, and exact byte counts skipped during resume flows.
- **View-to-Table Migration**: Automatically reverse-engineers the schema of a source view, generates a CREATE TABLE statement, and materializes the view as a physical table on the destination server.
- **Smart Schema Modification**: Automatically extracts source table schemas and updates them for compatibility (InnoDB, utf8mb4) while skipping source triggers. Injects an optional suffix (e.g., `_v2`, `_v3`) into target table names. Both the intact raw schema and the newly processed schema are preserved for reference.
- **Interruptible & Resumable**: Automatically logs progress to `migration_state.json`. If a network error or crash occurs mid-migration, simply run the script again to resume precisely where it left off, down to the exact byte in the CSV file.
- **Detailed Summary Reports**: At the end of a run, a `.csv` report is generated (and seamlessly appended to across multiple runs) in the `output/` directory, detailing table names, execution times, DDL statements used, row counts, and any errors encountered.
- **Automatic Fallbacks**: Gracefully switches from UNIX Socket to TCP/IP connections if needed, ensuring local migrations are as frictionless as possible.

## Project Structure
```text
python-data-migration-v3/
├── .env                        # Environment variables for credentials (optional)
├── requirements.txt            # Python dependencies
├── migration_state.json        # Auto-generated state file tracking progress
├── output/                     # Generated schema and data artifacts
│   ├── migration_summary_<suffix>.csv
│   ├── csv/                    # Exported CSV data chunks
│   ├── export/                 # Downloaded SQL/CSV files from standalone exports
│   ├── processed/              # Adjusted SQL schema definitions
│   └── raw/                    # Original intact raw mysqldump schemas
└── src/
    ├── config.py               # Database connection info and executable paths
    └── table_csv_migration.py  # Main execution script
```

## Setup
1. Create a Python virtual environment: `python -m venv venv`
2. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - Mac/Linux: `source venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`
4. Update `src/config.py` with default database connection details or create a `.env` file containing variables like `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DEST_DB_HOST`, etc.

## Usage

### Interactive Mode
Run the interactive console application:
```bash
python src/table_csv_migration.py
```
A menu-driven wizard will guide you through connecting to source and destination databases, supplying a table suffix (e.g., `_v3`), and selecting your desired operation:
1. Migrate by table name pattern (Regex)
2. Migrate exact table names
3. Migrate from View to Table
4. Export Schema and Data only (Download)
5. Import Schema or Data from file (Upload)
6. Exit

### Headless Mode (Automated)
You can automate migrations by passing a JSON configuration file via the `--headless` flag. This bypasses all interactive prompts and uses passwords securely read from your `.env` file.

```bash
python src/table_csv_migration.py --headless config.json
```

**Example `config.json` using a regex pattern:**
```json
{
  "suffix": "_v3",
  "db_host": "10.10.10.96",
  "db_database": "source_db_name",
  "db_user": "source_user",
  "dest_db_host": "10.10.10.133",
  "dest_db_database": "dest_db_name",
  "dest_db_user": "dest_user",
  "pattern": "^lib_.*",
  "multithreaded": true,
  "skip_extract": false,
  "force_restart": false
}
```

**Example `config.json` using an exact list of tables:**
```json
{
  "suffix": "_v2",
  "db_host": "127.0.0.1",
  "db_database": "source_db_name",
  "db_user": "root",
  "dest_db_host": "127.0.0.1",
  "dest_db_database": "dest_db_name",
  "dest_db_user": "root",
  "tables": ["lib_users", "lib_address"],
  "force_restart": true
}
```

**Example `config.json` for Standalone Export:**
```json
{
  "action": "export",
  "export_format": "sql",
  "suffix": "_v3",
  "db_host": "127.0.0.1",
  "db_database": "source_db_name",
  "db_user": "root",
  "pattern": "^lib_.*"
}
```

**Example `config.json` for Standalone CSV Import:**
```json
{
  "action": "import",
  "import_format": "csv",
  "import_filepath": "C:\\path\\to\\file.csv",
  "target_table": "lib_address_v3",
  "existing_table_action": "truncate",
  "multithreaded": true,
  "dest_db_host": "10.10.10.133",
  "dest_db_database": "dest_db_name",
  "dest_db_user": "dest_user"
}
```

## State & Resumption
If the migration halts due to connection timeout or manual interruption (`Ctrl+C`), do not delete `migration_state.json`. 
- **Interactive Mode**: Rerun the command or restart the interactive session and select "Yes" when prompted to resume. 
- **Headless Mode**: The script will automatically detect the state file and resume precisely where it left off. To force a fresh migration and ignore previous progress, add `"force_restart": true` to your `config.json`.
