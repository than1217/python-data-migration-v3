# Python CSV Data Migration Utility

This utility is a highly efficient, chunk-based CSV data migration tool for MySQL databases. It streams data out of a source database into local CSV files, adjusts schema options for compatibility (forcing `InnoDB`, `utf8mb4` character set, `utf8mb4_0900_ai_ci` collation, and `DYNAMIC` row format), and rapidly loads the data into a destination database using `LOAD DATA INFILE`.

By avoiding massive `.sql` dump files filled with `INSERT` statements, this utility avoids memory spikes, dramatically reduces export/import times, and easily recovers from connection drops or process interruptions via a robust state-tracking system.

## Key Features
- **Low Memory Footprint**: Uses Python's native `mysql.connector` with unbuffered cursors to stream source data directly into chunked `.csv` files.
- **High-Speed Ingestion**: Leverages MySQL's native `LOAD DATA INFILE` for bulk data loading, falling back to `LOAD DATA LOCAL INFILE` or standard batch `INSERT` logic if server restrictions apply.
- **Smart Schema Modification**: Automatically extracts source table schemas and updates them for compatibility (InnoDB, utf8mb4) while injecting an optional suffix (e.g., `_v2`, `_v3`) into the target table names.
- **Interruptible & Resumable**: Automatically logs progress to `csv_migration_state.json`. If a network error or crash occurs mid-migration, simply run the script again to resume precisely where it left off, down to the exact byte in the CSV file.
- **Detailed Summary Reports**: At the end of a run, a `.csv` report is generated in the `output/` directory, detailing table names, execution times, DDL statements used, row counts, and any errors encountered.
- **Automatic Fallbacks**: Gracefully switches from UNIX Socket to TCP/IP connections if needed, ensuring local migrations are as frictionless as possible.

## Project Structure
```text
python-data-migration-v3/
├── .env                        # Environment variables for credentials (optional)
├── requirements.txt            # Python dependencies
├── csv_migration_state.json    # Auto-generated state file tracking progress
├── output/                     # Generated schema and data artifacts
│   ├── migration_summary_<suffix>.csv
│   ├── csv/                    # Exported CSV data chunks
│   ├── processed/              # Adjusted SQL schema definitions
│   └── raw/                    # Temporary raw mysqldump schemas
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
A menu-driven wizard will guide you through connecting to source and destination databases, supplying a table suffix (e.g., `_v3`), and selecting table patterns (Regex) or exact lists to migrate.

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
  "dest_db_host": "10.10.10.133",
  "dest_db_database": "dest_db_name",
  "pattern": "^lib_.*"
}
```

**Example `config.json` using an exact list of tables:**
```json
{
  "suffix": "_v2",
  "db_host": "127.0.0.1",
  "db_database": "source_db_name",
  "dest_db_host": "127.0.0.1",
  "dest_db_database": "dest_db_name",
  "tables": ["lib_users", "lib_address"]
}
```

## State & Resumption
If the migration halts due to connection timeout or manual interruption (`Ctrl+C`), do not delete `csv_migration_state.json`. Simply rerun the command or restart the interactive session and select "Yes" when prompted to resume. The tool will automatically skip completed tables and resume CSV loading from the exact byte offset where it stopped.
