# Python Data Migration Utility

This utility is a high-performance, resilient, and feature-rich data migration tool for MySQL databases. It is designed to handle large-scale data transfers efficiently by leveraging chunk-based CSV processing, multi-threading, and robust error handling. It avoids the pitfalls of traditional `mysqldump` files by streaming data, which minimizes memory usage and dramatically reduces migration times.

The tool can be run in a user-friendly interactive mode or fully automated via a JSON configuration for headless execution.

## Key Features

- **High-Speed Data Transfer**: Achieves rapid data extraction (100k-300k+ rows/sec) by using the `mysql-connector-python` C-extension and high-speed `LOAD DATA INFILE` for ingestion.
- **Low Memory Footprint**: Streams data using unbuffered cursors and chunking to prevent memory spikes, even with terabyte-scale tables.
- **Multi-threaded Loading**: Drastically accelerates data insertion by splitting large CSV files into chunks and loading them into the destination database concurrently.
- **Intelligent Chunking & Fallbacks**: Uses Primary Key chunking for optimal export speed. Automatically falls back to `LIMIT/OFFSET` pagination for views or tables without a suitable primary key.
- **Resilient & Resumable**: Tracks progress in a `migration_state.json` file. If the process is interrupted, it can be resumed exactly where it left off, down to the byte.
- **Comprehensive Migration Modes**:
    - **Single Table/View Migration**: Migrate individual tables or views with fine-grained control.
    - **Batch Migration**: Migrate multiple tables at once using a regex pattern or a comma-separated list.
    - **View-to-Table Materialization**: Automatically generates a `CREATE TABLE` statement from a source view and migrates the data into a physical table.
    - **Multi-Table Merge**: Merges data from multiple source tables or views into a single destination table. The schema is inferred from the first source table, and data from all other sources is appended.
- **Standalone Operations**:
    - **Export**: Download schema and data as full SQL dumps or chunked CSVs.
    - **Import**: Upload data from a local SQL or CSV file directly to the destination.
- **Smart Schema Handling**: Automatically extracts and modifies source schemas for compatibility (e.g., forcing InnoDB, utf8mb4) and can add a suffix to table names (e.g., `_v2`, `_v3`).
- **Automatic Trigger Management**: Drops triggers on target tables before loading data to prevent execution errors and `DEFINER` permission issues.
- **Detailed Reporting**: Generates a `migration_summary.csv` file with detailed statistics for each run, including execution times, row counts, DDL statements, and success/failure remarks.
- **User-Friendly Interface**: Offers both an interactive, menu-driven CLI for ease of use and a headless mode for automation.

## Project Structure

```
python-data-migration-v3/
├── .env                        # (Optional) Environment variables for credentials
├── requirements.txt            # Python dependencies
├── migration_state.json        # Auto-generated state file to track progress
├── output/
│   ├── migration_summary_<suffix>.csv
│   ├── csv/                    # Exported CSV data files
│   ├── export/                 # Files from standalone export operations
│   ├── processed/              # Modified SQL schema files
│   └── raw/                    # Original (raw) SQL schema files
└── src/
    ├── config.py               # Database connections and executable paths
    └── table_csv_migration.py  # The main script
```

## Setup

1.  **Create a virtual environment**:
    ```bash
    python -m venv venv
    ```
2.  **Activate it**:
    -   Windows: `venv\Scripts\activate`
    -   macOS/Linux: `source venv/bin/activate`
3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure connections**:
    -   Update `src/config.py` with your default database credentials.
    -   *Alternatively*, create a `.env` file in the project root for sensitive information like passwords (e.g., `DB_PASSWORD="your_pass"`).

## Usage

### Interactive Mode

To start the menu-driven interface, run:
```bash
python src/table_csv_migration.py
```

You will be guided through selecting the migration type (e.g., PPISv2, PPISv3), connecting to source and destination databases, and choosing a migration operation.

**Main Menu Options:**
1.  **PPISv2 / PPISv3 / Custom**: Choose a predefined configuration or a custom one.
    -   **Specify pattern**: Migrate all tables matching a regex.
    -   **Specify exact names**: Migrate a comma-separated list of tables.
    -   **Migrate from View to Table**: Materialize a single view.
    -   **Merge Multiple Tables/Views**: Load data from multiple sources into one destination table.
    -   **Export Only**: Download data as SQL or CSV.
    -   **Import Only**: Upload data from a local SQL or CSV file.

### Headless Mode (for Automation)

Automate migrations using a JSON configuration file with the `--headless` flag.

```bash
python src/table_csv_migration.py --headless config.json
```

**Example `config.json` for a standard migration:**
```json
{
  "suffix": "_v3",
  "db_host": "10.10.10.96",
  "db_database": "source_db",
  "db_user": "source_user",
  "dest_db_host": "10.10.10.133",
  "dest_db_database": "dest_db",
  "dest_db_user": "dest_user",
  "pattern": "^lib_.*",
  "multithreaded": true,
  "existing_table_action": "truncate",
  "force_restart": false
}
```

**Example `config.json` for a multi-table merge:**
```json
{
  "action": "merge",
  "suffix": "_v3",
  "source_tables": ["quarter1_sales", "quarter2_sales", "quarter3_sales_v"],
  "dest_table": "annual_sales_report",
  "db_host": "10.10.10.96",
  "db_database": "source_db",
  "db_user": "source_user",
  "dest_db_host": "10.10.10.133",
  "dest_db_database": "dest_db",
  "dest_db_user": "dest_user"
}
```

| Headless Config Key         | Description                                                                    |
| --------------------------- | ------------------------------------------------------------------------------ |
| `action`                    | `migrate` (default), `merge`, `export`, or `import`.                           |
| `suffix`                    | Suffix to add to destination table names (e.g., `_v3`).                        |
| `db_host`, `db_user`, etc.  | Source and destination database connection details.                            |
| `pattern`                   | Regex pattern for selecting tables.                                            |
| `tables` / `source_tables`  | A list of exact table/view names.                                              |
| `dest_table`                | The single destination table for a merge operation.                            |
| `multithreaded`             | `true` to enable multi-threaded CSV loading.                                   |
| `num_threads`               | Number of threads to use (default 4).                                          |
| `existing_table_action`     | What to do if a table exists: `drop`, `truncate`, `skip`, or `append`.           |
| `force_restart`             | `true` to ignore `migration_state.json` and start fresh.                       |
| `export_format` / `import_format` | `sql` or `csv`.                                                                |
| `import_filepath`           | Full path to the file for the `import` action.                                 |

## State Management and Resumption

The script automatically saves its progress in `migration_state.json`. If a migration is interrupted, simply restart the script.
-   **Interactive Mode**: You will be prompted to resume.
-   **Headless Mode**: Resumption is automatic unless `"force_restart": true` is set in your config file.
