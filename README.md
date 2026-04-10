# Python CSV Data Migration Utility

This utility is a spin-off of the original Python Data Migration utility. Rather than processing huge SQL dump files filled with `INSERT` statements, this project uses a more efficient CSV-based approach:

1. **Schema Dump**: Dumps only the database schema using `mysqldump --no-data`.
2. **Schema Processing**: Modifies the table engine (InnoDB), character set (`utf8mb4`), and collation (`utf8mb4_0900_ai_ci`) for the new table structure, and saves it. The raw schema file is then automatically deleted to save space.
3. **Data Export to CSV**: Uses Python's native MySQL connections and `csv` library to export data directly to a `.csv` file in chunks, ensuring perfect memory stability and `utf8mb4` encoding support.
4. **Schema Load**: Runs the modified `.sql` schema script to create the table structure in the destination DB.
5. **Data Load (CSV)**: Executes `LOAD DATA LOCAL INFILE` to safely and rapidly bulk insert the CSV data into the destination database.

## Project Structure
```text
csv-data-migration/
├── requirements.txt
└── src/
    ├── config.py         # Database connection info and executable paths
    └── csv_migration.py  # Main execution script
```

## Setup
1. Create a Python virtual environment: `python -m venv venv`
2. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - Mac/Linux: `source venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`
4. Update `src/config.py` with default database connection details or create a `.env` file.

## Usage
Run the interactive console application:
```bash
python src/csv_migration.py
```
Follow the prompts to connect to the source and destination databases and select table patterns or exact lists to migrate.
