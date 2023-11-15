# boost-snowflake-load-workaround

simple python script to dump table data from Snowflake for loading into Boost database

- `boost_snowflake_postgres_loader/table_tweaks.py` rearranges columns from `snowflake_boost_load` tables to match Postgres target tables in `boost_reporting` database, creating new tables in a schema named `snowflake_boost_adjusted`.
- `boost_snowflake_postgres_loader/unload_snowflake.py` - copies all the tables in `snowflake_boost_adjusted` to directories in a named stage, and downloads the staged `csv.gz` files to a local directory, which can be configured by adjusting `local_stage` setting near the top of the Python file.
