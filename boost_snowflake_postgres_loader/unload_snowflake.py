#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import pathlib

from raw import db

snowflake_url = os.getenv("SNOWFLAKE_URL")
postgres_url = os.getenv("POSTGRES_URL")
local_stage = "/disk1/pgsql/snowflake-boost-load"


def copy_table(tbl):
    db.engine(snowflake_url)
    db.result("remove @boost_unload_stage", returns="proxy")
    db.result(f"copy into @boost_unload_stage/{tbl}/ from {tbl}")
    print(f"{tbl} copied!")


def get_data_files(tbl):
    db.engine(snowflake_url)
    pathlib.Path(f"{local_stage}/{tbl}").mkdir(parents=True, exist_ok=True)
    db.result(f"get @boost_unload_stage/{tbl} file://{local_stage}/{tbl}")
    print(f"files fetched to {local_stage}/{tbl}")


def list_tables():
    db.engine(snowflake_url)
    tbls_result = db.result(
        "select table_name from information_schema.tables "
        "where table_schema = 'SNOWFLAKE_BOOST_ADJUSTED' order by table_name"
    )
    tbls = [tbl["table_name"] for tbl in tbls_result]
    return tbls


def main():
    db.engine(snowflake_url)
    db.result("use schema snowflake_boost_adjusted")
    db.result(
        "create or replace file format boost_unload_csv "
        "type = 'CSV' FIELD_DELIMITER = '|' "
        "field_optionally_enclosed_by = '\"'; "
    )
    db.result(
        "create or replace stage boost_unload_stage file_format = boost_unload_csv;"
    )
    tbls = list_tables()
    for tbl in tbls:
        copy_table(tbl)
        get_data_files(tbl)


if __name__ == "__main__":
    main()
