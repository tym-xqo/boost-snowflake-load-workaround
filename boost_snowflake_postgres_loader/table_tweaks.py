#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from raw import db

snowflake_url = os.getenv("SNOWFLAKE_URL")
postgres_url = os.getenv("POSTGRES_URL")
local_stage = "/tmp/"


def columns_from_table(tbl):
    db.engine(postgres_url)
    sql = (
        "select column_name from information_schema.columns "
        "where table_name = :tbl "
        "order by ordinal_position;"
    )
    column_result = db.result(sql, tbl=tbl)
    cols = [col["column_name"] for col in column_result]
    col_str_ = ", ".join(cols)
    col_str = col_str_.replace("refreshed_at", "current_timestamp as refreshed_at")
    return col_str


def create_adjusted_snowflake_table(tbl):
    col_str = columns_from_table(tbl)
    if len(col_str) < 1:
        print(f"skipping {tbl} - not in legacy boost")
        return
    db.engine(snowflake_url)

    sql = f"""
    create or replace table snowflake_boost_adjusted.{tbl} as
        select {col_str}
        from snowflake_boost_load.{tbl}
    """
    db.result(sql, returns="proxy")
    print(f"adjusted {tbl}!")
    # print(sql)


def list_tables():
    db.engine(snowflake_url)
    tbls_result = db.result(
        "select table_name from information_schema.tables "
        "where table_schema = 'SNOWFLAKE_BOOST_LOAD' order by table_name"
    )
    tbls = [tbl["table_name"].lower() for tbl in tbls_result]
    return tbls


def main():
    db.engine(snowflake_url)
    db.result("drop schema if exists snowflake_boost_adjusted", returns="proxy")
    db.result("create schema if not exists snowflake_boost_adjusted", returns="proxy")

    tbls = list_tables()
    for tbl in tbls:
        create_adjusted_snowflake_table(tbl)


if __name__ == "__main__":
    main()
