#!/bin/sh

echo "Loading data into MySQL"

mysql  -u root --local-infile --database=tpch < copy_all_tables.sql

echo "DONE loading data into MySQL."

