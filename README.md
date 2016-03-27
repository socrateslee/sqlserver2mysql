# sqlserver2mysql

A simple tool for exporting Mysql create table sql statement from a Sql Server database.

## Install and Requirements
sqlserver2mysql requires freetds and pymssql to work. Install on Ubuntu:

```
# freetds
sudo apt-get install freetds-dev
# pymssql
pip install pymssql
```

## Example
Export msdb from a Sql Server in 192.168.1.200:

```
# Export to msdb
python sqlserver2mysql.py --server 192.168.1.200 --user sa --password sa --database msdb --drop_if_exists > msdb.sql
# Get expoted sql into mysql
mysql -uroot msdb < msdb.sql
```

## Usage

```
usage: sqlserver2mysql.py [-h] [--server SERVER] [--port PORT] --user USER
                          --password PASSWORD --database DATABASE
                          [--table_schema TABLE_SCHEMA]
                          [--table_name TABLE_NAME] [--table_type TABLE_TYPE]
                          [--drop_if_exists]

Generate mysql create table and create index statement from a Sql Server.

optional arguments:
  -h, --help            show this help message and exit
  --server SERVER       Server address of Sql Server.
  --port PORT           Server port of Sql Server.
  --user USER           Username of Sql Server.
  --password PASSWORD   Password of Sql Server.
  --database DATABASE   Database name of Sql Server.
  --table_schema TABLE_SCHEMA
                        Optional, filter tables based on the schema.
  --table_name TABLE_NAME
                        Optional, filter name based on the schema.
  --table_type TABLE_TYPE
                        Optional, filter type based on the schema.
  --drop_if_exists      Optional, add the drop table if exists statement.
```