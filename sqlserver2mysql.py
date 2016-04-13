# sqlserver2mysql
# A simple tool for exporting Mysql create table sql statement
# from a Sql Server database.
import argparse
import pprint
import re
import cPickle as pickle
import pymssql

__VERSION__ = '0.1.0'


class Connection(object):
   '''
   A wrapper of sql server connection.
   '''
   def __init__(self, server=None, port="1433", user=None, password=None, database=None):
       self._conn = None
       self._conn_params = None
       if server and port and user and password and database:
           self._conn_params = {'server': server, 'port': port, 'user': user,
                                'password': password, 'database': database}

   def set_connection(self, server=None, port="1433", user=None, password=None, database=None):
       if self._conn:
           self._conn.close()
           self._conn = None
       if server and port and user and password and database:
           self._conn_params = {'server': server, 'port': port, 'user': user,
                                'password': password, 'database': database}

   def refresh(self):
       if not self._conn_params:
           raise Exception("No conn params.")
       if self._conn:
           self._conn.close()
           self._conn = None      

   def get_connection(self):
       if self._conn is None:
           self._conn = pymssql.connect(timeout=90, **self._conn_params)
       return self._conn

   def get_cursor(self):
       return self.get_connection().cursor()

conn = Connection()

class ColumnDesc:
    TABLE_CATALOG = 0
    TABLE_SCHEMA = 1
    TABLE_NAME = 2
    COLUMN_NAME = 3
    ORDINAL_POSITION = 4
    COLUMN_DEFAULT = 5
    IS_NULLABLE = 6
    DATA_TYPE = 7
    CHARACTER_MAXIMUM_LENGTH = 8
    CHARACTER_OCTET_LENGTH = 9
    NUMERIC_PRECISION = 10
    NUMERIC_PRECISION_RADIX = 11
    NUMERIC_SCALE = 12
    DATETIME_PRECISION = 13
    CHARACTER_SET_CATALOG = 14
    CHARACTER_SET_SCHEMA = 15
    CHARACTER_SET_NAME = 16
    COLLATION_CATALOG = 17
    COLLATION_SCHEMA = 18
    COLLATION_NAME = 19
    DOMAIN_CATALOG = 20
    DOMAIN_SCHEMA = 21
    DOMAIN_NAME = 22
    IS_IDENTITY = 23


def handle_decimal_type(column_desc):
    precision = column_desc[ColumnDesc.NUMERIC_PRECISION]
    scale = column_desc[ColumnDesc.NUMERIC_SCALE]
    return "DECIMAL(%s, %s)" % (precision, scale)


def handle_char_type(column_desc):
    length = column_desc[ColumnDesc.CHARACTER_MAXIMUM_LENGTH]
    if length <= 255:
        return "CHAR(%s)" % length
    else:
        return "LONGTEXT"

def handle_text_type(column_desc):
    length = column_desc[ColumnDesc.CHARACTER_MAXIMUM_LENGTH]
    if 0 <= length <= 65535:
        return "VARCHAR(%s)" % length
    elif 0 <= length <= 166777215:
        return "MEDIUMTEXT"
    else:
        return "LONGTEXT"

def handle_blob_type(column_desc):
    data_type = column_desc[ColumnDesc.DATA_TYPE].upper()
    length = column_desc[ColumnDesc.CHARACTER_MAXIMUM_LENGTH]
    if data_type == "BINARY" and length <= 255:
        return "BINARY(%s)" % length
    elif data_type == "VARBINARY" and 0<= length <= 65535:
        return "VARBINARY(%s)" % length
    elif 0 <= length <= 65535:
        return "BLOB"
    elif 0 <= length <= 166777215:
        return "MEDIUMBLOB"
    else:
        return "LONGBLOB"

TYPES_MAP = {
    'INT': 'INT',
    'TINYINT': 'TINYINT',
    'SMALLINT': 'SMALLINT',
    'BIGINT': 'BIGINT',
    'BIT': 'TINYINT(1)',
    'FLOAT': 'FLOAT',
    'REAL': 'FLOAT',
    'NUMERIC': handle_decimal_type,
    'DECIMAL': handle_decimal_type,
    'MONEY': handle_decimal_type,
    'SMALLMONEY': handle_decimal_type,
    'CHAR': handle_char_type,
    'NCHAR': handle_char_type,
    'VARCHAR': handle_text_type,
    'NVARCHAR': handle_text_type,
    'DATE': 'DATE',
    'DATETIME': 'DATETIME',
    'DATETIME2': 'DATETIME',
    'SMALLDATETIME': 'DATETIME',
    'DATETIMEOFFSET': 'DATETIME',
    'TIME': 'TIME',
    'TIMESTAMP': 'TIMESTAMP',
    'ROWVERSION': 'TIMESTAMP',
    'BINARY': handle_blob_type,
    'VARBINARY': handle_blob_type,
    'TEXT': handle_text_type,
    'NTEXT': handle_text_type,
    'IMAGE': handle_blob_type,
    'SQL_VARIANT': handle_blob_type,
    'TABLE': handle_blob_type,
    'HIERARCHYID': handle_blob_type,
    'UNIQUEIDENTIFIER': 'VARCHAR(64)',
    'SYSNAME': 'VARCHAR(160)',
    'XML': 'TEXT'
}


def get_column_type(column_desc):
    source_type = column_desc[ColumnDesc.DATA_TYPE].upper()
    target_type = TYPES_MAP.get(source_type)
    if target_type is None:
        return None
    elif isinstance(target_type, basestring):
        return target_type
    else:
        return target_type(column_desc)


def convert_column_default(col):
    default_value = col[ColumnDesc.COLUMN_DEFAULT]
    if default_value is None:
        return ''
    if default_value.startswith('((') and default_value.endswith('))'):
        default_value = default_value[2:-2]
    elif default_value.startswith('(') and default_value.endswith(')'):
        default_value = default_value[1:-1]

    if '(' in default_value and ')' in default_value:
        default_value = None
    elif default_value.startswith('CREATE'):
        default_value = None
    return ' DEFAULT %s' % default_value if default_value else ''


def get_create_table(columns, primary_key_column=None, indexes=None,
                     drop_if_exists=False, create_if_not_exists=False):
    columns = sorted(columns, key=lambda x: x[ColumnDesc.ORDINAL_POSITION])
    indexes = indexes or []
    table_name = columns[0][ColumnDesc.TABLE_NAME]
    cols = []
    auto_increment_column = None
    for col in columns:
        cols.append("`%s` %s %s%s%s" % (col[ColumnDesc.COLUMN_NAME],
                                          get_column_type(col),
                                          " AUTO_INCREMENT" if col[ColumnDesc.IS_IDENTITY] else '', 
                                          convert_column_default(col),
                                          " NOT NULL" if col[ColumnDesc.IS_NULLABLE] == 'NO' else ''))
        auto_increment_column = col[ColumnDesc.COLUMN_NAME] if col[ColumnDesc.IS_IDENTITY] else auto_increment_column

    if primary_key_column:
        cols.append('PRIMARY KEY (`%s`)' % primary_key_column)
        if auto_increment_column and\
                not filter(lambda x: auto_increment_column.upper() in x[2].upper(), indexes):
            indexes.append(('ix_unique_%s' % auto_increment_column,
                            'unique', auto_increment_column))
    elif auto_increment_column:
        cols.append('PRIMARY KEY (`%s`)' % auto_increment_column)

    for index in indexes:
        unique = 'UNIQUE' if 'unique' in index[1].lower() else ''
        cols.append('%s KEY `%s` (%s)' % (unique, index[0][:64], re.sub("\([+-]+\)", "", index[2])))
    sql = 'CREATE TABLE %s`%s` (\n%s);' %\
          ('IF NOT EXISTS ' if create_if_not_exists else '',
           table_name,
           ',\n'.join(cols))
    if drop_if_exists:
        sql = 'DROP TABLE IF EXISTS %s;\n %s' % (table_name, sql)
    return sql


def filter_tables(table_schema=None, table_name=None, table_type=None):
    def match_rules(row):
        return False if (table_schema and row[1] != table_schema)\
                        or (table_name and row[2] != table_name)\
                        or (table_type and row[3] != table_type)\
               else True                        
    cursor = conn.get_cursor()
    sql = "select * from information_schema.TABLES"
    cursor.execute(sql)
    tables = cursor.fetchall()
    cursor.close()
    tables = filter(match_rules, tables)
    return tables


def query_table_columns(table_name):
    cursor = conn.get_cursor()
    sql = "select *, COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') as IS_IDENTITY from information_schema.COLUMNS where TABLE_NAME='%s'" % table_name
    cursor.execute(sql)
    columns = list(cursor.fetchall())
    cursor.close()
    return columns


def query_table_primary_key(table_name):
    sql = "select CONSTRAINT_NAME from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='%s' and CONSTRAINT_TYPE='PRIMARY KEY'" % table_name
    cursor = conn.get_cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    cursor.close()
    if not r:
        return None
    constraint_name = r[0][0]
    sql = "select COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where TABLE_NAME='%s' and CONSTRAINT_NAME='%s'"\
          % (table_name, constraint_name)
    cursor = conn.get_cursor()
    cursor.execute(sql)
    r = cursor.fetchall()
    cursor.close()
    return r[0][0] if r else None


def query_table_indexes(table_name):
    cursor = conn.get_cursor()
    sql = "sp_helpindex '%s'" % table_name
    cursor.execute(sql)
    indexes = list(cursor.fetchall()) if cursor.description else []
    cursor.close()
    return indexes


def generate(table_schema=None, table_name=None, table_type=None,
             drop_if_exists=False, create_if_not_exists=False):
    tables = filter_tables(table_schema=table_schema, table_name=table_name, table_type=table_type)
    for table in tables:
        table_name = table[2]
        columns = query_table_columns(table_name)
        primary_key_column = query_table_primary_key(table_name)
        indexes = query_table_indexes(table_name)
        print get_create_table(columns,
                               indexes=indexes,
                               primary_key_column=primary_key_column,
                               drop_if_exists=drop_if_exists,
                               create_if_not_exists=create_if_not_exists)
        print


def get_args():
    parser = argparse.ArgumentParser(description='Generate mysql create table and create index statement from a Sql Server.')
    parser.add_argument('--server', type=str, default="127.0.0.1",
                        help='Server address of Sql Server.')
    parser.add_argument('--port', type=str, default="1433",
                        help='Server port of Sql Server.')
    parser.add_argument('--user', type=str, required=True,
                        help='Username of Sql Server.')
    parser.add_argument('--password', type=str, required=True,
                        help='Password of Sql Server.')
    parser.add_argument('--database', type=str, required=True,
                        help='Database name of Sql Server.')
    parser.add_argument('--table_schema', type=str,
                        help='Optional, filter tables based on the schema.')
    parser.add_argument('--table_name', type=str,
                        help='Optional, filter name based on the schema.')
    parser.add_argument('--table_type', type=str,
                        help='Optional, filter type based on the schema.')
    parser.add_argument('--drop_if_exists', action='store_true', default=False,
                        help='Optional, add the drop table if exists statement.')
    parser.add_argument('--create_if_not_exists', action='store_true', default=False,
                        help='Optional, add if not exists in the create statement.')
    return parser.parse_args()


def main():
    args = get_args()
    conn.set_connection(server=args.server, port=args.port, user=args.user,
                        password=args.password, database=args.database)
    generate(table_schema=args.table_schema,
             table_name=args.table_name,
             table_type=args.table_type,
             drop_if_exists=args.drop_if_exists,
             create_if_not_exists=args.create_if_not_exists)


if __name__ == '__main__':
    main()
