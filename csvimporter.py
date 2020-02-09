# -*- coding: UTF-8 -*-
import csv
import operator
import itertools

MULTIPLE_INSERT_CHUNK_SIZE = 75

TYPE_CODE_CHAR = 1
TYPE_CODE_INT = 3
TYPE_CODE_NUMERIC = 5
TYPE_CODE_DATETIME = 4


class _CsvImporterBase:
    def __init__(self, connection):
        """
        :param connection: Set the DB-API connection object.
        """
        self._con = connection  # DB-API connection object

    def read_csv(self, file, table, header=False, mapping=None, skiprows=0):
        """
        Read source csv file and set target table.
        :param file: Set the source csv filename.
        :param table: Set the target table name.
        :param header: Set the presence or absence of a header.
        :param mapping: Set the condition to convert the column name. {"oldname": 'newname'}
        :param skiprows: Set number of skipping first rows.
        """
        if header is False and mapping:
            msg = 'Can not set mapping if header is not True'
            raise CsvImporterError(msg)

    def add_column(self, option: str, name: str, type_code: int, value=None):
        """
        Add column manually.
        :param option: Set the mode.(only 'f' supported.)
        :param name: Set the column name.
        :param type_code: Set the column format.
        :param value: Set the column value.
        """
        pass

    def execute(self):
        """
        Exucute insert SQL.
        """
        pass


class CsvImporter(_CsvImporterBase):
    def __init__(self, connection):
        super().__init__(connection)

        self._csv = None  # source csv object
        self._table = None  # target table object
        self._rst = None  # recordset object

    def read_csv(self, file, table, header=False, mapping=None, skiprows=None):
        super().read_csv(file, table, header, mapping, skiprows)

        self._csv = Csv(file, header, skiprows)
        self._table = Table(self._con, table)
        self._rst = Recordset(self._csv, self._table, mapping)

    def add_column(self, option: str, name: str, type_code: int, value=None):
        super().add_column(option, name, type_code, value)

        self._rst.add_column(option, name, type_code, value)

    def execute(self):
        cursor = self._con.cursor()
        for sql in self._rst.build_insert_sql():
            print("Execute this SQL:", sql)
            cursor.execute(sql)
            self._con.commit()

        cursor.close()

    @property
    def rowcount(self):
        return self._csv.rowcount


class Csv:
    def __init__(self, file, has_header=False, skiprows=0):
        self._file = file
        self._has_header = has_header
        self._skiprows = skiprows
        self._header = None
        self._rows = []
        self._rowcount = 0

        with open(self._file, "r") as f:
            reader = csv.reader(f)

            if self._skiprows:
                for i in range(self._skiprows):
                    next(reader)

            _rows = list(reader)

            if self._has_header:
                self._header = _rows[0]
                self._rows = _rows[1:]
            else:
                self._rows = _rows

            self._rowcount = len(self._rows)

    def reader(self):
        for row in self._rows:
            yield row

    @property
    def header(self):
        return self._header

    @property
    def rowcount(self):
        return self._rowcount


class Table:
    CURSOR_DESCRIPTION_KEYS = ('name', 'type_code', 'display_size', 'internal_size', 'precision', 'scale', 'null_ok')

    def __init__(self, connection, table):
        self._table = table

        cursor = connection.cursor()
        cursor.execute('SELECT * FROM ' + self._table + ' WHERE 1=0')
        self._description = [dict(zip(self.CURSOR_DESCRIPTION_KEYS, column)) for column in cursor.description]
        cursor.close()

    @property
    def name(self) -> str:
        return self._table

    # Return table description.(It is same to expanded "cursor.description".)
    @property
    def description(self):
        return self._description


class Recordset:
    ESCAPES = (1, 4)

    def __init__(self, file: Csv, table: Table, mapping=None):
        self._csv = file
        self._table = table
        self._mapping = mapping

        self._add_columns = []

        if self._mapping:
            self._masks = [True if column in self._mapping.keys() else False for column in self._csv.header]
        else:
            self._masks = [True] * len(self.names)

    def add_column(self, option: str, name: str, type_code: int, value=None):
        self._add_columns.append(
            {
                "option": option,
                "name": name,
                "type_code": type_code,
                "value": value
            }
        )

    @property
    def names(self) -> list:
        if self._mapping:
            rst_names = [self._mapping[item] if mask else False for item, mask in zip(self._csv.header, self._masks)]
        elif self._csv.header:
            rst_names = self._csv.header
        else:
            rst_names = [column['name'] for column in self._table.description]

        rst_names.extend([column['name'] for column in self._add_columns])

        # Column name duplicate check.
        if any([True if rst_names.count(name) > 1 else False for name in rst_names if name]):
            msg = 'Duplicate column name. ' + ','.join([name for name in rst_names if name])
            raise CsvImporterError(msg)

        return rst_names

    @property
    def type_codes(self) -> list:
        table_tc = {field['name']: field['type_code'] for field in self._table.description}

        for column in self._add_columns:
            table_tc.update({column['name']: column['type_code']})

        tc = [table_tc[column] if column in table_tc.keys() else False for column in self.names]
        return tc

    @property
    def escapes(self):
        esc = [True if type_code in self.ESCAPES else False for type_code in self.type_codes]
        esc.extend([column['type_code'] for column in self._add_columns])
        return esc

    def build_insert_sql(self, multiple=MULTIPLE_INSERT_CHUNK_SIZE):
        sql_insert = 'INSERT INTO ' + self._table.name

        sql_columns = ' (' + ','.join(['[' + name + ']' for name in self.names if name]) + ')'

        for rows in _chunk(self._csv.reader(), multiple):
            sql_values = [self._convert_insert_sql_values(row) for row in rows]
            sql_string = sql_insert + sql_columns + ' VALUES' + ','.join(sql_values)
            yield sql_string

    def _convert_insert_sql_values(self, row):
        for add_column in self._add_columns:
            if add_column['option'] == 'f':
                row.append(add_column['value'])

        items = [self._escape(item, esc) for item, col, esc in zip(row, self.names, self.escapes) if col]
        sql_string = '(' + ','.join(items) + ')'
        return sql_string

    @staticmethod
    def _escape(data, is_escape):
        if isinstance(data, type(None)) or data == '':
            return 'NULL'
        elif is_escape:
            return "'" + data + "'"
        else:
            return data


# Split iterable to chunk.
def _chunk(iterable, n):
    op = operator.itemgetter(1)
    for key, sub_iter in itertools.groupby(enumerate(iterable), lambda x: x[0] // n):
        yield map(op, sub_iter)


class CsvImporterError(Exception):
    pass
