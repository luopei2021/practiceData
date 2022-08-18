import sqlparse

class Column(object):
    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type

    def __str__(self):
        return '{} {}'.format(self.name, self.data_type)


class Table(object):
    def __init__(self, name):
        self.name = name.replace('`', '').replace('"', '')
        self.columns = []
        self.constants = []
        self.columnStr = ""

    def add_column(self, column):
        self.columns.append(column)

    def __str__(self):
        return '{} ({})'.format(self.name, ', '.join(map(str, self.columns)))


class Tables(object):
    def __init__(self):
        self.tables = dict()

    def add_table(self, table):
        self.tables[table.name] = table

    def get_table(self, name):
        return self.tables[name]

    def __str__(self):
        return '\n'.join(map(str, self.tables.values()))


def __get_table_name(tokens):
    for token in reversed(tokens):
        if token.ttype is None:
            return token.value
    return " "


def parse_hive_sql(sql):
    tables = Tables()
    parse = sqlparse.parse(sql)
    for stmt in parse:
        tokens = [t for t in sqlparse.sql.TokenList(stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]
        for i, token in enumerate(tokens):
            if token.ttype is None:
                txt = token.value
                table = Table(txt[:txt.find("\n(")])
                columnStrs = txt[txt.find("\n(") + 2:txt.rfind(")")].strip()
                for colStr in columnStrs.split("\n"):
                    colName = colStr.split(' ')[0].replace(',', '').strip()
                    colType = colStr.split(' ')[1].replace(',', '').strip()
                    table.add_column(Column(colName, colType))
                tables.add_table(table)
                break
    return tables


if __name__ == "__main__":
    sql = open('../resources/sql/hiveddl.sql', 'r').read()
    tables = parse_hive_sql(sql).tables
    for tableName in tables.keys():
        if "product" in tableName:
            print(tableName)
            print(["{0} {1}".format(col.name, col.data_type) for col in tables[tableName].columns])
