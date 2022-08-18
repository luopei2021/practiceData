import logging
import os
import pandas as pd

from scripts.table import TableModel
from scripts.hive_sql_parser import parse_hive_sql

logging.basicConfig()

logger = logging.getLogger(__name__)

DEFAULT_TEMPLATE = 'normal.sql'


class ScriptGenerator:

    def __init__(self, resource_dir, output_dir, overwrite, env='prod'):
        self.resource_dir = resource_dir
        self.output_dir = output_dir
        self.overwrite = overwrite
        self.target_db_name = "default"
        self.created_files = []
        sql_file = os.path.join(self.resource_dir, 'sql', 'hiveddl.sql')
        sql = open(sql_file, 'r').read()
        self.tables = parse_hive_sql(sql)

    def process_on_csv(self, name, primary_key, write_mode, mocha_db_name, timestamp, template, past_day):
        target_table = self.tables.tables[name]
        columns = [col for col in target_table.columns]

        table = TableModel(self.resource_dir, self.output_dir, name, primary_key, write_mode, mocha_db_name,
                           timestamp, columns, self.target_db_name, past_day)
        etl = table.render_etl_sql(template, self.overwrite)
        if etl:
            self.created_files.append(etl)


class DDLGenerator:
    def __getSql(self, sqlFilePath):
        sql_file = os.path.join(self.resource_dir, 'sql', sqlFilePath)
        return open(sql_file, 'r').read()

    def __init__(self, resource_dir, tableConfigs):
        self.resource_dir = resource_dir

        self.tableConfigs = tableConfigs


def generate_script(resource_dir, output_dir, overwrite, env):
    generator = ScriptGenerator(resource_dir, output_dir, overwrite, env)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    config_file = os.path.join(resource_dir, 'config', 'config.csv')
    df = pd.read_csv(config_file)
    df.fillna('', inplace=True)
    for row in df.itertuples():
        generator.process_on_csv(row.name, row.primary_key, row.write_mode, row.mocha_db_name, row.timestamp,
                                 row.template, row.past_day)
    return generator.created_files
