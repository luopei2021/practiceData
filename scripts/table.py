import logging
import os

import jinja2

logger = logging.getLogger(__name__)


class TableModel:
    def __init__(self, resource_dir, output_dir, name, primary_key, write_mode, mocha_db_name, timestamp, columns,
                 target_db_name, past_day):
        self.name = name
        self.ods_db_name = 'default'
        self.target_db_name = target_db_name
        self.target_schema = 'default'
        self.write_mode = write_mode
        self.primary_key = primary_key.split(",")
        self.output_dir = output_dir
        self.timestamp = timestamp
        self.past_day = int(past_day) if past_day != '' else 90
        self.mocha_db_name = mocha_db_name
        template_path = os.path.join(resource_dir, 'templates')
        self.jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=template_path))
        self.columns = columns

    def __str__(self):
        return self.name + "(" + ",".join([str(c) for c in self.columns]) + ")"

    def render_file(self, template_name, output_file_name, output_dir, overwrite=False):
        template = self.jinja_env.get_template(template_name)
        ddl_scripts = template.render(
            dict((name, getattr(self, name)) for name in dir(self) if not name.startswith('__')))
        current_dir = os.getcwd()
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        os.chdir(output_dir)
        if os.path.exists(output_file_name) and not overwrite:
            logger.info(f"{output_file_name} already exists, skipping")
            os.chdir(current_dir)
            return
        with open(output_file_name, 'w') as f:
            f.write(ddl_scripts)
        os.chdir(current_dir)
        return os.path.join(output_dir, output_file_name)

    def render_etl_sql(self, template, overwrite=False, file_suffix=""):
        output_dir = self.output_dir
        sql_file_name = f"{self.name}{file_suffix}.sql"
        return self.render_file(template, sql_file_name.lower(), output_dir, overwrite)
