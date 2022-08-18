import click
import os

from scripts.etl_script_generator import generate_script


@click.group('etl-cli')
def etl_script_generator_cli():
    pass


@click.command('create')
@click.option('--resource', default='resources', help='resources folder')
@click.option('--output', default='sql', help='output folder')
@click.option('--overwrite/--no-overwrite', default=False, help='overwrite existing file or not')
@click.option('--env', default='prod', help='environment')
def etl_generator(resource, output, overwrite, env):
    files = generate_script(resource, output, overwrite, env)
    click.echo(f'{len(files)} files generated')
    click.echo(f'Files:')
    for file in files:
        click.echo(f'\t{file}')
    click.echo('Done')


@click.command('clean')
@click.option('--output', default='sql', help='output folder')
def etl_cleaner(output):
    for root, dirs, files in os.walk(output):
        if root == "sql":
            for file in files:
                if 'ETL' in file:
                    os.remove(os.path.join(root, file))
                    click.echo(f'remove file: {file}')


etl_script_generator_cli.add_command(etl_generator)
etl_script_generator_cli.add_command(etl_cleaner)

if __name__ == '__main__':
    etl_script_generator_cli()
