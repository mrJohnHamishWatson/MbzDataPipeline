import click

from transform.data import DataTransform


@click.group()
def cli():
    pass


@cli.command(name="process_music_data")
def process() -> None:
    DataTransform().process()


def main():
    cli()
    #process()


if __name__ == '__main__':
    main()
