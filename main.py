import sys

import click

from manager.images import ImagesDB
from manager.jolpica import JolpicaDB


@click.group("manager")
def cli():
    """Database management CLI"""
    pass


@cli.command("update")
@click.option("--directory", "-d", help="Directory of database", required=True)
def update_db(directory: str):
    jolpica_db = JolpicaDB(directory)
    jolpica_db.update()


@cli.command("update-images")
@click.option("--images-directory", "-di", help="Directory of images", required=True)
@click.option("--data-directory", "-dd", help="Directory of database", required=True)
def update_images_db(images_directory: str, data_directory: str):
    jolpica_db = JolpicaDB(data_directory)
    images_manager = ImagesDB(images_directory)

    # update drivers
    drivers = jolpica_db.get_drivers()
    images_manager.update_images_drivers(drivers)

    # update constructors
    constructors = jolpica_db.get_constructors()
    images_manager.update_images_constructors(constructors)


@cli.command("create")
@click.option("--directory", "-d", help="Directory of database")
def create_db(directory: str):
    jolpica_db = JolpicaDB(directory)
    jolpica_db.create()


if __name__ == "__main__":
    try:
        sys.exit(cli())
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)