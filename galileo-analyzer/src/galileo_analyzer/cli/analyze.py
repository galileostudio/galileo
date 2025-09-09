import click


@click.command()
def main():
    """Run deep analysis on selected jobs"""
    click.echo("Deep analysis functionality - Coming soon!")


if __name__ == "__main__":
    main()

# poetry run galileo-inventory --provider aws --region us-west-2 --profile default
