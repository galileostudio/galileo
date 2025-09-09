import click


@click.command()
@click.version_option()
def main():
    """Galileo - Intelligent Data Jobs Analyzer"""
    click.echo("Welcome to Galileo Analyzer!")
    click.echo("Use 'galileo-inventory' for job inventory analysis")
    click.echo("Use 'galileo-analyze' for deep analysis")


if __name__ == "__main__":
    main()
