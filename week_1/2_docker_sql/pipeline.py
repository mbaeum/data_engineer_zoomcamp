import click 

@click.command()
@click.option("-d", "--date", type=str, required=True)
def main(date: str) -> None:
    click.secho(f"started pipline for {date=}")
    click.secho(f"finished pipline for {date=}")

if __name__=="__main__":
    main()