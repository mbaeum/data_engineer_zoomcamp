from pathlib import Path
import click
from typing import Iterator
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dataclasses import dataclass

# from dynaconf import settings, LazySettings

# from dynaconf import settings

# conf = Dynaconf(root_path=Path(__file__).parent)
# settings = LazySettings(ROOT_PATH_FOR_DYNACONF=Path(__file__).parent)


@dataclass
class S3Fetcher:
    path: str
    # conf: LazySettings
    bucket: str = "nyc-tlc"
    base: str = "https://s3.amazonaws.com"
    iterator: bool = True
    chunksize: int = 100000

    def _full_path(self) -> str:
        return f"{self.base}/{self.bucket}/{self.path}"

    def get_data(self) -> Iterator[pd.DataFrame]:
        url = self._full_path()
        print(f"fetching file from {url}")
        return pd.read_csv(
            url,
            iterator=self.iterator,
            chunksize=self.chunksize,
        )


@dataclass
class PostgresConnection:
    host: str = "localhost"
    api: str = "postgresql"
    port: int = 5432
    user: str = "root"
    password: str = "root"
    database: str = "ny_taxi"

    def _to_dns(self) -> str:
        return f"{self.api}://{self.user}:{self.password}@{self.host}/{self.database}"

    def get_connected(self) -> Engine:
        dns = self._to_dns()
        print(f"connected to {dns}")
        return create_engine(dns)


@dataclass
class Pipeline:
    fetcher: S3Fetcher
    connector: PostgresConnection
    datetime_cols: list[str]

    def __post_init__(self):
        fname = self.fetcher.path.split("/")[-1]
        self.name = fname.split(".")[0]

    def extract(self) -> Iterator[pd.DataFrame]:
        return self.fetcher.get_data()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.datetime_cols:
            df[col] = pd.to_datetime(df[col])
        print(f"transformed {self.datetime_cols}")
        return df

    def load(self, dfs: Iterator[pd.DataFrame], create_table: bool = True) -> None:
        engine = self.connector.get_connected()
        print(dfs)
        for df in dfs:
            if create_table:
                df.head(0).to_sql(name=self.name, con=engine, if_exists="replace")
            create_table = False
            print(f"writing...")
            self.transform(df).to_sql(name=self.name, con=engine, if_exists="append")

    def run(self):
        extraced = self.extract()
        # transformed = self.transform(extraced)
        self.load(extraced)


@click.command()
@click.option("-p", "--path", type=str, required=True)
@click.option("-d", "--datetime-col", multiple=True)
def main(path: str, datetime_col: list[str]) -> None:
    fetcher = S3Fetcher(path)
    connector = PostgresConnection()
    pipeline = Pipeline(fetcher, connector, datetime_cols=datetime_col)
    pipeline.run()


if __name__ == "__main__":
    main()
    # print(settings.s3)
