from pathlib import Path
from typing import Iterator
import pandas as pd
from sqlalchemy import create_engine
from dataclasses import dataclass


@dataclass
class PostgresCreds:
    host: str = "localhost"
    api: str = "postgresql"
    port: int = 5432
    user: str = "root"
    password: str = "root"
    database: str = "ny_taxi"

    def to_dns(self) -> str:
        return f"{self.api}://{self.user}:{self.password}@{self.host}/{self.database}"


def create_pg_engine(creds: PostgresCreds):
    dns = creds.to_dns()
    return create_engine(dns)

def create_pg_table(df: pd.DataFrame, con, name: str = "yellow_taxi_data"):
    df.to_sql(name=name, con=con, if_exists="replace")

def insert_into_pg_table(df_iter: Iterator[pd.DataFrame], con, name: str = "yellow_taxi_data"):
    for df in df_iter:
        df = convert_to_dt(df)
        df.to_sql(name=name, con=con, if_exists="append")

def load_data(fname: str = "yellow_tripdata_2021-01.csv") -> Iterator[pd.DataFrame]:
    path = Path(__file__).parent.joinpath(fname)
    if path.is_file:
        return pd.read_csv(path, iterator=True, chunksize=100000)

def convert_to_dt(
    df: pd.DataFrame, 
    cols: list[str] = [
        "tpep_pickup_datetime", 
        "tpep_dropoff_datetime"]
        ) -> pd.DataFrame:
    # out = df.copy()
    for col in cols:
        df[col] = pd.to_datetime(df[col])
    return df

def get_create_table_stmt(df: pd.DataFrame, name: str = "yellow_tripdata_2021-01.csv") -> str:
    return pd.io.sql.get_schema(df, name=name)


def main():
    df_iter = load_data()
    creds = PostgresCreds()
    engine = create_pg_engine(creds)
    df = next(df_iter)
    df = convert_to_dt(df)
    create_pg_table(df.head(0), engine)
    insert_into_pg_table(df_iter, engine)


if __name__=="__main__":
    main()
