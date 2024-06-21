import pandas as pd
from sqlalchemy import create_engine


# PostgreSQL connection details
username = "myuser"
password = "mypassword"
host = "127.0.0.1"
port = "5432"
database = "postgres"
tx_table_name = "transaction"
tx_type_table_name = "transaction_type"

# Create SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
)


def import_data(file_name: str):
    table_name = tx_table_name if file_name.startswith("txs") else tx_type_table_name
    pd.read_csv(file_name).to_sql(table_name, engine, if_exists="append", index=False)

if __name__ == "__main__":
    filenames = [
        "txs.ETH.0.csv",
        "txs.ETH.1.csv",
        "txs.ETH.2.csv",
        "txs.ETH.3.csv",
        "txs.ETH.4.csv",
    ]
    for file_name in filenames:
        import_data(file_name)
