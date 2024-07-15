import argparse
import sys
from generate_dummy_records import generate_n_random_addresses


def generate_addr_index_update_statement(
    id_start: int, id_end: int, num_addr=15, col_name="full_text_index"
):
    value = f"""|{"|".join(generate_n_random_addresses(num_addr, ""))}"""
    return f"""UPDATE transaction SET {col_name}='{value}' WHERE id BETWEEN {id_start} AND {id_end}"""


def generate_addr_index_update_statements(
    id_start=1,
    id_end=10_000_000,
    batch_size=100,
    num_addr=20,
    col_name="full_text_index",
):
    for _id_start in range(id_start, id_end + 1, batch_size):
        _id_end = _id_start + batch_size - 1
        yield _id_start, _id_end, generate_addr_index_update_statement(
            _id_start, _id_end, num_addr, col_name
        )


from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

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
    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}", echo=False
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="import_address_index",
        description="Import address index to database"
    )
    parser.add_argument("--id_start", default=1)
    parser.add_argument("--id_end", default=None)
    parser.add_argument("--buffer_size", default=10)

    args = parser.parse_args()
    
    id_start = int(args.id_start)
    id_end = int(args.id_end) if args.id_end is not None else None
    buffer_size = int(args.buffer_size)
    print(id_start, id_end, buffer_size)
    
    buffer_size = 10
    buffer = list[str]()
    create_session = sessionmaker(
        engine,
        autoflush=True,
    )
    session = create_session()
    id_end = id_end or int(session.execute(text("SELECT MAX(id) FROM transaction")).one()[0])
    batch_size = 100
    print("updating from id", id_start, "until id ", id_end)

    for _id_start, _id_end, stmt in generate_addr_index_update_statements(
        id_start, id_end, col_name="free_text_index", batch_size=batch_size
    ):
        if len(buffer) == buffer_size:
            for _stmt in buffer:
                session.execute(text(_stmt))
            session.commit()
            print("committed", _id_start, "->", _id_start + batch_size * buffer_size - 1)
            buffer.clear()
        buffer.append(stmt)
