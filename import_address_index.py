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
    num_addr=15,
    col_name="full_text_index",
): 
    for _id_start in range(id_start, id_end + 1, batch_size):
        _id_end = _id_start + batch_size - 1
        yield generate_addr_index_update_statement(_id_start, _id_end, num_addr, col_name)


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
    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
)

if __name__ == "__main__":
    buffer_size = 10
    buffer = list[str]()
    create_session = sessionmaker(engine, autoflush=True)
    session = create_session()
    id_end = session.execute(text("SELECT MAX(id) FROM transaction")).one()[0]
    print("updating from id 1 until id ", id_end)
    for stmt in generate_addr_index_update_statements(1, id_end, col_name="free_text_index"):
        if len(buffer) == buffer_size:
            for _stmt in buffer:
                session.execute(text(_stmt))
            print(buffer)
            print()
            buffer.clear()
        buffer.append(stmt)
