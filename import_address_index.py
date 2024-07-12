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
    for _id_start in range(id_start, id_end, batch_size):
        _id_end = _id_start + batch_size - 1
        yield generate_addr_index_update_statement(_id_start, _id_end, num_addr, col_name)


if __name__ == "__main__":
    buffer_size = 10
    buffer = list[str]()
    for stmt in generate_addr_index_update_statements(1, 100_000):
        if len(buffer) == buffer_size:
            print(buffer)
            print()
            buffer.clear()
        buffer.append(stmt)
