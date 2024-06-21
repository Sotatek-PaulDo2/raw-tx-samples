import datetime
from functools import lru_cache, partial
import json
import math
from multiprocessing import Pool
import random
from typing import Iterable, Sequence
import pandas as pd
from sqlalchemy import create_engine

"""
1. 10M records/1 chain/100 Tx per address = 100K addresses
2. 
"""

all_tx_types = [
    "swap",
    "token_send",
    "token_receive",
    "nft_send",
    "nft_receive",
    "mint",
    "burn",
    "approve",
]

chains = [
    "Ethereum",
    "Bitcoin",
    "THORChain",
    "Solana",
    "Cosmos",
]

addresses = [
    "0x40181e2B1B2d21587d0806E60D151C7e8Ec2A6E0",
    "bc1p9k295akkfdvewfra9nu7s3zu4832q6aftdw9zglayaauzveyaxcqanzm85",
    "thor14mh37ua4vkyur0l5ra297a4la6tmf95mt96a55",
    "GFaFZ7GSgf4bvJkidoMATmvVvYSw4YgUYTNPPAFWZnE7",
    "cosmos1g6psfwt4ehw06m0j60ghrlwgv2q69yq40fnwwa",
]

timestamp_range = (
    1483272000,  # 2017-01-01 00:00:00
    1704110400,  # 2024-01-01 00:00:00
)


def random_timestamp():
    ts = random.randint(*timestamp_range)
    return datetime.datetime.fromtimestamp(ts)


@lru_cache
def raw_data():
    reader = open("EVM/moralis/wallet_activity/token_single_swap.json", mode="r")
    return json.dumps(json.load(reader))


def random_amount(min=1, max=1000):
    return random.randint(min, max)


def dummy_record(
    tx_id: int,
    address: str,
    chain: str,
    tx_type: str,
):
    now = datetime.datetime.now()
    tx_record = {
        "block_height": random_amount(),
        "block_index": random_amount(),
        "address": address,
        "chain": chain,
        "date_time": random_timestamp(),
        "provider": chain,
        "raw_data": raw_data(),
        "token_involved": chain,
        "token_amount": random_amount(),
        "created_at": now,
        "updated_at": now,
        "id": tx_id,
    }
    tx_type_record = {
        "id": tx_id,
        "type": tx_type,
        "transaction_id": tx_id,
        "created_at": now,
        "updated_at": now,
    }
    return tx_record, tx_type_record


def generate_random_eth_address():
    """Generates a random Ethereum address."""
    return "0x" + "".join(random.choices("0123456789abcdef", k=40))


def generate_n_random_addresses(n: int, prefix: str):
    """Generates `n` random Ethereum addresses based on a reference address."""
    return tuple(f"{prefix}{generate_random_eth_address()}" for _ in range(n))


# PostgreSQL connection details
username = "postgres"
password = "changeme"
host = "127.0.0.1"
port = "54320"
database = "postgres"
tx_table_name = "transaction"
tx_type_table_name = "transaction_type"

# Create SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
)


def batch_insert_tx_with_types(batch_tx, batch_tx_types):
    batch_tx_df = pd.DataFrame(batch_tx)
    batch_tf_types_df = pd.DataFrame(batch_tx_types)
    batch_tx_df.to_sql(tx_table_name, engine, if_exists="append", index=False)
    batch_tf_types_df.to_sql(
        tx_type_table_name, engine, if_exists="append", index=False
    )
    batch_tx.clear()
    batch_tx_types.clear()
    print("batch inserted")


def create_records(addresses: Sequence[str], chain: str, id: int):
    tx_type = all_tx_types[id % len(all_tx_types)]
    address = addresses[id % len(addresses)]
    tx_record, tx_type_record = dummy_record(id, address, chain, tx_type)
    return tx_record, tx_type_record


def create_single_chain_dummy_record_batch(
    start_id: int, end_id: int, addresses: Sequence[str], chain: str = chains[0]
):
    """Generate by ID range, inclusive"""
    txs_and_types = map(
        lambda id: create_records(addresses, chain, id), range(start_id, end_id + 1)
    )
    txs, types = zip(*txs_and_types)
    return txs, types


def handle_batch(
    batch_size: int,
    count: int,
    chain: str,
    addresses: Sequence[str],
    offset: int,
    batch_idx: int,
):
    start_id = batch_idx * batch_size + offset
    end_id = min(count, (batch_idx + 1) * batch_size - 1) + offset
    txs, types = create_single_chain_dummy_record_batch(
        start_id, end_id, addresses, chain
    )
    print("saving batch", batch_idx)
    pd.DataFrame(txs).to_csv(f"txs.{chain}.{batch_idx}.csv", index=False)
    pd.DataFrame(types).to_csv(f"tx_types.{chain}.{batch_idx}.csv", index=False)
    print(f"saved batch {batch_idx} on chain {chain}")


def generate_dummy_records_by_chain_offset(
    chain: str = chains[0],
    count: int = 10_000_000,
    address_count=100_000,
    batch_size: int = 1_000_000,
    offset: int = 0,
):
    """Generate CSV files containing records to be copied to PSQL table"""
    addresses = generate_n_random_addresses(address_count, chain)
    batch_count = math.ceil(count / batch_size)
    print(f"start generating for chain {chain}")
    process_pool = Pool(4)
    handle_batch_partial = partial(
        handle_batch, batch_size, count, chain, addresses, offset
    )

    with process_pool:
        process_pool.starmap(handle_batch_partial, [(i,) for i in range(batch_count)])
    process_pool.join()


if __name__ == "__main__":
    """Modify chain, count, offset"""
    generate_dummy_records_by_chain_offset(
        chain="ETH",
        count=100_000,
        address_count=1_000,
        batch_size=20_000,
    )
