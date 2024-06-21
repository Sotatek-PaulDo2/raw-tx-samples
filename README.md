# raw_tx_samples

## Dependency Installation

```(shell)
pip install -r requirements.txt
```

## Create CSV files

1. Modify chain name, count, offset, batch size, address count in `generate_dummy_records.py`'s main section
2. Run command:

```(shell)
python generate_dummy_records.py
```

## Import data to DB

1. Modify filename list in main section of file `import_data.py`
2. Run command:

```(shell)
python import_data.py
```
