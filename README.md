# CSV Normalizer

### Turn a CSV file into a  Relational DB

This project provides a way to normalize CSV data by transforming categorical columns into lookup tables and replacing the categorical values with corresponding IDs. 
The data is then either saved into a PostgreSQL database or as CSV files. 
The normalization is done based on a dynamic threshold calculated as 70% of the total number of rows in the dataset.


## Requirements

- **Apache Spark**: The project uses Spark for DataFrame manipulation.
- **Docker**: To run PostgreSQL locally.
- **Scala**: Version 2.13.
- **Java**: Version 17.
- **DBeaver**: Postgres connection.

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/mmenalla/dataset-normalizer.git
cd dataset-normalizer
```

### 2. Create postgres container
```bash
docker compose up
```

### 3. Prepare environment
1. Add a CSV file (with headers) under the `input` folder or use the `test_data.csv`.
2. Entrypoint `scala/CSVNormalizerApp.scala`.
3. Add configuration parameters  `--input input/test_data.csv --saveAsFiles false`. 
4. Add VM option `--add-exports java.base/sun.nio.ch=ALL-UNNAMED`.

### 4. Run CSVNormalizerApp

Output: There will new `Schema` created in the PostgreSQL database named after the input file name.

If `--saveAsFiles` is set to true, the relational schema will be saved as csv files under `output` folder.

