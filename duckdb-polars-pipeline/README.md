# Python + Polars Lazy + DuckDB: Hybrid Pipelines That Auto-Optimize

A high-performance data processing pipeline demonstrating the power of combining Polars' lazy evaluation with DuckDB's analytical capabilities.

## About the Project

This project showcases an efficient hybrid pipeline approach:
1.  **Polars Lazy API**: Handles data scanning, filtering, joins, and aggregations with query optimization.
2.  **DuckDB**: Performs advanced SQL analytical queries (window functions) on the processed data.

This architecture enables processing large datasets efficiently by leveraging the strengths of both tools.

Reference: [Python + Polars Lazy + DuckDB: Hybrid Pipelines That Auto-Optimize](https://medium.com/@komalbaparmar007/python-polars-lazy-duckdb-hybrid-pipelines-that-auto-optimize-e432e8f0d93d)

## Prerequisites

-   Python 3.13+
-   `uv` (recommended for dependency management) or `pip`

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd duckdb-polars-pipeline
    ```

2.  **Install dependencies:**
    
    Using `uv` (faster, recommended):
    ```bash
    uv sync
    ```

    Or using `pip`:
    ```bash
    pip install -r requirements.txt
    ```
    *(Note: If strict versions are needed, refer to `pyproject.toml` or `uv.lock`)*

## Usage

### 1. Generate Fake Data
First, generate the sample dataset (customers and orders Parquet files):

```bash
python generate_data.py
```
This will create a `data/warehouse` directory with `customers` and `orders` subdirectories.

### 2. Run the Pipeline
Execute the main pipeline:

```bash
python main.py
```

## Performance Monitoring
The project includes a `measure_performance` decorator in `utils.py` that tracks execution time and memory usage for key functions.