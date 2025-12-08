import duckdb as dd
from pathlib import Path
from time import perf_counter
from contextlib import contextmanager

RAW = Path(__file__).parent / "data" / "raw"
STAGE = Path(__file__).parent / "data" / "stage"
OUT = Path(__file__).parent / "data" / "out"

STAGE.mkdir(parents=True, exist_ok=True); OUT.mkdir(parents=True, exist_ok=True)

con = dd.connect(database=":memory:", read_only=False) # pure in-memory engine

@contextmanager
def timer(name):
    t0 = perf_counter()
    yield
    dt = perf_counter() - t0
    print(f"{name}: {dt:.2f} seconds")

# 1) Ingest: lazily scan columns you actually need
con.execute(f"""
    CREATE OR REPLACE VIEW life_expectancy_data AS
    SELECT
        Country,
        Year,
        Status,
        "Life expectancy",
        "Adult Mortality",
        "infant deaths",
        Alcohol,
        "percentage expenditure",
        "Hepatitis B",
        Measles,
        BMI,
        "under-five deaths",
        Polio,
        "Total expenditure",
        Diphtheria,
        "HIV/AIDS",
        GDP,
        Population,
        "thinness  1-19 years",
        "thinness 5-9 years",
        "Income composition of resources",
        Schooling
    FROM read_csv_auto('{RAW / "*.csv"}')
    """)

# 2) Transform: tidy enums, filter junk, standardize dates
con.execute("""
    CREATE OR REPLACE TABLE life_expectancy_data_clean AS
    SELECT
        Country as country,
        Year as year,
        Status as status,
        "Life expectancy" as life_expectancy,
        "Adult Mortality" as adult_mortality,
        "infant deaths" as infant_deaths,
        Alcohol as alcohol,
        "percentage expenditure" as percentage_expenditure,
        "Hepatitis B" as hepatitis_b,
        Measles as measles,
        BMI as bmi,
        "under-five deaths" as under_five_deaths,
        Polio as polio,
        "Total expenditure" as total_expenditure,
        Diphtheria as diphtheria,
        "HIV/AIDS" as hiv_aids,
        GDP as gdp,
        Population as population,
        "thinness  1-19 years" as thinness_1_19_years,
        "thinness 5-9 years" as thinness_5_9_years,
        "Income composition of resources" as income_composition_of_resources,
        Schooling as schooling
    FROM life_expectancy_data
    WHERE Country IS NOT NULL
        AND Year IS NOT NULL
        AND Status IS NOT NULL
        AND "Life expectancy" IS NOT NULL
        AND population IS NOT NULL
""")

# 3) Validate: cheap data tests that actually catch pain
result = con.execute("""
    SELECT
        sum(CASE WHEN life_expectancy < 0 OR life_expectancy > 120 THEN 1 ELSE 0 END) AS invalid_life_expectancy,
        sum(CASE WHEN year < 1800 OR year > 2100 THEN 1 ELSE 0 END) AS invalid_years,
        sum(CASE WHEN status NOT IN ('Developed', 'Developing') THEN 1 ELSE 0 END) AS invalid_status
    FROM life_expectancy_data_clean
""")
tests = result.fetchone()

assert tests is not None, "No test results returned"
assert tests[0] == 0, "Invalid life expectancy values found"
assert tests[1] == 0, "Invalid years found"
assert tests[2] == 0, "Unexpected status values"

# 4) Aggregate & publish
with timer("aggregate & write"):
    con.execute(f"""
        COPY (
            SELECT country, 
                AVG(life_expectancy) AS avg_life_expectancy, 
                sum(population) AS total_population
            FROM life_expectancy_data_clean
            GROUP BY country
        ) TO '{OUT / "life_expectancy_by_country.parquet"}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)
con.close()

print("OK →", OUT / "life_expectancy_by_country.parquet")

    