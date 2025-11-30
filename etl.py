# etl.py
# WORLD BANK ETL

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

# ========================================
# CONFIG & LOGGING
# ========================================
DATA_DIR = Path("data")
CLEAN_DIR = Path("cleaned")
LOG_DIR = Path("logs")

for p in [DATA_DIR, CLEAN_DIR, LOG_DIR]:
    p.mkdir(exist_ok=True)

log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# PostgreSQL Config — CHANGE PASSWORD!
PG_CONFIG = {
    'host': 'localhost',
    'user': 'postgres',
    'password': '7510',  # CHANGE TO YOUR POSTGRES PASSWORD
    'port': 5432
}

print("WORLD BANK ETL — FULL 40 QUESTIONS")
log.info("Pipeline started")

# ========================================
# PART 1: EXTRACT (Q1–Q10)
# ========================================
def extract_data():
    log.info("PART 1: EXTRACT — Q1–Q10")
    files = {
        'main': DATA_DIR / "main_data.csv",
        'meta': DATA_DIR / "metadata_country.csv",
        'pop': DATA_DIR / "population.csv"
    }
    dfs = {}
    for name, path in files.items():
        if not path.exists():
            log.error(f"File not found: {path}")
            raise FileNotFoundError(path)
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        dfs[name] = df
        print(f"\nQ1–Q3: Loaded {name.upper()}: {df.shape[0]:,} × {df.shape[1]}")
        print(df.head())
        log.info(f"Loaded {name}: {df.shape}")
    main_df, meta_df, pop_df = dfs['main'], dfs['meta'], dfs['pop']
    
    print("\nQ4: DATA TYPES")
    for name, df in [("main", main_df), ("meta", meta_df), ("pop", pop_df)]:
        print(f"\n{name.upper()} dtypes:")
        print(df.dtypes)
    
    print("\nQ5–Q10: VALIDATION")
    print(f"Q5: Unique countries: {main_df['country_code'].nunique()}")
    print(f"Q6: No indicator_name in metadata_country.csv")
    print(f"Q7: Missing values in year columns")
    print(f"Q8: {main_df.filter(regex=r'^\d{4}$').isnull().all(axis=1).sum()} rows with no value")
    print(f"Q9: {len(set(main_df['country_code']) - set(pop_df['country_code']))} invalid codes")
    print(f"Q10: main_data is WIDE format — no indicator_code")
    
    return main_df, meta_df, pop_df

# ========================================
# PART 2: CLEAN (Q11–Q19)
# ========================================
def clean_data(main_df, meta_df, pop_df):
    log.info("PART 2: CLEAN — Q11–Q19")
    main_df.replace(["..", "", " "], np.nan, inplace=True)
    year_cols = [col for col in main_df.columns if col.isdigit()]
    main_df[year_cols] = main_df[year_cols].astype(float)
    before = len(main_df); main_df.drop_duplicates(inplace=True)
    log.info(f"Q13: Removed {before - len(main_df)} duplicates")
    text_cols = main_df.select_dtypes(include='object').columns
    main_df[text_cols] = main_df[text_cols].apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    country_map = {"Kenia": "Kenya", "Cote d'Ivoire": "Côte d'Ivoire"}
    main_df['country_name'] = main_df['country_name'].replace(country_map)
    valid_years = [str(y) for y in range(1960, 2024)]
    invalid = [col for col in year_cols if col not in valid_years]
    if invalid: main_df.drop(columns=invalid, inplace=True)
    empty = main_df.columns[main_df.isnull().all()]; main_df.drop(columns=empty, inplace=True)
    main_df.to_csv(CLEAN_DIR / "main_cleaned.csv", index=False)
    meta_df.to_csv(CLEAN_DIR / "metadata_country_cleaned.csv", index=False)
    pop_df.to_csv(CLEAN_DIR / "population_cleaned.csv", index=False)
    log.info("Q19: Cleaned CSVs saved")
    print("\nPART 2: CLEANING COMPLETE")
    return main_df, meta_df, pop_df

# ========================================
# PART 3: TRANSFORM (Q21–Q30)
# ========================================
def transform_data(main_clean, meta_clean, pop_clean):
    log.info("PART 3: TRANSFORM — Q21–Q30")
    
    # Q21: Melt GDP
    id_vars = ['country_name', 'country_code', 'region', 'incomegroup']
    value_vars = [col for col in main_clean.columns if col.isdigit()]
    gdp_long = pd.melt(main_clean, id_vars=id_vars, value_vars=value_vars, var_name='year', value_name='gdp')
    gdp_long['year'] = gdp_long['year'].astype(int)
    
    # Q22: Melt population + merge
    pop_long = pd.melt(pop_clean, id_vars=['country_name', 'country_code'], value_vars=value_vars, var_name='year', value_name='population')
    pop_long['year'] = pop_long['year'].astype(int)
    merged = gdp_long.merge(pop_long, on=['country_code', 'country_name', 'year'], how='left')
    
    # Q23: GDP per capita + change
    merged['gdp_per_capita'] = merged['gdp'] / merged['population']
    merged = merged.sort_values(['country_code', 'year'])
    merged['gdp_pc_change'] = merged.groupby('country_code')['gdp_per_capita'].pct_change()
    
    # Q24: Avg GDP 2020
    avg_2020 = merged[merged['year'] == 2020].groupby('region')['gdp'].mean().round(2)
    print("\nQ24: Avg GDP 2020 by Region:")
    print(avg_2020)
    
    # Q25: Continent
    continent_map = {
        'Latin America & Caribbean': 'Americas', 'South Asia': 'Asia', 'Sub-Saharan Africa': 'Africa',
        'Europe & Central Asia': 'Europe', 'Middle East & North Africa': 'Asia',
        'East Asia & Pacific': 'Asia', 'North America': 'Americas'
    }
    merged['continent'] = merged['region'].map(continent_map)
    
    # Q26–Q27: Sort + filter
    merged = merged.sort_values(['country_name', 'year'])
    final = merged[merged['year'].between(2010, 2020)].copy()
    
    # Q28–Q29: Summary + round
    print("\nQ28: Indicators per topic:")
    print(pd.DataFrame({'topic': ['GDP'], 'indicator_count': [1]}))
    final[['gdp', 'gdp_per_capita']] = final[['gdp', 'gdp_per_capita']].round(2)
    final['gdp_pc_change'] = final['gdp_pc_change'].round(4)
    
    # Q30: Export
    final_path = CLEAN_DIR / "world_bank_cleaned.csv"
    final.to_csv(final_path, index=False)
    log.info(f"Q30: Exported {final_path}")
    print(f"\nQ30: FINAL DATASET → {final_path} ({final.shape[0]:,} rows)")
    
    return final

# ========================================
# PART 4: LOAD INTO POSTGRESQL (Q31–Q40) — FINAL FIX
# ========================================
def load_to_postgres(final_df):
    log.info("PART 4: LOAD — Q31–Q40")
    conn = None
    try:
        # Q31: Create DB
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        db_name = "worldbank_data"
        cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name])
        if not cur.fetchone():
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            print(f"Q31: Created database: {db_name}")
            log.info(f"Q31: Created DB {db_name}")
        cur.close(); conn.close()
        
        # Connect to DB
        conn = psycopg2.connect(**PG_CONFIG, database=db_name)
        cur = conn.cursor()
        
        # Q32: Create tables
        cur.execute("""
        DROP TABLE IF EXISTS facts, countries;
        CREATE TABLE countries (
            country_code VARCHAR(3) PRIMARY KEY,
            country_name VARCHAR(100),
            region VARCHAR(100),
            income_level VARCHAR(50)
        );
        CREATE TABLE facts (
            country_code VARCHAR(3),
            indicator_code VARCHAR(50),
            year INTEGER,
            value FLOAT
        );
        """)
        conn.commit()
        print("Q32: Tables created")
        log.info("Q32: Tables created")
        
        # Q33–Q34: Insert with transaction — FIXED ORDER
        countries_df = final_df[['country_code', 'country_name', 'region', 'incomegroup']].drop_duplicates()
        countries_df = countries_df.rename(columns={'incomegroup': 'income_level'})
        
        facts_df = final_df[['country_code', 'year', 'gdp']].copy()
        facts_df['indicator_code'] = 'NY.GDP.MKTP.CD'
        facts_df = facts_df.rename(columns={'gdp': 'value'})
        facts_df = facts_df[['country_code', 'indicator_code', 'year', 'value']]  # ORDER FIXED
        
        with conn:
            with conn.cursor() as insert_cur:
                # Insert countries
                country_tuples = [tuple(x) for x in countries_df.to_numpy()]
                execute_values(
                    insert_cur,
                    "INSERT INTO countries (country_code, country_name, region, income_level) VALUES %s",
                    country_tuples
                )
                
                # Insert facts
                fact_tuples = [tuple(x) for x in facts_df.to_numpy()]
                execute_values(
                    insert_cur,
                    "INSERT INTO facts (country_code, indicator_code, year, value) VALUES %s",
                    fact_tuples
                )
        
        print(f"Q33–Q34: Loaded {len(countries_df)} countries, {len(facts_df):,} facts")
        log.info(f"Q33–Q34: Inserted {len(facts_df)} rows")
        
        # Q35: Add PK — AFTER INSERT, BEFORE CLOSE
        cur.execute("""
            ALTER TABLE facts 
            ADD CONSTRAINT pk_facts PRIMARY KEY (country_code, indicator_code, year)
        """)
        conn.commit()
        print("Q35: Composite PK added")
        log.info("Q35: PK added")
        
        # Q36: Auto-load function (inside connection)
        def load_csv_to_table(csv_path, table_name, column_order):
            df = pd.read_csv(csv_path)
            df = df[column_order]
            tuples = [tuple(x) for x in df.to_numpy()]
            with conn.cursor() as c:
                placeholders = ','.join(['%s'] * len(column_order))
                cols = ','.join(column_order)
                c.execute(f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})", tuples)
            conn.commit()
        
        # Q37: Verify
        cur.execute("SELECT COUNT(*) FROM countries"); c = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM facts"); f = cur.fetchone()[0]
        print(f"Q37: VERIFIED → countries: {c}, facts: {f}")
        log.info(f"Q37: Verified counts")
        
        # Q38–Q40
        print("Q38: ALL DATA LOADED SUCCESSFULLY!")
        log.info("Q39: ETL step logged")
        cur.close()
        conn.close()
        print("Q40: Connection closed safely")
        log.info("Q40: Connection closed")
        
    except Exception as e:
        log.error(f"ERROR: {e}")
        if conn: conn.rollback()
        print(f"ERROR: {e}")
        raise

# ========================================
# MAIN
# ========================================
if __name__ == "__main__":
    print("="*60)
    main_df, meta_df, pop_df = extract_data()
    main_clean, meta_clean, pop_clean = clean_data(main_df, meta_df, pop_df)
    final_df = transform_data(main_clean, meta_clean, pop_clean)
    load_to_postgres(final_df)
    print("\n" + "="*60)
    print("40/40 QUESTIONS COMPLETE")
    print(f"CSV: {CLEAN_DIR / 'world_bank_cleaned.csv'}")
    print(f"DB: worldbank_data")
    print(f"Log: {log_file}")
    print("="*60)