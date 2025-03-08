import polars as pl
import pandas as pd
import numpy as np
import time
from joblib import Parallel, delayed
# Generate small dataset (50 rows, 10 columns)
N = 50
data = {f"col_{i}": np.random.randint(1, 100, N) for i in range(10)}

df_pandas = pd.DataFrame(data)
df_polars = pl.DataFrame(data).lazy()

# Column transformations (overwrite columns instead of adding new ones)
def transform_pandas(df):
    df = df.copy()  # ✅ Ensures new columns overwrite existing ones
    df["col_sum"] = df.iloc[:, :10].sum(axis=1)
    df["col_mean"] = df.iloc[:, :10].mean(axis=1)
    df["col_std"] = df.iloc[:, :10].std(axis=1)
    df["col_log"] = np.log1p(df["col_0"])
    return df

def transform_polars(df, i):
    df_new = df.with_columns([
        pl.sum_horizontal(pl.all().exclude(["col_sum", "col_mean", "col_std", "col_log"])).alias(f"col_sum{i}"),
        pl.mean_horizontal(pl.all().exclude(["col_sum", "col_mean", "col_std", "col_log"])).alias(f"col_mean{i}"),
        pl.all().exclude(["col_sum", "col_mean", "col_std", "col_log"]).std().alias(f"col_std{i}"),
        pl.col("col_0").log1p().alias(f"col_log{i}")
    ])

    return df_new

# Function for joblib parallel processing
def transform_pandas_parallel(df, num):
    return Parallel(n_jobs=-1)(
        delayed(transform_pandas)(df.copy()) for _ in range(num)
    )


# Measure execution time for 20 iterations
num=10000
##### Pandas
start = time.time()
for _ in range(num):
    df_pandas = transform_pandas(df_pandas)  # ✅ Ensures fresh transformation
pandas_time = time.time() - start



###### Joblib parallel processing
start = time.time()
df_pandas_parallel = transform_pandas_parallel(df_pandas, num)
pandas_parallel_time = time.time() - start



###### Polars
start = time.time()
for i in range(num):
    df_polars = transform_polars(df_polars, i)  # ✅ Overwrites columns correctly
polars_time = time.time() - start


# Compute speedup factors
joblib_vs_pandas_speedup = pandas_time / pandas_parallel_time
polars_vs_joblib_speedup = pandas_parallel_time / polars_time
polars_vs_pandas_speedup = pandas_time / polars_time

# Output results
results = pd.DataFrame({
    "Library": ["Pandas", "Pandas + joblib Parallel", "Polars"],
    "Execution Time (s)": [pandas_time, pandas_parallel_time, polars_time],
    "Speedup Factor (vs Pandas)": [1, joblib_vs_pandas_speedup, pandas_time / polars_time],
    "Speedup Factor (vs Pandas + Joblib)": [pandas_time / pandas_parallel_time, 1, polars_vs_joblib_speedup],
    "Speedup Factor (vs Polars)": [polars_vs_pandas_speedup, polars_time / pandas_parallel_time, 1]
})





# Output results




print(results)


#                       Library     Execution Time (s)          Speedup Factor (vs Pandas)  
# 0                    Pandas           15.485952                    1.000000   
# 1  Pandas + joblib Parallel            6.453373                    2.399668   
# 2                    Polars            0.314211                   49.285181   
