Potential Pandas operations to convert to Polars
# Differences
- Polars is faster: >100k rows

# Column assignment / Transformations
We want to add columns whos values are derived from other columns:
In pandas this would be:

	df.assign(
		tenXValue=lambda df_: df_.value * 10,
		hundredXValue=lambda df_: df_.value * 100
	)

These column assignments are executed sequentially.

In Polars we add columns to df using the .with_columns method:

	df.with_columns(
		tenXValue=pl.col("value") * 10,
		hundredXValue=pl.col("value") * 100,
	)

These column assignments are executed in parallel.

# Filtering
**Method 1: Filtering in Pandas**  
```
filtered_pandas = df_pandas[(df_pandas["value"] > 50) & (df_pandas["category"] == "A")]  
print(f"Pandas Filtering Time: {end_time - start_time:.4f} seconds") 
```
- Slower due to row-based memory layout; Creates a copy of the Data


**Method 2: Filtering in Polars**  
```
filtered_polars = df_polars.filter((pl.col("value") > 50) & (pl.col("category") == "A"))
print(f"Polars Filtering Time: {end_time - start_time:.4f} seconds")
print(filtered_polars.head())
```

- Multi-threaded execution (Faster); Memory-efficient w/a lazy API(auto query optimization); Does not copy unnecessary data
- Syntax slightly different from Pandas