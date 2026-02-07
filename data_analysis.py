# %%
import pandas as pd
from general_funtions import *
import s3fs 
import json 

# %%
config = load_config("config.yaml")

aws_bucket_name = config["aws_s3_connection_info"]["bucket_name"]

data_usa_s3_directory = config["data_usa_connection_info"]["s3_directory"]
data_usa_s3_file_name = config["data_usa_connection_info"]["s3_file_name"]

bls_data_s3_directory = config["bureau_labor_statistics_connection_info"]["s3_directory"]
bls_data_s3_file_name = "pr.data.0.Current"


# %% [markdown]
# Get Data USA Data and apply basic data cleansing

# %%
data_usa_s3_path = f"s3://{aws_bucket_name}/{data_usa_s3_directory}/{data_usa_s3_file_name}"

fs = s3fs.S3FileSystem()

with fs.open(data_usa_s3_path) as f:
    raw = json.load(f)

df_data_usa = pd.json_normalize(raw["data"])

# Trim whitespace from all column headers
df_data_usa.columns = df_data_usa.columns.str.strip()

# Trim whitespace from all column values
df_data_usa = df_data_usa.applymap(lambda x: x.strip() if isinstance(x, str) else x)

print(df_data_usa)

# %% [markdown]
# Load BLS Data and apply some basic data cleansing based on data quality issues found when using data downstream in analysis

# %%
bls_data_s3_path = f"s3://{aws_bucket_name}/{bls_data_s3_directory}/{bls_data_s3_file_name}"

df_bls_data = pd.read_csv(bls_data_s3_path, sep="\t", compression="gzip", dtype=str)

# Trim whitespace from all column headers
df_bls_data.columns = df_bls_data.columns.str.strip()

# Trim whitespace from all column values
df_bls_data = df_bls_data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Convert value to numeric for calculations
df_bls_data['value'] = pd.to_numeric(df_bls_data['value'], errors='coerce')

print(df_bls_data)

# %% [markdown]
# Using the dataframe from the population data API (Part 2), generate the mean and the standard deviation of the annual US population across the years [2013, 2018] inclusive.

# %%
# Filter df_data_usa for years 2013-2018 and calculate mean and std of Population
df_filtered = df_data_usa[(df_data_usa['Year'] >= 2013) & (df_data_usa['Year'] <= 2018)].copy()
df_filtered['Population'] = pd.to_numeric(df_filtered['Population'])

mean_population = df_filtered['Population'].mean()
std_population = df_filtered['Population'].std()

print(f"Mean Population (2013-2018): {mean_population}")
print(f"Standard Deviation (2013-2018): {std_population}")

# %% [markdown]
# Using the dataframe from the time-series (Part 1), For every series_id, find the best year: the year with the max/largest sum of "value" for all quarters in that year. Generate a report with each series id, the best year for that series, and the summed value for that year. 

# %%
# Group by series_id and year, sum the values for all quarters
yearly_sums = df_bls_data.groupby(['series_id', 'year'])['value'].sum().reset_index()
yearly_sums.columns = ['series_id', 'year', 'total_value']

# For each series_id, find the year with the maximum total value
best_year_per_series = yearly_sums.loc[yearly_sums.groupby('series_id')['total_value'].idxmax()]

# Rename columns for clarity
best_year_per_series = best_year_per_series.rename(columns={'total_value': 'value'})

# Display the report
print(best_year_per_series)

# %% [markdown]
# Using both dataframes from Part 1 and Part 2, generate a report that will provide the value for series_id = PRS30006032 and period = Q01 and the population for that given year (if available in the population dataset).

# %%
# Filter df_bls_data for the specific series_id and period
filtered_bls = df_bls_data[(df_bls_data['series_id'] == 'PRS30006032') & (df_bls_data['period'] == 'Q01')].copy()

# Convert year to numeric for merging
filtered_bls['year'] = pd.to_numeric(filtered_bls['year'], errors='coerce')

# Merge with population data
report = filtered_bls.merge(df_data_usa[['Year', 'Population']], 
                            left_on='year', 
                            right_on='Year', 
                            how='inner')

# Drop duplicate Year column and display
report = report.drop(['Year', 'footnote_codes'], axis=1)

print(report)


