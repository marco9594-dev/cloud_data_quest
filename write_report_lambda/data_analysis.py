import pandas as pd
from general_functions import *
import s3fs 
import json 

def lambda_handler(event, context):
    # Load config
    config = load_config("config.yaml")

    aws_bucket_name = config["aws_s3_connection_info"]["bucket_name"]

    data_usa_s3_directory = config["data_usa_connection_info"]["s3_directory"]
    data_usa_s3_file_name = config["data_usa_connection_info"]["s3_file_name"]

    bls_data_s3_directory = config["bureau_labor_statistics_connection_info"]["s3_directory"]
    bls_data_s3_file_name = "pr.data.0.Current"

    fs = s3fs.S3FileSystem()

    # ---- DataUSA ----
    data_usa_s3_path = f"s3://{aws_bucket_name}/{data_usa_s3_directory}/{data_usa_s3_file_name}"
    with fs.open(data_usa_s3_path) as f:
        raw = json.load(f)
    df_data_usa = pd.json_normalize(raw["data"])
    df_data_usa.columns = df_data_usa.columns.str.strip()
    df_data_usa = df_data_usa.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # ---- BLS ----
    bls_data_s3_path = f"s3://{aws_bucket_name}/{bls_data_s3_directory}/{bls_data_s3_file_name}"
    df_bls_data = pd.read_csv(bls_data_s3_path, sep="\t", compression="gzip", dtype=str)
    df_bls_data.columns = df_bls_data.columns.str.strip()
    df_bls_data = df_bls_data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df_bls_data['value'] = pd.to_numeric(df_bls_data['value'], errors='coerce')

    # ---- Mean / Std for DataUSA ----
    df_filtered = df_data_usa[(df_data_usa['Year'] >= 2013) & (df_data_usa['Year'] <= 2018)].copy()
    df_filtered['Population'] = pd.to_numeric(df_filtered['Population'])
    mean_population = df_filtered['Population'].mean()
    std_population = df_filtered['Population'].std()

    # ---- Best year per series ----
    yearly_sums = df_bls_data.groupby(['series_id', 'year'])['value'].sum().reset_index()
    yearly_sums.columns = ['series_id', 'year', 'total_value']
    best_year_per_series = yearly_sums.loc[yearly_sums.groupby('series_id')['total_value'].idxmax()]
    best_year_per_series = best_year_per_series.rename(columns={'total_value': 'value'})

    # ---- Merge report for series_id PRS30006032 ----
    filtered_bls = df_bls_data[(df_bls_data['series_id'] == 'PRS30006032') & (df_bls_data['period'] == 'Q01')].copy()
    filtered_bls['year'] = pd.to_numeric(filtered_bls['year'], errors='coerce')
    report = filtered_bls.merge(df_data_usa[['Year', 'Population']], 
                                left_on='year', 
                                right_on='Year', 
                                how='inner')
    report = report.drop(['Year', 'footnote_codes'], axis=1)

    # ---- Return results ----
    return {
        "mean_population_2013_2018": mean_population,
        "std_population_2013_2018": std_population,
        "best_year_per_series": best_year_per_series.to_dict(orient="records"),
        "report_prs30006032": report.to_dict(orient="records")
    }
