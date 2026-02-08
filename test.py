import json
import gzip
import csv
from collections import defaultdict
import s3fs
from general_functions import load_config
import io

def analyze_bls_and_data_usa_data(event, context):
    # Load config
    config = load_config("config.yaml")
    aws_bucket_name = config["aws_s3_connection_info"]["bucket_name"]

    data_usa_s3_directory = config["data_usa_connection_info"]["s3_directory"]
    data_usa_s3_file_name = config["data_usa_connection_info"]["s3_file_name"]

    bls_data_s3_directory = config["bureau_labor_statistics_connection_info"]["s3_directory"]
    bls_data_s3_file_name = "pr.data.0.Current"

    fs = s3fs.S3FileSystem()

    # ---- Load DataUSA JSON ----
    data_usa_s3_path = f"s3://{aws_bucket_name}/{data_usa_s3_directory}/{data_usa_s3_file_name}"
    with fs.open(data_usa_s3_path) as f:
        raw = json.load(f)

    data_usa = []
    for row in raw["data"]:
        clean_row = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        data_usa.append(clean_row)

    # ---- Load BLS CSV (tab-separated, gzipped) ----
    bls_data_s3_path = f"s3://{aws_bucket_name}/{bls_data_s3_directory}/{bls_data_s3_file_name}"
    bls_data = []
    with fs.open(bls_data_s3_path, "rb") as f:
        raw_bytes = f.read()

        # Try gzip first
        try:
            gzfile = gzip.open(io.BytesIO(raw_bytes), mode='rt', newline='')
            reader = csv.DictReader(gzfile, delimiter="\t")
            for row in reader:
                clean_row = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
                try:
                    clean_row['value'] = float(clean_row.get('value') or 0)
                except (ValueError, TypeError):
                    clean_row['value'] = 0
                bls_data.append(clean_row)

        except gzip.BadGzipFile:
            # Fall back to plain text
            text_file = io.StringIO(raw_bytes.decode("utf-8"))
            reader = csv.DictReader(text_file, delimiter="\t")
            for row in reader:
                clean_row = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
                try:
                    clean_row['value'] = float(clean_row.get('value') or 0)
                except (ValueError, TypeError):
                    clean_row['value'] = 0
                bls_data.append(clean_row)

    # ---- Mean / Std for DataUSA 2013–2018 ----
    populations = []
    for row in data_usa:
        try:
            year = int(row['Year'])
            pop = int(row['Population'])
            if 2013 <= year <= 2018:
                populations.append(pop)
        except (ValueError, KeyError, TypeError):
            continue

    mean_population = sum(populations) / len(populations) if populations else 0
    variance = sum((x - mean_population) ** 2 for x in populations) / len(populations) if populations else 0
    std_population = variance ** 0.5

    # ---- Best year per series ----
    series_totals = defaultdict(lambda: defaultdict(float))
    for row in bls_data:
        series_totals[row['series_id']][row['year']] += row['value']

    best_year_per_series = []
    for series_id, year_dict in series_totals.items():
        best_year, best_value = max(year_dict.items(), key=lambda x: x[1])
        best_year_per_series.append({
            "series_id": series_id,
            "year": best_year,
            "value": best_value
        })

    # ---- Merge report for series_id PRS30006032 Q01 ----
    report = []
    for row in bls_data:
        if row['series_id'] == 'PRS30006032' and row['period'] == 'Q01':
            try:
                year = int(row['year'])
                population_row = next((d for d in data_usa if int(d['Year']) == year), None)
                if population_row:
                    report.append({
                        "series_id": row['series_id'],
                        "year": year,
                        "period": row['period'],
                        "value": row['value'],
                        "Population": int(population_row['Population'])
                    })
            except (ValueError, KeyError, TypeError):
                continue


        ---- Return results ----
        return {
            "mean_population_2013_2018": mean_population,
            "std_population_2013_2018": std_population,
            "best_year_per_series": best_year_per_series,
            "report_prs30006032": report
        }