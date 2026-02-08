import json
import csv
import io
from collections import defaultdict
import s3fs
from general_functions import load_config


def analyze_bls_and_data_usa_data(event, context):

    # ---- Load config ----
    config = load_config("config.yaml")

    aws_bucket_name = config["aws_s3_connection_info"]["bucket_name"]

    data_usa_s3_directory = config["data_usa_connection_info"]["s3_directory"]
    data_usa_s3_file_name = config["data_usa_connection_info"]["s3_file_name"]

    bls_data_s3_directory = config["bureau_labor_statistics_connection_info"]["s3_directory"]
    bls_data_s3_file_name = "pr.data.0.Current"

    fs = s3fs.S3FileSystem()

    # ==========================================================
    # ---- DataUSA ----
    # ==========================================================

    data_usa_s3_path = f"s3://{aws_bucket_name}/{data_usa_s3_directory}/{data_usa_s3_file_name}"

    with fs.open(data_usa_s3_path) as f:
        raw = json.load(f)

    data_usa = []
    for row in raw["data"]:
        clean_row = {
            k.strip(): (v.strip() if isinstance(v, str) else v)
            for k, v in row.items()
        }
        data_usa.append(clean_row)

    # ==========================================================
    # ---- BLS ----
    # ==========================================================

    bls_data_s3_path = f"s3://{aws_bucket_name}/{bls_data_s3_directory}/{bls_data_s3_file_name}"

    bls_data = []

    with fs.open(bls_data_s3_path, "r") as f:
        reader = csv.DictReader(f, delimiter="\t")

        for row in reader:
            clean_row = {
                k.strip(): (v.strip() if isinstance(v, str) else v)
                for k, v in row.items()
            }

            try:
                clean_row["value"] = float(clean_row.get("value") or 0)
            except (ValueError, TypeError):
                clean_row["value"] = 0.0

            bls_data.append(clean_row)

    # ==========================================================
    # ---- Mean / Std for DataUSA (2013–2018) ----
    # ==========================================================

    populations = []

    for row in data_usa:
        try:
            year = int(row["Year"])
            pop = int(row["Population"])
            if 2013 <= year <= 2018:
                populations.append(pop)
        except (ValueError, KeyError):
            continue

    mean_population = sum(populations) / len(populations)

    variance = sum((x - mean_population) ** 2 for x in populations) / len(populations)
    std_population = variance ** 0.5

    # ==========================================================
    # ---- Best year per series ----
    # ==========================================================

    yearly_sums = defaultdict(float)

    for row in bls_data:
        series_id = row.get("series_id")
        year = row.get("year")
        value = row.get("value", 0)

        if series_id and year:
            yearly_sums[(series_id, year)] += value

    best_by_series = {}

    for (series_id, year), total in yearly_sums.items():
        if (
            series_id not in best_by_series
            or total > best_by_series[series_id]["value"]
        ):
            best_by_series[series_id] = {
                "series_id": series_id,
                "year": year,
                "value": total,
            }

    best_year_per_series = list(best_by_series.values())

    # ==========================================================
    # ---- Merge report for PRS30006032 Q01 ----
    # ==========================================================

    population_lookup = {}

    for row in data_usa:
        try:
            population_lookup[int(row["Year"])] = int(row["Population"])
        except (ValueError, KeyError):
            continue

    report = []

    for row in bls_data:
        if row.get("series_id") == "PRS30006032" and row.get("period") == "Q01":
            try:
                year = int(row["year"])
            except (ValueError, TypeError):
                continue

            if year in population_lookup:
                report.append({
                    "series_id": row.get("series_id"),
                    "year": year,
                    "period": row.get("period"),
                    "value": row.get("value"),
                    "Population": population_lookup[year],
                })

    # ==========================================================
    # ---- Return results ----
    # ==========================================================

    print(f"Mean population (2013-2018): {mean_population}")
    print(f"Std population (2013-2018): {std_population}")
    print(f"Best year per series: {best_year_per_series}")
    print(f"Report for PRS30006032 Q01: {report}")
    
    return {
        "mean_population_2013_2018": mean_population,
        "std_population_2013_2018": std_population,
        "best_year_per_series": best_year_per_series,
        "report_prs30006032": report,
    }
