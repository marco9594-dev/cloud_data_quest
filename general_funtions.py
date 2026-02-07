import requests
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime, timezone
import yaml
import boto3

def load_config(file_path):
    with open(file_path) as f:
        config = yaml.safe_load(f)

    return config

def get_data_via_rest_api(url, headers=None):

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response

def parse_and_format_bls_api_response(api_response, bls_api_base_url, bls_api_directory_extension):
    api_response = BeautifulSoup(api_response.text, "html.parser")
    
    pre_block = api_response.find("pre")
    if pre_block is None:
        return []

    # regex for IIS directory format
    pattern = re.compile(
        r"(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M).*?<a href=\"[^\"]+\">([^<]+)</a>"
    )

    results = []
    for timestamp_raw, filename in pattern.findall(str(pre_block)):
        if filename.startswith("["):  # skip folders
            continue

        # parse the IIS timestamp assuming local time, then convert to UTC
        datetime_local = datetime.strptime(timestamp_raw, "%m/%d/%Y %I:%M %p")
        datetime_utc = datetime_local.replace(tzinfo=timezone.utc)  # assume Zulu/UTC
        timestamp_zulu = datetime_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        full_url = (
            f"{bls_api_base_url}/"
            f"{bls_api_directory_extension}/"
            f"{filename}"
        )

        results.append({
            "file_name": filename,
            "last_updated": timestamp_zulu,
            "full_url": full_url
        })

    return results

def build_s3_client(region_name):
    s3 = boto3.client(
        "s3",
        region_name=region_name 
    )

    return s3

def write_to_s3(s3_client, bucket_name, file_name, data, content_type="application/octet-stream"):
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=data,
        ContentType=content_type
    )

    return("Upload successful")

def read_from_s3(s3_client, bucket_name, file_name):
    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=file_name
    )

    return response["Body"].read()

def delete_from_s3(s3_client, bucket_name, file_name):
    s3_client.delete_object(Bucket=bucket_name, Key=file_name)
    
    return "Delete successful"

def s3_object_exists(s3_client, bucket_name, file_name):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_name)
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise

def sync_bls_files_to_s3(source_file_list, s3_client, bucket_name, bucket_directory, manifest_log_flile, api_request_headers=None):
    
    session = requests.Session()

    # Load manifest log file from S3 or create empty if it doesn't exist
    if s3_object_exists(s3_client, bucket_name, manifest_log_flile):
        manifest = json.loads(
            read_from_s3(s3_client, bucket_name, manifest_log_flile)
        )
    else:
        manifest = []

    manifest_dict = {f["file_name"]: f for f in manifest}
    source_dict = {f["file_name"]: f for f in source_file_list}

    # Determine files to upload
    files_to_upload = [
        src for file_name, src in source_dict.items()
        if file_name not in manifest_dict
        or src["last_updated"] > manifest_dict[file_name]["last_updated"]
    ]

    # Determine files to delete
    files_to_delete = [
        file_name for file_name in manifest_dict
        if file_name not in source_dict
    ]

    # Upload files
    for file_meta in files_to_upload:
        url = file_meta["full_url"]
        key = f"{bucket_directory}/{file_meta['file_name']}"

        with session.get(url, stream=True, headers=api_request_headers) as request:
            request.raise_for_status()
            file_data = request.content  # or request.raw.read() if you prefer
            
            write_to_s3(
                s3_client,
                bucket_name,
                key,
                file_data,
                request.headers.get("Content-Type", "application/octet-stream")
            )

    # Delete files
    for fname in files_to_delete:
        delete_from_s3(s3_client, bucket_name, f"{bucket_directory}/{fname}")

    # Update manifest
    updated_manifest = list(source_dict.values())
    write_to_s3(
        s3_client,
        bucket_name,
        manifest_log_flile,
        json.dumps(updated_manifest, indent=4).encode("utf-8"),
        "application/json"
    )

    return {
        "uploaded": len(files_to_upload),
        "deleted": len(files_to_delete)
    }