from general_funtions import *

def get_bls_and_data_usa_data(event, context):

    config = load_config("config.yaml")

    # Assign Variables from config file 
    bls_api_base_url = config['bureau_labor_statistics_connection_info']['base_url']
    bls_api_directory_extension = config['bureau_labor_statistics_connection_info']['directory_extension']
    bls_api_headers = config['bureau_labor_statistics_connection_info']['api_headers']
    bls_s3_directory = config['bureau_labor_statistics_connection_info']['s3_directory']
    mainfest_log_flile = config['bureau_labor_statistics_connection_info']['manifest_log_file']

    data_usa_api_url = config['data_usa_connection_info']['base_url']
    data_usa_s3_directory = config['data_usa_connection_info']['s3_directory']
    data_usa_s3_file_name = config['data_usa_connection_info']['s3_file_name']

    aws_region = config['aws_s3_connection_info']['aws_region']
    aws_bucket_name = config['aws_s3_connection_info']['bucket_name']

    # Get list of files from BLS API to determine which files to upload, update, or delete in S3
    response_bls_data = get_data_via_rest_api(
        url=f"{bls_api_base_url}/{bls_api_directory_extension}",
        headers=bls_api_headers
    )

    # Format HTTP response from BLS API into list of dictionaries with file information
    source_file_list = parse_and_format_bls_api_response(response_bls_data, bls_api_base_url, bls_api_directory_extension)

    s3 = build_s3_client(aws_region)
    
    # Utilize log manifest in S3 to determine which files to upload, update, or delete in S3
    result_bls_upload = sync_bls_files_to_s3(source_file_list, s3, aws_bucket_name, bls_s3_directory, mainfest_log_flile, bls_api_headers)

    response_data_usa_data = get_data_via_rest_api(data_usa_api_url)
    
    result_data_usa_upload = write_to_s3(s3, aws_bucket_name, f"{data_usa_s3_directory}/{data_usa_s3_file_name}", response_data_usa_data.text)

    return {
        "bls_upload_result": result_bls_upload,
        "data_usa_upload_result": result_data_usa_upload
    }