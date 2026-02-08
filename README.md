# Cloud Data Quest

A cloud-based data pipeline that fetches, processes, and analyzes data from the U.S. Bureau of Labor Statistics (BLS) and DataUSA APIs, storing results in AWS S3 with infrastructure managed by Terraform.

## Overview

This project automates the collection and analysis of labor statistics and population data from two public APIs, synchronizes the data with AWS S3, and provides analysis capabilities through Lambda functions triggered on a schedule.

## Features

- **Automated Data Fetching**: Retrieves time series data from BLS and population data from DataUSA
- **S3 Synchronization**: Intelligently syncs files to S3 with manifest-based change tracking
- **Data Analysis**: Processes and analyzes combined datasets from multiple sources
- **Infrastructure as Code**: Complete AWS infrastructure defined with Terraform
- **Scheduled Execution**: CloudWatch-triggered Lambda functions for automated runs

## Project Structure

```
├── config.yaml                           # Configuration for API endpoints and AWS resources
├── requirements.txt                      # Python dependencies
├── general_functions.py                  # Shared utility functions
├── get_bls_and_data_usa_data.py         # Lambda function to fetch and sync data
├── analyze_bls_and_data_usa_data.py     # Lambda function to analyze data
├── test_general_functions.py            # Unit tests for general functions
├── data_analysis_notebook.ipynb         # Jupyter notebook for interactive analysis
└── terraform/                            # Infrastructure as Code
    ├── main.tf                          # Main Terraform configuration
    ├── outputs.tf                       # Terraform outputs
    ├── variables.tf                     # Terraform variables
    └── modules/
        ├── lambda/                      # Lambda function module
        ├── s3_bucket/                   # S3 bucket module
        ├── sqs_queue/                   # SQS queue module
        └── cloudwatch_schedule/         # CloudWatch scheduling module
```

## Prerequisites

- AWS Account with appropriate IAM permissions
- Terraform >= 1.0
- Python 3.8+
- pip or conda for dependency management

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd cloud_data_quest
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure AWS credentials**
   ```bash
   aws configure
   ```

## Configuration

The project is configured via `config.yaml`:

```yaml
bureau_labor_statistics_connection_info:
  base_url: "https://download.bls.gov"
  directory_extension: "pub/time.series/pr"
  s3_directory: "bls_data"

data_usa_connection_info:
  base_url: "https://honolulu-api.datausa.io/tesseract/data.jsonrecords..."
  s3_directory: "data_usa_data"

uploaded_data_can_be_viewed_via_following_s3_link:
  s3_bucket_url: "https://us-east-2.console.aws.amazon.com/s3/buckets/cloud-data-quest-926?region=us-east-2&tab=objects"
```

Modify the configuration as needed for your AWS environment and API endpoints.

**To Review the some Example Data Outpus in an Interactive setting:**
Open `data_analysis_notebook.ipynb` in Jupyter for interactive data exploration and visualization.

### Deployed on AWS

The infrastructure is deployed using Terraform and runs Lambda functions on a schedule via CloudWatch Events.

**Deploy infrastructure:**
```bash
cd terraform
terraform init
terraform plan
terraform apply
```

**Destroy infrastructure:**
```bash
terraform destroy
```

## Lambda Deployment

Lambda functions in this project reference zip files that are deployed to an S3 bucket. This allows the Terraform configuration to pull the latest function code without modifying the Lambda modules.

**Automated Deployment:**
A GitHub Actions workflow automatically packages the Lambda functions into zip files and uploads them to the designated S3 bucket whenever code changes are pushed. This ensures that:

- Lambda function code is always in sync with your repository
- Deployments are automated and consistent
- No manual zip file creation is required

## AWS Resources

The Terraform configuration creates:

- **S3 Bucket**: 
- **Lambda Functions**: 
- **SQS Queue**: 
- **CloudWatch Events**:

## Dependencies

- **requests** - HTTP client for API calls
- **beautifulsoup4** - HTML parsing for BLS directory listings
- **PyYAML** - YAML configuration file handling
- **boto3** - AWS SDK for Python
- **s3fs** - S3 filesystem interface
- **tabulate** - Pretty-print tabular data

## Data Sources

- **Bureau of Labor Statistics (BLS)**: Puerto Rico time series data
  - URL: `https://download.bls.gov/pub/time.series/pr`
  
- **DataUSA**: U.S. population statistics
  - URL: `https://honolulu-api.datausa.io/`

## API Integration

Both APIs are accessed via REST calls. The BLS data is parsed from HTML directory listings, while DataUSA returns structured JSON responses.

## Logging & Monitoring

The project uses:
- CloudWatch Logs for Lambda function execution logs
- Manifest JSON files in S3 for tracking file synchronization status
- Error handling and HTTP status validation

## Development

To contribute or extend this project:

1. Update `general_functions.py` for common functionality changes
2. Modify API logic in `get_bls_and_data_usa_data.py`
3. Enhance analysis in `analyze_bls_and_data_usa_data.py`
4. Test locally with the Jupyter notebook before deploying

## Testing

This project includes unit tests for critical functions. As this is an MVP/POC, test coverage is initiated but will be expanded over time as the project matures.

**Run tests:**
```bash
python -m unittest test_general_functions.py
```

**Current test coverage:**
- `sync_bls_files_to_s3()` - Tests for file uploads, deletion, and manifest management
- `load_config()` - Configuration loading validation
- `build_s3_client()` - S3 client initialization

**Future testing improvements:**
- Expand test coverage for all utility functions
- Add integration tests for Lambda functions
- Add tests for data analysis pipeline
- Implement CI/CD pipeline for automated testing
- Add performance and load testing

## Troubleshooting

- **AWS Credentials**: Ensure AWS credentials are configured and have S3/Lambda permissions
- **API Access**: Verify API endpoints are accessible and not blocked by firewalls
- **Terraform State**: Keep `terraform.tfstate` secure and backed up
- **S3 Bucket**: Ensure bucket name is globally unique if modifying

## License

Specify your license here

## Contact

patrickmarcoux594@gmail.com
