# ChildhoodCancerDataInitiative-cBioPortal-Workflows
CCDI Prefect Workflows for data transformation, ingestion and export to/from CCDI cBioPortal

## Prerequisites

- Python 3.9+
- MySQL client tools (mysqldump)
- AWS credentials with access to:
  - AWS Secrets Manager
  - S3 bucket

## Installation

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Workflows

- **cbio-mysql-dump**: A Prefect workflow that can create a MySQL database dump of cBioPortal data from a specified env/tier (e.g. dev, qa, prod etc.) and save the mysql dump to a specified S3 bucket. 
