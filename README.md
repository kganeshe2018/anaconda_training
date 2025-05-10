markdown
# Fund Data Processing and Analysis System

## Overview
A comprehensive ETL (Extract, Transform, Load) system designed for processing and analyzing fund data. The system handles data ingestion from various sources, data transformation, and generates performance and reconciliation reports.

## Features
- **Database Management**: Automated SQLite database setup and maintenance
- **Data Ingestion**: Support for multiple date formats and fund data sources
- **Data Processing**: Comprehensive ETL pipeline for fund data
- **Reporting**: 
  - Performance reports generation
  - Reconciliation reports
  - Data validation and quality checks
- **Logging**: Detailed logging system for tracking operations and debugging

## Installation

### Prerequisites
- Python 3.13.0 or higher
- SQLite3

### Setup
1. Clone the repository:

bash git clone [repository-url] cd [repository-name]
 

2. Create and activate a virtual environment:

bash python -m venv venv source venv/bin/activate # On Windows: venv\Scripts\activate
 

3. Install required packages:

bash pip install -r requirements.txt
 

## Configuration
The system configuration is managed through the `Config` class in `src/app/config.py`. Key configurations include:
- Database paths
- SQL script locations
- Input/output directories
- Report file paths

## Usage

### Running the Application
Execute the main script to run the complete ETL pipeline:

bash python src/app/main.py
 

The pipeline executes the following steps in sequence:
1. Database setup
2. Data ingestion from external sources
3. Data preprocessing
4. Data publishing
5. Report generation (reconciliation and performance)

### Working with Data
- Place fund data files in the `data/ext-funds/` directory
- SQL scripts for table creation should be in `data/reference-data/`
- Generated reports will be saved in the `outputs/` directory

## Development

### Project Components

#### 1. Base Model
- `BaseModel`: Abstract base class for all processing models
- Provides common initialization and execution patterns

#### 2. Database Management
- `SetupDB`: Handles database creation and table setup
- `db_utils.py`: Database utility functions for common operations

#### 3. ETL Pipeline
- Data Ingestion: `IngestFundsData`
- Preprocessing: `PreprocessData`
- Publishing: `PublishData`
- Reporting: `GenerateReconReport`, `GeneratePerfReport`

#### 4. Utilities
- `utils.py`: Common utility functions
- `logger.py`: Logging configuration and management

### Adding New Features
1. Create new model classes inheriting from `BaseModel`
2. Implement the `_compute()` method
3. Add configuration parameters to `Config` class if needed
4. Update the pipeline in `main.py`
## Logging
- Logs are configured using the `LoggerFactory`
- Console output is enabled by default
- File logging can be enabled by providing a log file path

