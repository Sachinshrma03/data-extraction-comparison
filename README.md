# Data Extraction and Comparison

## Overview
This repository contains a Python script designed to scrape data and marker information from the official sources provided by the Land Transport Authority (LTA) in Singapore. The script is configured to run automatically every day at 9:00 am using GitHub Actions. The extracted data is then compared with the previous day's data to identify any changes.


## Features

- **Data Extraction**: The script fetches toll plaza information from a KML file and performs preprocessing to create a structured DataFrame. The extracted data includes plaza names, coordinates, and IDs, which are saved in a CSV file for future reference.

- **Category Dictionary Creation**: The script creates dictionaries mapping plaza IDs to names and vehicle categories to their corresponding IDs by scraping information from a specified URL.

- **Data Fetching and Processing**: The script iterates through the plaza data, fetches toll rates from web sources, and processes the data. The toll rates are saved in a CSV file, including information such as plaza name, vehicle category, time, and rates.

- **Comparison of Changes**: The latest toll data and marker files are retrieved, and a comparison is made with the previous day's data. Any changes in toll rates or markers are identified and logged.

## Usage

1. **Configuration**: Modify the script to include the correct URLs, file paths, and patterns based on your requirements.

2. **GitHub Actions**: The script is set to run automatically using GitHub Actions. Ensure that your GitHub Actions workflow is correctly configured to trigger the script at the desired time.

3. **Logging**: Detailed logs are maintained in the "data_extraction.log" file for monitoring and troubleshooting.

## Dependencies

- requests: For making HTTP requests to fetch data.
- BeautifulSoup: For parsing HTML content.
- pandas: For data manipulation and DataFrame operations.

## Installation

1. Clone the repository:
   git clone https://github.com/your-username/toll-data-scraper.git

   cd toll-data-scraper

3. Install dependencies:
   pip install -r requirements.txt

4. Run the script manually:
   python script.py


