import logging
import requests
import re
import pandas as pd
import datetime
from bs4 import BeautifulSoup
import os
import glob

# Configure logging settings
log_format = "%(asctime)s [%(levelname)s] - %(message)s"
logging.basicConfig(
    level=logging.INFO, filename="data_extraction.log", filemode="a", format=log_format
)

# Define file paths and URLs
kml_path = "https://onemotoring.lta.gov.sg/mapapp/kml/erp-kml/erp-kml-0.kml"
link = "https://datamall.lta.gov.sg/mapapp/pages/ddls/1_ddl.html"
data_directory = "data"  # Directory to store data files
toll_pattern = "toll-rates"
markers_pattern = "markers"

def extract_plaza_info_from_kml(kml_path):
    """
    Extract data from a KML file and perform preprocessing.

    Args: 
        kml_path (str): URL to the KML file.

    Returns:
        pandas.DataFrame: Extracted and processed data.
    """
    try:
        logging.info("Fetching data from KML file.")
        response = requests.get(kml_path)
        response.raise_for_status()

        data = response.text
        name_pattern = r"<td>([^<]+)</td>"
        coordinates_pattern = r"<coordinates>\s*([^<]+)\s*</coordinates>"
        name_matches = re.findall(name_pattern, data)
        coordinates_matches = re.findall(coordinates_pattern, data)

        if len(name_matches) == len(coordinates_matches):
            data_list = []
            for name, coordinates in zip(name_matches, coordinates_matches):
                longitude, latitude, _ = coordinates.split(",")

                longitude = round(float(longitude.strip()), 7)
                latitude = round(float(latitude.strip()), 7)
                data_list.append([name.strip(), longitude, latitude])

            df = pd.DataFrame(data_list, columns=["Name", "Longitude", "Latitude"])
            df["id"] = df["Name"].str.extract(r"\((\d+)\)$")

            today_date = datetime.datetime.now().strftime("%Y-%m-%d")
            log_file_path = os.path.join(data_directory, f"markers-{today_date}.csv")
            logging.info(f"Saving data to {log_file_path}")
            df.to_csv(log_file_path, index=False)

            return df
        else:
            raise Exception(
                "Number of names and coordinates don't match. Data extraction failed."
            )

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from the URL: {e}")
        raise Exception(f"Failed to fetch data from the URL: {e}")
    

def categ_dict(df, link):
    """
    Create dictionaries for ID to place and category mapping.

    Args:
        df (pandas.DataFrame): Dataframe containing extracted data.
        link (str): URL to fetch vehicle_categories information.

    Returns:
        tuple: Tuple containing dictionaries (id_name, cat_dict).
    """
    try:
        id_name = dict(zip(df.id, df.Name))
        category_content = requests.get(link).text
        cat_soup = BeautifulSoup(category_content, "lxml")
        categories = cat_soup.find("select", {"class": "selectstyle"})
        cat_text = categories.get_text()
        cat_list = list(cat_text.split("\n"))
        cat_list = cat_list[1::]
        cat_id = [0, 1, 2, 3, 4, 5, 6, 7]
        cat_dict = dict(zip(cat_id, cat_list))
        logging.info("Category dictionary created.")
        return id_name, cat_dict
    except Exception as e:
        logging.error(f"Error in creating category dictionary: {e}")
        raise
    

def get_data(df, id_name, cat_dict):
    """
    Fetch and process toll data from web sources and save to CSV.

    Args:
        df (pandas.DataFrame): Dataframe containing plaza information data.
        id_name (dict): ID name to place mapping.
        cat_dict (dict): Category mapping.
    """
    df_final = pd.DataFrame()

    for i in df.id:
        data_to_concat = []

        for j in range(8):
            link = f"https://datamall.lta.gov.sg/mapapp/pages/tables/{i}_table_{j}.html"
            try:
                html_content = requests.get(link).text
                soup = BeautifulSoup(html_content, "lxml")
                rate_table = soup.find("table", {"class": "styler"})

                if len(rate_table) != 3:
                    rows_data = []

                    for row in rate_table.find_all("tr"):
                        columns = row.find_all("td")

                        place = i
                        days = j
                        time = columns[0].text.strip()
                        rates = columns[1].text.strip().replace("$", "")

                        rows_data.append(
                            {
                                "plaza_name": place,
                                "vehicle_cat": days,
                                "time": time,
                                "rates": rates,
                            }
                        )

                    df_temp = pd.DataFrame(rows_data)
                    df_temp = df_temp.replace(
                        {"plaza_name": id_name, "vehicle_cat": cat_dict}
                    )
                    df_temp["rates"] = pd.to_numeric(df_temp["rates"], errors="coerce")
                    data_to_concat.append(df_temp)
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to fetch data from URL {link}: {e}")
                # Log the error and continue gracefully

        if data_to_concat:
            df_final = pd.concat([df_final] + data_to_concat, ignore_index=True)
            # logging.info(f"Data concatenated and appended to df_final for plaza_id {i}")

    if not df_final.empty:
        df_final[["vehicle_cat", "weekdays/weekends"]] = df_final[
            "vehicle_cat"
        ].str.split("(", expand=True)
        df_final["weekdays/weekends"] = df_final["weekdays/weekends"].str.replace(
            ")", ""
        )

        today_date = datetime.datetime.now().strftime("%Y-%m-%d")
        log_file_path = os.path.join(data_directory, f"toll-rates-{today_date}.csv")
        df_final.to_csv(
            log_file_path, mode="a", encoding="utf-8-sig", header=True, index=False
        )
        logging.info(f"Saved data to {log_file_path}")


def get_latest_files(directory, file_pattern):
    """
    Get the latest files from the directory based on the given pattern.

    Args:
        directory (str): Directory path.
        file_pattern (str): File name pattern.

    Returns:
        list: List of the latest files.
    """
    if not os.path.exists(directory):
        logging.error(f"The directory '{directory}' does not exist.")
        raise FileNotFoundError(f"The directory '{directory}' does not exist.")

    files_with_timestamps = []
    for file in glob.glob(os.path.join(directory, f"{file_pattern}*.csv")):
        files_with_timestamps.append((file, os.path.getmtime(file)))

    files_with_timestamps.sort(key=lambda x: x[1], reverse=True)

    latest_files = [file[0] for file in files_with_timestamps[:2]]
    logging.info(f"Latest files matching pattern '{file_pattern}' are {latest_files}")
    return latest_files


def comparison(previous_file_path, current_file_path):
    """
    Compare markers and toll data between previous and current data and save differences to CSV.

    Args:
        previous_file_path (str): Path to the previous file.
        current_file_path (str): Path to the current file.
    """
    if " toll" in previous_file_path or "toll" in current_file_path:
        previous_df = pd.read_csv(previous_file_path)
        current_df = pd.read_csv(current_file_path)
        file = "toll"
    else:
        previous_df = pd.read_csv(previous_file_path, index_col="id")
        current_df = pd.read_csv(current_file_path, index_col="id")
        file = "markers"

    if previous_df.shape == current_df.shape:
        df_diff = previous_df.compare(current_df)
        if len(df_diff) == 0:
            print(f"No change in {file} data")
            logging.info(f"No change in {file} data")

        else:
            # renaming columns to appropriate names
            df_diff.columns = df_diff.columns.set_levels(
                ["previous_df", "current_df"], level=1
            )

            # flattening the multi-index
            df_diff.columns = ["_".join(col).strip() for col in df_diff.columns.values]

            # getting the current date and time
            today_date = datetime.datetime.now().strftime("%Y-%m-%d")

            # saving the difference file
            df_diff.to_csv(f"{file}-difference-{today_date}.csv", encoding="utf-8-sig")
            logging.info(f"Difference file saved to {file}-difference-{today_date}.csv")
            print(" Difference found")

    else: 
        print("Previous and current data shapes are different. Can't Compare")
        logging.info("Previous and current data shapes are different. Can't  Compare")
    
    return


def main():
    try:
        # Create the data directory if it doesn't exist
        if not os.path.exists(data_directory):
            os.makedirs(data_directory)
        
        df = extract_plaza_info_from_kml(kml_path)
        id_name, cat_dict = categ_dict(df, link)
        get_data(df, id_name, cat_dict)
        
        # Get the latest toll data and markers files
        latest_tolldata_files = get_latest_files(data_directory, "toll")
        latest_markers_files = get_latest_files(data_directory, "markers")
        
        # Compare toll rates and markers between the latest files
        comparison(latest_markers_files[1], latest_markers_files[0])
        comparison(latest_tolldata_files[1], latest_tolldata_files[0])

    except Exception as e:
        logging.error(f"Error in extraction and comparison: {e}")
        raise

if __name__ == "__main__":
    main()
