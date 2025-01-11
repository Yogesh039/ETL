import pandas as pd
import sqlite3
import logging
import json
from datetime import datetime
from utils import calculate_age, days_since_last_consulted, safe_parse_date


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path="config/config.json"):
    """Load configuration from a JSON file."""
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        logging.info(f"Configuration loaded successfully from {config_path}")
        return config
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        raise

def read_file(file_path):
    """Read and return data from the input file."""
    try:
        data = pd.read_csv(
            file_path,
            sep="|",
            skiprows=1,
            header=None,
            names=[
                "Record_Type",
                "Customer_Name",
                "Customer_Id",
                "Open_Date",
                "Last_Consulted_Date",
                "Vaccination_Id",
                "Dr_Name",
                "State",
                "Country",
                "DOB",
                "Is_Active",
            ],
        )
        data = data[data["Record_Type"] == "D"].drop(columns=["Record_Type"])
        logging.info(f"Number of records after reading the file: {len(data)}")
        return data
    except Exception as e:
        logging.error(f"Error reading file: {e}")
        return pd.DataFrame()

def validate_data(df):
    """Validate data by checking required columns and types."""
    try:
        required_columns = [
            "Customer_Name", "Customer_Id", "Open_Date", "Last_Consulted_Date",
            "Vaccination_Id", "Dr_Name", "State", "Country", "DOB", "Is_Active"
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing columns: {missing_columns}")
            return pd.DataFrame()

        # Check for proper data types
        df["Customer_Id"] = df["Customer_Id"].astype(str)
        df["Open_Date"] = df["Open_Date"].astype(str)
        df["Last_Consulted_Date"] = df["Last_Consulted_Date"].astype(str)
        df["DOB"] = df["DOB"].astype(str)
        df["Is_Active"] = df["Is_Active"].astype(str)
        
        logging.info("Data validation completed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error during data validation: {e}")
        return pd.DataFrame()

def transform_data(df):
    """Transform data by parsing dates and calculating age."""
    try:
        # Parse dates and remove invalid ones
        df["DOB"] = df["DOB"].apply(safe_parse_date)
        df["Last_Consulted_Date"] = df["Last_Consulted_Date"].apply(safe_parse_date)
        df = df.dropna(subset=["DOB", "Last_Consulted_Date"])

        # Calculate Age and Days Since Last Consulted
        df["Age"] = df["DOB"].apply(calculate_age)
        df["Days_Since_Last_Consulted"] = df["Last_Consulted_Date"].apply(days_since_last_consulted)

        logging.info("Data transformation completed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        return pd.DataFrame()

def load_data(df, db_path):
    """Load the transformed data into the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        loaded_records = 0

        # Create tables dynamically by country
        for country in df["Country"].unique():
            table_name = f"Table_{country}"
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                Customer_Name TEXT,
                Customer_Id TEXT,
                Open_Date TEXT,
                Last_Consulted_Date TEXT,
                Vaccination_Id TEXT,
                Dr_Name TEXT,
                State TEXT,
                Country TEXT,
                DOB TEXT,
                Is_Active TEXT,
                Age INTEGER,
                Days_Since_Last_Consulted INTEGER
            )
            """
            cursor.execute(create_table_query)

        # Insert or overwrite data
        for _, row in df.iterrows():
            table_name = f"Table_{row['Country']}"
            row_data = row.copy()
            row_data["DOB"] = row_data["DOB"].strftime("%Y-%m-%d")
            row_data["Last_Consulted_Date"] = row_data["Last_Consulted_Date"].strftime("%Y-%m-%d")

            # Delete existing record with same Customer_Id and DOB
            cursor.execute(
                f"DELETE FROM {table_name} WHERE Customer_Id = ? AND DOB = ?",
                (row_data["Customer_Id"], row_data["DOB"]),
            )
            cursor.execute(
                f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tuple(row_data),
            )
            loaded_records += 1

        conn.commit()
        conn.close()
        logging.info(f"Data successfully loaded. Records loaded: {loaded_records}")
    except Exception as e:
        logging.error(f"Error during data load: {e}")

def etl_process(file_path, db_path, config_path="config/config.json"):
    """Main ETL process: Extract, Validate, Transform, and Load."""
    # Load Configuration
    config = load_config(config_path)

    # Extract
    data = read_file(file_path)
    if data.empty:
        logging.warning("No data to process.")
        return
    logging.info("Data extraction completed.")

    # Validate
    data = validate_data(data)
    if data.empty:
        logging.warning("Data validation failed.")
        return
    logging.info("Data validation completed.")

    # Transform
    data = transform_data(data)
    if data.empty:
        logging.warning("Data transformation failed.")
        return
    logging.info("Data transformation completed.")

    # Load
    load_data(data, db_path)
