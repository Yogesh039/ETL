import pandas as pd
import sqlite3
from datetime import datetime



#  function to calculate age
def calculate_age(dob):
    today = datetime.now()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

#  function to calculate days since last consultation
def days_since_last_consulted(last_date):
    today = datetime.now()
    return (today - last_date).days

#  function to safely parse dates with multiple formats
def safe_parse_date(date_str):
    formats = ["%Y%m%d", "%m%d%Y", "%Y-%m-%d %H:%M:%S"]  # Add all possible formats
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    print(f"Error parsing date: {date_str}")
    return None

# Read the data file
def read_file(file_path):
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
        print(f"Number of records after reading the file: {len(data)}")
        return data
    except Exception as e:
        print(f"Error reading file: {e}")
        return pd.DataFrame()

# Validate data
def validate_data(df):
    try:
        # Check required columns
        required_columns = [
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
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing columns: {missing_columns}")
            return pd.DataFrame()

        # Check for non-null and proper types in specific columns
        df["Customer_Id"] = df["Customer_Id"].astype(str)  # Convert Customer_Id to string
        df["Open_Date"] = df["Open_Date"].astype(str)
        df["Last_Consulted_Date"] = df["Last_Consulted_Date"].astype(str)
        df["DOB"] = df["DOB"].astype(str)
        df["Is_Active"] = df["Is_Active"].astype(str)

        print("Validation completed successfully.")
        return df
    except Exception as e:
        print(f"Error during validation: {e}")
        return pd.DataFrame()

# Display sample data before transformation
def view_sample_data_before(df):
    print("Sample Data Before Transformation:")
    print(df.head(5))

# Transform data
def transform_data(df):
    try:
        # Parse dates
        df["DOB"] = df["DOB"].apply(safe_parse_date)
        df["Last_Consulted_Date"] = df["Last_Consulted_Date"].apply(safe_parse_date)

        # Drop rows with invalid date parsing
        df = df.dropna(subset=["DOB", "Last_Consulted_Date"])

        # Calculate age and days since last consulted
        df["Age"] = df["DOB"].apply(calculate_age)
        df["Days_Since_Last_Consulted"] = df["Last_Consulted_Date"].apply(days_since_last_consulted)

        print("Transformation completed successfully.")
        return df
    except Exception as e:
        print(f"Error during transformation: {e}")
        return pd.DataFrame()

# Display sample data after transformation
def view_sample_data_after(df):
    print("Sample Data After Transformation:")
    print(df.head(5))

# Load data into SQLite database
def load_data(df, db_path):
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

        # Insert or overwrite data into respective tables
        for _, row in df.iterrows():
            table_name = f"Table_{row['Country']}"
            # Convert dates to strings
            row_data = row.copy()
            row_data["DOB"] = row_data["DOB"].strftime("%Y-%m-%d")
            row_data["Last_Consulted_Date"] = row_data["Last_Consulted_Date"].strftime("%Y-%m-%d")
            # Delete existing record with same Customer_Id and DOB
            cursor.execute(
                f"DELETE FROM {table_name} WHERE Customer_Id = ? AND DOB = ?",
                (row_data["Customer_Id"], row_data["DOB"]),
            )
            # Insert new record
            cursor.execute(
                f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tuple(row_data),
            )
            loaded_records += 1

        conn.commit()
        conn.close()
        print(f"Data successfully loaded. Records loaded: {loaded_records}")
    except Exception as e:
        print(f"Error during data load: {e}")

# View data from SQLite database
def view_data_in_db(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # List all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("Tables in Database:", [table[0] for table in tables])

        # Display data from each table
        for table in tables:
            print(f"\nData from {table[0]}:")
            data = pd.read_sql_query(f"SELECT * FROM {table[0]}", conn)
            print(data.head(5))  # Show only first 5 rows for brevity

        conn.close()
    except Exception as e:
        print(f"Error viewing data in database: {e}")

# Main ETL process
def etl_process(file_path, db_path):
    # Extract
    data = read_file(file_path)
    if data.empty:
        print("No data to process.")
        return
    print("Data extraction completed.")

    # Validate
    data = validate_data(data)
    if data.empty:
        print("Data validation failed.")
        return
    print("Data validation completed.")

    # View sample data before transformation
    view_sample_data_before(data)

    # Transform
    data = transform_data(data)
    if data.empty:
        print("Data transformation failed.")
        return
    print("Data transformation completed.")

    # View sample data after transformation
    view_sample_data_after(data)

    # Load
    load_data(data, db_path)
    print("Data loading completed.")

    # View data in SQLite database
    view_data_in_db(db_path)


