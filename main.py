from src.etl import etl_process

if __name__ == "__main__":
    FILE_PATH = "C:/Users/Yogesh/Downloads/Assignment/customer_data.txt"
    DB_PATH = "C:/Users/Yogesh/Downloads/Assignment/hospital_data.db"
    
    etl_process(FILE_PATH, DB_PATH)
