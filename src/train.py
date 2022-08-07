# Change working directory to the directory of the script
import os 
os.chdir(os.path.dirname(os.path.abspath(__file__))) 

# Import libraries
import pandas as pd
from utils.ETL import ETL

# Create instance of ETL class
etl = ETL()
try:
    # Extract data from API
    df_temp = etl.extract_data()

except Exception as e:
    print("Error: Unable to extract data from API")
    print("Error: {}".format(e))
    print("Loading raw data from local file if available")
    if os.path.exists("data/raw/data_raw.csv"):
        df_temp = pd.read_csv("data/raw/rent_Valencia_raw.csv")
    else:
        print("Error: No local data file found")
        exit()

# Process data
df_temp = etl.transform(df_temp)

# Load data into database
df = etl.load_data(df_temp)
