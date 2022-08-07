# Libraries required for the ETL process
import os
import base64
import urllib
import json
import time
import ast
import warnings
import pickle

import pandas as pd
import requests as rq

from dotenv import load_dotenv, find_dotenv
from pyod.models.knn import KNN
from sklearn.cluster import KMeans

warnings.filterwarnings('ignore')

# =============================================================================

class ETL():
    
    # Defaults for the ETL process
    def __init__(self):
        self.bearer_token = self.get_oauth_token()
        self.params = {
            "country" : 'es', 
            "operation" : "rent", 
            "propertyType" : "homes", 
            "locationId" : "0-EU-ES-46", 
            "maxItems" : 50
            }
        
    # Get authentication token
    def get_oauth_token(self):

        url = "https://api.idealista.com/oauth/token"

        load_dotenv(find_dotenv('creds.env')) # Load .env file
        apikey = os.environ.get("API_KEY")
        secret = os.environ.get("SECRET")
        apikey_secret = apikey + ':' + secret

        auth = str(base64.b64encode(bytes(apikey_secret, 'utf-8')))[2:][:-1] # Get base64 encoded string

        headers = {'Authorization' : 'Basic ' + auth,'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'}
        params = urllib.parse.urlencode({'grant_type':'client_credentials'}) #,'scope':'read'
        content = rq.post(url, headers = headers, params=params) # Get response
        bearer_token = json.loads(content.text)['access_token'] # Get access token

        return bearer_token
    
    # Get list of properties
    def search_api(self, token, params):
        url = "https://api.idealista.com/3.5/es/search"

        headers = {'Content-Type': 'Content-Type: multipart/form-data;', 'Authorization' : 'Bearer ' + token} 
        content = rq.post(url, headers=headers, params=params) # Get response
        
        print(content)
        return content

    # extract data from API response
    def extract_data(self):
        self.result = self.search_api(self.bearer_token, self.params)
        self.total_pages = json.loads(self.result.text)['totalPages']

        df_tot = pd.DataFrame(json.loads(self.result.text)["elementList"])

        for i in range(2, self.total_pages):
            try:
                self.params['numPage'] = i
                self.result = self.search_api(self.bearer_token, self.params)
                df = pd.DataFrame(json.loads(self.result.text)["elementList"])
                df_tot = pd.concat([df_tot, df])
                time.sleep(5)
            
            except Exception as e:
                print("Permission denied")
                print(e, "Page: ", i, " of ", self.total_pages)

                df_tot.reset_index(drop=True, inplace=True)
                
                print("Making a corrupted file")
                df_tot.to_csv('data/raw/data_raw_corrupted.csv', index=False)

                return df_tot
        
        print("The extraction was successful")
        df_tot.reset_index(drop=True, inplace=True)
        df_tot.to_csv('data/raw/data_raw.csv', index=False)
        
        return df_tot

    # process data
    def transform(self, df):
        
        # Function that unifies the dataframe if exists
        def unify_df(df):
            if os.path.exists('data/raw/data_raw.csv'):
                df_raw = pd.read_csv('data/raw/data_raw.csv')

                # Is the same dataframe?
                if df.equals(df_raw):
                    print("The dataframe is the same, no need to unify")
                    df.drop_duplicates(inplace=True) # Drop duplicates
                
                else:
                    print("The dataframe is different, unifying")
                    df = pd.concat([df_raw, df]) # Concatenate dataframes
                    df.drop_duplicates(inplace=True) # Drop duplicates
                    df.reset_index(drop=True, inplace=True) # Reset index
            
            else:
                print("data_raw.csv does not exist")
            
            return df 
        
        def drop_columns(df):
            cols_needed = [
                'propertyCode', 'price', 'numPhotos', 'size', 
                'floor', 'rooms', 'bathrooms', 'latitude', 'longitude', 
                'propertyType', 'status', 'parkingSpace', 'exterior', 'hasLift', 
                'hasPlan', 'has360', 'has3DTour', 'hasVideo', 
                'newDevelopmentFinished'
                ]
            df = df[cols_needed] # Keep only columns needed

            # Set propertyCode as index
            df.set_index('propertyCode', inplace=True)

            print("drop_columns process was successful")
            return df

        def process_status(df):
            '''
            This function process the status of the property.

            Parameters:
            -----------
            df: DataFrame with the raw data of status, newDevelopmentFinished.

            Returns:
            --------
            df: DataFrame with the raw data processed.
            '''

            # Create the columns that will replace the status column: renew and new_development.
            df['renew'] = df['status'].apply(lambda x: True if x == 'renew' else False)
            df['new_development'] = df['status'].apply(lambda x: True if x == 'newdevelopment' else False)

            # drop the status column
            df.drop('status', axis=1, inplace=True)

            # Same with newDevelopmentFinished
            df['isFinished'] = df['newDevelopmentFinished'].apply(lambda x: False if x == False else True)
            df.drop('newDevelopmentFinished', axis=1, inplace=True)

            print("process_status process was successful")
            return df

        def process_parkingSpace(df):
            '''
            This function process the bad formatted parkingSpace

            Parameters:
            -----------
            df: DataFrame with the raw data of parkingSpace

            Returns:
            --------
            df: DataFrame with the raw data processed.
            '''
            # replace all the ' to " in the parkingSpace column
            df['parkingSpace'] = df['parkingSpace'].str.replace('\'', '"')

            # convert the string to a dictionary
            df['parkingSpace'] = df['parkingSpace'].apply(
                lambda x: ast.literal_eval(x) 
                if type(x) == str else x
                )

            # get the hasParkingSpace of the dict of the parkingSpace column
            df['hasParkingSpace'] = df['parkingSpace'].apply(
                lambda x: x['hasParkingSpace'] if type(x) == dict else False
                )

            # get isParkingSpaceIncludedInPrice 
            df['isParkingSpaceIncludedInPrice'] = df['parkingSpace'].apply(
                lambda x: x['isParkingSpaceIncludedInPrice'] if type(x) == dict else False
                )

            # get the parkingSpacePrice 
            df['parkingSpacePrice'] = df['parkingSpace'].apply(
                lambda x: x['parkingSpacePrice'] 
                if type(x) == dict and 'parkingSpacePrice' in x else 0
                )

            # drop the parkingSpace column
            df.drop(columns=['parkingSpace'], inplace=True)
            
            print("process_parkingSpace process was successful")
            return df

        def process_floor(df):
            ''' 
            This function process the bad formatted floor

            Parameters:
            -----------
            df: DataFrame with the raw data of floor

            Returns:
            --------
            df: DataFrame with the raw data processed.
            '''

            floor_nan = df[df['floor'].isnull()] # get the nan values
            property_type = floor_nan['propertyType'].value_counts().index.to_list()

            for i in property_type:
                # get the mode of the propertyType column
                mode = df['floor'][df['propertyType'] == i].mode()[0]
                # get the index of the rows with the propertyType i and the floor is null
                index = df[(df['propertyType'] == i) & (df['floor'].isnull())].index
                # replace the nan with the mode
                df.loc[index, 'floor'] = mode

            # where floor is 'bj' or 'en' put 0
            df.loc[df['floor'] == 'bj', 'floor'] = 0
            df.loc[df['floor'] == 'en', 'floor'] = 0

            # where floor is 'st' put 1
            df.loc[df['floor'] == 'st', 'floor'] = -1

            print("process_floor process was successful")
            return df
        
        def process_hasLift(df):
            ''' 
            This function process all nan values of hasLift

            Parameters:
            -----------
            df: DataFrame with the raw data of hasLift

            Returns:
            --------
            df: DataFrame with the raw data processed.
            '''

            # get the index of the rows with the hasLift is null
            index = df[df['hasLift'].isnull()].index
            # replace the nan with False
            df.loc[index, 'hasLift'] = False
            
            print("process_hasLift process was successful")
            return df
        
        def retype_data(df):
            ''' 
            This function forcetype all columns of the dataset

            Parameters:
            -----------
            df: DataFrame with the raw data

            Returns:
            --------
            df: DataFrame with secure types.
            '''

            # int types
            df['numPhotos'] = df['numPhotos'].astype(int)
            df['floor'] = df['floor'].astype(int)
            df['rooms'] = df['rooms'].astype(int)
            df['bathrooms'] = df['bathrooms'].astype(int)

            # float types
            df['price'] = df['price'].astype(float)
            df['size'] = df['size'].astype(float)
            df['parkingSpacePrice'] = df['parkingSpacePrice'].astype(float)
            df['latitude'] = df['latitude'].astype(float)
            df['longitude'] = df['longitude'].astype(float)

            # boolean types
            df['exterior'] = df['exterior'].astype(bool)
            df['renew'] = df['renew'].astype(bool)
            df['new_development'] = df['new_development'].astype(bool)
            df['hasParkingSpace'] = df['hasParkingSpace'].astype(bool)
            df['isParkingSpaceIncludedInPrice'] = df['isParkingSpaceIncludedInPrice'].astype(bool)
            df['isFinished'] = df['isFinished'].astype(bool)
            df['hasLift'] = df['hasLift'].astype(bool)
            df['hasPlan'] = df['hasPlan'].astype(bool)
            df['has360'] = df['has360'].astype(bool)
            df['has3DTour'] = df['has3DTour'].astype(bool)
            df['hasVideo'] = df['hasVideo'].astype(bool)

            # object types
            df['propertyType'] = df['propertyType'].astype(str)

            order_of_cols = ['price', 'numPhotos', 'floor', 'rooms', 'bathrooms',
            'size', 'parkingSpacePrice', 'latitude', 'longitude', 'exterior', 'renew',
            'new_development', 'hasParkingSpace', 'isParkingSpaceIncludedInPrice',
            'isFinished', 'hasLift', 'hasPlan', 'has360', 'has3DTour', 'hasVideo',
            'propertyType']
            
            df = df[order_of_cols]
            
            print("retype_data process was successful")
            return df

        def drop_outliers(df):
            '''
            This function drop the outliers of the dataset by KNN method

            Parameters:
            -----------
            df: DataFrame with the outliers

            Returns:
            --------
            df: DataFrame with the outliers dropped.
            '''
            num_cols = df.select_dtypes(include=['int64', 'float64']).columns
            
            clf = KNN()
            clf.fit(df[num_cols])
            y_pred = clf.predict(df[num_cols])

            print(f'The percentage of outliers is {100*sum(y_pred)/len(y_pred)}%')
            
            df = df[y_pred == 0]

            print("drop_outliers process was successful")
            return df

        def clustering_address(df):
            '''
            This function create cluster from the latitude and longitude a append it to the dataset

            Parameters:
            -----------
            df: DataFrame with the latitude and longitude

            Returns:
            --------
            df: DataFrame with the latitude and longitude clustered.
            '''

            # create the cluster
            cluster = pickle.load(open('models/kmeans_clustering.pkl', 'rb'))
            # get the cluster of the latitude and longitude
            df['cluster'] = cluster.predict(df[['latitude', 'longitude']])

            # drop the latitude and longitude columns
            df.drop(columns=['latitude', 'longitude'], inplace=True)

            print("clustering_address process was successful")
            return df
        
        df_processed = unify_df(df) # unify the dataframe with the raw data (if needed)
        df_processed = drop_columns(df_processed) # drop the columns that are not needed

        df_processed = process_status(df_processed) # process the status column
        df_processed = process_parkingSpace(df_processed) # process the parkingSpace column
        df_processed = process_floor(df_processed) # process the floor column
        df_processed = process_hasLift(df_processed) # process the hasLift column

        df_processed = retype_data(df_processed) # force type the columns
        df_processed = drop_outliers(df_processed) # drop the outliers (by KNN)
        df_processed = clustering_address(df_processed) # cluster the latitude and longitude columns

        print("The data was processed successfully")
        return df_processed
        
    # Load the data
    def load_data(self, df_processed):
        '''
        This function load the data processed and update our datawarehouse

        Parameters:
        -----------
        df_processed: DataFrame with the data processed

        Returns:
        --------
        df: DataFrame of the data warehouse.
        '''
    
        # load the dataframe to the datawarehouse
        warehouse_path = 'data/processed/rent_Valencia.csv'

        # if the file exists, append the new data
        if os.path.exists(warehouse_path):
            print("Updating the data warehouse")
            
            df = pd.read_csv(warehouse_path, index_col=0)
            df = pd.concat([df, df_processed])
            
            df.drop_duplicates(inplace=True)
            df.to_csv(warehouse_path)
        else:
            print("Creating the data warehouse")
            df_processed.to_csv(warehouse_path)
            df = df_processed

        print("The data was loaded successfully")
        return df