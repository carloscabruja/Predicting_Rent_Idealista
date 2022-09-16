# =============================================================================
"""
-   With this script you can get the attributes of a house that 
has been published on Idealista by the URL of the ad.

- You can get the property code from the URL of the house page. (Index value)

- Columns that we need to get:
    * numPhotos
    * floor
    * rooms
    * bathrooms
    * size
    * parkingSpacePrice
    * exterior
    * hasParkingSpace
    * isParkingSpaceIncludedInPrice
    * hasLift
    * hasPlan
    * has360
    * has3DTour
    * hasVideo
    * propertyType
    * direction

"""
# =============================================================================

# Libraries needed to make petitions to the API of Idealista

import pickle
import os
import base64
import urllib
import json
import warnings
import re
import ast

import requests as rq
import pandas as pd

from dotenv import load_dotenv, find_dotenv
from scipy.special import inv_boxcox

# Warning to avoid the warning message
warnings.filterwarnings("ignore")

# get the API key from the .env file
def get_oauth_token():

    url = "https://api.idealista.com/oauth/token"

    load_dotenv(find_dotenv('../')) # Load .env file
    apikey ="09cltgbrb34frrxwrxcgg6koolvje1bn"
    secret = os.environ.get(apikey)
    apikey_secret = apikey + ':' + secret

    auth = str(base64.b64encode(bytes(apikey_secret, 'utf-8')))[2:][:-1] # Get base64 encoded string

    headers = {'Authorization' : 'Basic ' + auth,'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'}
    params = urllib.parse.urlencode({'grant_type':'client_credentials'}) #,'scope':'read'
    content = rq.post(url,headers = headers, params=params) # Get response
    bearer_token = json.loads(content.text)['access_token'] # Get access token

    return bearer_token

# Get the data from the API
def search_api(token, params):
    url = "https://api.idealista.com/3.5/es/search"

    headers = {'Content-Type': 'Content-Type: multipart/form-data;', 'Authorization' : 'Bearer ' + token} 
    content = rq.post(url, headers=headers, params=params) # Get response
    
    print(content)
    return content

# Validate the url
def get_id(URL):
    # ¿The url have the form of "https://www.idealista.com/inmueble"?
    if "https://www.idealista.com/inmueble" in URL:
        # Get the property code from the URL of the house page. (Index value)
        property_code = re.findall(r'\d+', URL)[0]
    else:
        return "The URL is not valid"

    return property_code

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
        lambda x: x['parkingSpacePrice'] if type(x) == dict and 'parkingSpacePrice' in x else 0
        )

    # drop the parkingSpace column
    df.drop(columns=['parkingSpace'], inplace=True)
            
    print("process_parkingSpace process was successful")
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
    df['hasParkingSpace'] = df['hasParkingSpace'].astype(bool)
    df['isParkingSpaceIncludedInPrice'] = df['isParkingSpaceIncludedInPrice'].astype(bool)
    df['hasLift'] = df['hasLift'].astype(bool)
    df['hasPlan'] = df['hasPlan'].astype(bool)
    df['has360'] = df['has360'].astype(bool)
    df['has3DTour'] = df['has3DTour'].astype(bool)
    df['hasVideo'] = df['hasVideo'].astype(bool)

    # object types
    df['propertyType'] = df['propertyType'].astype(str)

    order_of_cols = ['price', 'numPhotos', 'floor', 'rooms', 'bathrooms',
        'size', 'parkingSpacePrice', 'latitude', 'longitude', 'exterior', 
        'hasParkingSpace', 'isParkingSpaceIncludedInPrice', 
        'hasLift', 'hasPlan', 'has360', 'has3DTour', 'hasVideo',
        'propertyType'
        ]
            
    df = df[order_of_cols]
            
    print("retype_data process was successful")
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
    cluster = pickle.load(open('../src/models/kmeans_clustering.pkl', 'rb'))
    # get the cluster of the latitude and longitude
    df['direction'] = cluster.predict(df[['latitude', 'longitude']])
    df['direction'] = df['direction'].map({0: 'central', 1: 'south', 2: 'north', 3: 'west'})
            
    # drop the latitude and longitude columns
    df.drop(columns=['latitude', 'longitude'], inplace=True)

    print("clustering_address process was successful")
    return df

# Function to inverse Box-Cox transform a column
def inv_box_cox_transform(column, lamda):
    column = inv_boxcox(column, lamda)
    return column

def get_house_attributes(url):
    '''
    This function get the attributes of the house

    Parameters:
    -----------
    url: URL of the house

    Returns:
    --------
    df: DataFrame with the attributes of the house.
    '''

    # get the property code from the url
    property_code = get_id(url)

   # build the params
    params = {
    "country" : 'es',
    "operation" : "rent",
    "propertyType" : "homes",
    "locationId" : "0-EU-ES-46",
    "adIds": [property_code]
    }
    
    # get the data from the API
    token = get_oauth_token()
    data = search_api(token, params)

    # create the DataFrame
    df = pd.DataFrame(json.loads(data.text)["elementList"])

    # drop the columns that we don't need
    df = df[['propertyCode', 
        'price', 
        'numPhotos',
        'floor',
        'exterior',
        'hasLift',
        'rooms',
        'bathrooms',
        'size',
        'parkingSpace',
        'hasPlan',
        'hasVideo',
        'has360',
        'has3DTour',
        'propertyType',
        'latitude',
        'longitude'
        ]]
    
    # set the propertyCode as index
    df.set_index('propertyCode', inplace=True)

    # process the parkingSpace column
    df = process_parkingSpace(df)

    # retype the data
    df = retype_data(df)

    # cluster the address
    df = clustering_address(df)

    print("get_house_attributes process was successful")
    return df