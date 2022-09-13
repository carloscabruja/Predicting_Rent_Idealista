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

# Libraries to webscraping
import pickle 
import time

import pandas as pd
import regex as re

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from scipy.special import inv_boxcox

def get_house_attributes(URL):
    """
    Get the attributes of a house that has been published on Idealista by its URL.

    Params:
    -------
    URL: string
        URL of the house page.
        
    Returns:
    --------
    house_attributes: dataframe
        dataframe with the attributes of the house.
    """
    # ¿The url have the form of "https://www.idealista.com/inmueble"?
    if "https://www.idealista.com/inmueble" in URL:
        # Get the property code from the URL of the house page. (Index value)
        property_code = re.findall(r'\d+', URL)[0]

        # Create a Chrome webdriver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

        # Create dictionary with the attributes of the house
        house_attributes = {}
        house_attributes["propertyCode"] = property_code

        # Make a connection with the URL of the house page.
        driver.get(URL)

        # Wait until the page is loaded
        time.sleep(3)

        # Get the attributes of the house
        # =====================================================================
        # Price
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[3]/span/span'
        price = float(driver.find_element(by=By.XPATH, value=xpath).text)

        # because it doesn't covert well to float (Ex: 1.500 = 1.5)
        # so we catch that error and convert it to real price
        if price < 99:
            price = price * 1000

        house_attributes['price'] = price
        # =====================================================================
        # numPhotos
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[1]/button[1]/span'
        numPhotos = driver.find_element(by=By.XPATH, value=xpath).text
        numPhotos = int(re.findall(r'\d+', numPhotos)[0])
        
        house_attributes['numPhotos'] = numPhotos
        # =====================================================================
        # floor, exterior, hasLift
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[4]/span[3]/span'
    
        try:
            floor = driver.find_element(by=By.XPATH, value=xpath).text
        
            if floor in 'Bajo':
                floor = 0
            else:
                floor = int(re.findall(r'\d+', floor)[0])
        
        except:
            floor = 0

        house_attributes['floor'] = floor
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[4]/span[3]' 
        exterior = driver.find_element(by=By.XPATH, value=xpath).text

        # make exterior in lowercase
        exterior = exterior.lower()

        # Check if exterior contains 'exterior'
        if 'exterior con ascensor' in exterior:
            exterior = True
            hasLift = True
        elif 'exterior sin ascensor' in exterior:
            exterior = True
            hasLift = False
        else:
            exterior = False
            hasLift = False

        house_attributes['exterior'] = exterior
        house_attributes['hasLift'] = hasLift
        # =====================================================================
        # rooms
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[4]/span[2]/span'
        numRooms = int(driver.find_element(by=By.XPATH, value=xpath).text)
        
        house_attributes['rooms'] = numRooms
        # =====================================================================
        # bathrooms
        # =====================================================================
        xpath = '//*[@id="details"]/div[3]/div[1]/div[1]/ul/li[3]'
        numBathrooms = driver.find_element(by=By.XPATH, value=xpath).text
        numBathrooms = int(re.findall(r'\d+', numBathrooms)[0])
        
        house_attributes['bathrooms'] = numBathrooms
        # =====================================================================
        # size
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[4]/span[1]/span'
        size = float(driver.find_element(by=By.XPATH, value=xpath).text)
        
        house_attributes['size'] = size
        # =====================================================================
        # parkingSpacePrice, hasParkingSpace, isParkingSpaceIncludedInPrice
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[5]/span[4]/span/text()[2]'
        try:
            parkingSpacePrice = float(driver.find_element(by=By.XPATH, value=xpath).text)
            
            hasParkingSpace = True
            isParkingSpaceIncludedInPrice = False
            
            house_attributes['parkingSpacePrice'] = parkingSpacePrice
            house_attributes['hasParkingSpace'] = hasParkingSpace
            house_attributes['isParkingSpaceIncludedInPrice'] = isParkingSpaceIncludedInPrice

        except:
            parkingSpacePrice = 0
            isParkingSpaceIncludedInPrice = False
            
            house_attributes['parkingSpacePrice'] = parkingSpacePrice
            house_attributes['isParkingSpaceIncludedInPrice'] = isParkingSpaceIncludedInPrice
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[4]/span[4]/span'
        try:
            garage = driver.find_element(by=By.XPATH, value=xpath).text

            if 'Garaje incluido' in garage:
                hasParkingSpace = True
                isParkingSpaceIncludedInPrice = True

                house_attributes['hasParkingSpace'] = hasParkingSpace
                house_attributes['isParkingSpaceIncludedInPrice'] = isParkingSpaceIncludedInPrice
                
            else:
                hasParkingSpace = False
                
                house_attributes['hasParkingSpace'] = hasParkingSpace

        except:
            hasParkingSpace = False
            isParkingSpaceIncludedInPrice = False

            house_attributes['hasParkingSpace'] = hasParkingSpace
            house_attributes['isParkingSpaceIncludedInPrice'] = isParkingSpaceIncludedInPrice
        # =====================================================================
        # hasPlan
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[1]/button[2]/span'
        try:
            hasPlan = driver.find_element(by=By.XPATH, value=xpath).text

            if 'Plano' in hasPlan:
                hasPlan = True
            else:
                hasPlan = False

            house_attributes['hasPlan'] = hasPlan

        except:
            hasPlan = False

            house_attributes['hasPlan'] = hasPlan
        # =====================================================================
        # has360
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[1]/button[2]/span'
        try:
            has360 = driver.find_element(by=By.XPATH, value=xpath).text

            if 'Virtual Tour' in has360:
                has360 = True
            else:
                has360 = False

            house_attributes['has360'] = has360

        except:
            has360 = False

            house_attributes['has360'] = has360
        # =====================================================================
        # has3DTour
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[1]/button[3]/span'
        try:
            has3DTour = driver.find_element(by=By.XPATH, value=xpath).text

            if 'Visita 3D' in has3DTour:
                has3DTour = True
            else:
                has3DTour = False

            house_attributes['has3DTour'] = has3DTour

        except Exception as e:
            has3DTour = False

            house_attributes['has3DTour'] = has3DTour
        # =====================================================================
        # hasVideo
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[1]/button[3]/span'
        try:
            hasVideo = driver.find_element(by=By.XPATH, value=xpath).text

            if 'Vídeo' in hasVideo:
                hasVideo = True
            else:
                hasVideo = False

            house_attributes['hasVideo'] = hasVideo

        except Exception as e:
            hasVideo = False

            house_attributes['hasVideo'] = hasVideo
        # =====================================================================
        # propertyType
        # =====================================================================
        xpath = '//*[@id="main"]/div/main/section[2]/div[2]/h1/span'
        propertyType = driver.find_element(by=By.XPATH, value=xpath).text

        # put propertyType in lowercase
        propertyType = propertyType.lower()

        if 'casa de pueblo' in propertyType:
            propertyType = 'countryHouse'
        elif 'casa de campo' in propertyType:
            propertyType = 'countryHouse'
        elif 'estudio' in propertyType:
            propertyType = 'studio'
        elif 'dúplex' in propertyType:
            propertyType = 'duplex'
        elif 'ático' in propertyType:
            propertyType = 'penthouse'
        elif 'penthouse' in propertyType:
            propertyType = 'penthouse'
        elif 'chalet' in propertyType:
            propertyType = 'chalet'
        elif 'casa' in propertyType:
            propertyType = 'chalet'
        else:
            propertyType = 'flat'

        house_attributes['propertyType'] = propertyType
        # =====================================================================
        # direction
        # =====================================================================
        id = "sMap"

        # roll down the page until the map is visible
        while True:
            try:
                driver.find_element(by=By.ID, value=id)
                break
            except Exception as e:
                driver.execute_script("window.scrollBy(0, 100)")
                time.sleep(0.5)
                
        url_map = driver.find_element(by=By.ID, value=id).get_attribute('src')

        # get the center=lat,lng from the url
        center = re.findall(r'center=(-?\d+\.\d+),(-?\d+\.\d+)', url_map)[0]

        # transform the center into a tuple
        center = tuple(map(float, center))

        latitude = float(center[0])
        longitude = float(center[1])

        house_attributes['latitude'] = latitude
        house_attributes['longitude'] = longitude
        # =====================================================================
        # Making atributes of the house a DataFrame
        # =====================================================================
        df_house = pd.DataFrame(house_attributes, index=[0])

        # make propertyCode index
        df_house.set_index('propertyCode', inplace=True)

        #Clustering address
        cluster = pickle.load(open('../src/models/kmeans_clustering.pkl', 'rb'))
        df_house['direction'] = cluster.predict(df_house[['latitude', 'longitude']])
        df_house['direction'] = df_house['direction'].map({0: 'central', 1: 'south', 2: 'north', 3: 'west'})

        df_house.drop(columns=['latitude', 'longitude'], inplace=True)
        # =====================================================================
        
        return df_house

    else:
        print("The URL is not valid.")
        return None

# Function to inverse Box-Cox transform a column
def inv_box_cox_transform(column, lamda):
    column = inv_boxcox(column, lamda)
    return column