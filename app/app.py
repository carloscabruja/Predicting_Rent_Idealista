# Change working directory to the directory of the script
import os 
os.chdir(os.path.dirname(os.path.abspath(__file__))) 

# =============================================================================

# Librabries needed to create the application
import pickle
import numpy as np
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, jsonify
from flask_bootstrap import Bootstrap
from getter_houses_v2 import *



# =============================================================================

# Create the application object 
def create_app():
    app = Flask(__name__)
    Bootstrap(app)
    
    return app

app = create_app()

# Create a URL route in our application for "/"
@app.route('/')
def home():
    return render_template('index.html')

# =============================================================================

# Create a URL route in our application for "/predict"
@app.route('/predict', methods=['POST'])
def predict():
    # Get the data from the POST request.
    data = request.form.values()
    url = list(data)[0]

    # First let's get the property code
    property_code = get_id(url)

    if property_code != "The URL is not valid":

        # Get the house data
        house = get_house_attributes(url)

        # Load the model, preprocessor and lambda scaler
        model = pickle.load(open('../src/models/my_model_histgb.pkl', 'rb'))
        preprocessor = pickle.load(open('../src/models/preprocessor.pkl', 'rb'))
        lamda = pickle.load(open('../src/models/lamda_value.pkl', 'rb'))

        # Preprocess the data
        house = preprocessor.transform(house)

        # Predict the price
        price = model.predict(house)

        # Scale the price
        price = inv_box_cox_transform(price, lamda)

        
        
        return render_template('index.html', prediction_text=f"El precio de la casa según el algoritmo es de {price[0]}€")
    else:
        return render_template('index.html', prediction_text=f"La URL no es válida")


# =============================================================================

# run the application
if __name__ == '__main__':
    app.run()