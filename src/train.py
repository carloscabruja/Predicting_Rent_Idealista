# Change working directory to the directory of the script
import os 
os.chdir(os.path.dirname(os.path.abspath(__file__))) 

# =============================================================================

# Libraries needed to train the model
import pickle
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, GridSearchCV, RepeatedKFold
from sklearn.ensemble import RandomForestRegressor

from utils.ETL import ETL
from utils.target_preprocess import *

# =============================================================================

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
        df_temp = pd.read_csv("data/raw/data_raw.csv")
    else:
        print("Error: No local data file found")
        exit()

# Process data
df_temp = etl.transform(df_temp)

# Load data into database
df = etl.load_data(df_temp)

# =============================================================================

print(f"Preprocessing data...")

# Split data into train and test sets
X = df.drop(columns=['price'])
y = df['price']

X_train, X_test, y_train, y_test, train_index, test_index = train_test_split(
    X, 
    y, 
    df.index,
    test_size=0.2, 
    random_state=42, 
    shuffle=True
)

numeric_cols = X_train.select_dtypes(include=['float64', 'int']).columns.to_list()
cat_cols = X_train.select_dtypes(include=['object', 'category']).columns.to_list()
bool_features = X_train.select_dtypes(include=['bool']).columns

# Preprocessing pipeline for numeric features
numeric_transformer = Pipeline(
                        steps=[
                            ('imputer', SimpleImputer(strategy='mean')),
                            ('scaler', StandardScaler())
                        ]
                      )


# Preprocessing pipeline for categorical features
categorical_transformer = Pipeline(
                            steps=[
                                ('imputer', SimpleImputer(strategy='most_frequent')),
                                ('onehot', OneHotEncoder(handle_unknown='ignore'))
                            ]
                          )

# Preprocessor to run both numeric and categorical transformations
preprocessor = ColumnTransformer(
                    transformers=[
                        ('numeric', numeric_transformer, numeric_cols),
                        ('cat', categorical_transformer, cat_cols)
                    ],
                    remainder='passthrough'
                )

X_train_prep = preprocessor.fit_transform(X_train)
X_test_prep  = preprocessor.transform(X_test)

# Preprocess target variable
y_train_prep, lamda = box_cox_transform(y_train)

# Save preprocessor and lambda value for later use
pickle.dump(preprocessor, open("models/preprocessor.pkl", "wb"))
pickle.dump(lamda, open("models/lamda_value.pkl", "wb"))

print(f"Data preprocessing completed")

# =============================================================================

print(f"Training model...")

# Grid search parameters
param_grid = {
    'n_estimators': range(800, 806),
    'max_depth'   : [None, 16],
    'max_features': ['sqrt', 5]
}

# Create instance of RandomForestRegressor
regressor = RandomForestRegressor(random_state=42)

# Create cross-validation object
cv = RepeatedKFold(n_splits=10, n_repeats=3, random_state=42)

# Create instance of GridSearchCV
grid_search = GridSearchCV(
    estimator=regressor,
    param_grid=param_grid,
    scoring='neg_mean_squared_error',
    cv=cv,
    n_jobs=-1,
)

# Fit model to training data
grid_search.fit(X_train_prep, y_train_prep)

print(f"Model training completed")

# =============================================================================
model = grid_search.best_estimator_

# Print best parameters and best score
print(f"Best parameters: {grid_search.best_params_}")
print(f"Best score: {grid_search.best_score_}")

# =============================================================================

# Save model to disk
print(f"Saving model to disk...")
pickle.dump(model, open("models/new_model.pkl", "wb"))

print(f"Model saved successfully, exiting...")
exit()