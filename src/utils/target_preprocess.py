import scipy.stats as stats
from scipy.special import boxcox, inv_boxcox

# Function to Box-Cox transform a column
def box_cox_transform(column, lamda= None):
    if lamda is None:
        column, lamda = stats.boxcox(column)
        
        return column, lamda
    else:
        column = boxcox(column, lamda)
        return column

# Function to inverse Box-Cox transform a column
def inv_box_cox_transform(column, lamda):
    column = inv_boxcox(column, lamda)
    return column