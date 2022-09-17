# =============================================================================
"""
This script compress the models that are more than 100MB
"""
# =============================================================================
# Change working directory to the directory of the script
import os 
os.chdir(os.path.dirname(os.path.abspath(__file__))) 
# =============================================================================
# Libraries needed to compress the models
import pickle
from blosc import compress, decompress
import time
# =============================================================================
# Load the models
rf = pickle.load(open('models/my_model_rf.pkl', 'rb'))
new_model = pickle.load(open('models/new_model.pkl', 'rb'))
# =============================================================================
# Compress the models
start = time.time() # start a timer
print("Compressing models...")
rf_compressed = compress(pickle.dumps(rf))
new_model_compressed = compress(pickle.dumps(new_model))
print("Models compressed")
end = time.time() # end the timer
print("Time to compress: {} seconds".format(end - start))
# =============================================================================
# Decompress the models
start = time.time() # start a timer
print("Decompressing models...")
rf_decompressed = pickle.loads(decompress(rf_compressed))
new_model_decompressed = pickle.loads(decompress(new_model_compressed))
print("Models decompressed")
end = time.time() # end the timer
print("Time to decompress: {} seconds".format(end - start))
# =============================================================================
# Save the compressed models
pickle.dump(rf_compressed, open('models/my_model_rf_compressed.pkl', 'wb'))
pickle.dump(new_model_compressed, open('models/new_model_compressed.pkl', 'wb'))
#==============================================================================
