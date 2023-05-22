#from flask import Flask, request
from flask import Flask, request, jsonify
import joblib

app = Flask(__name__)

# Load the trained predictive model
def load_model():
    # Load trained model 
    model = joblib.load('random_forest_model.pkl')
    pass

# Predict function
def predict(vol_moving_avg, adj_close_rolling_med):
    volume = int(vol_moving_avg) - int(adj_close_rolling_med)
    return volume

@app.route('/predict', methods=['GET'])
def prediction():
    # Get the input values from the query parameters
    vol_moving_avg = request.args.get('vol_moving_avg')
    adj_close_rolling_med = request.args.get('adj_close_rolling_med')

    # Perform prediction using the trained model
    result = predict(vol_moving_avg, adj_close_rolling_med)

    # Return the prediction as a response
    return str(result)

if __name__ == '__main__':
    # Load the trained model
    model = load_model()

    # Run the API service
    app.run(debug=True)
