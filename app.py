from flask import Flask, jsonify
import snowflake.connector
import requests
from weather_to_snowflake import insert_weather_data

app = Flask(__name__)

@app.route('/')
def home():
    return "Hello, World!"

@app.route('/weather-data', methods=['GET'])
def get_weather_data():
    result = insert_weather_data()
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)