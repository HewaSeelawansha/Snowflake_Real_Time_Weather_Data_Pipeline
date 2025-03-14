import snowflake.connector
import requests
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env

def insert_weather_data():
    # Snowflake Connection
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    cur = conn.cursor()

    # Fetch weather data from the API
    url = os.getenv("WEATHER_API_URL")
    params = {"key": os.getenv("WEATHER_API_KEY"), "q": "6.8213291,80.0415729"}  # Example location "Paris" q=48.8567,2.3508
    response = requests.get(url, params=params)

    # Check if the response is successful (status code 200)
    if response.status_code != 200:
        print(f"Error: API returned status code {response.status_code}")
        return {"message": f"Error fetching data: {response.status_code}"}

    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return {"message": "Failed to decode JSON response from the weather API"}

    # Extract the required fields from the API response with get to avoid KeyError
    current_data = data.get("current", {})

    last_updated_epoch = current_data.get("last_updated_epoch")
    last_updated = current_data.get("last_updated")
    temp_c = current_data.get("temp_c")
    temp_f = current_data.get("temp_f")
    is_day = current_data.get("is_day")
    text = current_data.get("condition", {}).get("text")
    icon = current_data.get("condition", {}).get("icon")
    code = current_data.get("condition", {}).get("code")
    wind_mph = current_data.get("wind_mph")
    wind_kph = current_data.get("wind_kph")
    wind_degree = current_data.get("wind_degree")
    wind_dir = current_data.get("wind_dir")
    pressure_mb = current_data.get("pressure_mb")
    pressure_in = current_data.get("pressure_in")
    precip_mm = current_data.get("precip_mm")
    precip_in = current_data.get("precip_in")
    humidity = current_data.get("humidity")
    cloud = current_data.get("cloud")
    feelslike_c = current_data.get("feelslike_c")
    feelslike_f = current_data.get("feelslike_f")
    vis_km = current_data.get("vis_km")
    vis_miles = current_data.get("vis_miles")
    gust_mph = current_data.get("gust_mph")
    gust_kph = current_data.get("gust_kph")
    uv = current_data.get("uv")
    windchill_c = current_data.get("windchill_c")
    windchill_f = current_data.get("windchill_f")
    heatindex_c = current_data.get("heatindex_c")
    heatindex_f = current_data.get("heatindex_f")
    dewpoint_c = current_data.get("dewpoint_c")
    dewpoint_f = current_data.get("dewpoint_f")

    # Insert weather data into Snowflake
    cur.execute("""
        INSERT INTO WEATHER_DB.WEATHER_SCHEMA.WEATHER_TABLE (
            last_updated_epoch, last_updated, temp_c, temp_f, is_day, text, icon, code,
            wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in,
            precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f,
            vis_km, vis_miles, gust_mph, gust_kph, uv, windchill_c, windchill_f,
            heatindex_c, heatindex_f, dewpoint_c, dewpoint_f
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """, (
        last_updated_epoch, last_updated, temp_c, temp_f, is_day, text, icon, code,
        wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in,
        precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f,
        vis_km, vis_miles, gust_mph, gust_kph, uv, windchill_c, windchill_f,
        heatindex_c, heatindex_f, dewpoint_c, dewpoint_f
    ))

    cur.close()
    conn.close()

    return {"message": "Data Inserted Successfully!"}