import requests
import pandas as pd

"""
Extract data from the OpenWeather api by making a GET call:
- parameters: api_url, params (city name and api key)
- returns: json api response 
"""
def extract(api_url, params):
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        print("Weather Data successfully extracted from API.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Error during extraction: {}".format(e))
        raise

"""
Transforms the weather data by extracting specific fields from the json. 
- parameters: json data from the weather api
- returns: dataframe with the transformed data
"""
def transform(data):
    try:
       city_name = data.get("name")
       #Transform the temperature field from kelvin to celcius
       temperature = data["main"]["temp"] - 273.15
       humidity = data["main"]["humidity"]
       pressure = data["main"]["pressure"]
       weather_description = data["weather"][0]["description"]
       wind_speed = data["wind"]["speed"]

       #Create a dataframe with rows for each extracted data field
       transformed_weather_data = pd.DataFrame({
           "Metric": ["City", "Temp (C)", "Humidity (%)", "Pressure (hPa)", "Description", "Wind Speed (m/s)"],
           "Value": [city_name, temperature, humidity, pressure, weather_description, wind_speed]
       })

       print("Successfully transformed weather data.")
       return transformed_weather_data
    except KeyError as e:
        print("Error during transformation: Missing key {}".format(e))
        raise

"""
Loads the transformed data into an output CSV file.
- parameters: data (the transformed dataframe), output_file_path (csv filepath)
"""
def load(data, output_file_path):
    try:
        data.to_csv(output_file_path, index = False)
        print("Weather Data successfully loaded into {}".format(output_file_path))
    except Exception as e:
        print("Error during loading: {}".format(e))
        raise 

if __name__ == "__main__":
    api_endpoint = "https://api.openweathermap.org/data/2.5/weather"

    api_key = "{API_KEY}"
    city_name = "Atlanta"

    output_file = "weather_data.csv"

    print("Starting ETL Process...")

    try:
        params = {
            "q": city_name,
            "appid": api_key
        }
        raw_data = extract(api_endpoint, params)
        print(raw_data)
        transformed_data = transform(raw_data)
        load(transformed_data,output_file)
        print("ETL process completed successfully.")
    except Exception as e:
        print("Error during ETL Process: {}".format(e))