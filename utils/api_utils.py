import requests
import urllib
import json
from typing import List
import pandas as pd
import re
import numpy as np

base_url = 'https://developer.nps.gov/api/v1'
parks_endpoint = 'parks'
google_maps_api_base_url = 'https://maps.googleapis.com/maps/api/distancematrix/json'

# NOTE: hardcoding keys or passwords is not best practice. This is done for simplicity in this case, but
# in a professional implementation, a secrets manager or similar should be used to retrieve the secrets during runtime
API_KEY = 'mjELbhTebKqjTLy3tp4yDWkxumD5LhvRduTh0k4v'
google_maps_api_key = "AIzaSyBB89nRFhowPZxK8Low4y94xti14v1XU9w"


def get_parks(start: int, limit: int) -> (List[str], int):
    # This method returns data retrieved from the API, given a start and a limit.
    params = {
        'start': start,
        'limit': limit,
        'API_KEY': API_KEY
    }
    url = base_url + '/' + parks_endpoint + '?' + urllib.parse.urlencode(params)
    batch_json_data = []
    items = 0

    try:
        response = requests.get(url=url, )
        status = response.status_code
        if status == 200:
            items = json.loads(response.text).get('total', [])
            batch_json_data = json.loads(response.text).get('data', [])
        elif status == 400:
            raise Exception('Bad request. Please check out documentation for more information.')
        elif status == 403:
            raise Exception('There was an issue with the authentication. Please check your API token.')
        elif status == 404:
            raise Exception('Not found. Please verify the url and endpoint you are trying to use.')
        elif status == 503:
            raise Exception('Service unavailable, please try again later. '
                            'Contact National Park Service for more information.')
        return batch_json_data, int(items)
    except Exception as e:
        raise e


def parse_time_to_minutes(time_str: str) -> int:
    # Initialize variables to store parsed values
    days = 0
    hours = 0
    minutes = 0

    # Parse days, hours, and minutes using regular expressions
    days_match = re.search(r'(\d+) day[s]?', time_str)
    hours_match = re.search(r'(\d+) hour[s]?', time_str)
    minutes_match = re.search(r'(\d+) minute[s]?', time_str)

    # Extract and convert days, hours, and minutes if they exist in the string
    if days_match:
        days = int(days_match.group(1))
    if hours_match:
        hours = int(hours_match.group(1))
    if minutes_match:
        minutes = int(minutes_match.group(1))

    # Calculate the total minutes
    total_minutes = (days * 24 * 60) + (hours * 60) + minutes
    return total_minutes


def parse_distance(dist_string: str) -> int:
    return int(dist_string.replace(',', '').strip(' km'))


def calculate_dist_and_time(park_data: pd.DataFrame, lat, lon) -> pd.DataFrame:
    params = {
        "origins": f"{lat},{lon}",
        "destinations": "",  # You will add park destinations dynamically
        "key": google_maps_api_key,
        "mode": "driving",  # Adjust the travel mode as needed
    }

    park_data['mins_to_dest'] = np.nan
    park_data['distance_to_dest_km'] = np.nan
    for row, park in park_data.iterrows():
        params["destinations"] = f"{park['latitude']},{park['longitude']}"
        response = requests.get(google_maps_api_base_url, params=params)
        request_status = json.loads(response.text)['rows'][0]['elements'][0]['status']
        if request_status == 'OK':
            distance = parse_distance(json.loads(response.text)['rows'][0]['elements'][0]['distance']['text'])
            total_mins = parse_time_to_minutes(json.loads(response.text)['rows'][0]['elements'][0]['duration']['text'])
        else:
            distance = np.nan
            total_mins = np.nan
        park_data.loc[row, 'mins_to_dest'] = total_mins
        park_data.loc[row, 'distance_to_dest_km'] = distance
        print(row)
    return park_data


def parse_df_to_json(df: pd.DataFrame):
    df_json = df.to_json(orient='records')
    parsed = json.loads(df_json)
    return parsed
