from fastapi import FastAPI, Query
from utils.db_utils import query_data
from utils.api_utils import parse_df_to_json, calculate_dist_and_time

app = FastAPI()


@app.get("/parks")
async def get_parks(state: str = Query(default='', description="Filter parks by state")):
    park_data = query_data(state)
    parsed = parse_df_to_json(park_data)
    return parsed


@app.get("/nearest_parks")
async def get_nearest_parks(
        lat: float = Query(..., description="Latitude of your current location"),
        lon: float = Query(..., description="Longitude of your current location"),
        number_of_parks: int = Query(default=5, description="Number of top nearest parks the user wants to retrieve"),
        max_dist: int = Query(default='', description="Maximum distance to filter parks by")
):

    park_data = query_data(state='')
    park_data = calculate_dist_and_time(park_data, lat, lon)

    nearest = None
    if max_dist:
        # Convert column to numerical format.
        park_data['distance_to_dest_km'] = park_data['distance_to_dest_km'].astype(float)
        # Return the top results based on the provided distance threshold.
        sorted_park_data = park_data.sort_values(by='distance_to_dest_km', ascending=True, na_position='last', inplace=False)
        nearest = sorted_park_data[sorted_park_data['distance_to_dest_km'] < max_dist]
    elif number_of_parks:
        # Return the top results based on the number of parks provided.
        sorted_park_data = park_data.sort_values(by='mins_to_dest', ascending=True, na_position='last', inplace=False)
        nearest = sorted_park_data.head(number_of_parks)
    parsed = parse_df_to_json(nearest)
    return parsed
