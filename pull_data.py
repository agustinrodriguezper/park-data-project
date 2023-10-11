import math
import time
from typing import List
from utils.api_utils import get_parks
from utils.data_utils import process_data
from utils.db_utils import upload_data

# This is the limit of records per query
limit_per_page = 100


def get_parks_data() -> List:
    # The API works with a limit in the amount of records you want to retrieve in each request. We are going to
    # retrieve all the data making multiple calls in function of the total number of elements.

    # Do a first query and get the first batch of data. Also get the total number of records to determine
    # how many times we are going to need to make a request to retrieve all the data.
    print('Retrieving data batch 1...')
    data_list, items = get_parks(0, limit_per_page)

    # Get the total amount of pages (of 100 elements each) by rounding up the division between the total amount and
    # the limit (size of the page).
    pages = math.ceil(items / limit_per_page)

    # Start the for loop in 1 to avoid duplicating the data from the first batch.
    for i in range(1, pages):
        print(f'Retrieving data batch {i+1}...')
        batch_json_data, _ = get_parks(i * limit_per_page, limit_per_page)
        data_list += batch_json_data
        time.sleep(2)

    return data_list


if __name__ == '__main__':
    raw_parks_data = get_parks_data()

    print('Retrieved all parks data. Beginning data process.')
    processed_df = process_data(raw_parks_data)

    print('Processed all data. Uploading to RDS database.')
    upload_data(processed_df)

    print('Successfully uploaded data to RDS. Finishing execution.')
