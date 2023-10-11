import pandas as pd
from typing import List
import re
from datetime import datetime


TODAY = datetime.today()
TODAY_STR = TODAY.strftime('%m-%d-%Y %H:%M:%S')


def process_data(raw_data_list: List) -> pd.DataFrame:
    # Initialize an empty list to store DataFrame rows
    rows = []

    # Iterate through the 'data' list and extract the required information
    # NOTE: there is a more efficient way to do this, using 'json_normalize', but this way
    # we can also parse the activities and topics. In case users didn't use the activities
    # and topics data, we could remove them and improve efficiency.
    for item in raw_data_list:
        row = {
            "id": item["id"],
            "fullName": item["fullName"],
            "description": item["description"],
            "latitude": item["latitude"],
            "longitude": item["longitude"],
            "activities": ", ".join(activity["name"] for activity in item["activities"]),
            "topics": ", ".join(topic["name"] for topic in item["topics"]),
            "states": item["states"],
            "designation": item["designation"],
            "datetime": TODAY_STR
        }

        # Extract the voice phone number from contacts or set it to None if not found
        voice_phone_number = next(
            (
                contact["phoneNumber"]
                for contact in item["contacts"]["phoneNumbers"]
                if contact["type"] == "Voice"
            ),
            None,
        )
        row["contacts"] = voice_phone_number

        # Fill with 0 the rows that don't contain a fee value
        fees = item["fees"]
        row["fees"] = fees if fees else 0

        # Extract the 'line1' for type 'Physical' from addresses or set it to None if not found
        physical_address = next(
            (
                address["line1"]
                for address in item["addresses"]
                if address["type"] == "Physical"
            ),
            None,
        )
        row["addresses"] = physical_address

        rows.append(row)

    # Create a DataFrame from the extracted data
    df = pd.DataFrame(rows)
    df = df[df["designation"].apply(lambda x: 'national' in x.lower() and 'park' in x.lower())]

    df['contacts'] = df['contacts'].apply(lambda x: re.sub(r'[()\-\\s]', '', str(x)))

    return df
