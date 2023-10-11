import mysql.connector
import pandas as pd
import queries

# NOTE: hardcoding keys or passwords is not best practice. This is done for simplicity in this case, but
# in a professional implementation, a secrets manager or similar should be used to retrieve the secrets during runtime
host = 'myhamlet-tech-assessment.cq5e5oz9ou04.us-east-1.rds.amazonaws.com'
user = 'admin'
password = 'myhamletpassword'
db_name = 'myhamlet_tech_assessment'

db_config = {
    "host": host,
    "user": user,
    "password": password,
    "database": db_name
}


def get_db_conn():
    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        print("Connected to MySQL database")
        return connection
    except mysql.connector.Error as err:
        print("Connection failed:", err)


def query_data(state: str) -> pd.DataFrame:
    try:
        connection = get_db_conn()

        # Create a cursor object
        cursor = connection.cursor(buffered=True)

        sql = queries.RANKED_DATA

        if state:
            sql += f" AND States = '{state}'"

        cursor.execute(sql,)
        connection.commit()
        lista = []
        for item in cursor.fetchall():
            lista.append(item)

        column_query = queries.COLUMNS_QUERY

        cursor.execute(column_query)
        connection.commit()
        metadata = []
        for item in cursor.fetchall():
            metadata.append(item)
        column_list = [col[0] for col in metadata]

        park_data = pd.DataFrame(data=lista, columns=column_list)

        return park_data
    except mysql.connector.Error as err:
        print("Connection failed:", err)


def upload_data(df: pd.DataFrame) -> None:
    try:

        connection = get_db_conn()

        # Create a cursor object
        cursor = connection.cursor()

        # Define an INSERT INTO statement
        insert_query = queries.INSERT_QUERY

        # Iterate over rows in the DataFrame and insert each row into the table
        for index, row in df.iterrows():
            data_tuple = (
                row['id'], row['fullName'], row['description'], row['latitude'], row['longitude'],
                row['activities'], row['topics'], row['states'], row['contacts'], row['fees'], row['addresses'],
                row['datetime']
            )
            cursor.execute(insert_query, data_tuple)

        # Commit the changes
        connection.commit()
        print(f"Data inserted into table 'parks'")

    except mysql.connector.Error as err:
        print("Connection failed:", err)

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
