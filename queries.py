
RANKED_DATA = """WITH RankedData AS (
                  SELECT
                    id, 
                    fullName, 
                    description, 
                    latitude, 
                    longitude, 
                    activities, 
                    topics, 
                    states, 
                    contacts, 
                    fees, 
                    addresses, 
                    date_time,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY date_time DESC) AS rn
                  FROM parks
                )
                
                SELECT
                    id, 
                    fullName, 
                    description, 
                    latitude, 
                    longitude, 
                    activities, 
                    topics, 
                    states, 
                    contacts, 
                    fees, 
                    addresses, 
                    date_time
                FROM RankedData
                WHERE rn = 1"""

COLUMNS_QUERY = """SELECT
            COLUMN_NAME, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'parks'
            ORDER BY 2"""

INSERT_QUERY = """INSERT INTO parks (id, fullName, description, latitude, longitude, activities,
               topics, states, contacts, fees, addresses, date_time)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""