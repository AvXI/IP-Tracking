
import pandas as pd
from sklearn.cluster import KMeans
import psycopg2
from geolite2 import geolite2

# Load the IP address data from the database
conn = psycopg2.connect("dbname=ip_address_database user=postgres password=password")
cursor = conn.cursor()
cursor.execute("SELECT * FROM ip_addresses")
ip_data = pd.DataFrame(cursor.fetchall(), columns=['ip_address', 'date', 'user_id'])

# Preprocess the data
ip_data['date'] = pd.to_datetime(ip_data['date'])
ip_data['year'] = ip_data['date'].dt.year
ip_data['month'] = ip_data['date'].dt.month
ip_data['day'] = ip_data['date'].dt.day
ip_data['hour'] = ip_data['date'].dt.hour
ip_data.drop('date', axis=1, inplace=True)

# Add location information to the data
reader = geolite2.reader()
def add_location(ip):
    try:
        location = reader.get(ip)
        if location is not None:
            return location['country']['iso_code']
        else:
            return None
    except:
        return None
ip_data['location'] = ip_data['ip_address'].apply(lambda x: add_location(x))

# Cluster the data using KMeans algorithm
X = ip_data[['year', 'month', 'day', 'hour']].values
kmeans = KMeans(n_clusters=5, random_state=42)
kmeans.fit(X)
labels = kmeans.predict(X)
ip_data['label'] = labels

# Save the results back to the database
cursor.execute("CREATE TABLE IF NOT EXISTS ip_address_clusters (ip_address VARCHAR(255), user_id VARCHAR(255), location VARCHAR(255), label INTEGER)")
for index, row in ip_data.iterrows():
    cursor.execute("INSERT INTO ip_address_clusters (ip_address, user_id, location, label) VALUES (%s, %s, %s, %s)", (row['ip_address'], row['user_id'], row['location'], row['label']))
conn.commit()
cursor.close()
conn.close()