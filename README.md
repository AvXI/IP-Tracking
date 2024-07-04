# IP-Tracking
In this code, we first connect to the PostgreSQL database that stores the IP address data. We then load the data into a pandas dataframe and preprocess it by converting the date column into separate year, month, day, and hour columns.
Next, we use the geolite2 library to add location information to each IP address in the data. We then use the KMeans algorithm from scikit-learn library to cluster the data into 5 groups based on year, month, day, and hour features. We then add a new column to the dataframe to indicate the cluster label.
Finally, we save the results back to the database by creating a new table and inserting each row of the dataframe into it.
