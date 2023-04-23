# Import required libraries
from neo4j import GraphDatabase
import pandas as pd
import psycopg2

# Define Neo4j connection details
neo4j_uri = "neo4j+s://d01170b1.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "Y0KzgTDhQqQ3OSQQSutRVYaM81p5y5w8OzoYlkyuX78"

# Define Postgres connection details
pg_host = '104.131.120.201'
pg_database = 'call_log_db'
pg_user = 'postgres'
pg_password = 'pg_W33k8'

# Define Neo4j query to extract data
query1 = """
    MATCH (c:Customer)-[s:SUBSCRIBES_TO]->(svc:Service)
    RETURN c.customer_id, s.subscription_id, svc.service_id,s.start_date, s.end_date, s.price
"""
neo4j_query = """
    MATCH (c:Customer)-[sb:SUBSCRIBED_TO]->(s:Subscription)
    RETURN c.customer_id, s.subscription_id,s.service_id,s.start_date,s.end_date,s.price
"""

# Define function to extract data from Neo4j and return a Pandas DataFrame
def extract_data():
    # Connect to Neo4j
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    with driver.session() as session:
        result = session.run(neo4j_query) #List of dictionaries
        # df = pd.DataFrame([dict(record) for record in result])
        df = pd.DataFrame([r.values() for r in result], columns=result.keys())
        # df = result.to_df()
        #df = pd.DataFrame(result)
        return df

# Define function to transform data
def transform_data(df):
    # Convert date fields to datetime objects
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    
    # Remove null values
    df.dropna(inplace=True)
     
    return df

# Define function to load data into Postgres
def load_data(df):
    # Connect to Postgres
    conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)
    # Create table if it doesn't exist
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS telecom_graph_data (
            customer_id INTEGER,
            subscription_id INTEGER,
            service_id INTEGER,
            start_date DATE,
            end_date DATE,
            price FLOAT
        )
        """)
        # Insert data into table
        for index, row in df.iterrows():
            cursor.execute("""
            INSERT INTO telecom_graph_data (customer_id, subscription_id, service_id, start_date, end_date, price)
            VALUES (row['customer_id'], row['subscription_id'], row['service_id'],
                        row['start_date'], row['end_date'], row['price'])
            """)
    
    conn.commit()
    cursor.close()
    conn.close()

# Define main function
def main():
    # Extract data from Neo4j
    data = extract_data()
    
    # Transform data using Pandas
    df = transform_data(data)
    
    # Load data into Postgres
    load_data(df)
    

# Call main function
if __name__ == "__main__":
    main()
