from neo4j import GraphDatabase

neo4j_uri = "neo4j+s://d01170b1.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "Y0KzgTDhQqQ3OSQQSutRVYaM81p5y5w8OzoYlkyuX78"

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

query = """
    MATCH (c:Customer)-[sb:SUBSCRIBED_TO]->(s:Subscription)
    RETURN c.customer_id, s.subscription_id,s.service_id,s.start_date,s.end_date,s.price
"""

def customer_subscription_service(tx):
    tx.run("CREATE (:Customer {customer_id: toInteger($cust_id), customer_name: $cust_name, customer_phone: $cust_phone})-[:SUBSCRIBED_TO]->(:Subscription {subscription_id: toInteger($sub_id), service_id: toInteger($svc_id), service_name: $svc_name, start_date: date($start_date), end_date: date($end_date), price: toFloat($price)})",
        cust_id="1",cust_name="John Doe",cust_phone="254711223344",sub_id="101",svc_id="1001",svc_name="Voice",start_date="2023-01-01",end_date="2023-01-31",price="10")
    tx.run("CREATE (:Customer {customer_id: toInteger($cust_id), customer_name: $cust_name, customer_phone: $cust_phone})-[:SUBSCRIBED_TO]->(:Subscription {subscription_id: toInteger($sub_id), service_id: toInteger($svc_id), service_name: $svc_name, start_date: date($start_date), end_date: date($end_date), price: toFloat($price)})",
        cust_id="2",cust_name="Foo Bar",cust_phone="254722334455",sub_id="202",svc_id="1002",svc_name="Data",start_date="2023-02-01",end_date="2023-02-28",price="12.5")
    tx.run("CREATE (:Customer {customer_id: toInteger($cust_id), customer_name: $cust_name, customer_phone: $cust_phone})-[:SUBSCRIBED_TO]->(:Subscription {subscription_id: toInteger($sub_id), service_id: toInteger($svc_id), service_name: $svc_name, start_date: date($start_date), end_date: date($end_date), price: toFloat($price)})",
        cust_id="3",cust_name="Lorem Ipsum",cust_phone="254733445566",sub_id="303",svc_id="1003",svc_name="Hybrid",start_date="2023-03-01",end_date="2023-03-31",price="13.75")


with driver.session() as session:
    session.write_transaction(customer_subscription_service)
    result = session.run(query)
    for record in result:
        print(record)

driver.close()
