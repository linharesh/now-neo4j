from neo4j import GraphDatabase
from config import Config

config = Config()
driver = GraphDatabase.driver(config.NEO4J_DATABASE_URL, auth=(config.NEO4J_DATABASE_USERNAME, config.NEO4J_DATABASE_PASSWORD))
with driver.session() as session:
    result = session.run("MATCH (n) DETACH DELETE n")
    print(result)
    result = session.run(":style reset")
    print(result)
    
