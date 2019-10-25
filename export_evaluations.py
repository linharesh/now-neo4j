from config import Config
from Evaluation import Evaluation
import sqlite3
from neo4j import GraphDatabase
import exporter

def add_dependency(tx, source, target):
    tx.run("MATCH (source:Evaluation),(target:Evaluation) WHERE source.evaluation_id = $source_ev_id AND target.evaluation_id = $target_ev_id MERGE (source)-[:depends_on]->(target)",
        source_ev_id = source.ev_id, target_ev_id = target.ev_id)

def add_node(tx,node):
    tx.run("MERGE (node:Evaluation{evaluation_id:$evaluation_id, name:$name})",
        evaluation_id=node.ev_id, name=node.code_component_name)


def insert_dependencies_in_neo4j(dependencies):
    driver = GraphDatabase.driver(config.NEO4J_DATABASE_URL, auth=(config.NEO4J_DATABASE_USERNAME, config.NEO4J_DATABASE_PASSWORD))
    session = driver.session()
    print("Dependencies len:"+str(len(dependencies)))
    for source,target in dependencies:
        session.write_transaction(add_node,source)
        session.write_transaction(add_node,target)
        session.write_transaction(add_dependency, source, target)
    session.close()

config = Config()
dependencies = exporter.load_dependencies_from_sqlite(config.NOW_DB_PATH)
insert_dependencies_in_neo4j(dependencies)
print("Done.")
