from config import Config
from Evaluation import Evaluation
import sqlite3
from neo4j import GraphDatabase
import exporter

def add_dependency(tx, source, target):
    tx.run("MATCH (source:CodeComponent),(target:CodeComponent) WHERE source.code_component_id = $source_cc_id AND target.code_component_id = $target_cc_id MERGE (source)-[:depends_on]->(target)",
        source_cc_id = source.code_component_id, target_cc_id = target.code_component_id)

def add_node(tx,node):
    tx.run("MERGE (node:CodeComponent{code_component_id:$code_component_id, name:$name, code_component_type:$code_component_type})",
        code_component_id=node.code_component_id, name=node.code_component_name, code_component_type=node.code_component_type)


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
