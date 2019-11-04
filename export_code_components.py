from config import Config
from CodeComponent import CodeComponent
from Dependency import Dependency

import sqlite3
from neo4j import GraphDatabase

def load_dependencies_from_sqlite(sqlite_db_path):
    print(sqlite_db_path)
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    query = ("select D.type as 'DEPENDENCY_TYPE', "
            "CC_INFLU.trial_id, CC_INFLU.id, CC_INFLU.name, CC_INFLU.type, CC_INFLU.mode, "
            "CC_INFLU.first_char_line, CC_INFLU.first_char_column, CC_INFLU.last_char_line, CC_INFLU.last_char_column, CC_INFLU.container_id, "
            "CC_DEPEND.trial_id, CC_DEPEND.id, CC_DEPEND.name, CC_DEPEND.type, CC_DEPEND.mode, "
            "CC_DEPEND.first_char_line, CC_DEPEND.first_char_column, CC_DEPEND.last_char_line, CC_DEPEND.last_char_column, CC_DEPEND.container_id "
            "from dependency D "
            "join evaluation EV_DEPEND on D.dependent_id = EV_DEPEND.id "
            "join evaluation EV_INFLU on D.dependency_id = EV_INFLU.id "
            "join code_component CC_DEPEND on EV_DEPEND.code_component_id = CC_DEPEND.id "
            "join code_component CC_INFLU on EV_INFLU.code_component_id = CC_INFLU.id " )

    dependencies = []

    for tupl in cursor.execute(query,[]):
        typeof = tupl[0]
        target = CodeComponent(tupl[1],tupl[2],tupl[3],tupl[4],tupl[5],tupl[6],tupl[7],tupl[8],tupl[9],tupl[10])
        source = CodeComponent(tupl[11],tupl[12],tupl[13],tupl[14],tupl[15],tupl[16],tupl[17],tupl[18],tupl[19],tupl[20])
        dependencies.append(Dependency(source,target,typeof))
    conn.close()
    return dependencies



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
    for d in dependencies:
        print(d.typeof)
        print(d.source.name)
        print(d.target.name)
        print('.#.#.#.')
        
    for d in dependencies:
        session.write_transaction(add_node,d.source)
        session.write_transaction(add_node,d.target)
        session.write_transaction(add_dependency,source,target,typeof)
    session.close()

config = Config()
dependencies = load_dependencies_from_sqlite(config.NOW_DB_PATH)
insert_dependencies_in_neo4j(dependencies)
print("Done.")
