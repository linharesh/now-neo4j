from config import Config
from Evaluation import Evaluation
import sqlite3
from neo4j import GraphDatabase

def load_dependencies_from_sqlite(sqlite_db_path):
    print(sqlite_db_path)
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    query = ("select EV_INFLU.id as 'EV_INFLU_ID', CC_INFLU.id as 'CC_INFLU_ID', CC_INFLU.type as 'CC_INFLU_TYPEOF', CC_INFLU.name as 'CC_INFLU_NAME', "
            "EV_DEPEND.id as 'EV_DEPEND_ID', CC_DEPEND.id as 'CC_DEPEND_ID', CC_DEPEND.type as 'CC_DEPEND_TYPEOF', CC_DEPEND.name as 'CC_DEPEND_NAME' "
            "from dependency D "
            "join evaluation EV_DEPEND on D.dependent_id = EV_DEPEND.id "
            "join evaluation EV_INFLU on D.dependency_id = EV_INFLU.id "
            "join code_component CC_DEPEND on EV_DEPEND.code_component_id = CC_DEPEND.id "
            "join code_component CC_INFLU on EV_INFLU.code_component_id = CC_INFLU.id " )

    dependencies = []

    for tupl in cursor.execute(query,[]):
        source = Evaluation(tupl[4],tupl[5],tupl[6],tupl[7])
        target = Evaluation(tupl[0],tupl[1],tupl[2],tupl[3])
        dependencies.append((source,target))
    return dependencies

#def add_dependency(tx, source, target):
#        tx.run("MATCH (source:Evaluation),(target:Evaluation) WHERE source.evaluation_id = $source_ev_id AND target.evaluation_id = $target_ev_id MERGE (source)-[:depends_on]->(target)",
#           source_ev_id = source.ev_id, target_ev_id = target.ev_id)

#def add_node(tx,node):
#    tx.run("MERGE (node:Evaluation{evaluation_id:$evaluation_id, name:$name, code_component_id:$code_component_id, code_component_type:$code_component_type})",
#          evaluation_id=node.ev_id, name=node.code_component_name, code_component_id=node.code_component_id, code_component_type=node.code_component_type)


#def insert_dependencies_in_neo4j(dependencies):
#    driver = GraphDatabase.driver(config.NEO4J_DATABASE_URL, auth=(config.NEO4J_DATABASE_USERNAME, config.NEO4J_DATABASE_PASSWORD))
#    session = driver.session()
#    i = 0
#   print("Dependencies len:"+str(len(dependencies)))
#    for source,target in dependencies:
#        session.write_transaction(add_node,source)
#        session.write_transaction(add_node,target)
#        session.write_transaction(add_dependency, source, target)
#        i += 1
#        print(str(i)+" ~ ",end="")
#    session.close()

#config = Config()
#dependencies = load_dependencies_from_sqlite(config.NOW_DB_PATH)
#insert_dependencies_in_neo4j(dependencies)
#print("Done.")
