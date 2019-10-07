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

def add_dependency(tx, source, target):
    tx.run("MERGE (source:Evaluation {id:$source_id, name: $source_name}) "
           "MERGE (source)-[:CONTRIBUTED_TO]->(target:Evaluation {id:$target_id, name: $target_name})",
           source_id = source.ev_id, target_name=target.ev_id())


def insert_dependencies_in_neo4j(dependencies):
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))
    session = driver.session()
    for source,target in dependencies:
        session.write_transaction(add_dependency, source, target)

config = Config()
dependencies = load_dependencies_from_sqlite(config.NOW_DB_PATH)
print(len(dependencies))
insert_dependencies_in_neo4j(dependencies)