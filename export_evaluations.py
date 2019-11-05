from config import Config
from Evaluation import Evaluation
from Dependency import Dependency

import sqlite3
from neo4j import GraphDatabase

def load_dependencies(sqlite_db_path):
    print(sqlite_db_path)
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    query = ("select D.type as 'DEPENDENCY_TYPE', "
            "EV_INFLU.trial_id, EV_INFLU.id, EV_INFLU.checkpoint, EV_INFLU.code_component_id, EV_INFLU.activation_id, "
            "EV_INFLU.repr, EV_INFLU.member_container_activation_id, EV_INFLU.member_container_id, CC_INFLU.name, "
            "EV_DEPEND.trial_id, EV_DEPEND.id, EV_DEPEND.checkpoint, EV_DEPEND.code_component_id, EV_DEPEND.activation_id, "
            "EV_DEPEND.repr, EV_DEPEND.member_container_activation_id, EV_DEPEND.member_container_id, CC_DEPEND.name "
            "from dependency D "
            "join evaluation EV_DEPEND on D.dependent_id = EV_DEPEND.id "
            "join evaluation EV_INFLU on D.dependency_id = EV_INFLU.id "
            "join code_component CC_DEPEND on EV_DEPEND.code_component_id = CC_DEPEND.id "
            "join code_component CC_INFLU on EV_INFLU.code_component_id = CC_INFLU.id " )
    dependencies = []
    for tupl in cursor.execute(query,[]):
        typeof = tupl[0]
        target = Evaluation(tupl[1],tupl[2],tupl[3],tupl[4],tupl[5],tupl[6],tupl[7],tupl[8],tupl[9])
        source = Evaluation(tupl[10],tupl[11],tupl[12],tupl[13],tupl[14],tupl[15],tupl[16],tupl[17],tupl[18])
        dependencies.append(Dependency(source,target,typeof))
    conn.close()
    return dependencies

def add_node(tx,ev):
    tx.run("MERGE (node:Evaluation{trial_id:$trial_id,ev_id:$ev_id,checkpoint:$checkpoint,cc_id:$cc_id,activation_id:$activation_id,representation:$representation,member_container_activation_id:$member_container_activation_id,member_container_id:$member_container_id,name:$name})",
        trial_id = ev.trial_id,
        ev_id = ev.ev_id,
        checkpoint = ev.checkpoint,
        cc_id = ev.cc_id,
        activation_id = ev.activation_id,
        representation = ev.representation,
        member_container_activation_id = ev.member_container_activation_id,
        member_container_id = ev.member_container_id,
        name = ev.name)

def add_dependency(tx, dependency):
    tx.run("MATCH (source:Evaluation),(target:Evaluation) WHERE source.trial_id = $source_trial_id AND target.trial_id = $target_trial_id AND source.ev_id = $source_ev_id AND target.ev_id = $target_ev_id MERGE (source)-[:"+dependency.typeof+"]->(target)",
        source_trial_id = dependency.source.trial_id,
        target_trial_id = dependency.target.trial_id,
        source_ev_id = dependency.source.ev_id, 
        target_ev_id = dependency.target.ev_id)

def insert_dependencies_in_neo4j(dependencies):
    driver = GraphDatabase.driver(config.NEO4J_DATABASE_URL, auth=(config.NEO4J_DATABASE_USERNAME, config.NEO4J_DATABASE_PASSWORD))
    session = driver.session()
    print("Dependencies len:"+str(len(dependencies)))
    for d in dependencies:
        session.write_transaction(add_node,d.source)
        session.write_transaction(add_node,d.target)
        session.write_transaction(add_dependency, d)
    session.close()

config = Config()
dependencies = load_dependencies(config.NOW_DB_PATH)
insert_dependencies_in_neo4j(dependencies)
print("Done.")
