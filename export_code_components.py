from config import Config
from CodeComponent import CodeComponent
from Dependency import Dependency

import sqlite3
from neo4j import GraphDatabase

def load_dependencies(sqlite_db_path):
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

def add_node(tx,cc):
    tx.run("MERGE (node:CodeComponent:"+cc.typeof+"{trial_id:$trial_id,cc_id:$cc_id,name:$name,type:$typeof,mode:$mode,first_char_line:$first_char_line,first_char_column:$first_char_column,last_char_line:$last_char_line,last_char_columnm:$last_char_columnm,container_id:$container_id})",
        trial_id = cc.trial_id,
        cc_id = cc.cc_id,
        name = cc.name,
        typeof = cc.typeof,
        mode = cc.mode,
        first_char_line = cc.first_char_line,
        first_char_column = cc.first_char_column,
        last_char_line = cc.last_char_line,
        last_char_columnm = cc.last_char_columnm,
        container_id = cc.container_id)

def add_dependency(tx, dependency):
    tx.run("MATCH (source:CodeComponent),(target:CodeComponent) WHERE source.trial_id = $source_trial_id AND target.trial_id = $target_trial_id AND source.cc_id = $source_cc_id AND target.cc_id = $target_cc_id MERGE (source)-[:"+dependency.typeof+"]->(target)",
        source_trial_id = dependency.source.trial_id,
        target_trial_id = dependency.target.trial_id,
        source_cc_id = dependency.source.cc_id, 
        target_cc_id = dependency.target.cc_id)

def insert_dependencies_in_neo4j(dependencies):
    driver = GraphDatabase.driver(config.NEO4J_DATABASE_URL, auth=(config.NEO4J_DATABASE_USERNAME, config.NEO4J_DATABASE_PASSWORD))
    session = driver.session()
    print("Dependencies len:"+str(len(dependencies)))        
    for d in dependencies:
        session.write_transaction(add_node,d.source)
        session.write_transaction(add_node,d.target)
        session.write_transaction(add_dependency,d)
    session.close()

config = Config()
dependencies = load_dependencies(config.NOW_DB_PATH)
insert_dependencies_in_neo4j(dependencies)
print("Done.")
