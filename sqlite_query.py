import sqlite3
import sys

from config import Config
from Evaluation import Evaluation
from Dependency import Dependency

def load_dependencies(sqlite_db_path):
    print(sqlite_db_path)
    conn = sqlite3.connect(Config.NOW_DB_PATH)
    cursor = conn.cursor()
    query = ("select D.type as 'DEPENDENCY_TYPE', "
            "EV_INFLU.trial_id, EV_INFLU.id, EV_INFLU.checkpoint, EV_INFLU.code_component_id, EV_INFLU.activation_id, "            "EV_INFLU.repr, EV_INFLU.member_container_activation_id, EV_INFLU.member_container_id, CC_INFLU.name, CC_INFLU.type, "
            "EV_DEPEND.trial_id, EV_DEPEND.id, EV_DEPEND.checkpoint, EV_DEPEND.code_component_id, EV_DEPEND.activation_id, "
            "EV_DEPEND.repr, EV_DEPEND.member_container_activation_id, EV_DEPEND.member_container_id, CC_DEPEND.name, CC_DEPEND.type "
            "from dependency D "
            "join evaluation EV_DEPEND on D.dependent_id = EV_DEPEND.id "
            "join evaluation EV_INFLU on D.dependency_id = EV_INFLU.id "
            "join code_component CC_DEPEND on EV_DEPEND.code_component_id = CC_DEPEND.id "
            "join code_component CC_INFLU on EV_INFLU.code_component_id = CC_INFLU.id " )
    dependencies = []
    for tupl in cursor.execute(query,[]):
        typeof = tupl[0]
        target = Evaluation(tupl[1],tupl[2],tupl[3],tupl[4],tupl[5],tupl[6],tupl[7],tupl[8],tupl[9],tupl[10])
        source = Evaluation(tupl[11],tupl[12],tupl[13],tupl[14],tupl[15],tupl[16],tupl[17],tupl[18],tupl[19],tupl[20])
        dependencies.append(Dependency(source,target,typeof))
    conn.close()
    return dependencies

def get_ev_id(name,sqlite_db_path):
    conn = sqlite3.connect(Config.NOW_DB_PATH)
    cursor = conn.cursor()
    query = ("select ev.id from evaluation ev "
             "join code_component cc on ev.code_component_id = cc.id "
             "where cc.name = ? "
             "order by ev.id DESC")
    cursor.execute(query, [str(name)])
    result = cursor.fetchone()
    return result[0]

dependencies = load_dependencies(Config.NOW_DB_PATH)
first_ev = sys.argv[1]
second_ev = sys.argv[2]

first_ev_id = get_ev_id(first_ev,Config.NOW_DB_PATH)
second_ev_id = get_ev_id(second_ev,Config.NOW_DB_PATH)

print(first_ev)
print(first_ev_id)

print(second_ev)
print(second_ev_id)
