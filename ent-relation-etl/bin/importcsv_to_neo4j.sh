#!/bin/bash
NEO4J_HOME=/u01/neo4j-community-3.1.3-two/bin/
rm -rf /u01/neo4j-community-3.1.3-two/data/databases/graph.db
/u01/neo4j-community-3.1.3-two/bin/neo4j-import --into  /u01/neo4j-community-3.1.3-two/data/databases/graph.db --nodes:person "/u01/importn4j_data/person.csv" --nodes:ent "/u01/importn4j_data/entbaseinfo.csv" --nodes:paddr "/u01/importn4j_data/pri_person_addr.csv" --nodes:etel "/u01/importn4j_data/pri_ent_tel.csv" --nodes:eaddr "/u01/importn4j_data/pri_ent_addr.csv"  --relationships:staff "/u01/importn4j_data/position_relation.csv"  --relationships:legal "/u01/importn4j_data/lerepsign_relation.csv" --relationships:peraddr "/u01/importn4j_data/pri_person_addr_relation.csv" --relationships:enttel "/u01/importn4j_data/pri_ent_tel_relation.csv"  --relationships:entaddr "/u01/importn4j_data/pri_ent_addr_relation.csv" --relationships:inv "/u01/importn4j_data/ent_inv_relation.csv" --relationships:inv "/u01/importn4j_data/person_inv_relation.csv"  --delimiter '|' --bad-tolerance 10000000 --skip-duplicate-nodes > log.log
# create  neo4j index 
/u01/neo4j-community-3.1.3-two/bin/neo4j restart
sleep 30s
/u01/neo4j-community-3.1.3-two/bin/neo4j-shell -c "create  index on :person(name);";
/u01/neo4j-community-3.1.3-two/bin/neo4j-shell -c "create  index on :ent(name);";
/u01/neo4j-community-3.1.3-two/bin/neo4j-shell -c "create  index on :ent(regno);";
/u01/neo4j-community-3.1.3-two/bin/neo4j-shell -c "create  index on :ent(creditcode);";
/u01/neo4j-community-3.1.3-two/bin/neo4j-shell -c "create  index on :person(zsid);";
/u01/neo4j-community-3.1.3-two/bin/neo4j-shell -c "create  index on :ent(zsid);";
