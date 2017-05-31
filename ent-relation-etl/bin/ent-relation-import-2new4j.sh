#!/bin/bash
HDFS_PATH=/tmp/hive_export_inv/tmp/dstpath/
HOST=192.168.207.14
LOG_SUFFIX=`date +%Y%m%d-%H%M`
# step 1: spark pre-proess
/usr/lib/spark/spark-1.5.2-bin-hadoop2.4/bin/spark-submit --executor-memory 2G --num-executors 20 --executor-cores 3  --class com.chinadaas.association.etl.main.App --master yarn ent-relation-etl-1.0-SNAPSHOT1.jar > ./logs/association-$LOG_SUFFIX.log 2 >&1
# step 2:get csv data from hdfs
echo `date +%F" "%H:%M:%S`
if [ ! -d './import_data' ]; then
mkdir ./import_data
fi
rm -rf ./import_data/*
echo "begin get data from hdfs"
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/ent ./import_data/entbaseinfo.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/person ./import_data/person.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/eaddr ./import_data/pri_ent_addr.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/etel ./import_data/pri_ent_tel.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/paddr ./import_data/pri_person_addr.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/personinv ./import_data/person_inv_relation.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/entinv ./import_data/ent_inv_relation.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/legal ./import_data/lerepsign_relation.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/staff ./import_data/position_relation.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/entaddr ./import_data/pri_ent_addr_relation.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/enttel ./import_data/pri_ent_tel_relation.csv
hadoop fs -get /tmp/hive_export_inv/tmp/dstpath/peraddr ./import_data/pri_person_addr_relation.csv
echo "end!"
echo `date +%F" "%H:%M:%S`
echo "begin scp to dst server"
# step 3: scp to neo4j server
scp ./import_data/*.csv ops@192.168.207.14:/u01/importn4j_data
echo "end!"
# step 4:import csv to neo4j & create neo4j index 
ssh ops@192.168.207.14 '/u01/importn4j_data/importcsv_to_neo4j.sh'
echo 'done!'
echo `date +%F" "%H:%M:%S`
