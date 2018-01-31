#!/bin/bash
HDFS_PATH=/tmp/ent-relation/tmp/dstpath
HOST=192.168.207.14
LOG_SUFFIX=`date +%Y%m%d-%H%M`

SRC_PATH=/tmp/ent-relation/tmp/srcpath/
PARQUET_PATH=/tmp/ent-relation/tmp/parquet/
DST_PATH=/tmp/ent-relation/tmp/dstpath/
INV_PATH=/tmp/ent-relation/tmp/parquet/e_inv_investment_parquet/
SHORT_NAME_PATH=/tmp/ent-relation/ent_shortname/
CACHETABLE_PATH=/tmp/ent-relation/cachetable

# step 0 prepare
echo "check dir"
if [ ! -d './import_data' ]; then
mkdir ./import_data
fi

if [ ! -d './logs' ]; then
mkdir ./logs
fi

hadoop fs -test -d $SRC_PATH
if [ $? -ne 0 ] ;then
    hadoop fs -mkdir -p $SRC_PATH
fi

hadoop fs -test -d $PARQUET_PATH
if [ $? -ne 0 ] ;then
    hadoop fs -mkdir -p $PARQUET_PATH
fi

hadoop fs -test -d $DST_PATH
if [ $? -ne 0 ] ;then
    hadoop fs -mkdir -p $DST_PATH
fi

hadoop fs -test -d $INV_PATH
if [ $? -ne 0 ] ;then
    hadoop fs -mkdir -p $INV_PATH
fi

hadoop fs -test -d $SHORT_NAME_PATH
if [ $? -ne 0 ] ;then
    hadoop fs -mkdir -p $SHORT_NAME_PATH
    hadoop fs -put ./shortname.csv $SHORT_NAME_PATH
fi

hadoop fs -test -d $CACHETABLE_PATH
if [ $? -ne 0 ] ;then
    hadoop fs -mkdir -p $CACHETABLE_PATH
fi

rm -rf ./import_data/*
echo "check dir end"
# step 1: spark pre-proess
echo "step 1 start"
$SPARK_HOME/bin/spark-submit --executor-memory 10G --num-executors 10 --executor-cores 4  --class com.chinadaas.association.etl.common.CommonApp --master yarn ent-relation-etl.jar $1 > ./logs/association-$LOG_SUFFIX.log 2 >&1

if [ $? -ne 0 ] ;then
    echo "common error"
    exit 1
fi

/usr/lib/spark/spark-1.5.2-bin-hadoop2.4/bin/spark-submit --executor-memory 10G --num-executors 20 --executor-cores 4  --class com.chinadaas.association.etl.main.EntRelationApp --master yarn ent-relation-etl.jar $1 > ./logs/association-$LOG_SUFFIX.log 2 >&1

if [ $? -ne 0 ] ;then
    echo "ent-relation error"
    exit 1
fi

echo "step 1 end"
# step 2:get csv data from hdfs

echo `date +%F" "%H:%M:%S`
echo "begin step 2 get data from hdfs"
hadoop fs -get $HDFS_PATH/ent/ent ./import_data/entbaseinfo.csv
hadoop fs -get $HDFS_PATH/person/person ./import_data/person.csv
hadoop fs -get $HDFS_PATH/eaddr/eaddr ./import_data/pri_ent_addr.csv
hadoop fs -get $HDFS_PATH/etel/etel ./import_data/pri_ent_tel.csv
hadoop fs -get $HDFS_PATH/paddr/paddr ./import_data/pri_person_addr.csv
hadoop fs -get $HDFS_PATH/personinv/personinv ./import_data/person_inv_relation.csv
hadoop fs -get $HDFS_PATH/entinv/entinv ./import_data/ent_inv_relation.csv
hadoop fs -get $HDFS_PATH/legal/legal ./import_data/lerepsign_relation.csv
hadoop fs -get $HDFS_PATH/staff/staff ./import_data/position_relation.csv
hadoop fs -get $HDFS_PATH/entaddr/entaddr ./import_data/pri_ent_addr_relation.csv
hadoop fs -get $HDFS_PATH/enttel/enttel ./import_data/pri_ent_tel_relation.csv
hadoop fs -get $HDFS_PATH/peraddr/peraddr ./import_data/pri_person_addr_relation.csv
hadoop fs -get $HDFS_PATH/invhold/invhold ./import_data/ent_invhold_relation.csv
hadoop fs -get $HDFS_PATH/invjoin/invjoin ./import_data/ent_invjoin_relation.csv
hadoop fs -get $HDFS_PATH/personjoin/personjoin ./import_data/person_join_relation.csv
hadoop fs -get $HDFS_PATH/personhold/personhold ./import_data/person_hold_relation.csv
hadoop fs -get $HDFS_PATH/entandenttel/entandenttel ./import_data/ent_andenttel_relation.csv
hadoop fs -get $HDFS_PATH/entandentaddr/entandentaddr ./import_data/ent_andentaddr_relation.csv
hadoop fs -get $HDFS_PATH/personmerge/personmerge ./import_data/person_personmerge_relation.csv
hadoop fs -get $HDFS_PATH/invmerge/invmerge ./import_data/ent_invmerge_relation.csv
hadoop fs -get $HDFS_PATH/branch/branch ./import_data/ent_branch_relation.csv
hadoop fs -get $HDFS_PATH/org/org ./import_data/entorg.csv
hadoop fs -get $HDFS_PATH/orginv/orginv ./import_data/entorg_orginv_relation.csv
hadoop fs -get $HDFS_PATH/orghold/orghold ./import_data/entorg_orghold_relation.csv
hadoop fs -get $HDFS_PATH/mstaff/mstaff ./import_data/vistaff_relation.csv

echo "step 2 end "
echo `date +%F" "%H:%M:%S`
echo "begin step 3 scp to dst server"
# step 3: scp to neo4j server

#scp ./import_data/*.csv ops@192.168.207.14:/u01/importn4j_data
#scp ./import_data/*.csv ops@192.168.207.15:/u01/importn4j_data

echo "step 3 end"
# step 4:import csv to neo4j & create neo4j index
#ssh ops@192.168.207.14 '/u01/importn4j_data/importcsv_to_neo4j.sh'
echo 'done!'
echo `date +%F" "%H:%M:%S`
~