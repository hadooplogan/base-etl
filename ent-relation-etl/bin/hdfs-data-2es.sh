#!/bin/bash
HDFS_PATH=/tmp/ent-relation/tmp/dstpath
HOST=192.168.207.14
LOG_SUFFIX=`date +%Y%m%d-%H%M`

# step 1: spark pre-proess
echo "step 1 start"
/usr/lib/spark/spark-1.5.2-bin-hadoop2.4/bin/spark-submit --executor-memory 10G --num-executors 20 --executor-cores 4  --class com.chinadaas.association.etl.common.CommonApp --master yarn ent-relation-etl.jar $1 > ./logs/association-common-$LOG_SUFFIX.log 2 >&1

if [ $? -ne 0 ] ;then
    echo "common error"
    exit 1
fi

/usr/lib/spark/spark-1.5.2-bin-hadoop2.4/bin/spark-submit --executor-memory 10G --num-executors 20 --executor-cores 4  --class com.chinadaas.association.etl.main.EntInfo2EsApp --master yarn ent-relation-etl.jar $1 > ./logs/association-hdfs-2es-$LOG_SUFFIX.log 2 >&1

if [ $? -ne 0 ] ;then
    echo "ent-relation error"
    exit 1
fi

echo "step 1 end"
