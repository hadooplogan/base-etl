#!/bin/bash
LOG_SUFFIX=`date +%Y%m%d-%H%M`

# step 1: spark pre-proess

#$1 参数1:批次号 例如 20170831
#$2 参数2:增量或者全量   all 全量  inc 增量


echo "step 1 start"

if [ ! -d './logs' ]; then
mkdir ./logs
fi

$SPARK_HOME/bin/spark-submit --executor-memory 13G --num-executors 10 --executor-cores 4 --conf spark.network.timeout=300000 --class com.chinadaas.association.etl.main.EntInfo2EsApp --master yarn  hdfs-data-2es.jar  $1 $2 > ./logs/association-hdfs-2es-${LOG_SUFFIX}.log 2>&1

if [ $? -ne 0 ] ;then
    echo "ent-relation error"
    exit 1
fi

echo "step 1 end"

