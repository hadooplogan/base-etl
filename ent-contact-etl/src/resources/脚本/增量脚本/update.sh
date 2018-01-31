#!/bin/bash

LOG_SUFFIX=`date +%Y%m%d-%H%M`
#参数一 本次数据批次日期
#参数二 inc 的配置文件路径
#参数三 full 的配置文件路径

a=`hdfs dfs -ls /next/contact/baseinfo |tail -n 1| awk -F '/' '{print $5}'`

if [ "$#" -eq "3" ];then
    echo "ok，next step start....."
else
    echo "You provided $# parameters,but 4 are required."
    echo "next_tag erro"
    exit -1
fi

echo "全量联系方式数据开始增量更新"

$SPARK_HOME/bin/spark-submit --executor-memory 20G --num-executors 10 --executor-cores 10 --conf spark.network.timeout=300000 --class com.chinadaas.etl.main.fullcontact.FullContactInc --master yarn  ent-contact-etl-1.0-SNAPSHOT-jar-with-dependencies.jar $1 $2 $3 $a> ./logs/deal-ent-full-contact-update-${LOG_SUFFIX}.log 2>&1


echo "全量联系方式增量更新完成!"

echo "在营企业联系方式数据开始增量更新"

$SPARK_HOME/bin/spark-submit --executor-memory 20G --num-executors 10 --executor-cores 10 --conf spark.network.timeout=300000 --class com.chinadaas.etl.main.extractcontact.UpdateExtrEs --master yarn  ent-contact-etl-1.0-SNAPSHOT-jar-with-dependencies.jar $1 $2 $a > ./logs/deal-ent-extract-contact-update-${LOG_SUFFIX}.log 2>&1

echo "在营企业联系方式增量更新完成!"