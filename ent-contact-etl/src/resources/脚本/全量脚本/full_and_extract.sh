#!/bin/bash
LOG_SUFFIX=`date +%Y%m%d-%H%M`
#参数一：跑批日期
#参数二：table.list 全量文件 path

 #判断参数
if [ "$#" -eq "2" ];then
    echo "My first parameter is $1"
else
    echo "You provided $# parameters,but 2 are required."
    echo "next_tag erro"
    exit -1
fi

echo "step 1 produce data from 4 data source"

$SPARK_HOME/bin/spark-submit --executor-memory 20G --num-executors 10 --executor-cores 10 --conf spark.network.timeout=300000 --class com.chinadaas.etl.main.fullcontact.ContactInfoAPP --master yarn  ent-contact-etl-1.0-SNAPSHOT-jar-with-dependencies.jar $1 $2 > ./logs/deal-ent-full-contact-produce-${LOG_SUFFIX}.log 2>&1

echo "step 1 end"

echo "step 2 merge data prodeuce a new hdfs file"

$SPARK_HOME/bin/spark-submit --executor-memory 20G --num-executors 10 --executor-cores 10 --conf spark.network.timeout=300000 --class com.chinadaas.etl.main.fullcontact.SaveContactAsParquet --master yarn  ent-contact-etl-1.0-SNAPSHOT-jar-with-dependencies.jar $1 $2 > ./logs/deal-ent-full-contact-parquet-${LOG_SUFFIX}.log 2>&1

echo "step 2 end"

echo "step 3 save full contact information to ElasticSearch"

$SPARK_HOME/bin/spark-submit --executor-memory 20G --num-executors 10 --executor-cores 10 --conf spark.network.timeout=300000 --class com.chinadaas.etl.main.fullcontact.SaveToEs --master yarn  ent-contact-etl-1.0-SNAPSHOT-jar-with-dependencies.jar $1 > ./logs/deal-ent-full-contact-es-${LOG_SUFFIX}.log 2>&1

echo "step 3 end"

echo "HDFS and ElasticSearch contact-information complete!"

echo "start del extract-index"

echo "step one produce data and save model data to hdfs"

$SPARK_HOME/bin/spark-submit --executor-memory 20G --num-executors 10 --executor-cores 10 --conf spark.network.timeout=300000 --class com.chinadaas.etl.main.extractcontact.InStatusIndex --master yarn  ent-contact-etl-1.0-SNAPSHOT-jar-with-dependencies.jar $1 $2 > ./logs/deal-ent-extract-contact-parquet-${LOG_SUFFIX}.log 2>&1

echo "step one finsh"

echo "step two save merge data to hdfs and save to elasticSearch "

$SPARK_HOME/bin/spark-submit --executor-memory 20G --num-executors 10 --executor-cores 10 --conf spark.network.timeout=300000 --class com.chinadaas.etl.main.extractcontact.UniContactMerge --master yarn  ent-contact-etl-1.0-SNAPSHOT-jar-with-dependencies.jar $1 > ./logs/deal-ent-extract-contact-es-${LOG_SUFFIX}.log 2>&1

echo "step two fish"

echo "extract contact information complete!"